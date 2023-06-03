/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).
 * If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
import * as path from 'node:path';
import { ChildProcess, spawn } from 'node:child_process';
import { Readable, Writable, Duplex as DuplexStream, promises as streamPromises } from 'node:stream';

import { PoolClient, QueryResult } from 'pg';
import { PoolConnection } from 'mysql2';
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".
const { Transform: Json2CsvTransform } = require('json2csv'); // No declaration file for module "json2csv".

import { log, generateError } from './FsOps';
import { CopyStreamSerializableParams, MessageToDataWriter, MessageToDataReader } from './Types';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import { dataTransferred } from './ConsistencyEnforcer';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors, MessageToMaster } from './Types';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import { getDataPoolTableName } from './DataPoolManager';

/**
 * Processes incoming messages from the DataPipeManager.
 */
process.on('message', async (signal: MessageToDataReader): Promise<void> => {
    const { config, chunk } = signal;
    const conv: Conversion = new Conversion(config);
    log(conv, `\t--[NMIG loadData] Loading the data into "${ conv._schema }"."${ chunk._tableName }" table...`);

    const isRecoveryMode: boolean = await dataTransferred(conv, chunk._id);

    if (!isRecoveryMode) {
        await populateTableWorker(conv, chunk);
        return;
    }

    const client: PoolClient = await DBAccess.getPgClient(conv);
    return deleteChunk(conv, chunk._id, client);
});

/**
 * Wraps "process.send" method to avoid "cannot invoke an object which is possibly undefined" warning.
 */
const processSend = (x: any): void => {
    if (process.send) {
        process.send(x);
    }
};

/**
 * Deletes given record from the data-pool.
 */
export const deleteChunk = async (
    conversion: Conversion,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null = null,
): Promise<void> => {
    const sql: string = `DELETE FROM ${ getDataPoolTableName(conversion) } WHERE id = ${ dataPoolId };`;

    try {
        await client.query(sql);

        if (originalSessionReplicationRole) {
            await enableTriggers(conversion, client, originalSessionReplicationRole as string);
        }
    } catch (error) {
        generateError(conversion, `\t--[DataReader::deleteChunk] ${ error }`, sql);
    } finally {
        DBAccess.releaseDbClient(conversion, client);
    }
};

/**
 * Processes data-loading error.
 */
export const processDataError = async (
    conv: Conversion,
    streamError: string,
    sql: string,
    sqlCopy: string,
    tableName: string,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null,
): Promise<void> => {
    generateError(conv, `\t--[populateTableWorker] ${ streamError }`, sqlCopy);
    const rejectedData: string = `\t--[populateTableWorker] Error loading table data:\n${ sql }\n`;
    log(conv, rejectedData, path.join(conv._logsDirPath, `${ tableName }.log`));
    await deleteChunk(conv, dataPoolId, client, originalSessionReplicationRole);
    const messageToMaster: MessageToMaster = {
        tableName: tableName,
        totalRowsToInsert: 0,
    };

    processSend(messageToMaster);
};

/**
 * Loads a chunk of data using "PostgreSQL COPY".
 */
const populateTableWorker = async (conv: Conversion, chunk: any): Promise<void> => {
    const tableName: string = chunk._tableName;
    const strSelectFieldList: string = chunk._selectFieldList;
    const rowsCnt: number = chunk._rowsCnt;
    const dataPoolId: number = chunk._id;

    const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
    const sql: string = `SELECT ${ strSelectFieldList } FROM \`${ originalTableName }\`;`;
    const mysqlClient: PoolConnection = await DBAccess.getMysqlClient(conv);
    const sqlCopy: string = `COPY "${ conv._schema }"."${ tableName }" FROM STDIN DELIMITER '${ conv._delimiter }' CSV;`;
    const client: PoolClient = await DBAccess.getPgClient(conv);
    let originalSessionReplicationRole: string | null = null;

    if (conv.shouldMigrateOnlyData()) {
        originalSessionReplicationRole = await disableTriggers(conv, client);
    }

    const json2csvStream: DuplexStream = await getJson2csvStream(
        conv,
        originalTableName,
        dataPoolId,
        client,
        originalSessionReplicationRole,
    );

    const mysqlClientErrorHandler = async (err: string): Promise<void> => {
        await processDataError(conv, err, sql, sqlCopy, tableName, dataPoolId, client, originalSessionReplicationRole);
    };

    const cliArgs: string[] = [path.join(__dirname, 'DataWriter.js')];

    if (conv._readerMaxOldSpaceSize !== 'DEFAULT') {
        // Note, all the child-process params are equally applicable to both "DataReader" and "DataWriter" processes.
        cliArgs.push(`--max-old-space-size=${ conv._readerMaxOldSpaceSize }`);
    }

    const dataWriter: ChildProcess = spawn(process.execPath, cliArgs, { stdio: ['pipe', 1, 2, 'ipc'] });
    const messageToDataWriter: MessageToDataWriter = {
        config: conv._config,
        chunk: chunk,
        copyStreamSerializableParams: {
            sqlCopy,
            sql,
            tableName,
            dataPoolId,
            originalSessionReplicationRole,
        },
    };

    const _dataWriterOnExitCallback = async (code: number): Promise<void> => {
        // Note, no need to "kill" the DataWriter, since it has already exited successfully.
        log(
            conv,
            `\t--[NMIG loadData] DataWriter process (table "${ tableName }") exited with code ${ code }`,
        );

        // COPY FROM STDIN does not return the number of rows inserted.
        // But the transactional behavior still applies, meaning no records inserted if at least one failed.
        // That is why in case of 'on finish' the rowsCnt value is actually the number of records inserted.
        await deleteChunk(conv, dataPoolId, client);
        const messageToMaster: MessageToMaster = {
            tableName: tableName,
            totalRowsToInsert: rowsCnt,
        };

        processSend(messageToMaster);
    };

    dataWriter
        .on('exit', _dataWriterOnExitCallback)
        .send(messageToDataWriter);

    const dataReaderStream: Readable = mysqlClient
        .query(sql)
        .on('error', mysqlClientErrorHandler)
        .stream({ highWaterMark: conv._streamsHighWaterMark });

    // TODO: should I apply errors-handling using "catch"?
    await streamPromises.pipeline(dataReaderStream, json2csvStream, dataWriter.stdin as Writable);
    // mysqlClient
    //     .query(sql)
    //     .on('error', mysqlClientErrorHandler)
    //     .stream({ highWaterMark: conv._streamsHighWaterMark })
    //     .pipe(json2csvStream)
    //     .pipe(dataWriter.stdin as Writable);
};

/**
 * Returns new PostgreSQL copy stream object.
 */
export const getCopyStream = (
    conv: Conversion,
    client: PoolClient,
    copyStreamSerializableParams: CopyStreamSerializableParams,
): Writable => {
    const {
        sqlCopy,
        sql,
        tableName,
        dataPoolId,
        originalSessionReplicationRole,
    } = copyStreamSerializableParams;

    const copyStream: Writable = client.query(from(sqlCopy));

    copyStream.on('error', async (copyStreamError: string): Promise<void> => {
        await processDataError(
            conv,
            copyStreamError,
            sql,
            sqlCopy,
            tableName,
            dataPoolId,
            client,
            originalSessionReplicationRole,
        );
    });

    return copyStream;
};

/**
 * Returns new json-to-csv stream-transform object.
 */
const getJson2csvStream = async (
    conversion: Conversion,
    originalTableName: string,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null,
): Promise<DuplexStream> => {
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: 'DataReader::populateTableWorker',
        sql: `SHOW COLUMNS FROM \`${ originalTableName }\`;`,
        vendor: DBVendors.MYSQL,
        processExitOnError: true,
        shouldReturnClient: false,
    };

    const tableColumnsResult: DBAccessQueryResult = await DBAccess.query(params);

    const options: any = {
        delimiter: conversion._delimiter,
        header: false,
        fields: tableColumnsResult.data.map((column: any) => column.Field),
    };

    const streamTransformOptions: any = {
        highWaterMark: conversion._streamsHighWaterMark,
        objectMode: true,
        encoding: conversion._encoding,
    };

    const json2CsvTransformStream = new Json2CsvTransform(options, streamTransformOptions);

    json2CsvTransformStream.on('error', async (transformError: string): Promise<void> => {
        await processDataError(
            conversion,
            transformError,
            '',
            '',
            originalTableName,
            dataPoolId,
            client,
            originalSessionReplicationRole,
        );
    });

    return json2CsvTransformStream;
};

/**
 * Disables all triggers and rules for current database session.
 * !!!DO NOT release the client, it will be released after current data-chunk deletion.
 */
const disableTriggers = async (conversion: Conversion, client: PoolClient): Promise<string> => {
    let sql: string = `SHOW session_replication_role;`;
    let originalSessionReplicationRole: string = 'origin';

    try {
        const queryResult: QueryResult = await client.query(sql);
        originalSessionReplicationRole = queryResult.rows[0].session_replication_role;
        sql = 'SET session_replication_role = replica;';
        await client.query(sql);
    } catch (error) {
        generateError(conversion, `\t--[DataReader::disableTriggers] ${ error }`, sql);
    }

    return originalSessionReplicationRole;
};

/**
 * Enables all triggers and rules for current database session.
 * !!!DO NOT release the client, it will be released after current data-chunk deletion.
 */
const enableTriggers = async (
    conversion: Conversion,
    client: PoolClient,
    originalSessionReplicationRole: string,
): Promise<void> => {
    const sql: string = `SET session_replication_role = ${ originalSessionReplicationRole };`;

    try {
        await client.query(sql);
    } catch (error) {
        generateError(conversion, `\t--[DataReader::enableTriggers] ${ error }`, sql);
    }
};
