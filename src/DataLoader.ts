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
import { log, generateError } from './FsOps';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import MessageToMaster from './MessageToMaster';
import MessageToDataLoader from './MessageToDataLoader';
import { dataTransferred } from './ConsistencyEnforcer';
import IDBAccessQueryParams from './IDBAccessQueryParams';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import { getDataPoolTableName } from './DataPoolManager';
import * as path from 'path';
import { PoolClient, QueryResult } from 'pg';
import { PoolConnection } from 'mysql';
const { from } = require('pg-copy-streams'); // No declaration file for module "pg-copy-streams".
const { Transform: Json2CsvTransform } = require('json2csv'); // No declaration file for module "json2csv".

process.on('message', async (signal: MessageToDataLoader) => {
    const { config, chunk } = signal;
    const conv: Conversion = new Conversion(config);
    log(conv, `\t--[loadData] Loading the data into "${ conv._schema }"."${ chunk._tableName }" table...`);

    const isRecoveryMode: boolean = await dataTransferred(conv, chunk._id);

    if (!isRecoveryMode) {
        await populateTableWorker(conv, chunk._tableName, chunk._selectFieldList, chunk._rowsCnt, chunk._id);
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
const deleteChunk = async (
    conversion: Conversion,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null = null
): Promise<void> => {
    const sql: string = `DELETE FROM ${ getDataPoolTableName(conversion) } WHERE id = ${ dataPoolId };`;

    try {
        await client.query(sql);

        if (originalSessionReplicationRole) {
            await enableTriggers(conversion, client, <string>originalSessionReplicationRole);
        }
    } catch (error) {
        await generateError(conversion, `\t--[DataLoader::deleteChunk] ${ error }`, sql);
    } finally {
        await DBAccess.releaseDbClient(conversion, client);
    }
};

/**
 * Processes data-loading error.
 */
const processDataError = async (
    conv: Conversion,
    streamError: string,
    sql: string,
    sqlCopy: string,
    tableName: string,
    dataPoolId: number,
    client: PoolClient,
    originalSessionReplicationRole: string | null
): Promise<void> => {
    await generateError(conv, `\t--[populateTableWorker] ${ streamError }`, sqlCopy);
    const rejectedData: string = `\t--[populateTableWorker] Error loading table data:\n${ sql }\n`;
    log(conv, rejectedData, path.join(conv._logsDirPath, `${ tableName }.log`));
    await deleteChunk(conv, dataPoolId, client, originalSessionReplicationRole);
    processSend(new MessageToMaster(tableName, 0));
};

/**
 * Loads a chunk of data using "PostgreSQL COPY".
 */
const populateTableWorker = async (
    conv: Conversion,
    tableName: string,
    strSelectFieldList: string,
    rowsCnt: number,
    dataPoolId: number
): Promise<void> => {
    const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
    const sql: string = `SELECT ${ strSelectFieldList } FROM \`${ originalTableName }\`;`;
    const mysqlClient: PoolConnection = await DBAccess.getMysqlClient(conv);
    const sqlCopy: string = `COPY "${ conv._schema }"."${ tableName }" FROM STDIN DELIMITER '${ conv._delimiter }' CSV;`;
    const client: PoolClient = await DBAccess.getPgClient(conv);
    let originalSessionReplicationRole: string | null = null;

    if (conv.shouldMigrateOnlyData()) {
        originalSessionReplicationRole = await disableTriggers(conv, client);
    }

    const copyStream: any = getCopyStream(
        conv,
        client,
        sqlCopy,
        sql,
        tableName,
        rowsCnt,
        dataPoolId,
        originalSessionReplicationRole
    );

    const json2csvStream = await getJson2csvStream(conv, originalTableName, dataPoolId, client, originalSessionReplicationRole);
    const mysqlClientErrorHandler = async (err: string) => {
        await processDataError(conv, err, sql, sqlCopy, tableName, dataPoolId, client, originalSessionReplicationRole);
    };

    mysqlClient
        .query(sql)
        .on('error', mysqlClientErrorHandler)
        .stream({ highWaterMark: conv._streamsHighWaterMark })
        .pipe(json2csvStream)
        .pipe(copyStream);
};

/**
 * Returns new PostgreSQL copy stream object.
 */
const getCopyStream = (
    conv: Conversion,
    client: PoolClient,
    sqlCopy: string,
    sql: string,
    tableName: string,
    rowsCnt: number,
    dataPoolId: number,
    originalSessionReplicationRole: string | null
): any => {
    const copyStream: any = client.query(from(sqlCopy));

    copyStream
        .on('finish', async () => {
            // COPY FROM STDIN does not return the number of rows inserted.
            // But the transactional behavior still applies, meaning no records inserted if at least one failed.
            // That is why in case of 'on finish' the rowsCnt value is actually the number of records inserted.
            processSend(new MessageToMaster(tableName, rowsCnt));
            await deleteChunk(conv, dataPoolId, client);
        })
        .on('error', async (copyStreamError: string) => {
            await processDataError(conv, copyStreamError, sql, sqlCopy, tableName, dataPoolId, client, originalSessionReplicationRole);
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
    originalSessionReplicationRole: string | null
): Promise<any> => {
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'DataLoader::populateTableWorker',
        sql: `SHOW COLUMNS FROM \`${ originalTableName }\`;`,
        vendor: DBVendors.MYSQL,
        processExitOnError: true,
        shouldReturnClient: false
    };

    const tableColumnsResult: DBAccessQueryResult = await DBAccess.query(params);

    const options: any = {
        delimiter: conversion._delimiter,
        header: false,
        fields: tableColumnsResult.data.map((column: any) => column.Field)
    };

    const streamTransformOptions: any = {
        highWaterMark: conversion._streamsHighWaterMark,
        objectMode: true,
        encoding: conversion._encoding
    };

    const json2CsvTransformStream = new Json2CsvTransform(options, streamTransformOptions);

    json2CsvTransformStream.on('error', async (transformError: string) => {
        await processDataError(conversion, transformError, '', '', originalTableName, dataPoolId, client, originalSessionReplicationRole);
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
        await generateError(conversion, `\t--[DataLoader::disableTriggers] ${ error }`, sql);
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
    originalSessionReplicationRole: string
): Promise<void> => {
    const sql: string = `SET session_replication_role = ${ originalSessionReplicationRole };`;

    try {
        await client.query(sql);
    } catch (error) {
        await generateError(conversion, `\t--[DataLoader::enableTriggers] ${ error }`, sql);
    }
};
