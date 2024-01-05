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
import { Readable, Writable, Duplex as DuplexStream } from 'node:stream';
import * as streamPromises from 'node:stream/promises';

import { PoolClient } from 'pg';
import { PoolConnection } from 'mysql2';
const { Transform: Json2CsvTransform } = require('json2csv'); // No declaration file for module "json2csv".

import { log } from './FsOps';
import { dataTransferred } from './ConsistencyEnforcer';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import DataPipeManager from './DataPipeManager';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import {
    DBAccessQueryParams,
    DBAccessQueryResult,
    DBVendors,
    MessageToMaster,
    MessageToDataReader,
    MessageToDataWriter,
    CopyStreamSerializableParams,
    Table,
} from './Types';

/**
 * Processes incoming messages from the DataPipeManager.
 */
process.on('message', async (signal: MessageToDataReader): Promise<void> => {
    const { config, chunk } = signal;

    // Create Conversion instance, but avoid creating a separate logger process.
    const avoidLogger = true;
    const conv: Conversion = new Conversion(config, avoidLogger);
    await log(
        conv,
        `\t--[NMIG loadData] Loading the data into "${conv._schema}"."${chunk._tableName}" table...`,
    );

    const isRecoveryMode: boolean = await dataTransferred(conv, chunk._id);

    if (!isRecoveryMode) {
        await populateTable(conv, chunk);
        return;
    }

    const client: PoolClient = await DBAccess.getPgClient(conv);
    return DataPipeManager.deleteChunk(conv, chunk._id, client);
});

/**
 * Initializes data transfer for the table, related to given "chunk".
 */
const populateTable = async (conv: Conversion, chunk: any): Promise<void> => {
    const tableName: string = chunk._tableName;
    const strSelectFieldList: string = chunk._selectFieldList;
    const rowsCnt: number = chunk._rowsCnt;
    const dataPoolId: number = chunk._id;
    const originalTableName: string = extraConfigProcessor.getTableName(conv, tableName, true);
    const sql = `SELECT ${strSelectFieldList} FROM \`${originalTableName}\`;`;
    const mysqlClient: PoolConnection = await DBAccess.getMysqlClient(conv);
    // use dicTables to specify column order of data coming in
    const tableMap = new Map<string, Table>(conv._config._dicTables);
    let columnNames = '';
    const table = tableMap.get(tableName);
    if (table) columnNames = `(${table.arrTableColumns.map(c => `"${c.Field.toLowerCase()}"`).join()})`;
    const sqlCopy = `COPY "${conv._schema}"."${tableName}" ${columnNames} FROM STDIN 
                             WITH(FORMAT csv, DELIMITER '${conv._delimiter}',
                             ENCODING '${conv._targetConString.charset}');`;

    const client: PoolClient = await DBAccess.getPgClient(conv);
    let originalSessionReplicationRole: string | null = null;

    if (conv.shouldMigrateOnlyData()) {
        originalSessionReplicationRole = await DataPipeManager.disablePgTriggers(conv, client);
    }

    const json2csvStream: DuplexStream = await getJson2csvStream(conv, originalTableName);
    const dataWriter: ChildProcess = getDataWriter(conv);
    const copyStreamSerializableParams: CopyStreamSerializableParams = {
        sqlCopy,
        sql,
        tableName,
        dataPoolId,
        originalSessionReplicationRole,
    };

    const messageToDataWriter: MessageToDataWriter = {
        config: conv._config,
        chunk: chunk,
        copyStreamSerializableParams: copyStreamSerializableParams,
    };

    dataWriter
        .on('exit', getDataWriterOnExitCallback(conv, tableName, dataPoolId, client, rowsCnt))
        .send(messageToDataWriter);

    const dataReaderStream: Readable = mysqlClient
        .query(sql)
        .stream({ highWaterMark: conv._streamsHighWaterMark });

    try {
        await streamPromises.pipeline(
            dataReaderStream,
            json2csvStream,
            dataWriter.stdin as Writable,
        );
    } catch (pipelineError) {
        await DataPipeManager.processDataError(
            conv,
            pipelineError as string,
            sql,
            sqlCopy,
            tableName,
            dataPoolId,
            client,
            originalSessionReplicationRole,
        );
    }
};

/**
 * Spawns the data-writer child-process and returns its instance.
 */
const getDataWriter = (conv: Conversion): ChildProcess => {
    // Note, in runtime it points to ../dist/src/DataWriter.js and not DataWriter.ts
    const cliArgs: string[] = [path.join(__dirname, 'DataWriter.js')];

    if (conv._readerMaxOldSpaceSize !== 'DEFAULT') {
        // Note, all the child-process params are equally applicable to both "DataReader" and "DataWriter" processes.
        cliArgs.push(`--max-old-space-size=${conv._readerMaxOldSpaceSize}`);
    }

    return spawn(process.execPath, cliArgs, {
        stdio: ['pipe', 1, 2, 'ipc'],
    });
};

/**
 * Returns data-writer on-exit callback.
 */
const getDataWriterOnExitCallback = (
    conv: Conversion,
    tableName: string,
    dataPoolId: number,
    client: PoolClient,
    rowsCnt: number,
): ((code: number) => Promise<void>) => {
    return async (code: number): Promise<void> => {
        // Note, no need to "kill" the DataWriter, since it has already exited successfully.
        await log(
            conv,
            `\t--[NMIG loadData] DataWriter process (table "${tableName}") exited with code ${code}`,
        );

        // COPY FROM STDIN does not return the number of rows inserted.
        // But the transactional behavior still applies, meaning no records inserted if at least one failed.
        // That is why in case of 'on finish' the rowsCnt value is actually the number of records inserted.
        await DataPipeManager.deleteChunk(conv, dataPoolId, client);
        const messageToMaster: MessageToMaster = {
            tableName: tableName,
            totalRowsToInsert: rowsCnt,
        };

        await DataPipeManager.processSend(messageToMaster, conv);
    };
};

/**
 * Returns new json-to-csv stream-transform object.
 */
const getJson2csvStream = async (
    conversion: Conversion,
    originalTableName: string,
): Promise<DuplexStream> => {
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: 'DataReader::populateTable',
        sql: `SHOW COLUMNS FROM \`${originalTableName}\`;`,
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

    return new Json2CsvTransform(options, streamTransformOptions);
};
