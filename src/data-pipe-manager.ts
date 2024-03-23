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
import * as os from 'node:os';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';
import { ChildProcess, fork, ForkOptions } from 'node:child_process';

import { PoolClient, QueryResult } from 'pg';

import Conversion from './conversion';
import DbAccess from './db-access';
import { killProcess } from './utils';
import { log, generateError } from './fs-ops';
import { getDataPoolTableName } from './data-pool-manager';
import { processConstraintsPerTable } from './constraints-processor';
import * as migrationStateManager from './migration-state-manager';
import { MessageToDataReader, MessageToMaster } from './types';

export default class DataPipeManager {
    /**
     * A number of currently running reader processes.
     */
    private static readerProcessesCount = 0;

    /**
     * "tableLoadingFinished" event.
     */
    private static readonly tableLoadingFinishedEvent = 'tableLoadingFinished';

    /**
     * An EventEmitter instance.
     */
    private static readonly eventEmitter = new EventEmitter();

    /**
     * A path to the data-reader.js file.
     * Note, in runtime it points to ../dist/src/data-reader.js and not data-reader.ts
     */
    private static readonly dataReaderPath = path.join(__dirname, 'data-reader.js');

    /**
     * Returns the options object, which intended to be used upon creation of the data reader process.
     */
    private static getDataReaderOptions = (conversion: Conversion): ForkOptions => {
        const options: ForkOptions = Object.create(null);

        if (conversion._readerMaxOldSpaceSize !== 'DEFAULT') {
            options.execArgv = [`--max-old-space-size=${conversion._readerMaxOldSpaceSize}`];
        }

        return options;
    };

    /**
     * Checks if all data chunks were processed.
     */
    private static dataPoolProcessed = (conversion: Conversion): boolean =>
        conversion._dataPool.length === 0;

    /**
     * Calculates a number of data-reader processes that will run simultaneously.
     * In most cases it will be a number of logical CPU cores on the machine running Nmig,
     * unless a number of tables in the source database or the maximal number of DB connections is smaller.
     */
    private static getNumberOfReaderProcesses = (conversion: Conversion): number => {
        if (conversion._numberOfSimultaneouslyRunningReaderProcesses !== 'DEFAULT') {
            return Math.min(
                os.cpus().length || 1,
                conversion._dataPool.length,
                conversion._maxEachDbConnectionPoolSize,
                conversion._numberOfSimultaneouslyRunningReaderProcesses as number,
            );
        }

        const DEFAULT_NUMBER_OF_DATA_READER_PROCESSES = 2;
        return Math.min(
            DEFAULT_NUMBER_OF_DATA_READER_PROCESSES,
            os.cpus().length || 1,
            conversion._dataPool.length,
            conversion._maxEachDbConnectionPoolSize,
        );
    };

    /**
     * Runs the data reader process.
     */
    private static runDataReaderProcess = async (conversion: Conversion): Promise<void> => {
        if (DataPipeManager.dataPoolProcessed(conversion)) {
            // No more data to transfer.
            return;
        }

        // Start a new data-reader process.
        const readerProcess: ChildProcess = fork(
            DataPipeManager.dataReaderPath,
            DataPipeManager.getDataReaderOptions(conversion),
        );

        DataPipeManager.readerProcessesCount++;

        readerProcess.on('message', async (signal: MessageToMaster): Promise<void> => {
            // Following actions are performed when a message from the reader process is accepted:
            // 1. Log an info regarding the just-populated table.
            // 2. Kill the reader process to release unused RAM as quick as possible.
            // 3. Emit the "tableLoadingFinished" event to start constraints creation for the just loaded table.
            // 4. Call the "runDataReaderProcess" function recursively to transfer data to the next table.
            const msg: string =
                `\n\t--[${DataPipeManager.runDataReaderProcess.name}] For now inserted: ${signal.totalRowsToInsert} rows` +
                `\n\t--[${DataPipeManager.runDataReaderProcess.name}] Total rows to insert into` +
                ` "${conversion._schema}"."${signal.tableName}": ${signal.totalRowsToInsert}`;

            await log(conversion, msg);
            await killProcess(readerProcess.pid as number, conversion);
            DataPipeManager.readerProcessesCount--;
            DataPipeManager.eventEmitter.emit(
                DataPipeManager.tableLoadingFinishedEvent,
                signal.tableName,
            );
            await DataPipeManager.runDataReaderProcess(conversion);
        });

        // Sends a message to current data reader process,
        // which contains configuration info and a metadata of the next data-chunk.
        const chunk: Record<string, any> | undefined = conversion._dataPool.pop();

        if (!chunk) {
            await killProcess(readerProcess.pid as number, conversion);
            return;
        }

        const fullTableName = `"${conversion._schema}"."${chunk._tableName}"`;
        const msg: string =
            `\n\t--[${DataPipeManager.runDataReaderProcess.name}] ${fullTableName} DATA TRANSFER IN PROGRESS...` +
            `\n\t--[${DataPipeManager.runDataReaderProcess.name}] TIME REQUIRED FOR TRANSFER DEPENDS ON AMOUNT OF DATA...\n`;

        await log(conversion, msg);
        const messageToDataReader: MessageToDataReader = {
            config: conversion._config,
            chunk: chunk,
        };

        readerProcess.send(messageToDataReader);
    };

    /**
     * Runs the data pipe.
     */
    public static runDataPipe = (conversion: Conversion): Promise<Conversion> => {
        return new Promise<Conversion>(async resolve => {
            if (DataPipeManager.dataPoolProcessed(conversion)) {
                return resolve(conversion);
            }

            // Register a listener for the "tableLoadingFinished" event.
            DataPipeManager.eventEmitter.on(
                DataPipeManager.tableLoadingFinishedEvent,
                async (tableName: string): Promise<void> => {
                    await processConstraintsPerTable(
                        conversion,
                        tableName,
                        conversion.shouldMigrateOnlyData(),
                    );

                    // Check a number of active reader processes on the event of "tableLoadingFinished".
                    // If no active reader processes found, then all the data is transferred,
                    // hence Nmig can proceed to the next step.
                    if (DataPipeManager.readerProcessesCount === 0) {
                        await migrationStateManager.set(conversion, 'per_table_constraints_loaded');
                        return resolve(conversion);
                    }
                },
            );

            const numberOfReaderProcesses: number =
                DataPipeManager.getNumberOfReaderProcesses(conversion);

            // !!!Note, invoke the "DataPipeManager.runDataReaderProcess" method sequentially.
            // DO NOT use ".map(async _ => await DataPipeManager.runDataReaderProcess(...))" to avoid race condition.
            for (let i = 0; i < numberOfReaderProcesses; ++i) {
                await DataPipeManager.runDataReaderProcess(conversion);
            }
        });
    };

    /**
     * Enables all triggers (PostgreSQL) and rules for current database session.
     * !!!DO NOT release the client, it will be released after current data-chunk deletion.
     */
    public static enablePgTriggers = async (
        conversion: Conversion,
        client: PoolClient,
        originalSessionReplicationRole: string,
    ): Promise<void> => {
        const sql = `SET session_replication_role = ${originalSessionReplicationRole};`;

        try {
            await client.query(sql);
        } catch (error) {
            await generateError(
                conversion,
                `\t--[${DataPipeManager.enablePgTriggers.name}] ${error}`,
                sql,
            );
        }
    };

    /**
     * Disables all triggers and rules for current database session.
     * !!!DO NOT release the client, it will be released after current data-chunk deletion.
     */
    public static disablePgTriggers = async (
        conversion: Conversion,
        client: PoolClient,
    ): Promise<string> => {
        let sql = `SHOW session_replication_role;`;
        let originalSessionReplicationRole = 'origin';

        try {
            const queryResult: QueryResult = await client.query(sql);
            originalSessionReplicationRole = queryResult.rows[0].session_replication_role;
            sql = 'SET session_replication_role = replica;';
            await client.query(sql);
        } catch (error) {
            await generateError(
                conversion,
                `\t--[${DataPipeManager.disablePgTriggers.name}] ${error}`,
                sql,
            );
        }

        return originalSessionReplicationRole;
    };

    /**
     * Deletes given record from the data-pool.
     */
    public static deleteChunk = async (
        conversion: Conversion,
        dataPoolId: number,
        client: PoolClient,
        originalSessionReplicationRole: string | null = null,
    ): Promise<void> => {
        const sql = `DELETE FROM ${getDataPoolTableName(conversion)} WHERE id = ${dataPoolId};`;

        try {
            await client.query(sql);

            if (originalSessionReplicationRole) {
                await DataPipeManager.enablePgTriggers(
                    conversion,
                    client,
                    originalSessionReplicationRole as string,
                );
            }
        } catch (error) {
            await generateError(
                conversion,
                `\t--[${DataPipeManager.deleteChunk.name}] ${error}`,
                sql,
            );
        } finally {
            await DbAccess.releaseDbClient(conversion, client);
        }
    };

    /**
     * Wraps "process.send" method to avoid "cannot invoke an object which is possibly undefined" TypeScript warning.
     */
    public static processSend = async (
        message: MessageToDataReader | MessageToMaster,
        conv: Conversion,
    ): Promise<void> => {
        if (process.send) {
            process.send(message);
            return;
        }

        await generateError(
            conv,
            `\t--[${DataPipeManager.processSend.name}] Unable to send a message to parent process.`,
        );
        throw new Error();
    };

    /**
     * Processes data-loading error.
     */
    public static processDataError = async (
        conv: Conversion,
        streamError: string,
        sql: string,
        sqlCopy: string,
        tableName: string,
        dataPoolId: number,
        client: PoolClient,
        originalSessionReplicationRole: string | null,
    ): Promise<void> => {
        await generateError(
            conv,
            `\t--[${DataPipeManager.processDataError.name}] ${streamError}`,
            sqlCopy,
        );
        const rejectedData = `\t--[${DataPipeManager.processDataError.name}] Error loading table data:\n${sql}\n`;
        await log(conv, rejectedData, path.join(conv._logsDirPath, `${tableName}.log`));
        await DataPipeManager.deleteChunk(conv, dataPoolId, client, originalSessionReplicationRole);
        const messageToMaster: MessageToMaster = {
            tableName: tableName,
            totalRowsToInsert: 0,
        };

        await DataPipeManager.processSend(messageToMaster, conv);
    };
}
