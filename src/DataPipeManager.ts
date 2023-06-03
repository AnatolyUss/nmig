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
import { ChildProcess, fork, ForkOptions } from 'node:child_process';
import { EventEmitter } from 'node:events';

import { killProcess } from './Utils';
import { log } from './FsOps';
import { processConstraintsPerTable } from './ConstraintsProcessor';
import * as migrationStateManager from './MigrationStateManager';
import Conversion from './Conversion';
import { MessageToDataReader, MessageToMaster } from './Types';

/**
 * A number of currently running reader processes.
 */
let readerProcessesCount: number = 0;

/**
 * "tableLoadingFinished" event.
 */
const tableLoadingFinishedEvent: string = 'tableLoadingFinished';

/**
 * An EventEmitter instance.
 */
const eventEmitter: EventEmitter = new EventEmitter();

/**
 * A path to the DataReader.js file.
 * !!!Notice, in runtime it points to ../dist/src/DataReader.js and not DataReader.ts
 */
const dataReaderPath: string = path.join(__dirname, 'DataReader.js');

/**
 * Returns the options object, which intended to be used upon creation of the data reader process.
 */
const getDataReaderOptions = (conversion: Conversion): ForkOptions => {
    const options: ForkOptions = Object.create(null);

    if (conversion._readerMaxOldSpaceSize !== 'DEFAULT') {
        options.execArgv = [`--max-old-space-size=${ conversion._readerMaxOldSpaceSize }`];
    }

    return options;
};

/**
 * Checks if all data chunks were processed.
 */
const dataPoolProcessed = (conversion: Conversion): boolean => conversion._dataPool.length === 0;

/**
 * Calculates a number of data-reader processes that will run simultaneously.
 * In most cases it will be a number of logical CPU cores on the machine running Nmig,
 * unless a number of tables in the source database or the maximal number of DB connections is smaller.
 */
const getNumberOfSimultaneouslyRunningReaderProcesses = (conversion: Conversion): number => {
    if (conversion._numberOfSimultaneouslyRunningReaderProcesses !== 'DEFAULT') {
        return Math.min(
            (os.cpus().length || 1),
            conversion._dataPool.length,
            conversion._maxEachDbConnectionPoolSize,
            conversion._numberOfSimultaneouslyRunningReaderProcesses as number,
        );
    }

    const DEFAULT_NUMBER_OF_DATA_READER_PROCESSES: number = 2;
    return Math.min(
        DEFAULT_NUMBER_OF_DATA_READER_PROCESSES,
        (os.cpus().length || 1),
        conversion._dataPool.length,
        conversion._maxEachDbConnectionPoolSize,
    );
};

/**
 * Runs the data reader process.
 */
const runDataReaderProcess = (conversion: Conversion): void => {
    if (dataPoolProcessed(conversion)) {
        // No more data to transfer.
        return;
    }

    // Start a new data-reader process.
    const readerProcess: ChildProcess = fork(dataReaderPath, getDataReaderOptions(conversion));
    readerProcessesCount++;

    readerProcess.on('message', (signal: MessageToMaster): void => {
        // Following actions are performed when a message from the reader process is accepted:
        // 1. Log an info regarding the just-populated table.
        // 2. Kill the reader process to release unused RAM as quick as possible.
        // 3. Emit the "tableLoadingFinished" event to start constraints creation for the just loaded table immediately.
        // 4. Call the "runDataReaderProcess" function recursively to transfer data to the next table.
        const msg: string = `\n\t--[NMIG runDataReaderProcess] For now inserted: ${ signal.totalRowsToInsert } rows`
            + `\n\t--[NMIG runDataReaderProcess] Total rows to insert into`
            + ` "${ conversion._schema }"."${ signal.tableName }": ${ signal.totalRowsToInsert }`;

        log(conversion, msg);
        killProcess(readerProcess.pid as number, conversion);
        readerProcessesCount--;
        eventEmitter.emit(tableLoadingFinishedEvent, signal.tableName);
        runDataReaderProcess(conversion);
    });

    // Sends a message to current data reader process,
    // which contains configuration info and a metadata of the next data-chunk.
    const chunk: any = conversion._dataPool.pop();
    const fullTableName: string = `"${ conversion._schema }"."${ chunk._tableName }"`;
    const msg: string = `\n\t--[NMIG data transfer] ${ fullTableName } DATA TRANSFER IN PROGRESS...`
        + `\n\t--[NMIG data transfer] TIME REQUIRED FOR TRANSFER DEPENDS ON AMOUNT OF DATA...\n`;

    log(conversion, msg);
    const messageToDataReader: MessageToDataReader = {
        config: conversion._config,
        chunk: chunk,
    };

    readerProcess.send(messageToDataReader);
};

/**
 * Runs the data pipe.
 */
export default (conversion: Conversion): Promise<Conversion> => {
    return new Promise<Conversion>(resolve => {
        if (dataPoolProcessed(conversion)) {
            return resolve(conversion);
        }

        // Register a listener for the "tableLoadingFinished" event.
        eventEmitter.on(tableLoadingFinishedEvent, async (tableName: string): Promise<void> => {
            await processConstraintsPerTable(conversion, tableName, conversion.shouldMigrateOnlyData());

            // Check a number of active reader processes on the event of "tableLoadingFinished".
            // If no active reader processes found, then all the data is transferred,
            // hence Nmig can proceed to the next step.
            if (readerProcessesCount === 0) {
                await migrationStateManager.set(conversion, 'per_table_constraints_loaded');
                return resolve(conversion);
            }
        });

        const numberOfSimultaneouslyRunningReaderProcesses: number = getNumberOfSimultaneouslyRunningReaderProcesses(
            conversion
        );

        for (let i: number = 0; i < numberOfSimultaneouslyRunningReaderProcesses; ++i) {
            runDataReaderProcess(conversion);
        }
    });
};
