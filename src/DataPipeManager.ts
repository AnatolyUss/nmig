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
import * as os from 'os';
import * as path from 'path';
import { ChildProcess, fork } from 'child_process';
import { EventEmitter } from 'events';

import { log, generateError } from './FsOps';
import { processConstraintsPerTable } from './ConstraintsProcessor';
import * as migrationStateManager from './MigrationStateManager';
import Conversion from './Conversion';
import MessageToDataLoader from './MessageToDataLoader';
import MessageToMaster from './MessageToMaster';

/**
 * A number of currently running loader processes.
 */
let loaderProcessesCount: number = 0;

/**
 * "tableLoadingFinished" event.
 */
const tableLoadingFinishedEvent: string = 'tableLoadingFinished';

/**
 * An EventEmitter instance.
 */
const eventEmitter: EventEmitter = new EventEmitter();

/**
 * A path to the DataLoader.js file.
 * !!!Notice, in runtime it points to ../dist/src/DataLoader.js and not DataLoader.ts
 */
const dataLoaderPath: string = path.join(__dirname, 'DataLoader.js');

/**
 * Returns the options object, which intended to be used upon creation of the data loader process.
 */
const getDataLoaderOptions = (conversion: Conversion): any => {
    const options: any = Object.create(null);

    if (conversion._loaderMaxOldSpaceSize !== 'DEFAULT') {
        options.execArgv = [`--max-old-space-size=${ conversion._loaderMaxOldSpaceSize }`];
    }

    return options;
};

/**
 * Kills a process specified by the pid.
 */
const killProcess = async (pid: number, conversion: Conversion): Promise<void> => {
    try {
        process.kill(pid);
    } catch (killError) {
        await generateError(conversion, `\t--[killProcess] ${ killError }`);
    }
};

/**
 * Checks if all data chunks were processed.
 */
const dataPoolProcessed = (conversion: Conversion): boolean => {
    return conversion._dataPool.length === 0;
};

/**
 * Calculates a number of data-loader processes that will run simultaneously.
 * In most cases it will be a number of logical CPU cores on the machine running Nmig,
 * unless a number of tables in the source database or the maximal number of DB connections is smaller.
 */
const getNumberOfSimultaneouslyRunningLoaderProcesses = (conversion: Conversion): number => {
    if (conversion._numberOfSimultaneouslyRunningLoaderProcesses !== 'DEFAULT') {
        return Math.min(
            conversion._dataPool.length,
            conversion._maxEachDbConnectionPoolSize,
            <number>conversion._numberOfSimultaneouslyRunningLoaderProcesses,
        );
    }

    const DEFAULT_NUMBER_OF_DATA_LOADER_PROCESSES: number = 2;
    return Math.min(
        DEFAULT_NUMBER_OF_DATA_LOADER_PROCESSES,
        (os.cpus().length || 1),
        conversion._dataPool.length,
        conversion._maxEachDbConnectionPoolSize,
    );
};

/**
 * Runs the loader process.
 */
const runLoaderProcess = (conversion: Conversion): void => {
    if (dataPoolProcessed(conversion)) {
        // No more data to transfer.
        return;
    }

    // Start a new data-loader process.
    const loaderProcess: ChildProcess = fork(dataLoaderPath, getDataLoaderOptions(conversion));
    loaderProcessesCount++;

    loaderProcess.on('message', async (signal: MessageToMaster) => {
        // Following actions are performed when a message from the loader process is accepted:
        // 1. Log an info regarding the just-populated table.
        // 2. Kill the loader process to release unused RAM as quick as possible.
        // 3. Emit the "tableLoadingFinished" event to start constraints creation for the just loaded table immediately.
        // 4. Call the "runLoaderProcess" function recursively to transfer data to the next table.
        const msg: string = `\n\t--[NMIG runLoaderProcess] For now inserted: ${ signal.totalRowsToInsert } rows`
            + `\n\t--[NMIG runLoaderProcess] Total rows to insert into`
            + ` "${ conversion._schema }"."${ signal.tableName }": ${ signal.totalRowsToInsert }`;

        log(conversion, msg);
        await killProcess(<number>loaderProcess.pid, conversion);
        loaderProcessesCount--;
        eventEmitter.emit(tableLoadingFinishedEvent, signal.tableName);
        runLoaderProcess(conversion);
    });

    // Sends a message to current data loader process,
    // which contains configuration info and a metadata of the next data-chunk.
    const chunk: any = conversion._dataPool.pop();
    const fullTableName: string = `"${ conversion._schema }"."${ chunk._tableName }"`;
    const msg: string = `\n\t--[NMIG data transfer] ${ fullTableName } DATA TRANSFER IN PROGRESS...`
        + `\n\t--[NMIG data transfer] TIME REQUIRED FOR TRANSFER DEPENDS ON AMOUNT OF DATA...\n`;

    log(conversion, msg);
    loaderProcess.send(new MessageToDataLoader(conversion._config, chunk));
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
        eventEmitter.on(tableLoadingFinishedEvent, async tableName => {
            await processConstraintsPerTable(conversion, tableName, conversion.shouldMigrateOnlyData());

            // Check a number of active loader processes on the event of "tableLoadingFinished".
            // If no active loader processes found, then all the data is transferred,
            // hence Nmig can proceed to the next step.
            if (loaderProcessesCount === 0) {
                await migrationStateManager.set(conversion, 'per_table_constraints_loaded');
                return resolve(conversion);
            }
        });

        const numberOfSimultaneouslyRunningLoaderProcesses: number = getNumberOfSimultaneouslyRunningLoaderProcesses(
            conversion
        );

        for (let i: number = 0; i < numberOfSimultaneouslyRunningLoaderProcesses; ++i) {
            runLoaderProcess(conversion);
        }
    });
};
