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
import { ChildProcess, fork } from 'child_process';
import { EventEmitter } from 'events';
import * as path from 'path';
import * as os from 'os';
import { log, generateError } from './FsOps';
import Conversion from './Conversion';
import MessageToDataLoader from './MessageToDataLoader';
import MessageToMaster from './MessageToMaster';
import processConstraints from './ConstraintsProcessor';
import decodeBinaryData from './BinaryDataDecoder';

/**
 * A number of currently running loader processes.
 */
let loaderProcessesCount: number = 0;

/**
 * "dataPoolEmpty" event.
 */
const dataPoolEmptyEvent: string = 'dataPoolEmpty';

/**
 * An EventEmitter instance.
 */
const eventEmitter: EventEmitter = new EventEmitter();

/**
 * Runs the data pipe.
 */
export default async function(conversion: Conversion): Promise<void> {
    if (dataPoolProcessed(conversion)) {
        await continueConversionProcess(conversion);
        return;
    }

    // Register a listener for the "dataPoolEmpty" event.
    eventEmitter.on(dataPoolEmptyEvent, async () => {
        if (loaderProcessesCount === 0) {
            // On the event of "dataPoolEmpty" check a number of active loader processes.
            // If no active loader processes found, then all the data is transferred, so Nmig can proceed to the next step.
            await continueConversionProcess(conversion);
        }
    });

    // Determine a number of simultaneously running loader processes.
    // In most cases it will be a number of logical CPU cores on the machine running Nmig;
    // unless a number of tables in the source database is smaller.
    const numberOfSimultaneouslyRunningLoaderProcesses: number = Math.min(conversion._dataPool.length, getNumberOfCpus());

    for (let i: number = 0; i < numberOfSimultaneouslyRunningLoaderProcesses; ++i) {
        runLoaderProcess(conversion);
    }
}

/**
 * Continues the conversion process upon data transfer completion.
 */
async function continueConversionProcess(conversion: Conversion): Promise<void> {
    await decodeBinaryData(conversion);
    await processConstraints(conversion);
}

/**
 * Runs the loader process.
 */
function runLoaderProcess(conversion: Conversion): void {
    if (dataPoolProcessed(conversion)) {
        // Emit the "dataPoolEmpty" event if there are no more data to transfer.
        eventEmitter.emit(dataPoolEmptyEvent);
        return;
    }

    // Start a new data loader process.
    const loaderProcess: ChildProcess = fork(getDataLoaderPath(), getDataLoaderOptions(conversion));
    loaderProcessesCount++;

    loaderProcess.on('message', async (signal: MessageToMaster) => {
        // Following actions are performed when a message from the loader process is accepted:
        // 1. Log an info regarding the just-populated table.
        // 2. Kill the loader process to release unused RAM as quick as possible.
        // 3. Call the "runLoaderProcess" function recursively to transfer next data-chunk.
        const msg: string = `\t--[pipeData]  For now inserted: ${ signal.totalRowsToInsert } rows,`
            + `Total rows to insert into "${ conversion._schema }"."${ signal.tableName }": ${ signal.totalRowsToInsert }`;

        log(conversion, msg);
        await killProcess(loaderProcess.pid, conversion);
        loaderProcessesCount--;
        runLoaderProcess(conversion);
    });

    // Sends a message to current data loader process, which contains configuration info and a metadata of next data-chunk.
    loaderProcess.send(new MessageToDataLoader(conversion._config, conversion._dataPool.pop()));
}

/**
 * Returns a path to the DataLoader.js file.
 * !!!Note, in runtime it points to ../dist/src/DataLoader.js and not DataLoader.ts
 */
function getDataLoaderPath(): string {
    return path.join(__dirname, 'DataLoader.js');
}

/**
 * Returns the options object, which intended to be used upon creation of the data loader process.
 */
function getDataLoaderOptions(conversion: Conversion): any {
    const options: any = Object.create(null);

    if (conversion._loaderMaxOldSpaceSize !== 'DEFAULT') {
        options.execArgv = [`--max-old-space-size=${ conversion._loaderMaxOldSpaceSize }`];
    }

    return options;
}

/**
 * Returns a number of logical CPU cores.
 */
function getNumberOfCpus(): number {
    return os.cpus().length;
}

/**
 * Kills a process specified by the pid.
 */
async function killProcess(pid: number, conversion: Conversion): Promise<void> {
    try {
        process.kill(pid);
    } catch (killError) {
        await generateError(conversion, `\t--[killProcess] ${ killError }`);
    }
}

/**
 * Checks if all data chunks were processed.
 */
function dataPoolProcessed(conversion: Conversion): boolean {
    return conversion._dataPool.length === 0;
}
