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

// TODO: add more comments.

/**
 * An amount of currently running loader processes.
 */
let loaderProcessCount: number = 0;

/**
 * "onDataPoolEmpty" event.
 */
const onDataPoolEmptyEvent: string = 'onDataPoolEmpty';

/**
 * An EventEmitter instance.
 */
const eventEmitter: EventEmitter = new EventEmitter();

/**
 * Runs the DataPipe.
 */
export default async function(conversion: Conversion): Promise<void> {
    if (dataPoolProcessed(conversion)) {
        await continueConversionProcess(conversion);
        return;
    }

    eventEmitter.on(onDataPoolEmptyEvent, async () => {
        if (loaderProcessCount === 0) {
            await continueConversionProcess(conversion);
        }
    });

    const numberOfSimultaneouslyRunningLoaderProcesses: number = Math.min(conversion._dataPool.length, getNumberOfCpus());

    for (let i = 0; i < numberOfSimultaneouslyRunningLoaderProcesses; ++i) {
        runLoaderProcess(conversion);
    }
}

/**
 * Continues the conversion process upon data loading completion.
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
        eventEmitter.emit(onDataPoolEmptyEvent);
        return;
    }

    const loaderProcess: ChildProcess = fork(getDataLoaderPath(), getDataLoaderOptions(conversion));
    loaderProcessCount++;

    loaderProcess.on('message', async (signal: MessageToMaster) => {
        const msg: string = `\t--[pipeData]  For now inserted: ${ signal.totalRowsToInsert } rows,`
            + `Total rows to insert into "${ conversion._schema }"."${ signal.tableName }": ${ signal.totalRowsToInsert }`;

        log(conversion, msg);
        await killProcess(loaderProcess.pid, conversion);
        loaderProcessCount--;
        runLoaderProcess(conversion);
    });

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
 * Returns an amount of logical CPU cores.
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
