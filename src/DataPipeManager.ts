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
import * as path from 'path';
import log from './Logger';
import Conversion from './Conversion';
import generateError from './ErrorGenerator';
import MessageToDataLoader from './MessageToDataLoader';
import processConstraints from './ConstraintsProcessor';
import decodeBinaryData from './BinaryDataDecoder';

/**
 * Kills a process specified by the pid.
 */
function killProcess(pid: number, conversion: Conversion): void {
    try {
        process.kill(pid);
    } catch (killError) {
        generateError(conversion, `\t--[killProcess] ${ killError }`);
    }
}

/**
 * Checks if all data chunks were processed.
 */
function dataPoolProcessed(conversion: Conversion): boolean {
    return conversion._processedChunks === conversion._dataPool.length;
}

/**
 * Gets a size (in MB) of the smallest, non processed data chunk.
 * If all data chunks are processed then returns 0.
 */
function getSmallestDataChunkSizeInMb(conversion: Conversion): number {
    for (let i: number = conversion._dataPool.length - 1; i >= 0; --i) {
        if (conversion._dataPool[i]._processed === false) {
            return conversion._dataPool[i]._size_in_mb;
        }
    }

    return 0;
}

/**
 * Creates an array of indexes, that point to data chunks, that will be processed during current COPY operation.
 */
function fillBandwidth(conversion: Conversion): number[] {
    const dataChunkIndexes: number[] = [];

    // Loop through the data pool from the beginning to the end.
    // Note, the data pool is created with predefined order, the order by data chunk size descending.
    // Note, the "bandwidth" variable represents an actual amount of data, that will be loaded during current COPY operation.
    for (let i: number = 0, bandwidth = 0; i < conversion._dataPool.length; ++i) {
        // Check if current chunk has already been marked as "processed".
        // If yes, then continue to the next iteration.
        if (conversion._dataPool[i]._processed === false) {
            // Sum a size of data chunks, that are yet to be processed.
            bandwidth += conversion._dataPool[i]._size_in_mb;

            if (conversion._dataChunkSize - bandwidth >= getSmallestDataChunkSizeInMb(conversion)) {
                // Currently, the bandwidth is smaller than "data_chunk_size",
                // and the difference between "data_chunk_size" and the bandwidth
                // is larger or equal to currently-smallest data chunk.
                // This means, that more data chunks can be processed during current COPY operation.
                dataChunkIndexes.push(i);
                conversion._dataPool[i]._processed = true;
                continue;
            }

            if (conversion._dataChunkSize >= bandwidth) {
                // Currently, the "data_chunk_size" is greater or equal to the bandwidth.
                // This means, that no more data chunks can be processed during current COPY operation.
                // Current COPY operation will be performed with maximal possible bandwidth capacity.
                dataChunkIndexes.push(i);
                conversion._dataPool[i]._processed = true;
                break;
            }

            // This data chunk will not be processed during current COPY operation, because when it is added
            // to the bandwidth, the bandwidth's size may become larger than "data_chunk_size".
            // The bandwidth's value should be decreased prior the next iteration.
            bandwidth -= conversion._dataPool[i]._size_in_mb;
        }
    }

    return dataChunkIndexes;
}

/**
 * Instructs DataLoader which data chunks should be loaded.
 * No need to check the state-log.
 * If dataPool's length is zero, then nmig will proceed to the next step.
 */
async function pipeData(conversion: Conversion, dataLoaderPath: string, options: any): Promise<void> {
    if (dataPoolProcessed(conversion)) {
        conversion = await decodeBinaryData(conversion);
        return processConstraints(conversion);
    }

    const loaderProcess: ChildProcess = fork(dataLoaderPath, options);
    const bandwidth: number[] = fillBandwidth(conversion);
    const chunksToLoad: any[] = bandwidth.map((index: number) => conversion._dataPool[index]);

    loaderProcess.on('message', (signal: any) => {
        if (typeof signal === 'object') {
            conversion._dicTables[signal.tableName].totalRowsInserted += signal.rowsInserted;
            const msg: string = `\t--[pipeData]  For now inserted: ${ conversion._dicTables[signal.tableName].totalRowsInserted } rows, 
                Total rows to insert into "${ conversion._schema }"."${ signal.tableName }": ${ signal.totalRowsToInsert }`;

            log(conversion, msg);
            return;
        }

        killProcess(loaderProcess.pid, conversion);
        conversion._processedChunks += chunksToLoad.length;
        return pipeData(conversion, dataLoaderPath, options);
    });

    loaderProcess.send(new MessageToDataLoader(conversion._config, chunksToLoad));
}

/**
 * Manages the DataPipe.
 */
export default async function(conversion: Conversion): Promise<void> {
    if (dataPoolProcessed(conversion)) {
        conversion = await decodeBinaryData(conversion);
        return processConstraints(conversion);
    }

    // In runtime it points to ../dist/src/DataLoader.js and not DataLoader.ts
    const dataLoaderPath: string = path.join(__dirname, 'DataLoader.js');

    const options: any = conversion._loaderMaxOldSpaceSize === 'DEFAULT'
        ? Object.create(null)
        : { execArgv: [`--max-old-space-size=${ conversion._loaderMaxOldSpaceSize }`] };

    return pipeData(conversion, dataLoaderPath, options);
}
