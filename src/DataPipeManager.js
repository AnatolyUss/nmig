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
'use strict';

const childProcess        = require('child_process');
const path                = require('path');
const log                 = require('./Logger');
const generateError       = require('./ErrorGenerator');
const MessageToDataLoader = require('./Classes/MessageToDataLoader');
const processConstraints  = require('./ConstraintsProcessor');

/**
 * Kill a process specified by the pid.
 *
 * @param {Number} pid
 *
 * @returns {undefined}
 */
const killProcess = pid => {
    try {
        process.kill(pid);
    } catch (killError) {
        generateError(self, '\t--[killProcess] ' + killError);
    }
};

/**
 * Check if all data chunks were processed.
 *
 * @param {Conversion} self
 *
 * @returns {Boolean}
 */
const dataPoolProcessed = self => {
    return self._processedChunks === self._dataPool.length;
};

/**
 * Get a size (in MB) of the smallest, non processed data chunk.
 * If all data chunks are processed then return -1.
 *
 * @param {Conversion} self
 *
 * @returns {Number}
 */
const getSmallestDataChunkSizeInMb = self => {
    for (let i = self._dataPool.length - 1; i >= 0; --i) {
        if (self._dataPool[i]._processed === false) {
            return self._dataPool[i]._size_in_mb;
        }
    }

    return -1;
};

/**
 * Create an array of indexes, that point to data chunks, that will be processed during current COPY operation.
 *
 * @param {Conversion} self
 * @param {Number}     currentIndex
 *
 * @returns {Array}
 */
const fillBandwidth = (self, currentIndex) => {
    const dataChunkIndexes = [];

    /*
     * Loop through the data pool from current index to the end.
     * Note, the data pool is created with predefined order, the order by data chunk size descending.
     * Note, the "bandwidth" variable represents an actual amount of data,
     * that will be loaded during current COPY operation.
     */
    for (let i = currentIndex, bandwidth = 0; i < self._dataPool.length; ++i) {
        /*
         * Check if current chunk has already been marked as "processed".
         * If yes, then continue to the next iteration.
         */
        if (self._dataPool[i]._processed === false) {
            // Sum a size of data chunks, that are yet to be processed.
            bandwidth += self._dataPool[i]._size_in_mb;

            if (self._dataChunkSize - bandwidth >= getSmallestDataChunkSizeInMb(self)) {
                /*
                 * Currently, the bandwidth is smaller than "data_chunk_size",
                 * and the difference between "data_chunk_size" and the bandwidth
                 * is larger or equal to currently-smallest data chunk.
                 * This means, that more data chunks can be processed during current COPY operation.
                 */
                dataChunkIndexes.push(i);
                self._dataPool[i]._processed = true;
                continue;
            }

            if (self._dataChunkSize >= bandwidth) {
                /*
                 * Currently, the "data_chunk_size" is greater or equal to the bandwidth.
                 * This means, that no more data chunks can be processed during current COPY operation.
                 * Current COPY operation will be performed with maximal possible bandwidth capacity.
                 */
                dataChunkIndexes.push(i);
                self._dataPool[i]._processed = true;
                break;
            }

            /*
             * This data chunk will not be processed during current COPY operation, because when it is added
             * to the bandwidth, the bandwidth's size may become larger than "data_chunk_size".
             * The bandwidth's value should be decreased prior the next iteration.
             */
            bandwidth -= self._dataPool[i]._size_in_mb;
        }
    }

    return dataChunkIndexes;
};

/**
 * Calculate an index of the next data chunk to process.
 * If all data chunks are processed then return -1.
 *
 * @param {Conversion} self
 * @param {Number}     currentIndex
 *
 * @returns {Number}
 */
const getNextIndex = (self, currentIndex) => {
    for (let i = currentIndex + 1; i < self._dataPool.length; ++i) {
        if (self._dataPool[i]._processed === false) {
            return i;
        }
    }

    return -1;
};

/**
 * Instructs DataLoader which data chunks should be loaded.
 * No need to check the state-log.
 * If dataPool's length is zero, then nmig will proceed to the next step.
 *
 * @param {Conversion} self
 * @param {String}     strDataLoaderPath
 * @param {Object}     options
 * @param {Number}     currentIndex
 *
 * @returns {undefined}
 */
const pipeData = (self, strDataLoaderPath, options, currentIndex) => {
    if (dataPoolProcessed(self)) {
        return processConstraints(self);
    }

    const loaderProcess = childProcess.fork(strDataLoaderPath, options);
    const bandwidth     = fillBandwidth(self, currentIndex);
    const chunksToLoad  = bandwidth.map(index => {
        return self._dataPool[index];
    });

    loaderProcess.on('message', signal => {
        if (typeof signal === 'object') {
            self._dicTables[signal.tableName].totalRowsInserted += signal.rowsInserted;
            const msg = '\t--[pipeData]  For now inserted: ' + self._dicTables[signal.tableName].totalRowsInserted + ' rows, '
                + 'Total rows to insert into "' + self._schema + '"."' + signal.tableName + '": ' + signal.totalRowsToInsert;

            log(self, msg);
        } else {
            killProcess(loaderProcess.pid);
            self._processedChunks += chunksToLoad.length;
            return pipeData(self, strDataLoaderPath, options, getNextIndex(self, currentIndex));
        }
    });

    loaderProcess.send(new MessageToDataLoader(self._config, chunksToLoad));
};

/**
 * Manage the DataPipe.
 *
 * @param {Conversion} self
 *
 * @returns {undefined}
 */
module.exports = self => {
    if (dataPoolProcessed(self)) {
        return processConstraints(self);
    }

    const currentIndex      = 0;
    const strDataLoaderPath = path.join(__dirname, 'DataLoader.js');
    const options           = self._loaderMaxOldSpaceSize === 'DEFAULT'
        ? Object.create(null)
        : { execArgv: ['--max-old-space-size=' + self._loaderMaxOldSpaceSize] };

    return pipeData(self, strDataLoaderPath, options, currentIndex);
};
