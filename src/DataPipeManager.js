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
 *
 *
 * @param {Conversion} self
 * @param {Number}     currentIndex
 *
 * @returns {Number}
 */
const getBandwidth = (self, currentIndex) => {
    if (self._dataPool[currentIndex]._size_in_mb < self._dataChunkSize) {
        // Size of current chunk can never be larger than "data_chunk_size".
        // Current chunk is smaller than the "data_chunk_size".
        // More chunks can be processed.
        if (self._dataChunkSize - self._dataPool[currentIndex]._size_in_mb < self._smallestDataChunkSizeInMb) {
            // Smallest data chunk is larger than the gap between "data_chunk_size" and current chunk.
            // Currently, no more chunks can be processed.
            return 1;
        }

        //
    }

    return 1;
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

    const bandwidth     = getBandwidth(self, currentIndex);
    const endOfSlice    = self._dataPool.length - (self._dataPool.length - bandwidth - currentIndex);
    const nextPoolIndex = currentIndex + bandwidth;
    const loaderProcess = childProcess.fork(strDataLoaderPath, options);

    loaderProcess.on('message', signal => {
        if (typeof signal === 'object') {
            self._dicTables[signal.tableName].totalRowsInserted += signal.rowsInserted;
            const msg = '\t--[pipeData]  For now inserted: ' + self._dicTables[signal.tableName].totalRowsInserted + ' rows, '
                + 'Total rows to insert into "' + self._schema + '"."' + signal.tableName + '": ' + signal.totalRowsToInsert;

            log(self, msg);
        } else {
            killProcess(loaderProcess.pid);
            self._processedChunks += bandwidth;
            return pipeData(self, strDataLoaderPath, options, nextPoolIndex);
        }
    });

    loaderProcess.send(new MessageToDataLoader(self._config, self._dataPool.slice(currentIndex, endOfSlice)));
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

    const strDataLoaderPath = path.join(__dirname, 'DataLoader.js');
    const options           = self._loaderMaxOldSpaceSize === 'DEFAULT'
        ? Object.create(null)
        : { execArgv: ['--max-old-space-size=' + self._loaderMaxOldSpaceSize] };

    return pipeData(self, strDataLoaderPath, options, 0);
};
