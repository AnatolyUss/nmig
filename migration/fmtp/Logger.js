/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - 2017 Anatoly Khaytovich <anatolyuss@gmail.com>
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

const fs        = require('fs');
const getBuffer = +process.version.split('.')[0].slice(1) < 6
    ? require('./OldBuffer')
    : require('./NewBuffer');

/**
 * Outputs given log.
 * Writes given log to the "/all.log" file.
 * If necessary, writes given log to the "/{tableName}.log" file.
 *
 * @param {Conversion} self
 * @param {String}     log
 * @param {String}     tableLogPath
 * @param {Boolean}    isErrorLog
 *
 * @returns {undefined}
 */
module.exports = (self, log, tableLogPath, isErrorLog) => {
    let buffer = getBuffer(log + '\n\n', self._encoding);

    if (!isErrorLog) {
        console.log(log);
    }

    fs.open(self._allLogsPath, 'a', self._0777, (error, fd) => {
        if (!error) {
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                fs.close(fd, () => {
                    if (tableLogPath) {
                        fs.open(tableLogPath, 'a', self._0777, (error, fd) => {
                            if (!error) {
                                fs.write(fd, buffer, 0, buffer.length, null, () => {
                                    buffer = null;
                                    fs.close(fd, () => {
                                        // Each async function MUST have a callback (according to Node.js >= 7).
                                    });
                                });
                            }
                        });
                    }
                });
            });
        }
    });
};
