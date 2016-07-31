/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>
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

const fs = require('fs');
const colors = require('colors');
const async = require('async');

/**
 * Outputs given log.
 * Writes given log to the "/all.log" file.
 * If necessary, writes given log to the "/{tableName}.log" file.
 *
 * @param   {Conversion} self
 * @param   {String}     log
 * @param   {String}     tableLogPath
 * @param   {Boolean}    isErrorLog
 * @returns {undefined}
 */
module.exports = function(self, log, tableLogPath, isErrorLog) {
    let buffer = new Buffer(log + '\n\n', self._encoding);

    if (!isErrorLog) {
        console.log(log.green);
    }
    else {
        console.log(log.red);
    }
    
    async.waterfall([
        function(callback){
            fs.open(self._allLogsPath, 'a', self._0777, (error, fd) => {
                if (!error) {
                    callback(null, fd);
                }
                else {
                    callback(error);
                }
            });
        },
        function(fd, callback){
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                callback(null, fd);
            });
        },
        function(fd, callback){
            fs.close(fd, () => {
                if (tableLogPath) {
                    callback(null);
                }
            });
        },
        function(callback){
            fs.open(tableLogPath, 'a', self._0777, (error, fd) => {
                if(!error) {
                    callback(null, fd);
                }
                else {
                    callback(error);
                }
            });
        },
        function(fd, callback){
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                buffer = null;
                fs.close(fd, () => {
                    callback(null, true);
                });
            });
        }
    ], function (err, result) {
        if(err) {
            console.log(err);
            process.exit();
        }
        else if(!result) {
            console.log('Cannot write log file!'.red);
        }
    });
};
