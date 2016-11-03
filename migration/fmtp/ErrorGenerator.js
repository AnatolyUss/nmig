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

const fs  = require('fs');
const log = require('./Logger');

/**
 * Writes a ditailed error message to the "/errors-only.log" file
 *
 * @param   {Conversion} self
 * @param   {String}     message
 * @param   {String}     sql
 * @returns {undefined}
 */
module.exports = function(self, message, sql) {
    message    += '\n\n\tSQL: ' + (sql || '') + '\n\n';
    let buffer  = new Buffer(message, self._encoding);
    log(self, message, undefined, true);

    fs.open(self._errorLogsPath, 'a', self._0777, (error, fd) => {
        if (!error) {
            fs.write(fd, buffer, 0, buffer.length, null, () => {
                buffer = null;
                fs.close(fd);
            });
        }
    });
};
