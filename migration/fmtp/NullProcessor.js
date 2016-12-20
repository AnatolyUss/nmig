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

const connect              = require('./Connector');
const log                  = require('./Logger');
const generateError        = require('./ErrorGenerator');
const extraConfigProcessor = require('./ExtraConfigProcessor');

/**
 * Define which columns of the given table can contain the "NULL" value.
 * Set an appropriate constraint, if need.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
module.exports = function(self, tableName) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[processNull] Defines "NOT NULLs" for table: "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
            const processNullPromises = [];
            const originalTableName   = extraConfigProcessor.getTableName(self, tableName, true);

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Null.toLowerCase() === 'no') {
                    processNullPromises.push(
                        new Promise(resolveProcessNull => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    const msg = '\t--[processNull] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
                                    resolveProcessNull();
                                } else {
                                    const columnName = extraConfigProcessor.getColumnName(
                                        self,
                                        originalTableName,
                                        self._dicTables[tableName].arrTableColumns[i].Field,
                                        false
                                    );

                                    const sql = 'ALTER TABLE "' + self._schema + '"."' + tableName
                                        + '" ALTER COLUMN "' + columnName + '" SET NOT NULL;';

                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            const msg2 = '\t--[processNull] Error while setting NOT NULL for "' + self._schema + '"."'
                                                + tableName + '"."' + columnName + '"...\n' + err;

                                            generateError(self, msg2, sql);
                                            resolveProcessNull();
                                        } else {
                                            const success = '\t--[processNull] Set NOT NULL for "' + self._schema + '"."' + tableName
                                                + '"."' + columnName + '"...';

                                            log(self, success, self._dicTables[tableName].tableLogPath);
                                            resolveProcessNull();
                                        }
                                    });
                                }
                            });
                        })
                    );
                }
            }

            Promise.all(processNullPromises).then(() => resolve());
        });
    });
};
