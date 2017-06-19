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

const connect              = require('./Connector');
const log                  = require('./Logger');
const generateError        = require('./ErrorGenerator');
const extraConfigProcessor = require('./ExtraConfigProcessor');
const { mapDataTypes }     = require('./TableProcessor');

/**
 * Define which columns of the given table have default value.
 * Set default values, if need.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
module.exports = (self, tableName) => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            log(self, '\t--[processDefault] Defines default values for table: "' + self._schema + '"."' + tableName + '"', self._dicTables[tableName].tableLogPath);
            const processDefaultPromises = [];
            const originalTableName      = extraConfigProcessor.getTableName(self, tableName, true);
            const pgSqlNumericTypes      = ['money', 'numeric', 'decimal', 'double precision', 'real', 'bigint', 'int', 'smallint'];
            const sqlReservedValues      = {
                'CURRENT_DATE'        : 'CURRENT_DATE',
                '0000-00-00'          : "'-INFINITY'",
                'CURRENT_TIME'        : 'CURRENT_TIME',
                '00:00:00'            : '00:00:00',
                'CURRENT_TIMESTAMP'   : 'CURRENT_TIMESTAMP',
                '0000-00-00 00:00:00' : "'-INFINITY'",
                'LOCALTIME'           : 'LOCALTIME',
                'LOCALTIMESTAMP'      : 'LOCALTIMESTAMP',
                'NULL'                : 'NULL',
                'UTC_DATE'            : "(CURRENT_DATE AT TIME ZONE 'UTC')",
                'UTC_TIME'            : "(CURRENT_TIME AT TIME ZONE 'UTC')",
                'UTC_TIMESTAMP'       : "(NOW() AT TIME ZONE 'UTC')"
            };

            for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
                if (self._dicTables[tableName].arrTableColumns[i].Default) {
                    processDefaultPromises.push(
                        new Promise(resolveProcessDefault => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    const msg = '\t--[processDefault] Cannot connect to PostgreSQL server...\n' + error;
                                    generateError(self, msg);
                                    resolveProcessDefault();
                                } else {
                                    const pgSqlDataType = mapDataTypes(self._dataTypesMap, self._dicTables[tableName].arrTableColumns[i].Type);
                                    const columnName    = extraConfigProcessor.getColumnName(
                                        self,
                                        originalTableName,
                                        self._dicTables[tableName].arrTableColumns[i].Field,
                                        false
                                    );

                                    let sql = 'ALTER TABLE "' + self._schema + '"."' + tableName
                                        + '" ' + 'ALTER COLUMN "' + columnName + '" SET DEFAULT ';

                                    if (sqlReservedValues[self._dicTables[tableName].arrTableColumns[i].Default]) {
                                        sql += sqlReservedValues[self._dicTables[tableName].arrTableColumns[i].Default] + ';';
                                    } else if (pgSqlNumericTypes.indexOf(pgSqlDataType) === -1) {
                                        sql += "'" + self._dicTables[tableName].arrTableColumns[i].Default + "';";
                                    } else {
                                        sql += self._dicTables[tableName].arrTableColumns[i].Default + ';';
                                    }

                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            const msg2 = '\t--[processDefault] Error occurred when tried to set default value for "'
                                                + self._schema + '"."' + tableName + '"."' + columnName + '"...\n' + err;

                                            generateError(self, msg2, sql);
                                            resolveProcessDefault();
                                        } else {
                                            const success = '\t--[processDefault] Set default value for "' + self._schema + '"."'
                                                + tableName + '"."' + columnName + '"...';

                                            log(self, success, self._dicTables[tableName].tableLogPath);
                                            resolveProcessDefault();
                                        }
                                    });
                                }
                            });
                        })
                    );
                }
            }

            Promise.all(processDefaultPromises).then(() => resolve());
        });
    });
};
