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

const connect       = require('./Connector');
const log           = require('./Logger');
const generateError = require('./ErrorGenerator');

/**
 * Create primary key and indices.
 *
 * @param   {Conversion} self
 * @param   {String}     tableName
 * @returns {Promise}
 */
module.exports = function(self, tableName) {
    return connect(self).then(() => {
        return new Promise(resolveProcessIndexAndKey => {
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[processIndexAndKey] Cannot connect to MySQL server...\n\t' + error);
                    resolveProcessIndexAndKey();
                } else {
                    let sql = 'SHOW INDEX FROM `' + tableName + '`;';
                    connection.query(sql, (err, arrIndices) => {
                        connection.release();

                        if (err) {
                            generateError(self, '\t--[processIndexAndKey] ' + err, sql);
                            resolveProcessIndexAndKey();
                        } else {
                            let objPgIndices               = Object.create(null);
                            let indexType                  = '';
                            let processIndexAndKeyPromises = [];

                            for (let i = 0; i < arrIndices.length; ++i) {
                                if (arrIndices[i].Key_name in objPgIndices) {
                                    objPgIndices[arrIndices[i].Key_name].column_name.push('"' + arrIndices[i].Column_name + '"');
                                } else {
                                    objPgIndices[arrIndices[i].Key_name] = {
                                        is_unique   : arrIndices[i].Non_unique === 0 ? true : false,
                                        column_name : ['"' + arrIndices[i].Column_name + '"'],
                                        Index_type  : ' USING ' + (arrIndices[i].Index_type === 'SPATIAL' ? 'GIST' : arrIndices[i].Index_type)
                                    };
                                }
                            }

                            for (let attr in objPgIndices) {
                                processIndexAndKeyPromises.push(
                                    new Promise(resolveProcessIndexAndKeySql => {
                                        self._pg.connect((pgError, pgClient, done) => {
                                            if (pgError) {
                                                let msg = '\t--[processIndexAndKey] Cannot connect to PostgreSQL server...\n' + pgError;
                                                generateError(self, msg);
                                                resolveProcessIndexAndKeySql();
                                            } else {
                                                if (attr.toLowerCase() === 'primary') {
                                                    indexType = 'PK';
                                                    sql       = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" '
                                                              + 'ADD PRIMARY KEY(' + objPgIndices[attr].column_name.join(',') + ');';

                                                } else {
                                                    // "schema_idxname_{integer}_idx" - is NOT a mistake.
                                                    let columnName = objPgIndices[attr].column_name[0].slice(1, -1);
                                                    indexType      = 'index';
                                                    sql            = 'CREATE ' + (objPgIndices[attr].is_unique ? 'UNIQUE ' : '') + 'INDEX "'
                                                                   + tableName + '_' + columnName + '_idx" ON "'
                                                                   + self._schema + '"."' + tableName + '" '
                                                                   + objPgIndices[attr].Index_type + ' (' + objPgIndices[attr].column_name.join(',') + ');';
                                                }

                                                pgClient.query(sql, err2 => {
                                                    done();

                                                    if (err2) {
                                                        generateError(self, '\t--[processIndexAndKey] ' + err2, sql);
                                                        resolveProcessIndexAndKeySql();
                                                    } else {
                                                        resolveProcessIndexAndKeySql();
                                                    }
                                                });
                                            }
                                        });
                                    })
                                );
                            }

                            Promise.all(processIndexAndKeyPromises).then(() => {
                                let success = '\t--[processIndexAndKey] "' + self._schema + '"."' + tableName + '": PK/indices are successfully set...';
                                log(self, success, self._dicTables[tableName].tableLogPath);
                                resolveProcessIndexAndKey();
                            });
                        }
                    });
                }
            });
        });
    });
};
