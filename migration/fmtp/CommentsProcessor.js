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

const log           = require('./Logger');
const generateError = require('./ErrorGenerator');
const connect       = require('./Connector');

/**
 * Create table comments.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
function processTableComments(self, tableName) {
    return new Promise(resolve => {
        self._mysql.getConnection((error, connection) => {
            if (error) {
                // The connection is undefined.
                generateError(self, '\t--[processTableComments] Cannot connect to MySQL server...\n\t' + error);
                resolve();
            } else {
                let sql = "SELECT table_comment AS table_comment "
                        + "FROM information_schema.tables "
                        + "WHERE table_schema = '" + self._mySqlDbName + "' "
                        + "AND table_name = '" + tableName + "';";

                connection.query(sql, (err, rows) => {
                    connection.release();

                    if (err) {
                        generateError(self, '\t--[processTableComments] ' + err, sql);
                        resolve();
                    } else {
                        self._pg.connect((e, client, done) => {
                            if (e) {
                                let msg = '\t--[processTableComments] Cannot connect to PostgreSQL server...\n' + e;
                                generateError(self, msg);
                                resolve();
                            } else {
                                sql = 'COMMENT ON TABLE "' + self._schema + '"."' + tableName + '" IS ' + '\'' + rows[0].table_comment + '\';';

                                client.query(sql, queryError => {
                                    done();

                                    if (queryError) {
                                        let msg = '\t--[processTableComments] Error while processing comment for "'
                                                + self._schema + '"."' + tableName + '"...\n' + queryError;

                                        generateError(self, msg, sql);
                                        resolve();
                                    } else {
                                        let success = '\t--[processTableComments] Successfully set comment for table "'
                                                    + self._schema + '"."' + tableName + '"';

                                        log(self, success, self._dicTables[tableName].tableLogPath);
                                        resolve();
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    });
}

/**
 * Create columns comments.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
function processColumnsComments(self, tableName) {
    return new Promise(resolve => {
        let arrCommentPromises = [];

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Comment !== '') {
                arrCommentPromises.push(
                    new Promise(resolveComment => {
                        self._pg.connect((error, client, done) => {
                            if (error) {
                                let msg = '\t--[processColumnsComments] Cannot connect to PostgreSQL server...\n' + error;
                                generateError(self, msg);
                                resolveComment();
                            } else {
                                let sql = 'COMMENT ON COLUMN "' + self._schema + '"."' + tableName + '"."'
                                        + self._dicTables[tableName].arrTableColumns[i].Field
                                        + '" IS \'' + self._dicTables[tableName].arrTableColumns[i].Comment + '\';';

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        let msg = '\t--[processColumnsComments] Error while processing comment for "' + self._schema + '"."'
                                                + tableName + '"."' + self._dicTables[tableName].arrTableColumns[i].Field + '"...\n' + err;

                                        generateError(self, msg, sql);
                                        resolveComment();
                                    } else {
                                        let success = '\t--[processColumnsComments] Set comment for "' + self._schema + '"."' + tableName
                                                      + '" column: "' + self._dicTables[tableName].arrTableColumns[i].Field + '"...';

                                        log(self, success, self._dicTables[tableName].tableLogPath);
                                        resolveComment();
                                    }
                                });
                            }
                        });
                    })
                );
            }
        }

        Promise.all(arrCommentPromises).then(() => resolve());
    });
}

/**
 * Migrate comments.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 * 
 * @returns {Promise}
 */
module.exports = function(self, tableName) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            let msg = '\t--[CommentsProcessor] Creates comments for table "' + self._schema + '"."' + tableName + '"...';
            log(self, msg, self._dicTables[tableName].tableLogPath);
            let tableCommentsPromise   = processTableComments(self, tableName);
            let columnsCommentsPromise = processColumnsComments(self, tableName);
            Promise.all([tableCommentsPromise, columnsCommentsPromise]).then(() => resolve());
        });
    });
};
