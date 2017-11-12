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

const log                   = require('./Logger');
const generateError         = require('./ErrorGenerator');
const connect               = require('./Connector');
const extraConfigProcessor  = require('./ExtraConfigProcessor');

/**
 * Create table comments.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
const processTableComments = (self, tableName) => {
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
                        + "AND table_name = '" + extraConfigProcessor.getTableName(self, tableName, true) + "';";

                connection.query(sql, (err, rows) => {
                    connection.release();

                    if (err) {
                        generateError(self, '\t--[processTableComments] ' + err, sql);
                        resolve();
                    } else {
                        self._pg.connect((e, client, done) => {
                            if (e) {
                                generateError(self, '\t--[processTableComments] Cannot connect to PostgreSQL server...\n' + e);
                                resolve();
                            } else {
                                sql = 'COMMENT ON TABLE "' + self._schema + '"."' + tableName + '" IS ' + '\'' + rows[0].table_comment + '\';';

                                client.query(sql, queryError => {
                                    done();

                                    if (queryError) {
                                        const msg = '\t--[processTableComments] Error while processing comment for "'
                                            + self._schema + '"."' + tableName + '"...\n' + queryError;

                                        generateError(self, msg, sql);
                                        resolve();
                                    } else {
                                        const success = '\t--[processTableComments] Successfully set comment for table "'
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
const processColumnsComments = (self, tableName) => {
    return new Promise(resolve => {
        const arrCommentPromises = [];
        const originalTableName  = extraConfigProcessor.getTableName(self, tableName, true);

        for (let i = 0; i < self._dicTables[tableName].arrTableColumns.length; ++i) {
            if (self._dicTables[tableName].arrTableColumns[i].Comment !== '') {
                arrCommentPromises.push(
                    new Promise(resolveComment => {
                        self._pg.connect((error, client, done) => {
                            if (error) {
                                generateError(self, '\t--[processColumnsComments] Cannot connect to PostgreSQL server...\n' + error);
                                resolveComment();
                            } else {
                                const columnName = extraConfigProcessor.getColumnName(
                                    self,
                                    originalTableName,
                                    self._dicTables[tableName].arrTableColumns[i].Field,
                                    false
                                );

                                let comment = self._dicTables[tableName].arrTableColumns[i].Comment;
                                let regexp = new RegExp('\'', 'g');
                                comment = comment.replace(regexp, '\'\'');

                                const sql = 'COMMENT ON COLUMN "' + self._schema + '"."' + tableName + '"."'
                                    + columnName + '" IS \'' + comment + '\';';

                                client.query(sql, err => {
                                    done();

                                    if (err) {
                                        const msg = '\t--[processColumnsComments] Error while processing comment for "' + self._schema + '"."'
                                            + tableName + '"."' + columnName + '"...\n' + err;

                                        generateError(self, msg, sql);
                                        resolveComment();
                                    } else {
                                        const success = '\t--[processColumnsComments] Set comment for "' + self._schema + '"."' + tableName
                                            + '" column: "' + columnName + '"...';

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
module.exports = (self, tableName) => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            const msg = '\t--[CommentsProcessor] Creates comments for table "' + self._schema + '"."' + tableName + '"...';
            log(self, msg, self._dicTables[tableName].tableLogPath);
            Promise.all([
                processTableComments(self, tableName),
                processColumnsComments(self, tableName)
            ]).then(() => resolve());
        });
    });
};
