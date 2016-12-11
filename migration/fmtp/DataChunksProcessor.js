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

const connect            = require('./Connector');
const log                = require('./Logger');
const generateError      = require('./ErrorGenerator');
const arrangeColumnsData = require('./ColumnsDataArranger');

/**
 * Prepares an array of tables and chunk offsets.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 * @param {Boolean}    haveDataChunksProcessed
 *
 * @returns {Promise}
 */
module.exports = function(self, tableName, haveDataChunksProcessed) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            if (haveDataChunksProcessed) {
                return resolve();
            }

            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[prepareDataChunks] Cannot connect to MySQL server...\n\t' + error);
                    resolve();
                } else {
                    // Determine current table size, apply "chunking".
                    let sql = "SELECT (data_length / 1024 / 1024) AS size_in_mb "
                            + "FROM information_schema.tables "
                            + "WHERE table_schema = '" + self._mySqlDbName + "' "
                            + "AND table_name = '" + tableName + "';";

                    connection.query(sql, (err, rows) => {
                        if (err) {
                            connection.release();
                            generateError(self, '\t--[prepareDataChunks] ' + err, sql);
                            resolve();
                        } else {
                            let tableSizeInMb        = +rows[0].size_in_mb;
                            tableSizeInMb            = tableSizeInMb < 1 ? 1 : tableSizeInMb;
                            rows                     = null;
                            const strSelectFieldList = arrangeColumnsData(self._dicTables[tableName].arrTableColumns);
                            sql                      = 'SELECT COUNT(1) AS rows_count FROM `' + tableName + '`;';

                            connection.query(sql, (err2, rows2) => {
                                connection.release();

                                if (err2) {
                                    generateError(self, '\t--[prepareDataChunks] ' + err2, sql);
                                    resolve();
                                } else {
                                    const rowsCnt             = rows2[0].rows_count;
                                    rows2                     = null;
                                    let chunksCnt             = tableSizeInMb / self._dataChunkSize;
                                    chunksCnt                 = chunksCnt < 1 ? 1 : chunksCnt;
                                    const rowsInChunk         = Math.ceil(rowsCnt / chunksCnt);
                                    const arrDataPoolPromises = [];
                                    const msg                 = '\t--[prepareDataChunks] Total rows to insert into '
                                        + '"' + self._schema + '"."' + tableName + '": ' + rowsCnt;

                                    log(self, msg, self._dicTables[tableName].tableLogPath);

                                    for (let offset = 0; offset < rowsCnt; offset += rowsInChunk) {
                                        arrDataPoolPromises.push(new Promise(resolveDataUnit => {
                                            self._pg.connect((error, client, done) => {
                                                if (error) {
                                                    generateError(self, '\t--[prepareDataChunks] Cannot connect to PostgreSQL server...\n' + error);
                                                    resolveDataUnit();
                                                } else {
                                                    const strJson = '{"_tableName":"' + tableName
                                                        + '","_selectFieldList":"' + strSelectFieldList + '",'
                                                        + '"_offset":' + offset + ','
                                                        + '"_rowsInChunk":' + rowsInChunk + ','
                                                        + '"_rowsCnt":' + rowsCnt + '}';

                                                    sql = 'INSERT INTO "' + self._schema + '"."data_pool_' + self._schema
                                                        + self._mySqlDbName + '"("is_started", "json") VALUES(FALSE, $1);';

                                                    client.query(sql, [strJson], err => {
                                                        done();

                                                        if (err) {
                                                            generateError(self, '\t--[prepareDataChunks] INSERT failed...\n' + err, sql);
                                                        }

                                                        resolveDataUnit();
                                                    });
                                                }
                                            });
                                        }));
                                    }

                                    Promise.all(arrDataPoolPromises).then(() => resolve());
                                }
                            });
                        }
                    });
                }
            });
        });
    });
};
