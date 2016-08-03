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

const fs              = require('fs');
const pg              = require('pg');
const mysql           = require('mysql');
const csvStringify    = require('./CsvStringifyModified');
const isIntNumeric    = require('./IntegerValidator');
const log             = require('./Logger');
const generateError   = require('./ErrorGenerator');
const connect         = require('./Connector');
const Conversion      = require('./Conversion');
const MessageToMaster = require('./MessageToMaster');

let self = null;

process.on('message', signal => {
    self                 = new Conversion(signal.config);
    pg.defaults.poolSize = self._maxPoolSizeTarget;
    let arrPromises      = [];
    log(self, '\t--[loadData] Loading the data...');

    for (let i = 0; i < signal.chunks.length; ++i) {
        arrPromises.push(populateTableWorker(
            signal.chunks[i]._tableName,
            signal.chunks[i]._selectFieldList,
            signal.chunks[i]._offset,
            signal.chunks[i]._rowsInChunk,
            signal.chunks[i]._rowsCnt,
            signal.chunks[i]._id
        ));
    }

    Promise.all(arrPromises).then(() => process.send('processed'));
});

/**
 * Run given query as part of the transaction.
 * Return client to the connection pool, if necessary.
 *
 * @param   {String}         sql
 * @param   {Node-pg client} client
 * @param   {Function}       done
 * @param   {Function}       callback
 * @returns {undefined}
 */
function processTransaction(sql, client, done, callback) {
    client.query(sql, (error, result) => {
        if (error) {
            /*
             * Release the client in case of error.
             * Must run 'ROLLBACK' before releasing the client.
             */
            generateError(self, '\t--[processTransaction] ' + error, sql);
            client.query('ROLLBACK;', () => {
                done();
                return callback(true); // 'callback' has executed due to an error.
            });
        }

        if (sql === 'ROLLBACK;' || sql === 'COMMIT;') {
            /*
             * Running 'ROLLBACK' or 'COMMIT' - meaning that the transaction is over.
             * The client must be released back to the pool.
             * In any other case the client must be held for reusing.
             */
            done();
        }

        return callback(false, result); // 'processTransaction' has finished successfully.
    });
}

/**
 * Load a chunk of data using "PostgreSQL COPY".
 *
 * @param   {String} tableName
 * @param   {String} strSelectFieldList
 * @param   {Number} offset
 * @param   {Number} rowsInChunk
 * @param   {Number} rowsCnt
 * @param   {Number} dataPoolId
 * @returns {Promise}
 */
function populateTableWorker(tableName, strSelectFieldList, offset, rowsInChunk, rowsCnt, dataPoolId) {
    return connect(self).then(() => {
        return new Promise(resolvePopulateTableWorker => {
            self._mysql.getConnection((error, connection) => {
                if (error) {
                    // The connection is undefined.
                    generateError(self, '\t--[populateTableWorker] Cannot connect to MySQL server...\n\t' + error);
                    resolvePopulateTableWorker();
                } else {
                    let csvAddr = self._tempDirPath + '/' + tableName + offset + '.csv';
                    let sql     = 'SELECT ' + strSelectFieldList + ' FROM `' + tableName + '` LIMIT ' + offset + ',' + rowsInChunk + ';';

                    connection.query(sql, (err, rows) => {
                        connection.release();

                        if (err) {
                            generateError(self, '\t--[populateTableWorker] ' + err, sql);
                            resolvePopulateTableWorker();
                        } else {
                            rowsInChunk = rows.length;

                            csvStringify(rows, (csvError, csvString) => {
                                rows = null;

                                if (csvError) {
                                    generateError(self, '\t--[populateTableWorker] ' + csvError);
                                    resolvePopulateTableWorker();
                                } else {
                                    let buffer = new Buffer(csvString, self._encoding);
                                    csvString  = null;

                                    fs.open(csvAddr, 'a', self._0777, (csvErrorFputcsvOpen, fd) => {
                                        if (csvErrorFputcsvOpen) {
                                            buffer = null;
                                            generateError(self, '\t--[populateTableWorker] ' + csvErrorFputcsvOpen);
                                            resolvePopulateTableWorker();
                                        } else {
                                            fs.write(fd, buffer, 0, buffer.length, null, csvErrorFputcsvWrite => {
                                                buffer = null;

                                                if (csvErrorFputcsvWrite) {
                                                    generateError(self, '\t--[populateTableWorker] ' + csvErrorFputcsvWrite);
                                                    resolvePopulateTableWorker();
                                                } else {
                                                    pg.connect(self._targetConString, (error, client, done) => {
                                                        if (error) {
                                                            done();
                                                            generateError(self, '\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                                            resolvePopulateTableWorker();
                                                        } else {
                                                            processTransaction('START TRANSACTION;', client, done, boolErrorWhenBegan => {
                                                                if (boolErrorWhenBegan) {
                                                                    fs.unlink(csvAddr, () => {
                                                                        fs.close(fd, () => {
                                                                            resolvePopulateTableWorker();
                                                                        });
                                                                    });
                                                                } else {
                                                                    sql = 'COPY "' + self._schema + '"."' + tableName + '" FROM ' + '\'' + csvAddr + '\' DELIMITER \'' + ',\'' + ' CSV;';

                                                                    processTransaction(sql, client, done, (boolErr, result) => {
                                                                        if (boolErr || typeof result === 'undefined') {
                                                                            fs.unlink(csvAddr, () => {
                                                                                fs.close(fd, () => {
                                                                                    return resolvePopulateTableWorker();
                                                                                });
                                                                            });
                                                                        }

                                                                        if (isIntNumeric(result.rowCount)) {
                                                                            process.send(new MessageToMaster(tableName, result.rowCount, rowsCnt));
                                                                        }

                                                                        fs.unlink(csvAddr, () => {
                                                                            fs.close(fd, () => {
                                                                                sql = 'DELETE FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" '
                                                                                    + 'WHERE id = ' + dataPoolId + ';';

                                                                                processTransaction(sql, client, done, boolErrorWhenDelete => {
                                                                                    if (boolErrorWhenDelete) {
                                                                                        resolvePopulateTableWorker();
                                                                                    } else {
                                                                                        processTransaction('COMMIT;', client, done, () => {
                                                                                            resolvePopulateTableWorker();
                                                                                        });
                                                                                    }
                                                                                });
                                                                            });
                                                                        });
                                                                    });
                                                                }
                                                            });
                                                        }
                                                   });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        });
    });
}
