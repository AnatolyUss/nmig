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

const fs                 = require('fs');
const pgCopyStreams      = require('pg-copy-streams');
const csvStringify       = require('./CsvStringifyModified');
const log                = require('./Logger');
const generateError      = require('./ErrorGenerator');
const connect            = require('./Connector');
const Conversion         = require('./Conversion');
const MessageToMaster    = require('./MessageToMaster');
const enforceConsistency = require('./ConsistencyEnforcer');
const copyFrom           = pgCopyStreams.from;

let self      = null;
let getBuffer = null;
let version   = +process.version.split('.')[0].slice(1);

if (version < 6) {
    getBuffer = require('./OldBuffer');
} else {
    getBuffer = require('./NewBuffer');
}

process.on('message', signal => {
    self         = new Conversion(signal.config);
    let promises = [];
    log(self, '\t--[loadData] Loading the data...');

    for (let i = 0; i < signal.chunks.length; ++i) {
        promises.push(
            connect(self).then(() => {
                return enforceConsistency(self, signal.chunks[i]._id);
            }).then(isNormalFlow => {
                if (isNormalFlow) {
                    return populateTableWorker(
                        signal.chunks[i]._tableName,
                        signal.chunks[i]._selectFieldList,
                        signal.chunks[i]._offset,
                        signal.chunks[i]._rowsInChunk,
                        signal.chunks[i]._rowsCnt,
                        signal.chunks[i]._id
                    );
                }

                let sql = buildChunkQuery(
                    signal.chunks[i]._tableName,
                    signal.chunks[i]._selectFieldList,
                    signal.chunks[i]._offset,
                    signal.chunks[i]._rowsInChunk
                );

                let strTwelveSpaces = '            ';
                let rejectedData    = '\n\t--[loadData] Possible data duplication alert!\n\t ' + strTwelveSpaces
                                    + 'Data, retrievable by following MySQL query:\n' + sql + '\n\t ' + strTwelveSpaces
                                    + 'may already be migrated.\n\t' + strTwelveSpaces + ' Please, check it.';

                log(self, rejectedData, self._logsDirPath + '/' + signal.chunks[i]._tableName + '.log');
                return deleteChunk(signal.chunks[i]._id);
            })
        );
    }

    Promise.all(promises).then(() => process.send('processed'));
});

/**
 * Delete given record from the data-pool.
 *
 * @param {Number}                   dataPoolId
 * @param {Node-pg client|undefined} client
 * @param {Function|undefined}       done
 *
 * @returns {Promise}
 */
function deleteChunk(dataPoolId, client, done) {
    return new Promise(resolve => {
        if (client) {
            let sql = 'DELETE FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" ' + 'WHERE id = ' + dataPoolId + ';';

            client.query(sql, err => {
                done();

                if (err) {
                    generateError(self, '\t--[deleteChunk] ' + err, sql);
                }

                resolve();
            });
        } else {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[deleteChunk] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    let sql = 'DELETE FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" ' + 'WHERE id = ' + dataPoolId + ';';

                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[deleteChunk] ' + err, sql);
                        }

                        resolve();
                    });
                }
            });
        }
    });
}

/**
 * Delete given csv file.
 *
 * @param {String}         csvAddr
 * @param {FileDescriptor} fd
 *
 * @returns {Promise}
 */
function deleteCsv(csvAddr, fd) {
    return new Promise(resolve => {
        fs.unlink(csvAddr, () => {
            fs.close(fd, () => {
                resolve();
            });
        });
    });
}

/**
 * Build a MySQL query to retrieve the chunk of data.
 *
 * @param {String} tableName
 * @param {String} strSelectFieldList
 * @param {Number} offset
 * @param {Number} rowsInChunk
 *
 * @returns {String}
 */
function buildChunkQuery(tableName, strSelectFieldList, offset, rowsInChunk) {
    return 'SELECT ' + strSelectFieldList + ' FROM `' + tableName + '` LIMIT ' + offset + ',' + rowsInChunk + ';';
}

/**
 * Delete given record from the data-pool.
 * Deleted related csv file.
 *
 * @param {Number}                   dataPoolId
 * @param {Node-pg client|undefined} client
 * @param {Function|undefined}       done
 * @param {String}                   csvAddr
 * @param {Number}                   fd
 * @param {Function}                 callback
 *
 * @returns {undefined}
 */
function deleteChunkAndCsv(dataPoolId, client, done, csvAddr, fd, callback) {
    deleteChunk(dataPoolId, client, done).then(() => {
        deleteCsv(csvAddr, fd).then(() => callback());
    });
}

/**
 * Process data-loading error.
 *
 * @param {String}                   streamError
 * @param {String}                   sql
 * @param {String}                   sqlCopy
 * @param {String}                   tableName
 * @param {Number}                   dataPoolId
 * @param {Node-pg client|undefined} client
 * @param {Function|undefined}       done
 * @param {String}                   csvAddr
 * @param {Number}                   fd
 * @param {Function}                 callback
 *
 * @returns {undefined}
 */
function processDataError(streamError, sql, sqlCopy, tableName, dataPoolId, client, done, csvAddr, fd, callback) {
    generateError(self, '\t--[populateTableWorker] ' + streamError, sqlCopy);
    let rejectedData = '\t--[populateTableWorker] Error loading table data:\n' + sql + '\n';
    log(self, rejectedData, self._logsDirPath + '/' + tableName + '.log');
    deleteChunkAndCsv(dataPoolId, client, done, csvAddr, fd, callback);
}

/**
 * Load a chunk of data using "PostgreSQL COPY".
 *
 * @param {String} tableName
 * @param {String} strSelectFieldList
 * @param {Number} offset
 * @param {Number} rowsInChunk
 * @param {Number} rowsCnt
 * @param {Number} dataPoolId
 *
 * @returns {Promise}
 */
function populateTableWorker(tableName, strSelectFieldList, offset, rowsInChunk, rowsCnt, dataPoolId) {
    return new Promise(resolvePopulateTableWorker => {
        self._mysql.getConnection((error, connection) => {
            if (error) {
                // The connection is undefined.
                generateError(self, '\t--[populateTableWorker] Cannot connect to MySQL server...\n\t' + error);
                resolvePopulateTableWorker();
            } else {
                let csvAddr = self._tempDirPath + '/' + tableName + offset + '.csv';
                let sql     = buildChunkQuery(tableName, strSelectFieldList, offset, rowsInChunk);

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
                                let buffer = getBuffer(csvString, self._encoding);
                                csvString  = null;

                                fs.open(csvAddr, 'w', self._0777, (csvErrorFputcsvOpen, fd) => {
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
                                                self._pg.connect((error, client, done) => {
                                                    if (error) {
                                                        generateError(self, '\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                                        deleteCsv(csvAddr, fd).then(() => resolvePopulateTableWorker());
                                                    } else {
                                                        let sqlCopy    = 'COPY "' + self._schema + '"."' + tableName + '" FROM STDIN DELIMITER \'' + ',\'' + ' CSV;';
                                                        let copyStream = client.query(copyFrom(sqlCopy));
                                                        let readStream = fs.createReadStream(csvAddr);

                                                        copyStream.on('end', () => {
                                                            /*
                                                             * COPY FROM STDIN does not return the number of rows inserted.
                                                             * But the transactional behavior still applies (no records inserted if at least one failed).
                                                             * That is why in case of 'on end' the rowsInChunk value is actually the number of records inserted.
                                                             */
                                                            process.send(new MessageToMaster(tableName, rowsInChunk, rowsCnt));
                                                            deleteChunkAndCsv(dataPoolId, client, done, csvAddr, fd, resolvePopulateTableWorker);
                                                        });

                                                        copyStream.on('error', copyStreamError => {
                                                            processDataError(copyStreamError, sql, sqlCopy, tableName, dataPoolId, client, done, csvAddr, fd, resolvePopulateTableWorker);
                                                        });

                                                        readStream.on('error', readStreamError => {
                                                            processDataError(readStreamError, sql, sqlCopy, tableName, dataPoolId, client, done, csvAddr, fd, resolvePopulateTableWorker);
                                                        });

                                                        readStream.pipe(copyStream);
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
}
