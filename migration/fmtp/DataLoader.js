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
const csvStringify    = require('./CsvStringifyModified');
const log             = require('./Logger');
const generateError   = require('./ErrorGenerator');
const connect         = require('./Connector');
const Conversion      = require('./Conversion');
const MessageToMaster = require('./MessageToMaster');

let self = null;

process.on('message', signal => {
    self            = new Conversion(signal.config);
    let arrPromises = [];
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
 * Delete given record from the data-pool.
 *
 * @param   {Number}         dataPoolId
 * @param   {Node-pg client} client
 * @param   {Function}       done
 * @param   {Function}       callback
 * @returns {undefined}
 */
function deleteChunk(dataPoolId, client, done, callback) {
    let sql = 'DELETE FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" '
            + 'WHERE id = ' + dataPoolId + ';';

    client.query(sql, err => {
        done();

        if (err) {
            generateError(self, '\t--[deleteChunk] ' + err, sql);
        }

        return callback();
    });
}

/**
 * Delete given csv file.
 *
 * @param   {String}         csvAddr
 * @param   {FileDescriptor} fd
 * @param   {Function}       callback
 * @returns {undefined}
 */
function deleteCsv(csvAddr, fd, callback) {
    fs.unlink(csvAddr, () => {
        fs.close(fd, () => {
            return callback();
        });
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

                            csvStringify(rows, {quotedString: true}, (csvError, csvString) => {
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
                                                    self._pg.connect((error, client, done) => {
                                                        if (error) {
                                                            generateError(self, '\t--[populateTableWorker] Cannot connect to PostgreSQL server...\n' + error, sql);
                                                            deleteCsv(csvAddr, fd, () => resolvePopulateTableWorker());
                                                        } else {
                                                            let sqlCopy = 'COPY "' + self._schema + '"."' + tableName + '" FROM ' + '\'' + csvAddr + '\' DELIMITER \'' + ',\'' + ' CSV;';

                                                            client.query(sqlCopy, (error, result) => {
                                                                if (error) {
                                                                    generateError(self, '\t--[populateTableWorker] ' + err, sqlCopy);
                                                                    let rejectedData = '\t--[populateTableWorker] Following MySQL query will return a data set, rejected by PostgreSQL:\n' + sql + '\n';
                                                                    log(self, rejectedData, self._logsDirPath + '/' + tableName + '.log');
                                                                } else {
                                                                    process.send(new MessageToMaster(tableName, result.rowCount, rowsCnt));
                                                                }

                                                                deleteChunk(dataPoolId, client, done, () => {
                                                                    deleteCsv(csvAddr, fd, () => resolvePopulateTableWorker());
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
        });
    });
}
