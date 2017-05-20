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

const connect       = require('./Connector');
const log           = require('./Logger');
const generateError = require('./ErrorGenerator');

/**
 * Create the "{schema}"."data_pool_{self._schema + self._mySqlDbName} temporary table."
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.createDataPoolTable = self => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[DataPoolManager.createDataPoolTable] Cannot connect to PostgreSQL server...\n' + error);
                    process.exit();
                } else {
                    const sql = 'CREATE TABLE IF NOT EXISTS "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName
                        + '"("id" BIGSERIAL, "json" TEXT, "is_started" BOOLEAN);';

                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[DataPoolManager.createDataPoolTable] ' + err, sql);
                            process.exit();
                        } else {
                            log(self, '\t--[DataPoolManager.createDataPoolTable] table "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" is created...');
                            resolve(self);
                        }
                    });
                }
            });
        });
    });
};

/**
 * Drop the "{schema}"."data_pool_{self._schema + self._mySqlDbName} temporary table."
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.dropDataPoolTable = self => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[DataPoolManager.dropDataPoolTable] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    const sql = 'DROP TABLE "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '";';

                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[DataPoolManager.dropDataPoolTable] ' + err, sql);
                        } else {
                            log(self, '\t--[DataPoolManager.dropDataPoolTable] table "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '" is dropped...');
                        }

                        resolve();
                    });
                }
            });
        });
    });
};

/**
 * Reads temporary table, and generates Data-pool.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.readDataPool = self => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[DataPoolManager.readDataPool] Cannot connect to PostgreSQL server...\n' + error);
                    process.exit();
                } else {
                    const sql = 'SELECT id AS id, json AS json FROM "' + self._schema + '"."data_pool_' + self._schema + self._mySqlDbName + '";';
                    client.query(sql, (err, arrDataPool) => {
                        done();

                        if (err) {
                            generateError(self, '\t--[DataPoolManager.readDataPool] ' + err, sql);
                            process.exit();
                        }

                        for (let i = 0; i < arrDataPool.rows.length; ++i) {
                            const obj = JSON.parse(arrDataPool.rows[i].json);
                            obj._id   = arrDataPool.rows[i].id;
                            self._dataPool.push(obj);
                        }

                        log(self, '\t--[DataPoolManager.readDataPool] Data-Pool is loaded...');
                        resolve(self);
                    });
                }
            });
        });
    });
};
