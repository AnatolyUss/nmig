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
 * Get state-log.
 *
 * @param {Conversion} self
 * @param {String}     param
 *
 * @returns {Promise}
 */
module.exports.get = (self, param) => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[MigrationStateManager.get] Cannot connect to PostgreSQL server...\n' + error);
                    resolve(false);
                } else {
                    const sql = 'SELECT ' + param + ' FROM "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '";';

                    client.query(sql, (err, data) => {
                        done();

                        if (err) {
                            generateError(self, '\t--[MigrationStateManager.get] ' + err, sql);
                            resolve(false);
                        } else {
                            resolve(data.rows[0][param]);
                        }
                    });
                }
            });
        });
    });
};

/**
 * Update the state-log.
 *
 * @param {Conversion} self
 * @param {String}     param
 *
 * @returns {Promise}
 */
module.exports.set = (self, param) => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[MigrationStateManager.set] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    const sql = 'UPDATE "' + self._schema + '"."state_logs_'
                        + self._schema + self._mySqlDbName + '" SET ' + param + ' = TRUE;';

                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[MigrationStateManager.set] ' + err, sql);
                        }

                        resolve();
                    });
                }
            });
        });
    });
};

/**
 * Create the "{schema}"."state_logs_{self._schema + self._mySqlDbName} temporary table."
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.createStateLogsTable = self => {
    return connect(self).then(() => {
        return new Promise((resolve, reject) => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[createStateLogsTable] Cannot connect to PostgreSQL server...\n' + error);
                    reject();
                } else {
                    let sql = 'CREATE TABLE IF NOT EXISTS "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName
                            + '"('
                            + '"tables_loaded" BOOLEAN,'
                            + '"per_table_constraints_loaded" BOOLEAN,'
                            + '"foreign_keys_loaded" BOOLEAN,'
                            + '"views_loaded" BOOLEAN'
                            + ');';

                    client.query(sql, err => {
                        if (err) {
                            done();
                            generateError(self, '\t--[createStateLogsTable] ' + err, sql);
                            reject();
                        } else {
                            sql = 'SELECT COUNT(1) AS cnt FROM "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '";';
                            client.query(sql, (errorCount, result) => {
                                if (errorCount) {
                                    done();
                                    generateError(self, '\t--[createStateLogsTable] ' + errorCount, sql);
                                    reject();
                                } else if (+result.rows[0].cnt === 0) {
                                    sql = 'INSERT INTO "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName
                                        + '" VALUES(FALSE, FALSE, FALSE, FALSE);';

                                    client.query(sql, errorInsert => {
                                        done();

                                        if (errorInsert) {
                                            generateError(self, '\t--[createStateLogsTable] ' + errorInsert, sql);
                                            reject();
                                        } else {
                                            const msg = '\t--[createStateLogsTable] table "' + self._schema + '"."state_logs_'
                                                + self._schema + self._mySqlDbName + '" is created...';

                                            log(self, msg);
                                            resolve();
                                        }
                                    });
                                } else {
                                    const msg2 = '\t--[createStateLogsTable] table "' + self._schema + '"."state_logs_'
                                        + self._schema + self._mySqlDbName + '" is created...';

                                    log(self, msg2);
                                    resolve();
                                }
                            });
                        }
                    });
                }
            });
        });
    });
};

/**
 * Drop the "{schema}"."state_logs_{self._schema + self._mySqlDbName} temporary table."
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.dropStateLogsTable = self => {
    return connect(self).then(() => {
        return new Promise(resolve => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[dropStateLogsTable] Cannot connect to PostgreSQL server...\n' + error);
                    resolve();
                } else {
                    const sql = 'DROP TABLE "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '";';
                    client.query(sql, err => {
                        done();

                        if (err) {
                            generateError(self, '\t--[dropStateLogsTable] ' + err, sql);
                        } else {
                            log(self, '\t--[dropStateLogsTable] table "' + self._schema + '"."state_logs_' + self._schema + self._mySqlDbName + '" is dropped...');
                        }

                        resolve();
                    });
                }
            });
        });
    });
};
