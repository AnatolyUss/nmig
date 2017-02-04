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

const generateError        = require('./ErrorGenerator');
const extraConfigProcessor = require('./ExtraConfigProcessor');

/**
 * Update consistency state.
 *
 * @param {Conversion} self
 * @param {Number}     dataPoolId
 *
 * @returns {Promise}
 */
const updateConsistencyState = (self, dataPoolId) => {
    return new Promise(resolve => {
        self._pg.connect((error, client, done) => {
            if (error) {
                generateError(self, '\t--[ConsistencyEnforcer.updateConsistencyState] Cannot connect to PostgreSQL server...\n' + error);
                resolve();
            } else {
                const sql = 'UPDATE "' + self._schema + '"."data_pool_' + self._schema
                    + self._mySqlDbName + '" SET is_started = TRUE WHERE id = ' + dataPoolId + ';';

                client.query(sql, err => {
                    done();

                    if (err) {
                        generateError(self, '\t--[ConsistencyEnforcer.updateConsistencyState] ' + err, sql);
                    }

                    resolve();
                });
            }
        });
    });
}

/**
 * Get the `is_started` value of current chunk.
 *
 * @param {Conversion} self
 * @param {Number}     dataPoolId
 *
 * @returns {Promise}
 */

const getIsStarted = (self, dataPoolId) => {
    return new Promise(resolve => {
        self._pg.connect((error, client, done) => {
            if (error) {
                generateError(self, '\t--[ConsistencyEnforcer.getConsistencyState] Cannot connect to PostgreSQL server...\n' + error);
                resolve(false);
            } else {
                const sql = 'SELECT is_started AS is_started FROM "' + self._schema + '"."data_pool_' + self._schema
                    + self._mySqlDbName + '" WHERE id = ' + dataPoolId + ';';

                client.query(sql, (err, data) => {
                    done();

                    if (err) {
                        generateError(self, '\t--[ConsistencyEnforcer.getConsistencyState] ' + err, sql);
                        resolve(false);
                    } else {
                        resolve(data.rows[0].is_started);
                    }
                });
            }
        });
    });
}

/**
 * Current data chunk runs after a disaster recovery.
 * Must determine if current chunk has already been loaded.
 * This is in order to prevent possible data duplications.
 *
 * @param {Conversion} self
 * @param {Object}     chunk
 *
 * @returns {Promise}
 */
const hasCurrentChunkLoaded = (self, chunk) => {
    return new Promise(resolve => {
        self._pg.connect((pgError, client, done) => {
            if (pgError) {
                generateError(self, '\t--[ConsistencyEnforcer::hasCurrentChunkLoaded] Cannot connect to PostgreSQL server...\n' + pgError);
                resolve(true);
            } else {
                const originalTableName = extraConfigProcessor.getTableName(self, chunk._tableName, true);
                const sql               = 'SELECT EXISTS(SELECT 1 FROM "' + self._schema + '"."' + chunk._tableName
                    + '" WHERE "' + self._schema + '_' + originalTableName + '_data_chunk_id_temp" = ' + chunk._id + ');';

                client.query(sql, (err, result) => {
                    done();

                    if (err) {
                        generateError(self, '\t--[ConsistencyEnforcer::hasCurrentChunkLoaded] ' + err, sql);
                        resolve(true);
                    } else {
                        resolve(!!result.rows[0].exists);
                    }
                });
            }
        });
    });
}

/**
 * Get consistency state.
 *
 * @param {Conversion} self
 * @param {Object}     chunk
 *
 * @returns {Promise}
 */
const getConsistencyState = (self, chunk) => {
    return new Promise(resolve => {
        getIsStarted(self, chunk._id).then(isStarted => {
            if (isStarted) {
                hasCurrentChunkLoaded(self, chunk).then(result => resolve(result));
            } else {
                // Normal migration flow.
                resolve(false);
            }
        });
    });
}

/**
 * Enforce consistency before processing a chunk of data.
 * Ensure there are no any data duplications.
 * In case of normal execution - it is a good practice.
 * In case of rerunning nmig after unexpected failure - it is absolutely mandatory.
 *
 * @param {Conversion} self
 * @param {Object}     chunk
 *
 * @returns {Promise}
 */
module.exports.enforceConsistency = (self, chunk) => {
    return new Promise(resolve => {
        getConsistencyState(self, chunk).then(hasAlreadyBeenLoaded => {
            if (hasAlreadyBeenLoaded) {
                /*
                 * Current data chunk runs after a disaster recovery.
                 * It has already been loaded.
                 */
                resolve(false);
            } else {
                // Normal migration flow.
                updateConsistencyState(self, chunk._id).then(() => resolve(true));
            }
        })
    });
};

/**
 * Drop the {self._schema + '_' + originalTableName + '_data_chunk_id_temp'} column from current table.
 *
 * @param {Conversion} self
 * @param {String}     tableName
 *
 * @returns {Promise}
 */
module.exports.dropDataChunkIdColumn = (self, tableName) => {
    return new Promise(resolve => {
        self._pg.connect((pgError, client, done) => {
            if (pgError) {
                generateError(self, '\t--[ConsistencyEnforcer::dropDataChunkIdColumn] Cannot connect to PostgreSQL server...\n' + pgError);
                resolve();
            } else {
                const originalTableName = extraConfigProcessor.getTableName(self, tableName, true);
                const columnToDrop      = self._schema + '_' + originalTableName + '_data_chunk_id_temp';
                const sql               = 'ALTER TABLE "' + self._schema + '"."' + tableName + '" DROP COLUMN "' + columnToDrop + '";';

                client.query(sql, (err, result) => {
                    done();

                    if (err) {
                        const errMsg = '\t--[ConsistencyEnforcer::dropDataChunkIdColumn] Failed to drop column "' + columnToDrop + '"\n'
                            + '\t--[ConsistencyEnforcer::dropDataChunkIdColumn] '+ err;

                        generateError(self, errMsg, sql);
                    }

                    resolve();
                });
            }
        });
    });
};
