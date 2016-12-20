/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - 2017 Anatoly Khaytovich <anatolyuss@gmail.com>
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

/**
 * Update consistency state.
 *
 * @param {Conversion} self
 * @param {Number}     dataPoolId
 *
 * @returns {Promise}
 */
function updateConsistencyState(self, dataPoolId) {
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
 * Get consistency state.
 *
 * @param {Conversion} self
 * @param {Number}     dataPoolId
 *
 * @returns {Promise}
 */
function getConsistencyState(self, dataPoolId) {
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
 * Enforce consistency before processing a chunk of data.
 * Ensure there are no any data duplications.
 * In case of normal execution - it is a good practice.
 * In case of rerunning nmig after unexpected failure - it is absolutely mandatory.
 *
 * @param {Conversion} self
 * @param {Number}     chunkId
 *
 * @returns {Promise}
 */
module.exports = function(self, chunkId) {
    return new Promise(resolve => {
        getConsistencyState(self, chunkId).then(isStarted => {
            if (isStarted) {
                // Current data chunk runs after a disaster recovery.
                resolve(false);
            } else {
                // Normal migration flow.
                updateConsistencyState(self, chunkId).then(() => resolve(true));
            }
        })
    });
};
