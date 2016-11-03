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

const connect       = require('./Connector');
const log           = require('./Logger');
const generateError = require('./ErrorGenerator');

/**
 * Runs "vacuum full" and "analyze".
 *
 * @param   {Conversion} self
 * @returns {Promise}
 */
module.exports = function(self) {
    return connect(self).then(() => {
        return new Promise(resolve => {
            let vacuumPromises = [];

            for (let i = 0; i < self._tablesToMigrate.length; ++i) {
                if (self._noVacuum.indexOf(self._tablesToMigrate[i]) === -1) {
                    let msg = '\t--[runVacuumFullAndAnalyze] Running "VACUUM FULL and ANALYZE" query for table "'
                            + self._schema + '"."' + self._tablesToMigrate[i] + '"...';

                    log(self, msg);
                    vacuumPromises.push(
                        new Promise(resolveVacuum => {
                            self._pg.connect((error, client, done) => {
                                if (error) {
                                    generateError(self, '\t--[runVacuumFullAndAnalyze] Cannot connect to PostgreSQL server...');
                                    resolveVacuum();
                                } else {
                                    let sql = 'VACUUM (FULL, ANALYZE) "' + self._schema + '"."' + self._tablesToMigrate[i] + '";';
                                    client.query(sql, err => {
                                        done();

                                        if (err) {
                                            generateError(self, '\t--[runVacuumFullAndAnalyze] ' + err, sql);
                                            resolveVacuum();
                                        } else {
                                            let msg2 = '\t--[runVacuumFullAndAnalyze] Table "' + self._schema + '"."' + self._tablesToMigrate[i] + '" is VACUUMed...';
                                            log(self, msg2);
                                            resolveVacuum();
                                        }
                                    });
                                }
                            });
                        })
                    );
                }
            }

            Promise.all(vacuumPromises).then(() => resolve());
        });
    });
};
