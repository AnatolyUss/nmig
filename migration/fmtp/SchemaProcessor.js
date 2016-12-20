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

const connect       = require('./Connector');
const generateError = require('./ErrorGenerator');

/**
 * Create a new database schema.
 * Insure a uniqueness of a new schema name.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = function(self) {
    return connect(self).then(() => {
        return new Promise((resolve, reject) => {
            self._pg.connect((error, client, done) => {
                if (error) {
                    generateError(self, '\t--[createSchema] Cannot connect to PostgreSQL server...\n' + error);
                    reject();
                } else {
                    let sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + self._schema + "';";
                    client.query(sql, (err, result) => {
                        if (err) {
                            done();
                            generateError(self, '\t--[createSchema] ' + err, sql);
                            reject();
                        } else if (result.rows.length === 0) {
                            sql = 'CREATE SCHEMA "' + self._schema + '";';
                            client.query(sql, err => {
                                done();

                                if (err) {
                                    generateError(self, '\t--[createSchema] ' + err, sql);
                                    reject();
                                } else {
                                    resolve();
                                }
                            });
                        } else {
                            resolve();
                        }
                    });
                }
            });
        });
    });
};
