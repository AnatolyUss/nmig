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

const mysql          = require('mysql');
const pg             = require('pg');
const log            = require('./Logger');
const generateError  = require('./ErrorGenerator');
const generateReport = require('./ReportGenerator');

/**
 * Check if both servers are connected.
 * If not, than create connections.
 * Kill current process if can not connect.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = function(self) {
    return new Promise(resolve => {
        const mysqlConnectionPromise = new Promise((mysqlResolve, mysqlReject) => {
            if (!self._mysql) {
                self._sourceConString.connectionLimit = self._maxPoolSizeSource;
                const pool                            = mysql.createPool(self._sourceConString);

                if (pool) {
                    self._mysql = pool;
                    mysqlResolve();
                } else {
                    log(self, '\t--[connect] Cannot connect to MySQL server...');
                    mysqlReject();
                }
            } else {
                mysqlResolve();
            }
        });

        const pgConnectionPromise = new Promise((pgResolve, pgReject) => {
            if (!self._pg) {
                self._targetConString.max = self._maxPoolSizeTarget;
                const pool                = new pg.Pool(self._targetConString);

                if (pool) {
                    self._pg = pool;

                    self._pg.on('error', error => {
                        const message = 'Cannot connect to PostgreSQL server...\n' + error.message + '\n' + error.stack;
                        generateError(self, message);
                        generateReport(self, message);
                    });

                    pgResolve();
                } else {
                    log(self, '\t--[connect] Cannot connect to PostgreSQL server...');
                    pgReject();
                }
            } else {
                pgResolve();
            }
        });

        Promise.all([mysqlConnectionPromise, pgConnectionPromise]).then(
            () => resolve(),
            () => generateReport(self, 'NMIG just failed to establish db-connections.')
        );
    });
};
