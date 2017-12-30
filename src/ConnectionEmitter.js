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

const mysql         = require('mysql');
const pg            = require('pg');
const generateError = require('./ErrorGenerator');

module.exports = class ConnectionEmitter {

    /**
     * ConnectionEmitter constructor.
     *
     * @param {Conversion} conversion
     */
    constructor(conversion) {
        this._conversion = conversion;
    }

    /**
     * Ensure MySQL connection pool existence.
     *
     * @returns {undefined}
     */
    _getMysqlConnection() {
        if (!this._conversion._mysql) {
            this._conversion._sourceConString.connectionLimit = this._conversion._maxDbConnectionPoolSize;
            const pool                                        = mysql.createPool(this._conversion._sourceConString);

            if (!pool) {
                generateError(this._conversion, '\t--[getMysqlConnection] Cannot connect to MySQL server...');
                process.exit();
            }

            this._conversion._mysql = pool;
        }
    }

    /**
     * Ensure PostgreSQL connection pool existence.
     *
     * @returns {undefined}
     */
    _getPgConnection() {
        if (!this._conversion._pg) {
            this._conversion._targetConString.max = this._conversion._maxDbConnectionPoolSize;
            const pool                            = new pg.Pool(this._conversion._targetConString);

            if (!pool) {
                generateError(this._conversion, '\t--[getPgConnection] Cannot connect to PostgreSQL server...');
                process.exit();
            }

            this._conversion._pg = pool;

            this._conversion._pg.on('error', error => {
                const message = `Cannot connect to PostgreSQL server...\n${ error.message }\n${ error.stack }`;
                generateError(this._conversion, message);
                process.exit();
            });
        }
    }

    /**
     * Obtain Connection instance.
     *
     * @returns {Promise<Connection>}
     */
    async getMysqlClient() {
        try {
            this._getMysqlConnection();
            return await this._conversion._mysql.getConnection();
        } catch (error) {
            generateError(this._conversion, `\t--[getMysqlClient] Cannot connect to PostgreSQL server...\n${ error }`);
            process.exit();
        }
    }

    /**
     * Obtain pg.Client instance.
     *
     * @returns {Promise<pg.Client>}
     */
    async getPgClient() {
        try {
            this._getPgConnection();
            return await this._conversion._pg.connect();
        } catch (error) {
            generateError(this._conversion, `\t--[getPgClient] Cannot connect to PostgreSQL server...\n${ error }`);
            process.exit();
        }
    }

    /**
     * Runs a query on the first available idle client and returns its result.
     * Note, the pool does the acquiring and releasing of the client internally.
     *
     * @param {String} sql
     *
     * @returns {Promise<pg.Result>}
     */
    async runPgPoolQuery(sql) {
        try {
            this._getPgConnection();
            return await this._conversion._pg.query(sql);
        } catch (error) {
            generateError(this._conversion, `\t--[pgPoolQuery] Cannot connect to PostgreSQL server...\n${ error }`);
            process.exit();
        }
    }

    /**
     * Releases MySQL Client back to the pool.
     *
     * @param {Connection} mysqlClient
     *
     * @returns {undefined}
     */
    releaseMysqlClient(mysqlClient) {
        mysqlClient.release();
    }

    /**
     * Releases pg.Client back to the pool.
     *
     * @param {pg.Client} pgClient
     *
     * @returns {undefined}
     */
    releasePgClient(pgClient) {
        pgClient.release();
    }
};
