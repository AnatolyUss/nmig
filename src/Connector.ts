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
import { Pool as MySQLPool } from 'mysql';
import * as mysql from 'mysql';
import { Pool as PgPool } from 'pg';
import log from './Logger';
import generateError from './ErrorGenerator';
import generateReport from './ReportGenerator';
import Conversion from './Conversion';

/**
 * Check if both servers are connected.
 * If not, than create connections.
 * Kill current process if can not connect.
 */
export default (conversion: Conversion): Promise<Conversion> => {
    return new Promise(resolve => {
        const mysqlConnectionPromise: Promise<void> = new Promise((mysqlResolve, mysqlReject) => {
            if (!conversion._mysql) {
                conversion._sourceConString.connectionLimit = conversion._maxDbConnectionPoolSize;
                conversion._sourceConString.multipleStatements = true;
                const pool: MySQLPool = mysql.createPool(conversion._sourceConString);

                if (pool) {
                    conversion._mysql = pool;
                    mysqlResolve();
                } else {
                    log(conversion, '\t--[connect] Cannot connect to MySQL server...');
                    mysqlReject();
                }
            } else {
                mysqlResolve();
            }
        });

        const pgConnectionPromise: Promise<void> = new Promise((pgResolve, pgReject) => {
            if (!conversion._pg) {
                conversion._targetConString.max = conversion._maxDbConnectionPoolSize;
                const pool: PgPool = new PgPool(conversion._targetConString);

                if (pool) {
                    conversion._pg = pool;

                    conversion._pg.on('error', (error: Error) => {
                        const message: string = `Cannot connect to PostgreSQL server...\n' ${ error.message }\n${ error.stack }`;
                        generateError(conversion, message);
                        generateReport(conversion, message);
                    });

                    pgResolve();
                } else {
                    log(conversion, '\t--[connect] Cannot connect to PostgreSQL server...');
                    pgReject();
                }
            } else {
                pgResolve();
            }
        });

         Promise.all([mysqlConnectionPromise, pgConnectionPromise])
            .then(() => resolve(conversion))
            .catch(() => process.exit());
    });
}
