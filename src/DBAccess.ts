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
import * as mysql from 'mysql';
import { Pool as MySQLPool, PoolConnection, MysqlError } from 'mysql';
import { Pool as PgPool, PoolClient, QueryResult } from 'pg';
import generateError from './ErrorGenerator';
import Conversion from './Conversion';
import generateReport from './ReportGenerator';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';

export default class DBAccess {
    /**
     * Conversion instance.
     */
    private readonly _conversion: Conversion;

    /**
     * DBAccess constructor.
     */
    public constructor(conversion: Conversion) {
        this._conversion = conversion;
    }

    /**
     * Ensures MySQL connection pool existence.
     */
    private _getMysqlConnection(): void {
        if (!this._conversion._mysql) {
            this._conversion._sourceConString.connectionLimit = this._conversion._maxDbConnectionPoolSize;
            this._conversion._sourceConString.multipleStatements = true;
            const pool: MySQLPool = mysql.createPool(this._conversion._sourceConString);

            if (!pool) {
                generateError(this._conversion, '\t--[getMysqlConnection] Cannot connect to MySQL server...');
                process.exit();
            }

            this._conversion._mysql = pool;
        }
    }

    /**
     * Ensures PostgreSQL connection pool existence.
     */
    private _getPgConnection(): void {
        if (!this._conversion._pg) {
            this._conversion._targetConString.max = this._conversion._maxDbConnectionPoolSize;
            const pool: PgPool = new PgPool(this._conversion._targetConString);

            if (!pool) {
                generateError(this._conversion, '\t--[getPgConnection] Cannot connect to PostgreSQL server...');
                process.exit();
            }

            this._conversion._pg = pool;

            this._conversion._pg.on('error', (error: Error) => {
                const message: string = `Cannot connect to PostgreSQL server...\n' ${ error.message }\n${ error.stack }`;
                generateError(this._conversion, message);
                generateReport(this._conversion, message);
            });
        }
    }

    /**
     * Obtains PoolConnection instance.
     */
    public getMysqlClient(): Promise<PoolConnection> {
        this._getMysqlConnection();

        return new Promise<PoolConnection>((resolve, reject) => {
            (<MySQLPool>this._conversion._mysql).getConnection((err: MysqlError|null, connection: PoolConnection) => {
                return err ? reject(err) : resolve(connection);
            });
        });
    }

    /**
     * Obtains PoolClient instance.
     */
    public getPgClient(): Promise<PoolClient> {
        this._getPgConnection();
        return (<PgPool>this._conversion._pg).connect();
    }

    /**
     * Runs a query on the first available idle client and returns its result.
     * Note, the pool does the acquiring and releasing of the client internally.
     */
    public runPgPoolQuery(sql: string): Promise<QueryResult> {
        this._getPgConnection();
        return (<PgPool>this._conversion._pg).query(sql);
    }

    /**
     * Releases MySQL or PostgreSQL connection back to appropriate pool.
     */
    public releaseDbClient(dbClient?: PoolConnection|PoolClient): void {
        try {
            (<PoolConnection|PoolClient>dbClient).release();
            dbClient = undefined;
        } catch (error) {
            generateError(this._conversion, `\t--[DBAccess::releaseDbClient] ${ error }`);
        }
    }

    /**
     * Checks if there are no more queries to be sent using current client.
     * In such case the client should be released.
     */
    private _releaseDbClientIfNecessary(client: PoolConnection|PoolClient, shouldHoldClient: boolean): void {
        if (!shouldHoldClient) {
            this.releaseDbClient(client);
        }
    }

    /**
     * Sends given SQL query to specified DB.
     * Performs appropriate actions (requesting/releasing client) against target connections pool.
     */
    public async query(
        caller: string,
        sql: string,
        vendor: DBVendors,
        processExitOnError: boolean,
        shouldReturnClient: boolean,
        client?: PoolConnection|PoolClient,
        bindings?: any[]
    ): Promise<DBAccessQueryResult> {
        // Checks if there is an available client.
        if (!client) {
            try {
                // Client is undefined.
                // It must be requested from the connections pool.
                client = vendor === DBVendors.PG ? await this.getPgClient() : await this.getMysqlClient();
            } catch (error) {
                // An error occurred when tried to obtain a client from one of pools.
                generateError(this._conversion, `\t--[${ caller }] ${ error }`, sql);
                return processExitOnError ? process.exit() : { client: client, data: undefined, error: error };
            }
        }

        return vendor === DBVendors.PG
            ? this._queryPG(caller, sql, processExitOnError, shouldReturnClient, (<PoolClient>client), bindings)
            : this._queryMySQL(caller, sql, processExitOnError, shouldReturnClient, (<PoolConnection>client), bindings);
    }

    /**
     * Sends given SQL query to MySQL.
     */
    private _queryMySQL(
        caller: string,
        sql: string,
        processExitOnError: boolean,
        shouldReturnClient: boolean,
        client?: PoolConnection,
        bindings?: any[]
    ): Promise<DBAccessQueryResult> {
        return new Promise<DBAccessQueryResult>((resolve, reject) => {
            if (Array.isArray(bindings)) {
                sql = (<PoolConnection>client).format(sql, bindings);
            }

            (<PoolConnection>client).query(sql, (error: MysqlError|null, data: any) => {
                this._releaseDbClientIfNecessary((<PoolConnection>client), shouldReturnClient);

                if (error) {
                    generateError(this._conversion, `\t--[${ caller }] ${ error }`, sql);
                    return processExitOnError ? process.exit() : reject({ client: client, data: undefined, error: error });
                }

                return resolve({ client: client, data: data, error: undefined });
            });
        });
    }

    /**
     * Sends given SQL query to PostgreSQL.
     */
    private async _queryPG(
        caller: string,
        sql: string,
        processExitOnError: boolean,
        shouldReturnClient: boolean,
        client?: PoolClient,
        bindings?: any[]
    ): Promise<DBAccessQueryResult> {
        try {
            const data: any = Array.isArray(bindings) ? await (<PoolClient>client).query(sql, bindings) : await (<PoolClient>client).query(sql);
            this._releaseDbClientIfNecessary((<PoolClient>client), shouldReturnClient); // Sets the client undefined.
            return { client: client, data: data, error: undefined };
        } catch (error) {
            this._releaseDbClientIfNecessary((<PoolClient>client), shouldReturnClient); // Sets the client undefined.
            generateError(this._conversion, `\t--[${ caller }] ${ error }`, sql);
            return processExitOnError ? process.exit() : { client: client, data: undefined, error: error };
        }
    }
}
