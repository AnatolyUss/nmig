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
import { MysqlError, Pool as MySQLPool, PoolConnection } from 'mysql';
import { Pool as PgPool, PoolClient } from 'pg';
import { log, generateError } from './FsOps';
import Conversion from './Conversion';
import DBVendors from './DBVendors';
import DBAccessQueryResult from './DBAccessQueryResult';
import IDBAccessQueryParams from './IDBAccessQueryParams';

export default class DBAccess {
    /**
     * Ensures MySQL connection pool existence.
     */
    private static async _getMysqlConnection(conversion: Conversion): Promise<void> {
        if (!conversion._mysql) {
            conversion._sourceConString.connectionLimit = conversion._maxEachDbConnectionPoolSize;
            conversion._sourceConString.multipleStatements = true;
            const pool: MySQLPool = mysql.createPool(conversion._sourceConString);

            if (!pool) {
                await generateError(conversion, '\t--[getMysqlConnection] Cannot connect to MySQL server...');
                process.exit(1);
            }

            conversion._mysql = pool;
        }
    }

    /**
     * Ensures PostgreSQL connection pool existence.
     */
    private static async _getPgConnection(conversion: Conversion): Promise<void> {
        if (!conversion._pg) {
            conversion._targetConString.max = conversion._maxEachDbConnectionPoolSize;
            const pool: PgPool = new PgPool(conversion._targetConString);

            if (!pool) {
                await generateError(conversion, '\t--[getPgConnection] Cannot connect to PostgreSQL server...');
                process.exit(1);
            }

            conversion._pg = pool;

            conversion._pg.on('error', async (error: Error) => {
                const message: string = `Cannot connect to PostgreSQL server...\n' ${ error.message }\n${ error.stack }`;
                await generateError(conversion, message);
            });
        }
    }

    /**
     * Closes both connection-pools.
     */
    public static async closeConnectionPools(conversion: Conversion): Promise<Conversion> {
        const closeMySqlConnections = () => {
            return new Promise(resolve => {
                if (conversion._mysql) {
                    conversion._mysql.end(async error => {
                        if (error) {
                            await generateError(conversion, `\t--[DBAccess::closeConnectionPools] ${ error }`);
                        }

                        return resolve();
                    });
                }

                resolve();
            });
        };

        const closePgConnections = async () => {
            if (conversion._pg) {
                try {
                    await conversion._pg.end();
                } catch (error) {
                    await generateError(conversion, `\t--[DBAccess::closeConnectionPools] ${ error }`);
                }
            }
        };

        await Promise.all([closeMySqlConnections, closePgConnections]);
        log(conversion, `\t--[DBAccess::closeConnectionPools] Closed all DB connections.`);
        return conversion;
    }

    /**
     * Obtains PoolConnection instance.
     */
    public static getMysqlClient(conversion: Conversion): Promise<PoolConnection> {
        return new Promise<PoolConnection>(async (resolve, reject) => {
            await DBAccess._getMysqlConnection(conversion);
            (<MySQLPool>conversion._mysql).getConnection((err: MysqlError | null, connection: PoolConnection) => {
                return err ? reject(err) : resolve(connection);
            });
        });
    }

    /**
     * Obtains PoolClient instance.
     */
    public static async getPgClient(conversion: Conversion): Promise<PoolClient> {
        await DBAccess._getPgConnection(conversion);
        return (<PgPool>conversion._pg).connect();
    }

    /**
     * Releases MySQL or PostgreSQL connection back to appropriate pool.
     */
    public static async releaseDbClient(conversion: Conversion, dbClient?: PoolConnection | PoolClient): Promise<void> {
        try {
            (<PoolConnection | PoolClient>dbClient).release();
            dbClient = undefined;
        } catch (error) {
            await generateError(conversion, `\t--[DBAccess::releaseDbClient] ${ error }`);
        }
    }

    /**
     * Checks if there are no more queries to be sent using current client.
     * In such case the client should be released.
     */
    private static async _releaseDbClientIfNecessary(
        conversion: Conversion,
        client: PoolConnection | PoolClient,
        shouldHoldClient: boolean
    ): Promise<void> {
        if (!shouldHoldClient) {
            await this.releaseDbClient(conversion, client);
        }
    }

    /**
     * Sends given SQL query to specified DB.
     * Performs appropriate actions (requesting/releasing client) against target connections pool.
     */
    public static async query(queryParams: IDBAccessQueryParams): Promise<DBAccessQueryResult> {
        let { conversion, caller, sql, vendor, processExitOnError, shouldReturnClient, client, bindings } = queryParams;

        // Checks if there is an available client.
        if (!client) {
            try {
                // Client is undefined.
                // It must be requested from the connections pool.
                client = vendor === DBVendors.PG ? await DBAccess.getPgClient(conversion) : await DBAccess.getMysqlClient(conversion);
            } catch (error) {
                // An error occurred when tried to obtain a client from one of pools.
                await generateError(conversion, `\t--[${ caller }] ${ error }`, sql);
                return processExitOnError ? process.exit(1) : new DBAccessQueryResult(client, undefined, error);
            }
        }

        return vendor === DBVendors.PG
            ? DBAccess._queryPG(conversion, caller, sql, processExitOnError, shouldReturnClient, (<PoolClient>client), bindings)
            : DBAccess._queryMySQL(conversion, caller, sql, processExitOnError, shouldReturnClient, (<PoolConnection>client), bindings);
    }

    /**
     * Sends given SQL query to MySQL.
     */
    private static _queryMySQL(
        conversion: Conversion,
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

            (<PoolConnection>client).query(sql, async (error: MysqlError | null, data: any) => {
                await DBAccess._releaseDbClientIfNecessary(conversion, (<PoolConnection>client), shouldReturnClient);

                if (error) {
                    await generateError(conversion, `\t--[${ caller }] ${ error }`, sql);
                    return processExitOnError ? process.exit(1) : reject(new DBAccessQueryResult(client, undefined, error));
                }

                return resolve(new DBAccessQueryResult(client, data, undefined));
            });
        });
    }

    /**
     * Sends given SQL query to PostgreSQL.
     */
    private static async _queryPG(
        conversion: Conversion,
        caller: string,
        sql: string,
        processExitOnError: boolean,
        shouldReturnClient: boolean,
        client?: PoolClient,
        bindings?: any[]
    ): Promise<DBAccessQueryResult> {
        try {
            const data: any = Array.isArray(bindings) ? await (<PoolClient>client).query(sql, bindings) : await (<PoolClient>client).query(sql);
            return new DBAccessQueryResult(client, data, undefined);
        } catch (error) {
            await generateError(conversion, `\t--[${ caller }] ${ error }`, sql);
            return processExitOnError ? process.exit(1) : new DBAccessQueryResult(client, undefined, error);
        } finally {
            await DBAccess._releaseDbClientIfNecessary(conversion, (<PoolClient>client), shouldReturnClient); // Sets the client undefined.
        }
    }
}
