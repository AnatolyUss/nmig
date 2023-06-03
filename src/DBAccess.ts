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
import { Pool as MySQLPool, createPool as createMySQLPool, PoolConnection } from 'mysql2';
import { Pool as PgPool, PoolClient } from 'pg';

import { log, generateError } from './FsOps';
import Conversion from './Conversion';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './Types';

export default class DBAccess {
    /**
     * Ensures MySQL connection pool existence.
     */
    private static _getMysqlConnection = (conversion: Conversion): void => {
        if (!conversion._mysql) {
            conversion._sourceConString.connectionLimit = conversion._maxEachDbConnectionPoolSize;
            conversion._sourceConString.multipleStatements = true;
            const pool: MySQLPool = createMySQLPool(conversion._sourceConString);

            if (!pool) {
                generateError(conversion, '\t--[getMysqlConnection] Cannot connect to MySQL server...');
                process.exit(1);
            }

            conversion._mysql = pool;
        }
    };

    /**
     * Ensures PostgreSQL connection pool existence.
     */
    private static _getPgConnection = (conversion: Conversion): void => {
        if (!conversion._pg) {
            conversion._targetConString.max = conversion._maxEachDbConnectionPoolSize;
            const pool: PgPool = new PgPool(conversion._targetConString);

            if (!pool) {
                generateError(conversion, '\t--[getPgConnection] Cannot connect to PostgreSQL server...');
                process.exit(1);
            }

            conversion._pg = pool;

            conversion._pg.on('error', (error: Error): void => {
                generateError(
                    conversion,
                    `Cannot connect to PostgreSQL server...\n' ${ error.message }\n${ error.stack }`,
                );
            });
        }
    };

    /**
     * Closes both connection-pools.
     */
    public static closeConnectionPools = async (conversion: Conversion): Promise<Conversion> => {
        const closeMySqlConnections = (): Promise<void> => {
            return new Promise<void>(resolve => {
                if (conversion._mysql) {
                    conversion._mysql.end((error: NodeJS.ErrnoException | null): void => {
                        if (error) {
                            generateError(conversion, `\t--[DBAccess::closeConnectionPools] ${ error }`);
                        }

                        return resolve();
                    });
                }

                resolve();
            });
        };

        const closePgConnections = async (): Promise<void> => {
            if (conversion._pg) {
                try {
                    await conversion._pg.end();
                } catch (error) {
                    generateError(conversion, `\t--[DBAccess::closeConnectionPools] ${ error }`);
                }
            }
        };

        await Promise.all([closeMySqlConnections, closePgConnections]);
        log(conversion, `\t--[DBAccess::closeConnectionPools] Closed all DB connections.`);
        return conversion;
    };

    /**
     * Obtains PoolConnection instance.
     */
    public static getMysqlClient = (conversion: Conversion): Promise<PoolConnection> => {
        return new Promise<PoolConnection>((resolve, reject) => {
            DBAccess._getMysqlConnection(conversion);

            const _resolvePromise = (err: NodeJS.ErrnoException | null, connection: PoolConnection) => {
                return err ? reject(err) : resolve(connection);
            };

            (conversion._mysql as MySQLPool).getConnection(_resolvePromise);
        });
    };

    /**
     * Obtains PoolClient instance.
     */
    public static getPgClient = async (conversion: Conversion): Promise<PoolClient> => {
        DBAccess._getPgConnection(conversion);
        return await (conversion._pg as PgPool).connect();
    };

    /**
     * Releases MySQL or PostgreSQL connection back to appropriate pool.
     */
    public static releaseDbClient = (
        conversion: Conversion,
        dbClient?: PoolConnection | PoolClient,
    ): void => {
        try {
            (dbClient as PoolConnection | PoolClient).release();
            dbClient = undefined;
        } catch (error) {
            generateError(conversion, `\t--[DBAccess::releaseDbClient] ${ error }`);
        }
    };

    /**
     * Checks if there are no more queries to be sent using current client.
     * In such case the client should be released.
     */
    private static _releaseDbClientIfNecessary = (
        conversion: Conversion,
        client: PoolConnection | PoolClient,
        shouldHoldClient: boolean,
    ): void => {
        if (!shouldHoldClient) {
            this.releaseDbClient(conversion, client);
        }
    };

    /**
     * Sends given SQL query to specified DB.
     * Performs appropriate actions (requesting/releasing client) against target connections pool.
     */
    public static query = async (queryParams: DBAccessQueryParams): Promise<DBAccessQueryResult> => {
        let {
            conversion,
            caller,
            sql,
            vendor,
            processExitOnError,
            shouldReturnClient,
            client,
            bindings,
        } = queryParams;

        // Checks if there is an available client.
        if (!client) {
            try {
                // Client is undefined.
                // It must be requested from the connections pool.
                client = vendor === DBVendors.PG
                    ? await DBAccess.getPgClient(conversion)
                    : await DBAccess.getMysqlClient(conversion);
            } catch (error) {
                // An error occurred when tried to obtain a client from one of pools.
                generateError(conversion, `\t--[${ caller }] ${ error }`, sql);
                return processExitOnError ? process.exit(1) : { client, error };
            }
        }

        if (vendor === DBVendors.PG) {
            return await DBAccess._queryPG(
                conversion,
                caller,
                sql,
                processExitOnError,
                shouldReturnClient,
                client as PoolClient,
                bindings,
            );
        }

        return await DBAccess._queryMySQL(
            conversion,
            caller,
            sql,
            processExitOnError,
            shouldReturnClient,
            client as PoolConnection,
            bindings,
        );
    };

    /**
     * Sends given SQL query to MySQL.
     */
    private static _queryMySQL = (
        conversion: Conversion,
        caller: string,
        sql: string,
        processExitOnError: boolean,
        shouldReturnClient: boolean,
        client?: PoolConnection,
        bindings?: any[],
    ): Promise<DBAccessQueryResult> => {
        return new Promise<DBAccessQueryResult>((resolve, reject): void => {
            if (Array.isArray(bindings)) {
                sql = (client as PoolConnection).format(sql, bindings);
            }

            (client as PoolConnection).query(sql, (error: NodeJS.ErrnoException | null, data: any) => {
                DBAccess._releaseDbClientIfNecessary(conversion, (<PoolConnection>client), shouldReturnClient);

                if (error) {
                    generateError(conversion, `\t--[${ caller }] ${ error }`, sql);
                    return processExitOnError ? process.exit(1) : reject({ client, error });
                }

                return resolve({ client, data });
            });
        });
    };

    /**
     * Sends given SQL query to PostgreSQL.
     */
    private static _queryPG = async (
        conversion: Conversion,
        caller: string,
        sql: string,
        processExitOnError: boolean,
        shouldReturnClient: boolean,
        client?: PoolClient,
        bindings?: any[],
    ): Promise<DBAccessQueryResult> => {
        try {
            const data: any = Array.isArray(bindings)
                ? await (client as PoolClient).query(sql, bindings)
                : await (client as PoolClient).query(sql);

            return { client, data };
        } catch (error) {
            generateError(conversion, `\t--[${ caller }] ${ error }`, sql);
            return processExitOnError ? process.exit(1) : { client, error };
        } finally {
            // Sets the client undefined.
            DBAccess._releaseDbClientIfNecessary(conversion, client as PoolClient, shouldReturnClient);
        }
    };
}
