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

import Conversion from './conversion';
import { log, generateError } from './fs-ops';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './types';

export default class DbAccess {
  /**
   * Ensures MySQL connection pool existence.
   */
  private static _getMysqlConnection = async (conversion: Conversion): Promise<void> => {
    if (!conversion._mysql) {
      conversion._sourceConString.connectionLimit = conversion._maxEachDbConnectionPoolSize;
      conversion._sourceConString.multipleStatements = true;
      const pool: MySQLPool = createMySQLPool(conversion._sourceConString);

      if (!pool) {
        await generateError(
          conversion,
          '\t--[getMysqlConnection] Cannot connect to MySQL server...',
        );

        process.exit(1);
      }

      conversion._mysql = pool;
    }
  };

  /**
   * Ensures PostgreSQL connection pool existence.
   */
  private static _getPgConnection = async (conversion: Conversion): Promise<void> => {
    if (!conversion._pg) {
      conversion._targetConString.max = conversion._maxEachDbConnectionPoolSize;
      const pool: PgPool = new PgPool(conversion._targetConString);

      if (!pool) {
        await generateError(
          conversion,
          '\t--[getPgConnection] Cannot connect to PostgreSQL server...',
        );

        process.exit(1);
      }

      conversion._pg = pool;

      conversion._pg.on('error', async (error: Error): Promise<void> => {
        await generateError(
          conversion,
          `Cannot connect to PostgreSQL server...\n' ${error.message}\n${error.stack}`,
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
          conversion._mysql.end(async (error: NodeJS.ErrnoException | null): Promise<void> => {
            if (error) {
              await generateError(conversion, `\t--[DBAccess::closeConnectionPools] ${error}`);
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
          await generateError(conversion, `\t--[DBAccess::closeConnectionPools] ${error}`);
        }
      }
    };

    await Promise.all([closeMySqlConnections, closePgConnections]);
    await log(conversion, `\t--[DBAccess::closeConnectionPools] Closed all DB connections.`);
    return conversion;
  };

  /**
   * Obtains PoolConnection instance.
   */
  public static getMysqlClient = (conversion: Conversion): Promise<PoolConnection> => {
    return new Promise<PoolConnection>(async (resolve, reject): Promise<void> => {
      await DbAccess._getMysqlConnection(conversion);

      const _resolvePromise = (err: NodeJS.ErrnoException | null, connection: PoolConnection) =>
        err ? reject(err) : resolve(connection);

      (conversion._mysql as MySQLPool).getConnection(_resolvePromise);
    });
  };

  /**
   * Obtains PoolClient instance.
   */
  public static getPgClient = async (conversion: Conversion): Promise<PoolClient> => {
    await DbAccess._getPgConnection(conversion);
    return await (conversion._pg as PgPool).connect();
  };

  /**
   * Releases MySQL or PostgreSQL connection back to appropriate pool.
   */
  public static releaseDbClient = async (
    conversion: Conversion,
    dbClient?: PoolConnection | PoolClient,
  ): Promise<void> => {
    try {
      (dbClient as PoolConnection | PoolClient).release();
      dbClient = undefined;
    } catch (error) {
      await generateError(conversion, `\t--[DBAccess::releaseDbClient] ${error}`);
    }
  };

  /**
   * Checks if there are no more queries to be sent using current client.
   * In such case the client should be released.
   */
  private static _releaseDbClientIfNecessary = async (
    conversion: Conversion,
    client: PoolConnection | PoolClient,
    shouldHoldClient: boolean,
  ): Promise<void> => {
    if (!shouldHoldClient) {
      await this.releaseDbClient(conversion, client);
    }
  };

  /**
   * Sends given SQL query to specified DB.
   * Performs appropriate actions (requesting/releasing client) against target connections pool.
   */
  public static query = async (queryParams: DBAccessQueryParams): Promise<DBAccessQueryResult> => {
    let {
      conversion, // eslint-disable-line prefer-const
      caller, // eslint-disable-line prefer-const
      sql, // eslint-disable-line prefer-const
      vendor, // eslint-disable-line prefer-const
      processExitOnError, // eslint-disable-line prefer-const
      shouldReturnClient, // eslint-disable-line prefer-const
      client,
      bindings, // eslint-disable-line prefer-const
    } = queryParams;

    // Checks if there is an available client.
    if (!client) {
      try {
        // Client is undefined.
        // It must be requested from the connections pool.
        client =
          vendor === DBVendors.PG
            ? await DbAccess.getPgClient(conversion)
            : await DbAccess.getMysqlClient(conversion);
      } catch (error) {
        // An error occurred when tried to obtain a client from one of pools.
        await generateError(conversion, `\t--[${caller}] ${error}`, sql);
        return processExitOnError ? process.exit(1) : { client, error };
      }
    }

    if (vendor === DBVendors.PG) {
      return await DbAccess._queryPG(
        conversion,
        caller,
        sql,
        processExitOnError,
        shouldReturnClient,
        client as PoolClient,
        bindings,
      );
    }

    return await DbAccess._queryMySQL(
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

      (client as PoolConnection).query(
        sql,
        async (error: NodeJS.ErrnoException | null, data: any): Promise<void> => {
          await DbAccess._releaseDbClientIfNecessary(
            conversion,
            client as PoolConnection,
            shouldReturnClient,
          );

          if (error) {
            await generateError(conversion, `\t--[${caller}] ${error}`, sql);
            return processExitOnError ? process.exit(1) : reject({ client, error });
          }

          return resolve({ client, data });
        },
      );
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
      await generateError(conversion, `\t--[${caller}] ${error}`, sql);
      return processExitOnError ? process.exit(1) : { client, error };
    } finally {
      // Sets the client undefined.
      await DbAccess._releaseDbClientIfNecessary(
        conversion,
        client as PoolClient,
        shouldReturnClient,
      );
    }
  };
}
