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
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import DBVendors from './DBVendors';
import { log } from './FsOps';
import Conversion from './Conversion';

/**
 * Retrieves state-log.
 */
export async function get(conversion: Conversion, param: string): Promise<boolean> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const sql: string = `SELECT ${ param } FROM "${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }";`;
    const result: DBAccessQueryResult = await dbAccess.query('MigrationStateManager::get', sql, DBVendors.PG, true, false);
    return result.data.rows[0][param];
}

/**
 * Updates the state-log.
 */
export async function set(conversion: Conversion, param: string): Promise<void> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const sql: string = `UPDATE "${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }" SET ${ param } = TRUE;`;
    await dbAccess.query('MigrationStateManager::set', sql, DBVendors.PG, true, false);
}

/**
 * Creates the "{schema}"."state_logs_{self._schema + self._mySqlDbName}" temporary table.
 */
export async function createStateLogsTable(conversion: Conversion): Promise<Conversion> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    let sql: string = `CREATE TABLE IF NOT EXISTS "${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }"(
        "tables_loaded" BOOLEAN, "per_table_constraints_loaded" BOOLEAN, "foreign_keys_loaded" BOOLEAN, "views_loaded" BOOLEAN);`;

    let result: DBAccessQueryResult = await dbAccess.query('MigrationStateManager::createStateLogsTable', sql, DBVendors.PG, true, true);
    sql = `SELECT COUNT(1) AS cnt FROM "${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }";`;
    result = await dbAccess.query('MigrationStateManager::createStateLogsTable', sql, DBVendors.PG, true, true, result.client);

    if (+result.data.rows[0].cnt === 0) {
        sql = `INSERT INTO "${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }" VALUES (FALSE, FALSE, FALSE, FALSE);`;
        await await dbAccess.query('MigrationStateManager::createStateLogsTable', sql, DBVendors.PG, true, false, result.client);
        return conversion;
    }

    const msg: string = '\t--[MigrationStateManager::createStateLogsTable] table ' +
        '"${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }" is created...';

    log(conversion, msg);
    return conversion;
}

/**
 * Drop the "{schema}"."state_logs_{self._schema + self._mySqlDbName}" temporary table.
 */
export async function dropStateLogsTable(conversion: Conversion): Promise<void> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const sql: string = `DROP TABLE "${ conversion._schema }"."state_logs_${ conversion._schema }${ conversion._mySqlDbName }";`;
    await dbAccess.query('MigrationStateManager::dropStateLogsTable', sql, DBVendors.PG, false, false);
}
