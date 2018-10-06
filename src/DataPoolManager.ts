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
 * Creates the "{schema}"."data_pool_{self._schema + self._mySqlDbName}" temporary table.
 */
export async function createDataPoolTable(conversion: Conversion): Promise<Conversion> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const table: string = `"${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"`;
    const sql: string = `CREATE TABLE IF NOT EXISTS ${ table }
        ("id" BIGSERIAL, "json" TEXT, "is_started" BOOLEAN, "size_in_mb" DOUBLE PRECISION);`;

    await dbAccess.query('DataPoolManager::createDataPoolTable', sql, DBVendors.PG, true, false);
    log(conversion, `\t--[DataPoolManager.createDataPoolTable] table ${ table } is created...`);
    return conversion;
}

/**
 * Drops the "{schema}"."data_pool_{self._schema + self._mySqlDbName}" temporary table.
 */
export async function dropDataPoolTable(conversion: Conversion): Promise<void> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const table: string = `"${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"`;
    const sql: string = `DROP TABLE ${ table };`;
    await dbAccess.query('DataPoolManager::dropDataPoolTable', sql, DBVendors.PG, false, false);
    log(conversion, `\t--[DataPoolManager.dropDataPoolTable] table ${ table } is dropped...`);
}

/**
 * Reads temporary table, and generates Data-pool.
 */
export async function readDataPool(conversion: Conversion): Promise<Conversion> {
    const dbAccess: DBAccess = new DBAccess(conversion);
    const table: string = `"${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"`;
    const sql: string = `SELECT id AS id, json AS json, size_in_mb AS size_in_mb FROM ${ table } ORDER BY size_in_mb DESC;`;
    const result: DBAccessQueryResult = await dbAccess.query('DataPoolManager::dropDataPoolTable', sql, DBVendors.PG, true, false);

    result.data.rows.forEach((row: any) => {
        const obj: any = JSON.parse(row.json);
        obj._id =  row.id;
        obj._size_in_mb = +row.size_in_mb;
        obj._processed = false;
        conversion._dataPool.push(obj);
    });

    log(conversion, '\t--[DataPoolManager.readDataPool] Data-Pool is loaded...');
    return conversion;
}
