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
import DbAccess from './db-access';
import { log } from './fs-ops';
import Conversion from './conversion';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './types';

/**
 * Returns the data pool table name.
 */
export const getDataPoolTableName = (conversion: Conversion): string => {
    return `"${conversion._schema}"."data_pool_${conversion._schema}${conversion._mySqlDbName}"`;
};

/**
 * Creates the "{schema}"."data_pool_{self._schema + self._mySqlDbName}" temporary table.
 */
export const createDataPoolTable = async (conversion: Conversion): Promise<Conversion> => {
    const table: string = getDataPoolTableName(conversion);
    const sql = `CREATE TABLE IF NOT EXISTS ${table}("id" BIGSERIAL, "metadata" TEXT);`;
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: createDataPoolTable.name,
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false,
    };

    await DbAccess.query(params);
    await log(conversion, `\t--[${createDataPoolTable.name}] table ${table} is created...`);
    return conversion;
};

/**
 * Drops the "{schema}"."data_pool_{self._schema + self._mySqlDbName}" temporary table.
 */
export const dropDataPoolTable = async (conversion: Conversion): Promise<Conversion> => {
    const table: string = getDataPoolTableName(conversion);
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: dropDataPoolTable.name,
        sql: `DROP TABLE ${table};`,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false,
    };

    await DbAccess.query(params);
    await log(conversion, `\t--[${dropDataPoolTable.name}] table ${table} is dropped...`);
    return conversion;
};

/**
 * Reads temporary table, and generates Data-pool.
 */
export const readDataPool = async (conversion: Conversion): Promise<Conversion> => {
    const table: string = getDataPoolTableName(conversion);
    const params: DBAccessQueryParams = {
        conversion: conversion,
        caller: readDataPool.name,
        sql: `SELECT id AS id, metadata AS metadata FROM ${table};`,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false,
    };

    const result: DBAccessQueryResult = await DbAccess.query(params);

    result.data.rows.forEach((row: Record<string, any>): void => {
        const obj = JSON.parse(row.metadata) as Record<string, any>;
        obj._id = row.id;
        conversion._dataPool.push(obj);
    });

    await log(conversion, `\t--[${readDataPool.name}] Data-Pool is loaded...`);
    return conversion;
};
