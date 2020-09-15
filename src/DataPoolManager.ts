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
import IDBAccessQueryParams from './IDBAccessQueryParams';

/**
 * Returns the data pool table name.
 */
export const getDataPoolTableName = (conversion: Conversion): string => {
    return `"${ conversion._schema }"."data_pool_${ conversion._schema }${ conversion._mySqlDbName }"`;
};

/**
 * Creates the "{schema}"."data_pool_{self._schema + self._mySqlDbName}" temporary table.
 */
export const createDataPoolTable = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'DataPoolManager::createDataPoolTable';
    const table: string = getDataPoolTableName(conversion);
    const sql: string = `CREATE TABLE IF NOT EXISTS ${ table }("id" BIGSERIAL, "metadata" TEXT);`;
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false
    };

    await DBAccess.query(params);
    log(conversion, `\t--[${ logTitle }] table ${ table } is created...`);
    return conversion;
};

/**
 * Drops the "{schema}"."data_pool_{self._schema + self._mySqlDbName}" temporary table.
 */
export const dropDataPoolTable = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'DataPoolManager::dropDataPoolTable';
    const table: string = getDataPoolTableName(conversion);
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: `DROP TABLE ${ table };`,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false
    };

    await DBAccess.query(params);
    log(conversion, `\t--[${ logTitle }] table ${ table } is dropped...`);
    return conversion;
};

/**
 * Reads temporary table, and generates Data-pool.
 */
export const readDataPool = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'DataPoolManager::readDataPool';
    const table: string = getDataPoolTableName(conversion);
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: `SELECT id AS id, metadata AS metadata FROM ${ table };`,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);

    result.data.rows.forEach((row: any) => {
        const obj: any = JSON.parse(row.metadata);
        obj._id =  row.id;
        conversion._dataPool.push(obj);
    });

    log(conversion, `\t--[${logTitle}] Data-Pool is loaded...`);
    return conversion;
};
