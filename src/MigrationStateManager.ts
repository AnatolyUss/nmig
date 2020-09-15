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
import IDBAccessQueryParams from './IDBAccessQueryParams';
import DBVendors from './DBVendors';
import { log } from './FsOps';
import Conversion from './Conversion';

/**
 * Returns the state logs table name.
 */
export const getStateLogsTableName = (conversion: Conversion, getRowName: boolean = false): string => {
    const rowName: string = `state_logs_${ conversion._schema }${ conversion._mySqlDbName }`;
    return getRowName ? rowName : `"${ conversion._schema }"."${ rowName }"`;
};

/**
 * Retrieves state-log.
 */
export const get = async (conversion: Conversion, param: string): Promise<boolean> => {
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'MigrationStateManager::get',
        sql: `SELECT ${ param } FROM ${ getStateLogsTableName(conversion) };`,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);
    return result.data.rows[0][param];
};

/**
 * Updates the state-log.
 */
export const set = async (conversion: Conversion, ...states: string[]): Promise<void> => {
    const statesSql: string = states.map((state: string) => `${ state } = TRUE`).join(',');
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'MigrationStateManager::set',
        sql: `UPDATE ${ getStateLogsTableName(conversion) } SET ${ statesSql };`,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: false
    };

    await DBAccess.query(params);
};

/**
 * Creates the "{schema}"."state_logs_{self._schema + self._mySqlDbName}" temporary table.
 */
export const createStateLogsTable = async (conversion: Conversion): Promise<Conversion> => {
    const logTitle: string = 'MigrationStateManager::createStateLogsTable';
    const sql: string = `CREATE TABLE IF NOT EXISTS ${ getStateLogsTableName(conversion) }(
        "tables_loaded" BOOLEAN, "per_table_constraints_loaded" BOOLEAN, "foreign_keys_loaded" BOOLEAN, "views_loaded" BOOLEAN);`;

    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: logTitle,
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: true
    };

    let result: DBAccessQueryResult = await DBAccess.query(params);

    params.sql = `SELECT COUNT(1) AS cnt FROM ${ getStateLogsTableName(conversion) };`;
    params.client = result.client;
    result = await DBAccess.query(params);

    if (+result.data.rows[0].cnt === 0) {
        params.sql = `INSERT INTO ${ getStateLogsTableName(conversion) } VALUES (FALSE, FALSE, FALSE, FALSE);`;
        params.client = result.client; // !!!Notice, this line is not a mistake.
        params.shouldReturnClient = false;
        await DBAccess.query(params);
        return conversion;
    }

    const msg: string = `\t--[${ logTitle }] table ${ getStateLogsTableName(conversion) } is created...`;
    log(conversion, msg);
    return conversion;
};

/**
 * Drop the "{schema}"."state_logs_{self._schema + self._mySqlDbName}" temporary table.
 */
export const dropStateLogsTable = async (conversion: Conversion): Promise<Conversion> => {
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'MigrationStateManager::dropStateLogsTable',
        sql: `DROP TABLE ${ getStateLogsTableName(conversion) };`,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false
    };

    await DBAccess.query(params);
    return conversion;
};
