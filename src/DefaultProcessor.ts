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
import { log } from './FsOps';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import { mapDataTypes } from './TableProcessor';
import {
    DBAccessQueryParams,
    DBAccessQueryResult,
    DBVendors,
    Table,
} from './Types';

/**
 * Defines which columns of the given table have default value.
 * Sets default values, if needed.
 */
export default async (conversion: Conversion, tableName: string): Promise<void> => {
    const logTitle = 'DefaultProcessor::default';
    const fullTableName = `"${ conversion._schema }"."${ tableName }"`;
    const msg = `\t--[${ logTitle }] Defines default values for table: ${ fullTableName }`;
    await log(conversion, msg, (conversion._dicTables.get(tableName) as Table).tableLogPath);
    const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
    const pgSqlBitTypes = ['bit', 'bit varying'];
    const pgSqlBinaryTypes = ['bytea'];
    const pgSqlNumericTypes = [
        'smallint', 'integer', 'bigint', 'decimal', 'numeric', 'int',
        'real', 'double precision', 'smallserial', 'serial', 'bigserial',
    ];

    const sqlReservedValues = new Map<string, string>([
        ['CURRENT_DATE', 'CURRENT_DATE'],
        ['0000-00-00', "'-INFINITY'"],
        ['CURRENT_TIME', 'CURRENT_TIME'],
        ['00:00:00', "'00:00:00'"],
        ['CURRENT_TIMESTAMP()', 'CURRENT_TIMESTAMP'],
        ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP'],
        ['0000-00-00 00:00:00', "'-INFINITY'"],
        ['LOCALTIME', 'LOCALTIME'],
        ['LOCALTIMESTAMP', 'LOCALTIMESTAMP'],
        ['NULL', 'NULL'],
        ['null', 'NULL'],
        ['UTC_DATE', "(CURRENT_DATE AT TIME ZONE 'UTC')"],
        ['UTC_TIME', "(CURRENT_TIME AT TIME ZONE 'UTC')"],
        ['UTC_TIMESTAMP', "(NOW() AT TIME ZONE 'UTC')"],
    ]);

    const _cb = async (column: any): Promise<void> => {
        const pgSqlDataType: string = mapDataTypes(conversion._dataTypesMap, column.Type);
        const columnName: string = extraConfigProcessor.getColumnName(
            conversion,
            originalTableName,
            column.Field,
            false,
        );

        let sql = `ALTER TABLE ${ fullTableName } ALTER COLUMN "${ columnName }" SET DEFAULT `;
        const isOfBitType = !!(pgSqlBitTypes.find((bitType: string) => pgSqlDataType.startsWith(bitType)));

        if (sqlReservedValues.has(column.Default)) {
            sql += sqlReservedValues.get(column.Default);
        } else if (isOfBitType) {
            sql += `${ column.Default };`; // bit varying
        } else if (pgSqlBinaryTypes.indexOf(pgSqlDataType) !== -1) {
            sql += `'\\x${ column.Default }';`; // bytea
        } else if (pgSqlNumericTypes.indexOf(pgSqlDataType) === -1) {
            sql += `'${ column.Default }';`;
        } else {
            sql += `${ column.Default };`;
        }

        const params: DBAccessQueryParams = {
            conversion: conversion,
            caller: logTitle,
            sql: sql,
            vendor: DBVendors.PG,
            processExitOnError: false,
            shouldReturnClient: false,
        };

        const result: DBAccessQueryResult = await DBAccess.query(params);

        if (!result.error) {
            await log(
                conversion,
                `\t--[${ logTitle }] Set default value for ${ fullTableName }."${ columnName }"...`,
                (conversion._dicTables.get(tableName) as Table).tableLogPath,
            );
        }
    };

    const promises: Promise<void>[] = (conversion._dicTables.get(tableName) as Table).arrTableColumns
        .filter((column: any): boolean => column.Default !== null)
        .map(_cb);

    await Promise.all(promises);
};
