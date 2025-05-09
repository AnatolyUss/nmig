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
import { log } from './fs-ops';
import Conversion from './conversion';
import DbAccess from './db-access';
import * as extraConfigProcessor from './extra-config-processor';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors, Table } from './types';

/**
 * Defines which columns of the given table are of type "enum".
 * Sets an appropriate constraint, if needed.
 */
export default async (conversion: Conversion, tableName: string): Promise<void> => {
  const logTitle = 'EnumProcessor::default';
  const fullTableName = `"${conversion._schema}"."${tableName}"`;
  const msg = `\t--[${logTitle}] Defines "ENUMs" for table ${fullTableName}`;
  await log(conversion, msg, (conversion._dicTables.get(tableName) as Table).tableLogPath);
  const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);

  const _cb = async (column: any): Promise<void> => {
    if (column.Type.indexOf('(') !== -1) {
      const arrType: string[] = column.Type.split('(');

      if (arrType[0] === 'enum') {
        const columnName: string = extraConfigProcessor.getColumnName(
          conversion,
          originalTableName,
          column.Field,
          false,
        );

        const params: DBAccessQueryParams = {
          conversion: conversion,
          caller: logTitle,
          sql: `ALTER TABLE ${fullTableName} ADD CHECK ("${columnName}" IN (${arrType[1]});`,
          vendor: DBVendors.PG,
          processExitOnError: false,
          shouldReturnClient: false,
        };

        const result: DBAccessQueryResult = await DbAccess.query(params);

        if (!result.error) {
          await log(
            conversion,
            `\t--[${logTitle}] Set "ENUM" for ${fullTableName}."${columnName}"...`,
            (conversion._dicTables.get(tableName) as Table).tableLogPath,
          );
        }
      }
    }
  };

  const processEnumPromises: Promise<void>[] = (
    conversion._dicTables.get(tableName) as Table
  ).arrTableColumns.map(_cb);

  await Promise.all(processEnumPromises);
};
