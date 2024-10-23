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
import arrangeColumnsData from './ColumnsDataArranger';
import * as extraConfigProcessor from './ExtraConfigProcessor';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import { getDataPoolTableName } from './DataPoolManager';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors, Table } from './Types';

/**
 * Prepares an array of tables metadata.
 */
export default async (
  conversion: Conversion,
  tableName: string,
  haveDataChunksProcessed: boolean,
): Promise<void> => {
  if (haveDataChunksProcessed) {
    return;
  }

  const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);

  const logTitle = 'DataChunksProcessor::default';
  const arrTableColumns = (conversion._dicTables.get(tableName) as Table).arrTableColumns;
  const selectFieldList: string = arrangeColumnsData(
    arrTableColumns,
    +conversion._mysqlVersion.split('.').slice(0, 2).join('.'),
    conversion._encoding,
  );

  const sqlRowsCnt = `SELECT COUNT(1) AS rows_count FROM \`${originalTableName}\`;`;
  const params: DBAccessQueryParams = {
    conversion: conversion,
    caller: 'DataChunksProcessor::default',
    sql: sqlRowsCnt,
    vendor: DBVendors.MYSQL,
    processExitOnError: false,
    shouldReturnClient: false,
  };

  const countResult: DBAccessQueryResult = await DBAccess.query(params);
  const rowsCnt: number = countResult.data[0].rows_count;
  const fullTableName = `"${conversion._schema}"."${tableName}"`;

  await log(
    conversion,
    `\t--[${logTitle}] Total rows to insert into ${fullTableName}: ${rowsCnt}`,
    (conversion._dicTables.get(tableName) as Table).tableLogPath,
  );

  const metadata: string = JSON.stringify({
    _tableName: tableName,
    _rowsCnt: rowsCnt,
    _selectFieldList: selectFieldList,
    _copyColumnNamesList: arrTableColumns
      .map((column: any): string => {
        const columnName = extraConfigProcessor.getColumnName(
          conversion,
          originalTableName,
          column.Field,
          false,
        );

        return `"${columnName}"`;
      })
      .join(','),
  });

  params.sql = `INSERT INTO ${getDataPoolTableName(conversion)}("metadata") VALUES ($1);`;
  params.vendor = DBVendors.PG;
  params.client = undefined;
  params.bindings = [metadata];
  await DBAccess.query(params);
};
