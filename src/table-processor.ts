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
 * Converts MySQL data types to corresponding PostgreSQL data types.
 * This conversion performs in accordance to mapping rules in './config/data_types_map.json'.
 * './config/data_types_map.json' can be customized.
 */
export const mapDataTypes = (objDataTypesMap: any, mySqlDataType: string): string => {
  let retVal = '';
  const arrDataTypeDetails: string[] = mySqlDataType.split(' ');
  mySqlDataType = arrDataTypeDetails[0].toLowerCase();
  const increaseOriginalSize: boolean =
    arrDataTypeDetails.indexOf('unsigned') !== -1 || arrDataTypeDetails.indexOf('zerofill') !== -1;

  if (mySqlDataType.indexOf('(') === -1) {
    // No parentheses detected.
    retVal = increaseOriginalSize
      ? objDataTypesMap[mySqlDataType].increased_size
      : objDataTypesMap[mySqlDataType].type;
  } else {
    // Parentheses detected.
    const arrDataType: string[] = mySqlDataType.split('(');
    const strDataType: string = arrDataType[0].toLowerCase();
    const strDataTypeDisplayWidth: string = arrDataType[1];

    if ('enum' === strDataType || 'set' === strDataType) {
      retVal = 'character varying(255)';
    } else if ('decimal' === strDataType || 'numeric' === strDataType) {
      retVal = `${objDataTypesMap[strDataType].type}(${strDataTypeDisplayWidth}`;
    } else if (
      'decimal(19,2)' === mySqlDataType ||
      objDataTypesMap[strDataType].mySqlVarLenPgSqlFixedLen
    ) {
      // Should be converted without a length definition.
      retVal = increaseOriginalSize
        ? objDataTypesMap[strDataType].increased_size
        : objDataTypesMap[strDataType].type;
    } else {
      // Should be converted with a length definition.
      // Check if the "type" or the "increased_size" of the current data type
      // from the "data_types_map.json" contains length definition.
      // If contains - convert the type using provided definition
      // instead of the original one (taken from the original MySQL DB).
      const mapToPgTypeWithLengthDefinition: boolean = increaseOriginalSize
        ? objDataTypesMap[strDataType].increased_size.indexOf('(') !== -1
        : objDataTypesMap[strDataType].type.indexOf('(') !== -1;

      if (mapToPgTypeWithLengthDefinition) {
        retVal = increaseOriginalSize
          ? `${objDataTypesMap[strDataType].increased_size}`
          : `${objDataTypesMap[strDataType].type}`;
      } else {
        retVal = increaseOriginalSize
          ? `${objDataTypesMap[strDataType].increased_size}(${strDataTypeDisplayWidth}`
          : `${objDataTypesMap[strDataType].type}(${strDataTypeDisplayWidth}`;
      }
    }
  }

  // Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
  if (retVal === 'character(0)') {
    retVal = 'character(1)';
  } else if (retVal === 'character varying(0)') {
    retVal = 'character varying(1)';
  }

  return retVal;
};

/**
 * Migrates structure of a single table to PostgreSql server.
 */
export const createTable = async (conversion: Conversion, tableName: string): Promise<void> => {
  const logTitle = 'TableProcessor::createTable';
  await log(
    conversion,
    `\t--[${logTitle}] Currently creating table: \`${tableName}\``,
    (conversion._dicTables.get(tableName) as Table).tableLogPath,
  );

  const originalTableName: string = extraConfigProcessor.getTableName(conversion, tableName, true);
  const params: DBAccessQueryParams = {
    conversion: conversion,
    caller: logTitle,
    sql: `SHOW FULL COLUMNS FROM \`${originalTableName}\`;`,
    vendor: DBVendors.MYSQL,
    processExitOnError: false,
    shouldReturnClient: false,
  };

  const columns: DBAccessQueryResult = await DbAccess.query(params);

  if (columns.error) {
    return;
  }

  (conversion._dicTables.get(tableName) as Table).arrTableColumns = columns.data;

  if (conversion.shouldMigrateOnlyData()) {
    return;
  }

  const columnsDefinition: string = columns.data
    .map((column: any): string => {
      const colName: string = extraConfigProcessor.getColumnName(
        conversion,
        originalTableName,
        column.Field,
        false,
      );

      const colType: string = mapDataTypes(conversion._dataTypesMap, column.Type);
      return `"${colName}" ${colType}`;
    })
    .join(',');

  params.sql = `CREATE TABLE IF NOT EXISTS "${conversion._schema}"."${tableName}"(${columnsDefinition});`;
  params.processExitOnError = true;
  params.vendor = DBVendors.PG;
  const createTableResult: DBAccessQueryResult = await DbAccess.query(params);

  if (!createTableResult.error) {
    await log(
      conversion,
      `\t--[${logTitle}] Table "${conversion._schema}"."${tableName}" is created...`,
      (conversion._dicTables.get(tableName) as Table).tableLogPath,
    );
  }
};
