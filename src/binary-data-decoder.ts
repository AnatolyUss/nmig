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
import { PoolClient } from 'pg';

import { log } from './fs-ops';
import Conversion from './conversion';
import DbAccess from './db-access';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './types';

/**
 * Decodes binary data from textual representation in string.
 */
export default async (conversion: Conversion): Promise<Conversion> => {
  const logTitle = 'BinaryDataDecoder::decodeBinaryData';
  await log(
    conversion,
    `\t--[${logTitle}] Decodes binary data from textual representation in string.`,
  );

  const sql = `SELECT table_name, column_name 
        FROM information_schema.columns
        WHERE table_catalog = '${conversion._targetConString.database}' 
          AND table_schema = '${conversion._schema}' 
          AND data_type IN ('bytea', 'geometry');`;

  const params: DBAccessQueryParams = {
    conversion: conversion,
    caller: logTitle,
    sql: sql,
    vendor: DBVendors.PG,
    processExitOnError: false,
    shouldReturnClient: false,
  };

  const result: DBAccessQueryResult = await DbAccess.query(params);

  if (result.error) {
    // No need to continue if no 'bytea' or 'geometry' columns found.
    await DbAccess.releaseDbClient(conversion, result.client as PoolClient);
    return conversion;
  }

  const _cb = async (row: any): Promise<void> => {
    const tableName: string = row.table_name;
    const columnName: string = row.column_name;
    params.sql = `UPDATE ${conversion._schema}."${tableName}"
                SET "${columnName}" = DECODE(ENCODE("${columnName}", 'escape'), 'hex');`;

    await DbAccess.query(params);
  };

  const decodePromises: Promise<void>[] = result.data.rows.map(_cb);
  await Promise.all(decodePromises);
  return conversion;
};
