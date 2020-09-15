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
import DBAccessQueryResult from './DBAccessQueryResult';
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBVendors from './DBVendors';
import IDBAccessQueryParams from './IDBAccessQueryParams';
import { getDataPoolTableName } from './DataPoolManager';

/**
 * Enforces consistency before processing a chunk of data.
 * Ensures there are no any data duplications.
 * In case of normal execution - it is a good practice.
 * In case of rerunning Nmig after unexpected failure - it is absolutely mandatory.
 */
export const dataTransferred = async (conversion: Conversion, dataPoolId: number): Promise<boolean> => {
    const dataPoolTable: string = getDataPoolTableName(conversion);
    const sqlGetMetadata: string = `SELECT metadata AS metadata FROM ${ dataPoolTable } WHERE id = ${ dataPoolId };`;
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'ConsistencyEnforcer::dataTransferred',
        sql: sqlGetMetadata,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: true
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);
    const metadata: any = JSON.parse(result.data.rows[0].metadata);
    const targetTableName: string = `"${ conversion._schema }"."${ metadata._tableName }"`;

    params.sql = `SELECT * FROM ${ targetTableName } LIMIT 1 OFFSET 0;`;
    params.shouldReturnClient = false;
    params.client = result.client;

    const probe: DBAccessQueryResult = await DBAccess.query(params);
    return probe.data.rows.length !== 0;
};
