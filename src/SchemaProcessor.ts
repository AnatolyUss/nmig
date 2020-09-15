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
import Conversion from './Conversion';
import DBAccess from './DBAccess';
import DBAccessQueryResult from './DBAccessQueryResult';
import IDBAccessQueryParams from './IDBAccessQueryParams';
import DBVendors from './DBVendors';

/**
 * Creates a new PostgreSQL schema if it does not exist yet.
 */
export default async (conversion: Conversion): Promise<Conversion> => {
    const params: IDBAccessQueryParams = {
        conversion: conversion,
        caller: 'SchemaProcessor::createSchema',
        sql: `SELECT schema_name FROM information_schema.schemata WHERE schema_name = '${ conversion._schema }';`,
        vendor: DBVendors.PG,
        processExitOnError: true,
        shouldReturnClient: true
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);

    if (result.data.rows.length === 0) {
        params.sql = `CREATE SCHEMA "${ conversion._schema }";`;
        params.shouldReturnClient = false;
        params.client = result.client;
        await DBAccess.query(params);
    }

    return conversion;
};
