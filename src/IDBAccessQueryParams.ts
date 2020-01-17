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
import DBVendors from './DBVendors';
import { PoolConnection } from 'mysql';
import { PoolClient } from 'pg';

export default interface IDBAccessQueryParams {
    /**
     * Conversion, Nmig's configuration.
     */
    conversion: Conversion;

    /**
     * Function, that has sent current SQL query for execution.
     */
    caller: string;

    /**
     * SQL query, that was sent for execution.
     */
    sql: string;

    /**
     * Type of a database, to which current SQL query was sent.
     */
    vendor: DBVendors;

    /**
     * Flag, indicating whether to abort Nmig execution on error.
     */
    processExitOnError: boolean;

    /**
     * Flag, indicating whether a database client should be returned as a part of DBAccessQueryResult object.
     */
    shouldReturnClient: boolean;

    /**
     * A database client.
     */
    client?: PoolConnection | PoolClient;

    /**
     * SQL query bindings.
     */
    bindings?: any[];
}
