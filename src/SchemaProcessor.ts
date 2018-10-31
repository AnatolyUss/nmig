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
import DBVendors from './DBVendors';

/**
 * Creates a new PostgreSQL schema if it does not exist yet.
 */
export default async function(conversion: Conversion): Promise<Conversion> {
    const logTitle: string = 'SchemaProcessor::createSchema';
    let sql: string = `SELECT schema_name FROM information_schema.schemata WHERE schema_name = '${ conversion._schema }';`;
    const dbAccess: DBAccess = new DBAccess(conversion);
    const result: DBAccessQueryResult = await dbAccess.query(logTitle, sql, DBVendors.PG, true, true);

    if (result.data.rows.length === 0) {
        sql = `CREATE SCHEMA "${ conversion._schema }";`;
        await dbAccess.query(logTitle, sql, DBVendors.PG, true, false, result.client);
    }

    return conversion;
}
