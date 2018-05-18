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

export default class SchemaProcessor {
    /**
     * An instance of "Conversion".
     */
    private readonly _conversion: Conversion;

    /**
     * An instance of "DBAccess".
     */
    private readonly _dbAccess: DBAccess;

    /**
     * SchemaProcessor constructor.
     */
    public constructor(conversion: Conversion) {
        this._conversion = conversion;
        this._dbAccess = new DBAccess(this._conversion);
    }

    /**
     * Create a new database schema if it does not exist yet.
     */
    public async createSchema(): Promise<Conversion|void> {
        let sql: string = `SELECT schema_name FROM information_schema.schemata WHERE schema_name = '${ this._conversion._schema }';`;

        const result: DBAccessQueryResult = await this._dbAccess.query('SchemaProcessor::createSchema', sql, DBVendors.PG, true, true);

        if (result.data.rows.length === 0) {
            sql = `CREATE SCHEMA "${ this._conversion._schema }";`;
            await this._dbAccess.query('SchemaProcessor::createSchema', sql, DBVendors.PG, true, false, result.client);
        }

        return this._conversion;
    }
}
