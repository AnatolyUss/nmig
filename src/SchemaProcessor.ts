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
import ConnectionEmitter from './ConnectionEmitter';
import generateError from './ErrorGenerator';
import Conversion from './Classes/Conversion';

export default class SchemaProcessor {
    /**
     * An instance of "Conversion".
     */
    private readonly _conversion: Conversion;

    /**
     * An instance of "ConnectionEmitter".
     */
    private readonly _connectionEmitter: ConnectionEmitter;

    /**
     * SchemaProcessor constructor.
     */
    public constructor(conversion: Conversion) {
        this._conversion        = conversion;
        this._connectionEmitter = new ConnectionEmitter(this._conversion);
    }

    /**
     * Create a new database schema if it does not exist yet.
     */
    public async createSchema(): Promise<Conversion> {
        const client: any = await this._connectionEmitter.getPgClient();
        let sql: string = `SELECT schema_name FROM information_schema.schemata WHERE schema_name = '${ this._conversion._schema }';`;

        try {
            const result: any = await client.query(sql);

            if (result.rows.length === 0) {
                sql = `CREATE SCHEMA "${ this._conversion._schema }";`;
                await client.query(sql);
            }

            this._connectionEmitter.releasePgClient(client);
            return Promise.resolve(this._conversion);

        } catch (err) {
            generateError(this._conversion, `\t--[createSchema] ${ err }`, sql);
            return Promise.reject(err);
        }
    }
};
