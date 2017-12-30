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
'use strict';

const ConnectionEmitter = require('../../src/ConnectionEmitter');
const SchemaProcessor   = require('../../src/SchemaProcessor');
const TestBase          = require('./TestBase');

module.exports = class SchemaProcessorTest extends TestBase {

    /**
     * SchemaProcessorTest constructor.
     */
    constructor() {
        super();
    }

    /**
     * Creates a new schema for testing purposes.
     *
     * @returns {Promise<Conversion>}
     */
    async createSchema() {
        const withExistingSchema = false;
        const conversion         = await this.setUp(withExistingSchema);
        return await (new SchemaProcessor(conversion)).createSchema();
    }

    /**
     * Checks if the schema exists.
     *
     * @returns {Promise<Boolean>}
     */
    async hasSchemaCreated() {
        const connectionEmitter = new ConnectionEmitter(this._conversion);
        const sql               = `SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '${ this._conversion._schema }');`;
        const result            = await connectionEmitter.runPgPoolQuery(sql);
        return result.rows[0].exists;
    }
};
