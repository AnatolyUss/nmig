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

const path              = require('path');
const Main              = require('../../src/Main');
const SchemaProcessor   = require('../../src/SchemaProcessor');
const ConnectionEmitter = require('../../src/ConnectionEmitter');

module.exports = class TestBase {

    /**
     * TestBase constructor.
     */
    constructor() {
        this._app        = new Main();
        this._conversion = null;
    }

    /**
     * Runs before all tests in this suite.
     *
     * @param {Boolean} withExistingSchema
     *
     * @returns {Promise<Conversion>}
     */
    async setUp(withExistingSchema = true) {
        const baseDir    = path.join(__dirname, '..', '..');
        const baseConfig = await this._app.readConfig(baseDir);
        const fullConfig = await this._app.readExtraConfig(baseConfig, baseDir);
        this._conversion = await this._app.initializeConversion(fullConfig);

        return withExistingSchema
            ? await (new SchemaProcessor(this._conversion)).createSchema()
            : this._conversion;
    }

    /**
     * Runs after all tests in this suite.
     *
     * @returns {undefined}
     */
    async tearDown() {
        const connectionEmitter = new ConnectionEmitter(this._conversion);
        const sql               = `DROP SCHEMA "${ this._conversion._schema }" CASCADE;`;
        await connectionEmitter.runPgPoolQuery(sql);
    }
};
