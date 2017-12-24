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

const Main         = require('../../src/Main');
const createSchema = require('../../src/SchemaProcessor');

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
     * @returns {undefined}
     */
    setUp(withExistingSchema = true) {
        const flowPromise = this._app.readConfig()
            .then(this._app.readExtraConfig)
            .then(this._app.initializeConversion)
            .then(conversion => {
                // Make a Conversion instance available for derivative classes.
                this._conversion = conversion;

                return Promise.resolve(conversion);
            });

        if (withExistingSchema) {
            flowPromise.then(createSchema);
        }
    }

    /**
     * Runs after all tests in this suite.
     *
     * @returns {undefined}
     */
    tearDown() {
        //
    }
};
