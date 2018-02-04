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

const { test } = require('tape');
const connect  = require('../../src/Connector');

/**
 * Checks if the schema exists.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 *
 * @returns {Promise<Boolean>}
 */
const hasSchemaCreated = testSchemaProcessor => {
    return connect(testSchemaProcessor._conversion).then(() => {
        return new Promise(resolve => {
            testSchemaProcessor._conversion._pg.connect((error, client, release) => {
                if (error) {
                    testSchemaProcessor.processFatalError(testSchemaProcessor._conversion, error);
                }

                const sql = `SELECT EXISTS(SELECT schema_name FROM information_schema.schemata 
                         WHERE schema_name = '${ testSchemaProcessor._conversion._schema }');`;

                client.query(sql, (err, result) => {
                    release();

                    if (err) {
                        testSchemaProcessor.processFatalError(testSchemaProcessor._conversion, err);
                    }

                    resolve(!!result.rows[0].exists);
                });
            });
        });
    });
};

/**
 * Schema creation testing.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 *
 * @returns {Promise<any>}
 */
module.exports = testSchemaProcessor => {
    return new Promise(resolve => {
        test('Test schema should be created', tape => {
            const numberOfPlannedAssertions = 2;
            const autoTimeoutMs             = 3 * 1000; // 3 seconds.

            tape.plan(numberOfPlannedAssertions);
            tape.timeoutAfter(autoTimeoutMs);

            hasSchemaCreated(testSchemaProcessor).then(schemaExists => {
                tape.equal(typeof schemaExists, 'boolean');
                tape.equal(schemaExists, true);
                tape.end();
                resolve();
            });
        });
    });
};
