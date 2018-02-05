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

const TestSchemaProcessor = require('./TestModules/TestSchemaProcessor');
const processSchemaTest   = require('./TestModules/SchemaProcessorTest');

/**
 * Runs test suites.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 *
 * @returns {Function}
 */
const runTestSuites = testSchemaProcessor => {
    return () => {
        Promise.all([
            processSchemaTest(testSchemaProcessor),
        ])
        .then(() => testSchemaProcessor.removeTestResources())
        .then(() => process.exit());
    };
};

const testSchemaProcessor = new TestSchemaProcessor();

testSchemaProcessor
    .initializeConversion()
    .then(conversion => {
        // Registers callback, that will be invoked when the test database arrangement will be completed.
        conversion._eventEmitter.on(conversion._migrationCompletedEvent, runTestSuites(testSchemaProcessor));

        // Continues the test database arrangement.
        return Promise.resolve(conversion);
    })
    .then(testSchemaProcessor.arrangeTestMigration.bind(testSchemaProcessor));
