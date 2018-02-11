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

/**
 * Retrieve a data from `table_a`.
 *
 * @param testSchemaProcessor
 *
 * @returns {Promise<pg.Result>}
 */
const retrieveData = testSchemaProcessor => {
    const sql = `SELECT ENCODE(blob, 'escape') AS blob_text, table_a.* 
                 FROM ${ testSchemaProcessor._conversion._schema }.table_a AS table_a;`;

    return testSchemaProcessor
        .queryPg(sql)
        .then(data => data.rows[0]);
};

/**
 * The data content testing.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 *
 * @returns {Promise<Any>}
 */
module.exports = testSchemaProcessor => {
    return new Promise(resolve => {
        retrieveData(testSchemaProcessor).then(data => {
            test('Test the data content', tape => {
                const originalTestBlobText      = testSchemaProcessor.getTestBlob(testSchemaProcessor._conversion).toString();
                const autoTimeoutMs             = 3 * 1000; // 3 seconds.
                const numberOfPlannedAssertions = 20;

                tape.plan(numberOfPlannedAssertions);
                tape.timeoutAfter(autoTimeoutMs);

                tape.equal(typeof data.blob_text, 'string');
                tape.equal(data.blob_text, originalTestBlobText);

                tape.equal(typeof data.bit, 'string');
                tape.equal(data.bit, '1'); // BIT is actually a "bit string", for example: '1110' -> 14

                tape.equal(typeof data.id_test_unique_index, 'number');
                tape.equal(data.id_test_unique_index, 7384);

                tape.equal(typeof data.id_test_composite_unique_index_1, 'number');
                tape.equal(data.id_test_composite_unique_index_1, 125);

                tape.equal(typeof data.id_test_composite_unique_index_2, 'number');
                tape.equal(data.id_test_composite_unique_index_2, 234);

                tape.equal(typeof data.id_test_index, 'number');
                tape.equal(data.id_test_index, 123);

                tape.equal(typeof data.int_test_not_null, 'number');
                tape.equal(data.int_test_not_null, 123);

                tape.equal(typeof data.id_test_composite_index_1, 'number');
                tape.equal(data.id_test_composite_index_1, 11);

                tape.equal(typeof data.id_test_composite_index_2, 'number');
                tape.equal(data.id_test_composite_index_2, 22);

                tape.equal(typeof data.json_test_comment, 'object');
                tape.equal(JSON.stringify(data.json_test_comment), '{"prop1":"First","prop2":2}');

                tape.end();
                resolve();
            });
        });
    });
};
