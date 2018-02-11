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
 * @param {TestSchemaProcessor} testSchemaProcessor
 *
 * @returns {Promise<pg.Result>}
 */
const retrieveData = testSchemaProcessor => {
    const sql = `SELECT ENCODE(table_a.blob, 'escape') AS blob_text, table_a.* 
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
                const autoTimeoutMs             = 3 * 1000; // 3 seconds.
                const numberOfPlannedAssertions = 24;
                const originalTestBlobText      = testSchemaProcessor
                    .getTestBlob(testSchemaProcessor._conversion)
                    .toString();

                tape.plan(numberOfPlannedAssertions);
                tape.timeoutAfter(autoTimeoutMs);

                tape.equal(data.blob_text, originalTestBlobText);
                tape.equal(data.bit, '1'); // BIT is actually a "bit string", for example: '1110' -> 14
                tape.equal(data.id_test_unique_index, 7384);
                tape.equal(data.id_test_composite_unique_index_1, 125);
                tape.equal(data.id_test_composite_unique_index_2, 234);
                tape.equal(data.id_test_index, 123);
                tape.equal(data.int_test_not_null, 123);
                tape.equal(data.id_test_composite_index_1, 11);
                tape.equal(data.id_test_composite_index_2, 22);
                tape.equal(JSON.stringify(data.json_test_comment), '{"prop1":"First","prop2":2}');
                tape.equal(data.year, 1984);
                tape.equal(data.bigint, '1234567890123456800');
                tape.equal(data.float, 12345.5);
                tape.equal(data.double, 123456789.23);
                tape.equal(data.numeric, '1234567890');
                tape.equal(data.decimal, '1234567890');
                tape.equal(data.char_5, 'fghij');
                tape.equal(data.varchar_5, 'abcde');
                tape.equal(`${ data.date.getFullYear() }-${ data.date.getMonth() + 1 }-${ data.date.getDate() }`, '1984-11-30');
                tape.equal(data.time, '21:12:33');
                tape.equal(data.text, 'Test text');
                tape.equal(data.enum, 'e1');
                tape.equal(data.set, 's2');

                const date = `${ data.timestamp.getFullYear() }-${ data.timestamp.getMonth() + 1 }-${ data.timestamp.getDate() }`;
                const time = `${ data.timestamp.getHours() }:${ data.timestamp.getMinutes() }:${ data.timestamp.getSeconds() }`;
                tape.equal(`${ date } ${ time }`, '2018-11-11 22:21:20');

                tape.end();
                resolve();
            });
        });
    });
};
