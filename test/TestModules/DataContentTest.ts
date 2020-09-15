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
import Conversion from '../../src/Conversion';
import TestSchemaProcessor from './TestSchemaProcessor';
import DBAccess from '../../src/DBAccess';
import DBVendors from '../../src/DBVendors';
import DBAccessQueryResult from '../../src/DBAccessQueryResult';
import IDBAccessQueryParams from '../../src/IDBAccessQueryParams';
import { Test } from 'tape';

/**
 * Retrieves a data from `table_a`.
 */
const retrieveData = async (testSchemaProcessor: TestSchemaProcessor): Promise<any> => {
    const sql: string = `SELECT ENCODE(table_a.blob, 'escape') AS blob_text, table_a.* 
        FROM ${ (<Conversion>testSchemaProcessor.conversion)._schema }.table_a AS table_a;`;

    const params: IDBAccessQueryParams = {
        conversion: <Conversion>testSchemaProcessor.conversion,
        caller: 'DataContentTest::retrieveData',
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);

    if (result.error) {
        await testSchemaProcessor.processFatalError(result.error);
    }

    return result.data.rows[0];
};

/**
 * The data content testing.
 */
export default async (testSchemaProcessor: TestSchemaProcessor, tape: Test): Promise<void> => {
    const data: any = await retrieveData(testSchemaProcessor);
    const autoTimeoutMs: number = 3 * 1000; // 3 seconds.
    const numberOfPlannedAssertions: number = 24;
    const originalTestBlobText: string = testSchemaProcessor.getTestBlob(<Conversion>testSchemaProcessor.conversion).toString();

    tape.plan(numberOfPlannedAssertions);
    tape.timeoutAfter(autoTimeoutMs);

    tape.comment('Test blob_text column value');
    tape.equal(data.blob_text, originalTestBlobText);

    tape.comment('Test bit column value');
    tape.equal(data.bit, '1'); // BIT is actually a "bit string".

    tape.comment('Test id_test_unique_index column value');
    tape.equal(data.id_test_unique_index, 7384);

    tape.comment('Test id_test_composite_unique_index_1 column value');
    tape.equal(data.id_test_composite_unique_index_1, 125);

    tape.comment('Test id_test_composite_unique_index_2 column value');
    tape.equal(data.id_test_composite_unique_index_2, 234);

    tape.comment('Test id_test_index column value');
    tape.equal(data.id_test_index, 123);

    tape.comment('Test int_test_not_null column value');
    tape.equal(data.int_test_not_null, 123);

    tape.comment('Test id_test_composite_index_1 column value');
    tape.equal(data.id_test_composite_index_1, 11);

    tape.comment('Test id_test_composite_index_2 column value');
    tape.equal(data.id_test_composite_index_2, 22);

    tape.comment('Test json_test_comment column value');
    tape.equal(JSON.stringify(data.json_test_comment), '{"prop1":"First","prop2":2}');

    tape.comment('Test year column value');
    tape.equal(data.year, 1984);

    tape.comment('Test bigint column value');
    tape.equal(data.bigint, '9223372036854775807');

    tape.comment('Test float column value');
    tape.equal(data.float, 12345.5);

    tape.comment('Test double column value');
    tape.equal(data.double, 123456789.23);

    tape.comment('Test numeric column value');
    tape.equal(data.numeric, '1234567890');

    tape.comment('Test decimal column value');
    tape.equal(data.decimal, '99999999999999999223372036854775807.121111111111111345334523423220');

    tape.comment('Test char_5 column value');
    tape.equal(data.char_5, 'fghij');

    tape.comment('Test varchar_5 column value');
    tape.equal(data.varchar_5, 'abcde');

    tape.comment('Test date column value');
    tape.equal(`${ data.date.getFullYear() }-${ data.date.getMonth() + 1 }-${ data.date.getDate() }`, '1984-11-30');

    tape.comment('Test time column value');
    tape.equal(data.time, '21:12:33');

    tape.comment('Test text column value');
    tape.equal(data.text, 'Test text');

    tape.comment('Test enum column value');
    tape.equal(data.enum, 'e1');

    tape.comment('Test set column value');
    tape.equal(data.set, 's2');

    const date: string = `${ data.timestamp.getFullYear() }-${ data.timestamp.getMonth() + 1 }-${ data.timestamp.getDate() }`;
    const time: string = `${ data.timestamp.getHours() }:${ data.timestamp.getMinutes() }:${ data.timestamp.getSeconds() }`;
    tape.comment('Test timestamp column value');
    tape.equal(`${ date } ${ time }`, '2018-11-11 22:21:20');
};
