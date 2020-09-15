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
import TestSchemaProcessor from './TestSchemaProcessor';
import Conversion from '../../src/Conversion';
import DBAccess from '../../src/DBAccess';
import DBVendors from '../../src/DBVendors';
import DBAccessQueryResult from '../../src/DBAccessQueryResult';
import IDBAccessQueryParams from '../../src/IDBAccessQueryParams';
import { Test } from 'tape';

/**
 * Returns `table_a` column types.
 */
const getColumnTypes = async (testSchemaProcessor: TestSchemaProcessor): Promise<any[]> => {
    const sql: string = `SELECT column_name, data_type  
                 FROM information_schema.columns
                 WHERE table_catalog = '${ (<Conversion>testSchemaProcessor.conversion)._targetConString.database }' 
                   AND table_schema = '${ (<Conversion>testSchemaProcessor.conversion)._schema }' 
                   AND table_name = 'table_a';`;

    const params: IDBAccessQueryParams = {
        conversion: <Conversion>testSchemaProcessor.conversion,
        caller: 'ColumnTypesTest::getColumnTypes',
        sql: sql,
        vendor: DBVendors.PG,
        processExitOnError: false,
        shouldReturnClient: false
    };

    const result: DBAccessQueryResult = await DBAccess.query(params);

    if (result.error) {
        await testSchemaProcessor.processFatalError(result.error);
    }

    return result.data.rows;
};

/**
 * Returns expected column types map.
 */
const getExpectedColumnTypes = (): Map<string, string> => {
    return new Map<string, string>([
        ['id_test_sequence', 'bigint'],
        ['id_test_unique_index', 'integer'],
        ['id_test_composite_unique_index_1', 'integer'],
        ['id_test_composite_unique_index_2', 'integer'],
        ['id_test_index', 'integer'],
        ['int_test_not_null', 'integer'],
        ['id_test_composite_index_1', 'integer'],
        ['id_test_composite_index_2', 'integer'],
        ['json_test_comment', 'json'],
        ['bit', 'bit varying'],
        ['year', 'smallint'],
        ['tinyint_test_default', 'smallint'],
        ['smallint', 'smallint'],
        ['mediumint', 'integer'],
        ['bigint', 'bigint'],
        ['float', 'real'],
        ['double', 'double precision'],
        ['double_precision', 'double precision'],
        ['numeric', 'numeric'],
        ['decimal', 'numeric'],
        ['decimal_19_2', 'numeric'],
        ['char_5', 'character'],
        ['varchar_5', 'character varying'],
        ['date', 'date'],
        ['time', 'time without time zone'],
        ['datetime', 'timestamp without time zone'],
        ['timestamp', 'timestamp without time zone'],
        ['enum', 'character varying'],
        ['set', 'character varying'],
        ['tinytext', 'text'],
        ['mediumtext',  'text'],
        ['longtext', 'text'],
        ['text', 'text'],
        ['blob', 'bytea'],
        ['longblob', 'bytea'],
        ['mediumblob', 'bytea'],
        ['tinyblob', 'bytea'],
        ['varbinary', 'bytea'],
        ['binary', 'bytea'],
        ['null_char_in_varchar', 'character varying']
    ]);
};

/**
 * The data content testing.
 */
export default async (testSchemaProcessor: TestSchemaProcessor, tape: Test): Promise<void> => {
    const data: any[] = await getColumnTypes(testSchemaProcessor);
    const expectedColumnTypesMap: Map<string, string> = getExpectedColumnTypes();
    const autoTimeoutMs: number = 3 * 1000; // 3 seconds.
    const numberOfPlannedAssertions: number = data.length;

    tape.plan(numberOfPlannedAssertions);
    tape.timeoutAfter(autoTimeoutMs);

    for (let i: number = 0; i < numberOfPlannedAssertions; ++i) {
        const columnName: string = data[i].column_name;
        const actualColumnType: string = data[i].data_type;
        const expectedColumnType: string = <string>expectedColumnTypesMap.get(columnName);

        tape.comment(`Test ${ columnName } column type`);
        tape.equal(actualColumnType, expectedColumnType);
    }
};
