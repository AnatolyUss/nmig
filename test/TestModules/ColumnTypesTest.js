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

/**
 * Returns `table_a` column types.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 *
 * @returns {Promise<Array>}
 */
const getColumnTypes = testSchemaProcessor => {
    const sql = `SELECT column_name, data_type  
                 FROM information_schema.columns
                 WHERE table_catalog = '${ testSchemaProcessor._conversion._targetConString.database }' 
                   AND table_schema = '${ testSchemaProcessor._conversion._schema }' 
                   AND table_name = 'table_a';`;

    return testSchemaProcessor
        .queryPg(sql)
        .then(data => data.rows);
};

/**
 * Returns expected column types.
 *
 * @returns {Object}
 */
const getExpectedColumnTypes = () => {
    return {
        id_test_sequence                 : 'bigint',
        id_test_unique_index             : 'integer',
        id_test_composite_unique_index_1 : 'integer',
        id_test_composite_unique_index_2 : 'integer',
        id_test_index                    : 'integer',
        int_test_not_null                : 'integer',
        id_test_composite_index_1        : 'integer',
        id_test_composite_index_2        : 'integer',
        json_test_comment                : 'json',
        bit                              : 'bit varying',
        year                             : 'smallint',
        tinyint_test_default             : 'smallint',
        smallint                         : 'smallint',
        mediumint                        : 'integer',
        bigint                           : 'bigint',
        float                            : 'real',
        double                           : 'double precision',
        double_precision                 : 'double precision',
        numeric                          : 'numeric',
        decimal                          : 'numeric',
        decimal_19_2                     : 'numeric',
        char_5                           : 'character',
        varchar_5                        : 'character varying',
        date                             : 'date',
        time                             : 'time without time zone',
        datetime                         : 'timestamp without time zone',
        timestamp                        : 'timestamp without time zone',
        enum                             : 'character varying',
        set                              : 'character varying',
        tinytext                         : 'text',
        mediumtext                       : 'text',
        longtext                         : 'text',
        text                             : 'text',
        blob                             : 'bytea',
        longblob                         : 'bytea',
        mediumblob                       : 'bytea',
        tinyblob                         : 'bytea',
        varbinary                        : 'bytea',
        binary                           : 'bytea',
    };
};

/**
 * The data content testing.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 * @param {Tape} tape
 *
 * @returns {undefined}
 */
module.exports = (testSchemaProcessor, tape) => {
    getColumnTypes(testSchemaProcessor).then(data => {
        const expectedColumnTypes       = getExpectedColumnTypes();
        const autoTimeoutMs             = 3 * 1000; // 3 seconds.
        const numberOfPlannedAssertions = data.length;

        tape.plan(numberOfPlannedAssertions);
        tape.timeoutAfter(autoTimeoutMs);

        for (let i = 0; i < numberOfPlannedAssertions; ++i) {
            const columnName         = data[i].column_name;
            const actualColumnType   = data[i].data_type;
            const expectedColumnType = expectedColumnTypes[columnName];

            tape.comment(`Test ${ columnName } column type`);
            tape.equal(actualColumnType, expectedColumnType);
        }
    });
};
