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
import { TestSchemaProcessor } from './TestSchemaProcessor';
import Conversion from '../../src/Conversion';
import DBAccess from '../../src/DBAccess';
import DBVendors from '../../src/DBVendors';
import DBAccessQueryResult from '../../src/DBAccessQueryResult';

/**
 * Checks if the schema exists.
 */
async function hasSchemaCreated(testSchemaProcessor: TestSchemaProcessor): Promise<boolean> {
    const logTitle: string = 'SchemaProcessorTest::hasSchemaCreated';
    const sql: string = `SELECT EXISTS(SELECT schema_name FROM information_schema.schemata
         WHERE schema_name = '${ (<Conversion>testSchemaProcessor.conversion)._schema }');`;

    const result: DBAccessQueryResult = await (<DBAccess>testSchemaProcessor.dbAccess).query(
        logTitle,
        sql,
        DBVendors.PG,
        true,
        false
    );

    return !!result.data.rows[0].exists;
}

/**
 * TODO: check @types/tape.
 * Schema creation testing.
 *
 * @param {TestSchemaProcessor} testSchemaProcessor
 * @param {Tape} tape
 *
 * @returns {undefined}
 */
module.exports = (testSchemaProcessor, tape) => {
    hasSchemaCreated(testSchemaProcessor).then(schemaExists => {
        const numberOfPlannedAssertions = 1;
        const autoTimeoutMs             = 3 * 1000; // 3 seconds.

        tape.plan(numberOfPlannedAssertions);
        tape.timeoutAfter(autoTimeoutMs);
        tape.equal(schemaExists, true);
    });
}
