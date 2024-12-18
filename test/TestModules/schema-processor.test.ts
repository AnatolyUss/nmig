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
import { Test } from 'tape';

import TestSchemaProcessor from './test-schema-processor';
import Conversion from '../../src/conversion';
import DbAccess from '../../src/db-access';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from '../../src/types';

/**
 * Checks if the schema exists.
 */
const hasSchemaCreated = async (testSchemaProcessor: TestSchemaProcessor): Promise<boolean> => {
  const sql = `SELECT EXISTS(SELECT schema_name FROM information_schema.schemata
         WHERE schema_name = '${(testSchemaProcessor.conversion as Conversion)._schema}');`;

  const params: DBAccessQueryParams = {
    conversion: testSchemaProcessor.conversion as Conversion,
    caller: 'SchemaProcessorTest::hasSchemaCreated',
    sql: sql,
    vendor: DBVendors.PG,
    processExitOnError: false,
    shouldReturnClient: false,
  };

  const result: DBAccessQueryResult = await DbAccess.query(params);

  if (result.error) {
    await testSchemaProcessor.processFatalError(result.error);
  }

  return !!result.data.rows[0].exists;
};

/**
 * Tests schema creation.
 */
export default async (testSchemaProcessor: TestSchemaProcessor, tape: Test): Promise<void> => {
  const schemaExists: boolean = await hasSchemaCreated(testSchemaProcessor);
  const numberOfPlannedAssertions = 1;
  const autoTimeoutMs: number = 3 * 1000; // 3 seconds.

  tape.plan(numberOfPlannedAssertions);
  tape.timeoutAfter(autoTimeoutMs);
  tape.equal(schemaExists, true);
};
