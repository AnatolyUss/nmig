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
import * as path from 'path';
import * as fs from 'fs';
import { EventEmitter } from 'events';
import Conversion from '../../src/Conversion';
import DBAccess from '../../src/DBAccess';
import DBVendors from '../../src/DBVendors';
import { Main } from '../../src/Main';
import SchemaProcessor from '../../src/SchemaProcessor';
import readDataTypesMap from '../../src/DataTypesMapReader';
import loadStructureToMigrate from '../../src/StructureLoader';
import pipeData from '../../src/DataPipeManager';
import { createStateLogsTable } from '../../src/MigrationStateManager';
import { createDataPoolTable, readDataPool } from '../../src/DataPoolManager';
import generateError from '../../src/ErrorGenerator';

export default class TestSchemaProcessor {
    /**
     * Instance of class Main.
     */
    private readonly _app: Main;

    /**
     * Instance of class Conversion.
     */
    public conversion?: Conversion;

    /**
     * Instance of class DBAccess.
     */
    public dbAccess?: DBAccess;

    /**
     * TestSchemaProcessor constructor.
     */
    public constructor() {
        this._app = new Main();
        this.conversion = undefined;
        this.dbAccess = undefined;
    }

    /**
     * Stops the process in case of fatal error.
     */
    public processFatalError(conversion: Conversion, error: string): void {
        console.log(error);
        generateError(conversion, error);
        process.exit();
    }

    /**
     * Removes resources created by test scripts.
     */
    public async removeTestResources(): Promise<void> {
        const sqlDropMySqlDatabase: string = `DROP DATABASE ${ (<Conversion>this.conversion)._mySqlDbName };`;
        await (<DBAccess>this.dbAccess).query(
            'removeTestResources',
            sqlDropMySqlDatabase,
            DBVendors.MYSQL,
            true,
            false
        );

        const sqlDropPgDatabase: string = `DROP SCHEMA ${ (<Conversion>this.conversion)._schema } CASCADE;`;
        await (<DBAccess>this.dbAccess).query(
            'removeTestResources',
            sqlDropPgDatabase,
            DBVendors.PG,
            true,
            false
        );
    }

    /**
     * Creates test source database.
     */
    private async _createTestSourceDb(conversion: Conversion): Promise<Conversion> {
        const sql: string = `CREATE DATABASE IF NOT EXISTS ${ (<Conversion>this.conversion)._mySqlDbName };`;
        await (<DBAccess>this.dbAccess).query('_createTestSourceDb', sql, DBVendors.MYSQL, true, false);
        return conversion;
    }

    /**
     * Updates the "database" part of MySQL connection.
     */
    private _updateMySqlConnection(conversion: Conversion): Promise<Conversion> {
        return new Promise<Conversion>(resolve => {
            conversion._mysql = undefined;
            conversion._sourceConString.database = conversion._mySqlDbName;
            resolve(conversion);
        });
    }

    /**
     * Reads contents from the specified resource.
     */
    private _readFile(filePath: string): Promise<Buffer> {
        return new Promise<Buffer>(resolve => {
            fs.readFile(filePath, (error: Error, data: Buffer) => {
                if (error) {
                    console.log(`\t--[_readFile] Cannot read file from ${ filePath }`);
                    process.exit();
                }

                resolve(data);
            });
        });
    }

    /**
     * Reads test schema sql file.
     */
    private _readTestSchema(): Promise<Buffer> {
        const testSchemaFilePath: string = path.join(__dirname, '..', 'test_schema.sql');
        return this._readFile(testSchemaFilePath);
    }

    /**
     * Loads test schema into MySQL test database.
     */
    private async _loadTestSchema(conversion: Conversion): Promise<Conversion> {
        const sqlBuffer: Buffer = await this._readTestSchema.bind(this);
        await (<DBAccess>this.dbAccess).query(
            '_loadTestSchema',
            sqlBuffer.toString(),
            DBVendors.MYSQL,
            true,
            false
        );

        return conversion;
    }

    /**
     * Provides a blob for a sake of testing.
     */
    public getTestBlob(conversion: Conversion): Buffer {
        return Buffer.from('Automated tests development is in progress.', conversion._encoding);
    }

    /**
     * Loads test data into MySQL test database.
     */
    private async _loadTestData(conversion: Conversion): Promise<Conversion> {
        const insertParams: any = {
            id_test_unique_index: 7384,
            id_test_composite_unique_index_1: 125,
            id_test_composite_unique_index_2: 234,
            id_test_index: 123,
            int_test_not_null: 123,
            id_test_composite_index_1: 11,
            id_test_composite_index_2: 22,
            json_test_comment: '{"prop1":"First","prop2":2}',
            bit: 1,
            year: 1984,
            bigint: '1234567890123456800',
            float: 12345.5,
            double: 123456789.23,
            numeric: '1234567890',
            decimal: '1234567890',
            char_5: 'fghij',
            varchar_5: 'abcde',
            date: '1984-11-30',
            time: '21:12:33',
            timestamp: '2018-11-11 22:21:20',
            enum: 'e1',
            set: 's2',
            text: 'Test text',
            blob: this.getTestBlob(conversion),
        };

        const insertParamsKeys: string[] = Object.keys(insertParams);
        const sql: string = `INSERT INTO \`table_a\`(${ insertParamsKeys.join(',') }) 
            VALUES(${ insertParamsKeys.map((k: string) => '?').join(',') });`;

        await (<DBAccess>this.dbAccess).query(
            'TestSchemaProcessor::_loadTestData',
            sql,
            DBVendors.MYSQL,
            true,
            false,
            undefined,
            Object.values(insertParams)
        );

        return conversion;
    }

    /**
     * Initializes Conversion instance.
     */
    public async initializeConversion(): Promise<Conversion> {
        const baseDir: string = path.join(__dirname, '..', '..');
        const config: any = await this._app.readConfig(baseDir, 'test_config.json');
        const fullConfig: any = await this._app.readExtraConfig(config, baseDir);
        this.conversion = await this._app.initializeConversion(fullConfig);
        this.conversion._runsInTestMode = true;
        this.conversion._eventEmitter = new EventEmitter();
        this.dbAccess = new DBAccess(this.conversion);
        delete this.conversion._sourceConString.database;
        return this.conversion;
    }

    /**
     * Arranges test migration.
     * "migrationCompleted" event will fire on completion.
     */
    public arrangeTestMigration(conversion: Conversion): void {
        Promise.resolve(conversion)
            .then(this._createTestSourceDb.bind(this))
            .then(this._updateMySqlConnection.bind(this))
            .then(this._loadTestSchema.bind(this))
            .then(this._loadTestData.bind(this))
            .then(readDataTypesMap)
            .then(this._app.createLogsDirectory)
            .then(conversion => (new SchemaProcessor(conversion)).createSchema())
            .then(createStateLogsTable)
            .then(createDataPoolTable)
            .then(loadStructureToMigrate)
            .then(readDataPool)
            .then(pipeData)
            .catch(error => console.log(error));
    }
}
