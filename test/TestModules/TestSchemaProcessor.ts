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
import * as fs from 'node:fs';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';

import Conversion from '../../src/Conversion';
import DBAccess from '../../src/DBAccess';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from '../../src/Types';
import createSchema from '../../src/SchemaProcessor';
import loadStructureToMigrate from '../../src/StructureLoader';
import pipeData from '../../src/DataPipeManager';
import decodeBinaryData from '../../src/BinaryDataDecoder';
import generateReport from '../../src/ReportGenerator';
import { dropDataPoolTable } from '../../src/DataPoolManager';
import { processConstraints } from '../../src/ConstraintsProcessor';
import { createStateLogsTable, dropStateLogsTable } from '../../src/MigrationStateManager';
import { createDataPoolTable, readDataPool } from '../../src/DataPoolManager';
import { checkConnection, getLogo, getConfAndLogsPaths } from '../../src/BootProcessor';
import {
    createLogsDirectory,
    generateError,
    log,
    readConfig,
    readDataAndIndexTypesMap,
    readExtraConfig,
} from '../../src/FsOps';

export default class TestSchemaProcessor {
    /**
     * Instance of class Conversion.
     */
    public conversion?: Conversion;

    /**
     * TestSchemaProcessor constructor.
     */
    public constructor() {
        this.conversion = undefined;
    }

    /**
     * Stops the process in case of fatal error.
     */
    public processFatalError = async (error: string): Promise<void> => {
        console.log(error);
        await generateError(this.conversion as Conversion, error);
        process.exit(1);
    }

    /**
     * Removes resources created by test scripts.
     */
    public removeTestResources = async (): Promise<void> => {
        if (!(this.conversion as Conversion)._removeTestResources) {
            return;
        }

        const logTitle: string = 'TestSchemaProcessor::removeTestResources';
        const sqlDropMySqlDatabase: string = `DROP DATABASE ${ (this.conversion as Conversion)._mySqlDbName };`;
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: logTitle,
            sql: sqlDropMySqlDatabase,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        await DBAccess.query(params);

        params.sql = `DROP SCHEMA ${ (this.conversion as Conversion)._schema } CASCADE;`;
        params.vendor = DBVendors.PG;
        await DBAccess.query(params);
    }

    /**
     * Prevents tests from running if test dbs (both MySQL and PostgreSQL) already exist.
     */
    private _checkResources = async (conversion: Conversion): Promise<Conversion> => {
        const logTitle: string = 'TestSchemaProcessor::_checkResources';

        const sqlIsMySqlDbExist: string = `SELECT EXISTS (SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name = '${ (this.conversion as Conversion)._mySqlDbName }') AS \`exists\`;`;

        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: logTitle,
            sql: sqlIsMySqlDbExist,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        const mySqlResult: DBAccessQueryResult = await DBAccess.query(params);

        const mySqlExists: boolean = !!mySqlResult.data[0].exists;

        params.vendor = DBVendors.PG;
        params.sql = `SELECT EXISTS(SELECT schema_name FROM information_schema.schemata
            WHERE schema_name = '${ (this.conversion as Conversion)._schema }');`;

        const pgResult: DBAccessQueryResult = await DBAccess.query(params);

        const pgExists: boolean = !!pgResult.data.rows[0].exists;
        let msg: string = '';

        if (mySqlExists) {
            msg += `Please, remove '${ (this.conversion as Conversion)._mySqlDbName }' 
                database from your MySQL server prior to running tests.\n`;
        }

        if (pgExists) {
            const schemaName: string = `'${ (this.conversion as Conversion)._targetConString.database }.${ (this.conversion as Conversion)._schema }'`;
            msg += `Please, remove ${ schemaName } schema from your PostgreSQL server prior to running tests.`;
        }

        if (msg) {
            log(this.conversion as Conversion, msg);
            process.exit(0);
        }

        return conversion;
    }

    /**
     * Creates test source database.
     */
    private _createTestSourceDb = async (conversion: Conversion): Promise<Conversion> => {
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: 'TestSchemaProcessor::_createTestSourceDb',
            sql: `CREATE DATABASE IF NOT EXISTS ${ (this.conversion as Conversion)._mySqlDbName };`,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        await DBAccess.query(params);
        return conversion;
    }

    /**
     * Updates the "database" part of MySQL connection.
     */
    private _updateMySqlConnection = (conversion: Conversion): Promise<Conversion> => {
        return new Promise<Conversion>(resolve => {
            conversion._mysql = undefined;
            conversion._sourceConString.database = conversion._mySqlDbName;
            resolve(conversion);
        });
    }

    /**
     * Reads contents from the specified resource.
     */
    private _readFile = (filePath: string): Promise<Buffer> => {
        return new Promise<Buffer>(resolve => {
            fs.readFile(filePath, (error: NodeJS.ErrnoException | null, data: Buffer) => {
                if (error) {
                    console.log(`\t--[_readFile] Cannot read file from ${ filePath }`);
                    process.exit(1);
                }

                resolve(data);
            });
        });
    }

    /**
     * Reads test schema sql file.
     */
    private _readTestSchema = async (): Promise<Buffer> => {
        const testSchemaFilePath: string = path.join(__dirname, '..', '..', '..', 'test', 'test_schema.sql');
        return await this._readFile(testSchemaFilePath);
    }

    /**
     * Loads test schema into MySQL test database.
     */
    private _loadTestSchema = async (conversion: Conversion): Promise<Conversion> => {
        const sqlBuffer: Buffer = await this._readTestSchema();
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: 'TestSchemaProcessor::_loadTestSchema',
            sql: sqlBuffer.toString(),
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        await DBAccess.query(params);
        return conversion;
    }

    /**
     * Provides a blob for a sake of testing.
     */
    public getTestBlob = (conversion: Conversion): Buffer => {
        return Buffer.from('Automated tests development is in progress.', conversion._encoding);
    }

    /**
     * Loads test data into MySQL test database.
     */
    private _loadTestData = async (conversion: Conversion): Promise<Conversion> => {
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
            bigint: '9223372036854775807',
            float: 12345.5,
            double: 123456789.23,
            numeric: '1234567890',
            decimal: '99999999999999999223372036854775807.121111111111111345334523423220',
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

        const insertParamsKeys: string[] = Object.keys(insertParams).map((k: string): string => `\`${ k }\``);
        const valuesToInsert: string = insertParamsKeys.map((_: string): string => '?').join(',');
        const sql: string = `INSERT INTO \`table_a\`(${ insertParamsKeys.join(',') }) VALUES(${ valuesToInsert });`;
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: 'TestSchemaProcessor::_loadTestData',
            sql: sql,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
            client: undefined,
            bindings: Object.values(insertParams),
        };

        await DBAccess.query(params);
        return conversion;
    }

    /**
     * Initializes Conversion instance.
     */
    public initializeConversion = async (): Promise<Conversion> => {
        const { confPath, logsPath } = getConfAndLogsPaths();
        const config: any = await readConfig(confPath, logsPath, 'test_config.json');
        const fullConfig: any = await readExtraConfig(config, confPath);
        this.conversion = await Conversion.initializeConversion(fullConfig);
        this.conversion._runsInTestMode = true;
        this.conversion._eventEmitter = new EventEmitter();
        const logo: string = getLogo();
        console.log(logo);
        delete this.conversion._sourceConString.database;
        return this.conversion;
    }

    /**
     * Arranges test migration.
     * "migrationCompleted" event will fire on completion.
     */
    public arrangeTestMigration = async (conversion: Conversion): Promise<void> => {
        const connectionErrorMessage: string = await checkConnection(conversion);

        if (connectionErrorMessage) {
            console.log(connectionErrorMessage);
            process.exit(1);
        }

        Promise.resolve(conversion)
            .then(this._checkResources.bind(this))
            .then(this._createTestSourceDb.bind(this))
            .then(this._updateMySqlConnection.bind(this))
            .then(this._loadTestSchema.bind(this))
            .then(this._loadTestData.bind(this))
            .then(readDataAndIndexTypesMap)
            .then(createLogsDirectory)
            .then(createSchema)
            .then(createStateLogsTable)
            .then(createDataPoolTable)
            .then(loadStructureToMigrate)
            .then(readDataPool)
            .then(pipeData)
            .then(decodeBinaryData)
            .then(processConstraints)
            .then(dropDataPoolTable)
            .then(dropStateLogsTable)
            .then(DBAccess.closeConnectionPools)
            .then(generateReport);
    }
}
