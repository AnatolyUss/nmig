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
import { EOL } from 'node:os'; // TODO: check if necessary.
import * as fs from 'node:fs';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';
import { Readable, Writable, Duplex as DuplexStream } from 'node:stream';
import * as streamPromises from 'node:stream/promises';

import { faker } from '@faker-js/faker';
const { Transform: Json2CsvTransform } = require('json2csv'); // No declaration file for module "json2csv".

import Conversion from '../src/conversion';
import DbAccess from '../src/db-access';
import createSchema from '../src/schema-processor';
import loadStructureToMigrate from '../src/structure-loader';
import DataPipeManager from '../src/data-pipe-manager';
import decodeBinaryData from '../src/binary-data-decoder';
import generateReport from '../src/report-generator';
import { createDataPoolTable, readDataPool, dropDataPoolTable } from '../src/data-pool-manager';
import { processConstraints } from '../src/constraints-processor';
import { createStateLogsTable, dropStateLogsTable } from '../src/migration-state-manager';
import { checkConnection, getLogo, getDirectoriesPaths } from '../src/boot-processor';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from '../src/types';
import {
    createLogsDirectory,
    generateError,
    log,
    readConfig,
    readDataAndIndexTypesMap,
    readExtraConfig,
} from '../src/fs-ops';

export default class TestSchemaProcessor {
    /**
     * Instance of class Conversion.
     */
    public conversion?: Conversion;

    /**
     * Stops the process in case of fatal error.
     */
    public processFatalError = async (error: string): Promise<void> => {
        console.log(error);
        await generateError(this.conversion as Conversion, error);
        process.exit(1);
    };

    /**
     * Removes resources created by test scripts.
     */
    public removeTestResources = async (): Promise<void> => {
        if (!(this.conversion as Conversion)._removeTestResources) {
            return;
        }

        const sqlDropMySqlDatabase = `DROP DATABASE \`${
            (this.conversion as Conversion)._mySqlDbName
        }\`;`;
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: this.removeTestResources.name,
            sql: sqlDropMySqlDatabase,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        await DbAccess.query(params);

        params.sql = `DROP SCHEMA ${(this.conversion as Conversion)._schema} CASCADE;`;
        params.vendor = DBVendors.PG;
        await DbAccess.query(params);
    };

    /**
     * Prevents tests from running if test dbs (both MySQL and PostgreSQL) already exist.
     */
    private _checkResources = async (conversion: Conversion): Promise<Conversion> => {
        const sqlIsMySqlDbExist = `SELECT EXISTS (SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name = '${(this.conversion as Conversion)._mySqlDbName}') AS \`exists\`;`;

        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: this._checkResources.name,
            sql: sqlIsMySqlDbExist,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        const mySqlResult: DBAccessQueryResult = await DbAccess.query(params);

        const mySqlExists = !!mySqlResult.data[0].exists;

        params.vendor = DBVendors.PG;
        params.sql = `SELECT EXISTS(SELECT schema_name FROM information_schema.schemata
            WHERE schema_name = '${(this.conversion as Conversion)._schema}');`;

        const pgResult: DBAccessQueryResult = await DbAccess.query(params);

        const pgExists = !!pgResult.data.rows[0].exists;
        let msg = '';

        if (mySqlExists) {
            msg += `Please, remove '${(this.conversion as Conversion)._mySqlDbName}' 
                database from your MySQL server prior to running tests.\n`;
        }

        if (pgExists) {
            const schemaName = `'${(this.conversion as Conversion)._targetConString.database}.${
                (this.conversion as Conversion)._schema
            }'`;
            msg += `Please, remove ${schemaName} schema from your PostgreSQL server prior to running tests.`;
        }

        if (msg) {
            await log(this.conversion as Conversion, msg);
            process.exit(0);
        }

        return conversion;
    };

    /**
     * Creates test source database.
     */
    private _createTestSourceDb = async (conversion: Conversion): Promise<Conversion> => {
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: this._createTestSourceDb.name,
            sql: `CREATE DATABASE IF NOT EXISTS \`${
                (this.conversion as Conversion)._mySqlDbName
            }\`;`,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        await DbAccess.query(params);
        return conversion;
    };

    /**
     * Updates the "database" part of MySQL connection.
     */
    private _updateMySqlConnection = (conversion: Conversion): Promise<Conversion> => {
        return new Promise<Conversion>(resolve => {
            conversion._mysql = undefined;
            conversion._sourceConString.database = conversion._mySqlDbName;
            resolve(conversion);
        });
    };

    /**
     * Reads contents from the specified resource.
     */
    private _readFile = (filePath: string): Promise<Buffer> => {
        return new Promise<Buffer>(resolve => {
            fs.readFile(filePath, (error: NodeJS.ErrnoException | null, data: Buffer) => {
                if (error) {
                    console.log(`\t--[_readFile] Cannot read file from ${filePath}`);
                    process.exit(1);
                }

                resolve(data);
            });
        });
    };

    /**
     * Reads test schema sql file.
     */
    private _readTestSchema = async (): Promise<Buffer> => {
        const testSchemaFilePath: string = path.join(
            __dirname,
            '..',
            '..',
            'test',
            'test-schema.sql',
        );

        return await this._readFile(testSchemaFilePath);
    };

    /**
     * Loads test schema into MySQL test database.
     */
    private _loadTestSchema = async (conversion: Conversion): Promise<Conversion> => {
        const sqlBuffer: Buffer = await this._readTestSchema();
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: this._loadTestSchema.name,
            sql: sqlBuffer.toString(),
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
        };

        await DbAccess.query(params);
        return conversion;
    };

    /**
     * Provides a blob for a sake of testing.
     */
    public getTestBlob = (conversion: Conversion): Buffer => {
        return Buffer.from('Automated tests development is in progress.', conversion._encoding);
    };

    /**
     * Creates a test-data CSV file, which will be eventually loaded into MySQL.
     * TODO: cleanup.
     */
    private createTestDataFile = async (conversion: Conversion): Promise<void> => {
        const streamOptions: Record<string, any> = {
            highWaterMark: conversion._streamsHighWaterMark,
            objectMode: true,
            encoding: conversion._encoding,
        };

        const dataGenerator = this.getDataGenerator(conversion);
        const dataGeneratorStream = Readable.from(dataGenerator(), streamOptions);

        // TODO: remove asap.
        // let x = '';
        // for await (const chunk of dataGeneratorStream) {
        //     x += chunk;
        // }
        // console.log(x);

        const json2csvStream = this.getJson2csvStream(streamOptions);
        const fileWriterStream = this.getFileWriterStream(conversion, streamOptions);
        // await streamPromises.pipeline(dataGeneratorStream, json2csvStream, fileWriterStream);
        try {
            await streamPromises.pipeline(dataGeneratorStream, fileWriterStream);
        } catch (e) {
            console.error(e);
        }
    };

    /**
     * Returns file writer stream.
     */
    private getFileWriterStream = (
        conversion: Conversion,
        streamOptions: Record<string, any>,
    ): Writable => {
        return fs.createWriteStream(
            path.join(
                conversion.testDataPath as string,
                `test-data_${conversion.numberOfRecords}.csv`,
            ),
            {
                ...streamOptions,
                flags: 'w', // Note, the file is truncated, if it exists.
            },
        );
    };

    /**
     * Returns stream, transforming JSON to CSV.
     */
    private getJson2csvStream = (streamOptions: Record<string, any>): DuplexStream => {
        const options: Record<string, any> = {
            delimiter: ',',
            header: false,
            // TODO: find a way to initialize fields dynamically, if needed.
            fields: [
                'id_test_unique_index',
                'id_test_composite_unique_index_1',
                'id_test_composite_unique_index_2',
                'id_test_index',
                'int_test_not_null',
                'id_test_composite_index_1',
                'id_test_composite_index_2',
                'json_test_comment',
                'bit',
                'year',
                'bigint',
                'float',
                'double',
                'numeric',
                'decimal',
                'char_5',
                'varchar_5',
                'date',
                'time',
                'timestamp',
                'enum',
                'set',
                'text',
                'blob',
            ],
        };

        return new Json2CsvTransform(options, streamOptions);
    };

    /**
     * Returns a test data generator.
     */
    private getDataGenerator = (conversion: Conversion): (() => Generator<Record<string, any>>) => {
        return function* (): Generator<Record<string, any>> {
            const getRandomFloat = (min: number, max: number): number =>
                Math.random() < 0.5
                    ? (1 - Math.random()) * (max - min) + min
                    : Math.random() * (max - min) + min;

            const getRandomInt = (min: number, max: number): number =>
                Math.floor(getRandomFloat(min, max));

            const enumValue = ['e1', 'e2'][getRandomInt(0, 1)];
            const setValue = ['s1', 's2'][getRandomInt(0, 1)];

            for (let i = 0; i < conversion.numberOfRecords; ++i) {
                const record: Record<string, any> = {
                    id_test_unique_index: i + 1,
                    id_test_composite_unique_index_1: i + 2,
                    id_test_composite_unique_index_2: i + 3,
                    id_test_index: i + 4,
                    int_test_not_null: i,
                    id_test_composite_index_1: i + 1,
                    id_test_composite_index_2: i + 2,
                    json_test_comment: `{"prop1${i}":"${faker.lorem.word()}","prop2${i}":${getRandomInt(
                        4,
                        999,
                    )}}`,
                    bit: getRandomInt(0, 1),
                    year: getRandomInt(1934, 2024),
                    bigint: `${getRandomInt(1934, 20242347)}` + `${getRandomInt(1934, 20242347)}`,
                    float: +getRandomFloat(12.43, 27836.21).toFixed(2),
                    double: +getRandomFloat(1223.43, 278362344.21).toFixed(2),
                    numeric: `${getRandomFloat(1223.43, 278362344.21)}`,
                    decimal:
                        `${getRandomInt(1934, 20242347)}` +
                        `${getRandomInt(1934, 20242347)}.` +
                        `${getRandomInt(1934, 20242347)}` +
                        `${getRandomInt(1934, 20242347)}`,

                    char_5: faker.lorem.word({ strategy: 'shortest', length: { min: 1, max: 5 } }),
                    varchar_5: faker.lorem.word({
                        strategy: 'shortest',
                        length: { min: 1, max: 5 },
                    }),

                    date: faker.date
                        .between({
                            from: '2000-01-01T00:00:00.000Z',
                            to: '2030-01-01T00:00:00.000Z',
                        })
                        .toISOString()
                        .split('T')[0],

                    time: faker.date
                        .between({
                            from: '2000-01-01T00:00:00.000Z',
                            to: '2030-01-01T00:00:00.000Z',
                        })
                        .toISOString()
                        .split('T')[1]
                        .split('.')[0],

                    timestamp: faker.date
                        .between({
                            from: '2000-01-01T00:00:00.000Z',
                            to: '2030-01-01T00:00:00.000Z',
                        })
                        .toISOString(),

                    enum: enumValue,
                    set: setValue,
                    text: faker.lorem.sentences({ min: 100, max: 1000 }),
                    blob: Buffer.from(
                        faker.lorem.sentences({ min: 100, max: 1000 }),
                        conversion._encoding,
                    ),
                };

                // TODO: cleanup.
                yield `${i}`;
                //
                // const buffer = Buffer.from(Object.values(`${i}`).join(','));
                // const arrayBuffer = new ArrayBuffer(buffer.length);
                // const view = new Uint8Array(arrayBuffer);
                // for (let x = 0; x < buffer.length; ++x) {
                //     view[x] = buffer[x];
                // }
                // yield arrayBuffer;
                //
                // yield record;
                //
                // yield Buffer.from(Object.values(record).join(','));
                //
                // const buffer = Buffer.from(Object.values(record).join(','));
                // const arrayBuffer = new ArrayBuffer(buffer.length);
                // const view = new Uint8Array(arrayBuffer);
                // for (let x = 0; x < buffer.length; ++x) {
                //     view[x] = buffer[x];
                // }
                // yield arrayBuffer;
                //
                // yield Buffer.from(Object.values(record).join(','), conversion._encoding);
                // const buf = Buffer.from(Object.values(record).join(','), conversion._encoding);
                //
                // const buf = Object.values(record).join(',') + EOL;
                // yield buf;
            }
        };
    };

    /**
     * Loads test data into MySQL test database.
     */
    private _loadTestData = async (conversion: Conversion): Promise<Conversion> => {
        const insertParams: Record<string, any> = {
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

        const insertParamsKeys: string[] = Object.keys(insertParams).map(
            (k: string): string => `\`${k}\``,
        );
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const valuesToInsert: string = insertParamsKeys.map((_: string): string => '?').join(',');
        const sql = `INSERT INTO \`table_a\`(${insertParamsKeys.join(
            ',',
        )}) VALUES(${valuesToInsert});`;
        const params: DBAccessQueryParams = {
            conversion: this.conversion as Conversion,
            caller: this._loadTestData.name,
            sql: sql,
            vendor: DBVendors.MYSQL,
            processExitOnError: true,
            shouldReturnClient: false,
            client: undefined,
            bindings: Object.values(insertParams),
        };

        await DbAccess.query(params);
        return conversion;
    };

    /**
     * Initializes Conversion instance.
     */
    public initializeConversion = async (): Promise<Conversion> => {
        const { confPath, logsPath, testDataPath } = getDirectoriesPaths();
        const config: Record<string, any> = await readConfig(
            confPath,
            logsPath,
            'test_config.json',
        );

        const fullConfig: Record<string, any> = await readExtraConfig(config, confPath);
        this.conversion = await Conversion.initializeConversion(fullConfig);
        this.conversion.testDataPath = testDataPath;
        this.conversion._runsInTestMode = true;
        this.conversion._eventEmitter = new EventEmitter();
        console.log(getLogo());
        delete this.conversion._sourceConString.database;
        return this.conversion;
    };

    /**
     * Arranges test activities.
     * "migrationCompleted" event will fire on completion, when running test-suites.
     */
    public arrange = async (
        conversion: Conversion,
        dataGenerationMode: boolean = false, // eslint-disable-line @typescript-eslint/no-inferrable-types
    ): Promise<void> => {
        const connectionErrorMessage: string = await checkConnection(conversion);

        if (connectionErrorMessage) {
            console.log(connectionErrorMessage);
            process.exit(1);
        }

        conversion = await Promise.resolve(conversion)
            .then(this._checkResources.bind(this))
            .then(this._createTestSourceDb.bind(this))
            .then(this._updateMySqlConnection.bind(this))
            .then(this._loadTestSchema.bind(this))
            .then(this._loadTestData.bind(this))
            .then(readDataAndIndexTypesMap)
            .then(createLogsDirectory);

        if (dataGenerationMode) {
            await this.createTestDataFile(conversion);
        } else {
            conversion = await Promise.resolve(conversion)
                .then(createSchema)
                .then(createStateLogsTable)
                .then(createDataPoolTable)
                .then(loadStructureToMigrate)
                .then(readDataPool)
                .then(DataPipeManager.runDataPipe)
                .then(decodeBinaryData)
                .then(processConstraints)
                .then(dropDataPoolTable)
                .then(dropStateLogsTable);
        }

        Promise.resolve(conversion)
            .then(DbAccess.closeConnectionPools)
            .then(generateReport)
            .catch((error: Error) => console.log(`\t--[${this.arrange.name}] error: ${error}`));
    };
}
