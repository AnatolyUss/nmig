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

const fs                                    = require('fs');
const path                                  = require('path');
const { EventEmitter }                      = require('events');
const connect                               = require('../../src/Connector');
const Main                                  = require('../../src/Main');
const SchemaProcessor                       = require('../../src/SchemaProcessor');
const readDataTypesMap                      = require('../../src/DataTypesMapReader');
const loadStructureToMigrate                = require('../../src/StructureLoader');
const pipeData                              = require('../../src/DataPipeManager');
const { createStateLogsTable }              = require('../../src/MigrationStateManager');
const { createDataPoolTable, readDataPool } = require('../../src/DataPoolManager');
const generateError                         = require('../../src/ErrorGenerator');

module.exports = class TestSchemaProcessor {

    /**
     * TestSchemaProcessor constructor.
     */
    constructor() {
        this._app        = new Main();
        this._conversion = null;
    }

    /**
     * Stops the process in case of fatal error.
     *
     * @param {Conversion} conversion
     * @param {String}     error
     *
     * @returns {undefined}
     */
    processFatalError(conversion, error) {
        console.log(error);
        generateError(conversion, error);
        process.exit();
    }

    /**
     * Removes resources created by test scripts.
     *
     * @returns {Promise<any>}
     */
    removeTestResources() {
        return new Promise(resolve => {
            if (!this._conversion._removeTestResources) {
                return resolve();
            }

            return connect(this._conversion).then(() => {
                this._conversion._mysql.getConnection((mysqlConErr, connection) => {
                    if (mysqlConErr) {
                        // The connection is undefined.
                        this.processFatalError(this._conversion, mysqlConErr);
                    }

                    connection.query(`DROP DATABASE ${ this._conversion._mySqlDbName };`, mysqlDropErr => {
                        connection.release();

                        if (mysqlDropErr) {
                            // Failed to drop test source database.
                            this.processFatalError(this._conversion, mysqlDropErr);
                        }

                        this._conversion._pg.connect((pgConErr, client, release) => {
                            if (pgConErr) {
                                //The connection is undefined.
                                this.processFatalError(this._conversion, pgConErr);
                            }

                            client.query(`DROP SCHEMA ${ this._conversion._schema } CASCADE;`, pgDropErr => {
                                release();

                                if (pgDropErr) {
                                    // Failed to drop test target schema.
                                    this.processFatalError(this._conversion, pgDropErr);
                                }

                                resolve();
                            });
                        });
                    });
                });
            });
        });
    }

    /**
     * Creates test source database.
     *
     * @param {Conversion} conversion
     *
     * @returns {Promise<Conversion>}
     */
    createTestSourceDb(conversion) {
        return connect(conversion).then(() => {
            return new Promise(resolve => {
                conversion._mysql.getConnection((error, connection) => {
                    if (error) {
                        // The connection is undefined.
                        this.processFatalError(conversion, error);
                    }

                    connection.query(`CREATE DATABASE IF NOT EXISTS ${ this._conversion._mySqlDbName };`, err => {
                        connection.release();

                        if (err) {
                            // Failed to create test source database.
                            this.processFatalError(conversion, err);
                        }

                        resolve(conversion);
                    });
                });
            });
        });
    }

    /**
     * Update the "database" part of MySQL connection.
     *
     * @param {Conversion} conversion
     *
     * @returns {Promise<Conversion>}
     */
    updateMySqlConnection(conversion) {
        return new Promise(resolve => {
            conversion._mysql                    = null;
            conversion._sourceConString.database = conversion._mySqlDbName;
            resolve(conversion);
        });
    }

    /**
     * Reads contents from the specified resource.
     *
     * @param {String} filePath
     *
     * @returns {Promise<Buffer>}
     */
    readFile(filePath) {
        return new Promise(resolve => {
            fs.readFile(filePath, (error, data) => {
                if (error) {
                    console.log(`\t--[readFile] Cannot read file from ${ filePath }`);
                    process.exit();
                }

                resolve(data);
            });
        });
    }

    /**
     * Reads test schema sql file.
     *
     * @returns {Promise<Buffer>}
     */
    readTestSchema() {
        const testSchemaFilePath = path.join(__dirname, '..', 'test_schema.sql');
        return this.readFile(testSchemaFilePath);
    }

    /**
     * Loads test schema into MySQL test database.
     *
     * @param {Conversion} conversion
     *
     * @returns {Promise<Conversion>}
     */
    loadTestSchema(conversion) {
        return connect(conversion)
            .then(this.readTestSchema.bind(this))
            .then(sqlBuffer => {
                return new Promise(resolve => {
                    conversion._mysql.getConnection((error, connection) => {
                        if (error) {
                            this.processFatalError(conversion, error);
                        }

                        connection.query(sqlBuffer.toString(), err => {
                            connection.release();

                            if (err) {
                                this.processFatalError(conversion, err);
                            }

                            resolve(conversion);
                        });
                    });
                });
            });
    }

    /**
     * Provides a blob for a sake of testing.
     *
     * @param {Conversion} conversion
     *
     * @returns {Buffer}
     */
    getTestBlob(conversion) {
        return Buffer.from('Automated tests development is in progress.', conversion._encoding);
    }

    /**
     * Loads test data into MySQL test database.
     *
     * @param {Conversion} conversion
     *
     * @returns {Promise<Conversion>}
     */
    loadTestData(conversion) {
        return connect(conversion).then(() => {
            return new Promise(resolve => {
                conversion._mysql.getConnection((error, connection) => {
                    if (error) {
                        this.processFatalError(conversion, error);
                    }

                    const insertParams = {
                        id_test_unique_index             : 7384,
                        id_test_composite_unique_index_1 : 125,
                        id_test_composite_unique_index_2 : 234,
                        id_test_index                    : 123,
                        int_test_not_null                : 123,
                        id_test_composite_index_1        : 11,
                        id_test_composite_index_2        : 22,
                        json_test_comment                : '{"prop1":"First","prop2":2}',
                        bit                              : 1,
                        year                             : 1984,
                        bigint                           : 1234567890123456789,
                        float                            : 12345.56,
                        double                           : 123456789.23,
                        numeric                          : 1234567890,
                        decimal                          : 1234567890,
                        char_5                           : 'fghij',
                        varchar_5                        : 'abcde',
                        date                             : '1984-07-30',
                        time                             : '21:12:33',
                        timestamp                        : '2018-01-01 22:21:20',
                        enum                             : 'e1',
                        set                              : 's2',
                        text                             : 'Test text',
                        blob                             : this.getTestBlob(conversion),
                    };

                    connection.query('INSERT INTO `table_a` SET ?;', insertParams, err => {
                        connection.release();

                        if (err) {
                            this.processFatalError(conversion, err);
                        }

                        resolve(conversion);
                    });
                });
            });
        });
    }

    /**
     * Initializes Conversion instance.
     *
     * @returns {Promise<Conversion>}
     */
    initializeConversion() {
        const baseDir = path.join(__dirname, '..', '..');

        return this._app.readConfig(baseDir, 'test_config.json')
            .then(config => this._app.readExtraConfig(config, baseDir))
            .then(this._app.initializeConversion)
            .then(conversion => {
                this._conversion                 = conversion;
                this._conversion._runsInTestMode = true;
                this._conversion._eventEmitter   = new EventEmitter();
                delete this._conversion._sourceConString.database;
                return Promise.resolve(this._conversion);
            });
    }

    /**
     * Arranges test migration.
     * "migrationCompleted" event will fire on completion.
     *
     * @param {Conversion} conversion
     *
     * @returns {undefined}
     */
    arrangeTestMigration(conversion) {
        Promise.resolve(conversion)
            .then(this.createTestSourceDb.bind(this))
            .then(this.updateMySqlConnection.bind(this))
            .then(this.loadTestSchema.bind(this))
            .then(this.loadTestData.bind(this))
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

    /**
     * Query PostgreSQL server.
     *
     * @param {String} sql
     *
     * @returns {Promise<pg.Result>}
     */
    queryPg(sql) {
        return connect(this._conversion).then(() => {
            return new Promise(resolve => {
                this._conversion._pg.connect((error, client, release) => {
                    if (error) {
                        this.processFatalError(this._conversion, error);
                    }

                    client.query(sql, (err, data) => {
                        release();

                        if (err) {
                            this.processFatalError(this._conversion, err);
                        }

                        resolve(data);
                    });
                });
            });
        });
    }
};
