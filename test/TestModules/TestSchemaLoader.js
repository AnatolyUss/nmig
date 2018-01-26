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
const connect                               = require('../../src/Connector');
const Main                                  = require('../../src/Main');
const SchemaProcessor                       = require('../../src/SchemaProcessor');
const readDataTypesMap                      = require('../../src/DataTypesMapReader');
const loadStructureToMigrate                = require('../../src/StructureLoader');
const pipeData                              = require('../../src/DataPipeManager');
const { createStateLogsTable }              = require('../../src/MigrationStateManager');
const { createDataPoolTable, readDataPool } = require('../../src/DataPoolManager');
const generateError                         = require('../../src/ErrorGenerator');

module.exports = class TestSchemaLoader {

    /**
     * TestSchemaLoader constructor.
     */
    constructor() {
        this._app        = new Main();
        this._testDbName = 'nmig_test_db';
    }

    /**
     * Stops the process in case of fatal error.
     *
     * @param {Conversion} conversion
     * @param {String} error
     *
     * @returns {undefined}
     */
    processFatalError(conversion, error) {
        generateError(conversion, error);
        process.exit();
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

                    connection.query(`CREATE DATABASE IF NOT EXISTS ${ this._testDbName };`, err => {
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
     * Creates test target database.
     *
     * @param {Conversion} conversion
     *
     * @returns {Promise<Conversion>}
     */
    createTestTargetDb(conversion) {
        conversion._pg                       = null;
        conversion._targetConString.database = 'postgres';

        return connect(conversion).then(() => {
            return new Promise(resolve => {
                conversion._pg.connect((error, client, release) => {
                    if (error) {
                        this.processFatalError(conversion, error);
                    }

                    client.query(`SELECT 1 FROM pg_database WHERE datname = '${ this._testDbName }';`, (err, result) => {
                        if (err) {
                            this.processFatalError(conversion, err);
                        }

                        if (result.rows.length === 0) {
                            // Database 'nmig_test_db' does not exist.
                            client.query(`CREATE DATABASE ${ this._testDbName };`, createDbError => {
                                release();

                                if (createDbError) {
                                    this.processFatalError(conversion, createDbError);
                                }

                                resolve(conversion);
                            });

                        } else {
                            release();
                            resolve(conversion);
                        }
                    });
                });
            });
        });
    }

    /**
     * Update the "database" part of both connections.
     *
     * @param {Conversion} conversion
     *
     * @returns {Promise<Conversion>}
     */
    updateDbConnections(conversion) {
        return new Promise(resolve => {
            conversion._mysql                    = null;
            conversion._sourceConString.database = this._testDbName;
            conversion._pg                       = null;
            conversion._targetConString.database = this._testDbName;
            resolve(conversion);
        });
    }

    /**
     * Reads test schema sql file.
     *
     * @returns {Promise<Buffer>}
     */
    readTestSchema() {
        return new Promise(resolve => {
            const testSchemaFilePath = path.join(__dirname, '..', 'test_schema.sql');

            fs.readFile(testSchemaFilePath, (error, sql) => {
                if (error) {
                    console.log(`\t--[readTestSchema] Cannot read test schema from ${ testSchemaFilePath }`);
                    process.exit();
                }

                resolve(sql);
            });
        });
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
            .then(this.readTestSchema)
            .then(sqlBuffer => {
                const sqlStatements = sqlBuffer.toString().split(';');
                const promises      = [];

                for (let i = 0; i < sqlStatements.length; ++i) {
                    promises.push(new Promise(resolve => {
                        conversion._mysql.getConnection((error, connection) => {
                            if (error) {
                                this.processFatalError(conversion, error);
                            }

                            connection.query(`${ sqlStatements[i] };`, err => {
                                connection.release();

                                if (err) {
                                    this.processFatalError(conversion, err);
                                }

                                resolve();
                            });
                        });
                    }));
                }

                return Promise.all(promises).then(() => console.log('Completed!!!'));
            });
    }

    /**
     * Arranges test migration.
     *
     * @returns {undefined}
     */
    arrangeTestMigration() {
        const baseDir = path.join(__dirname, '..', '..');

        this._app.readConfig(baseDir, 'test_config.json')
            .then(config => this._app.readExtraConfig(config, baseDir))
            .then(this._app.initializeConversion)
            .then(this.createTestSourceDb.bind(this))
            .then(this.createTestTargetDb.bind(this))
            .then(this.updateDbConnections.bind(this))
            .then(this.loadTestSchema.bind(this))
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
};
