/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * This program is free software= you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful;
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).
 * If not; see <http=//www.gnu.org/licenses/gpl.txt>.
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
import * as path from 'path';
import { EventEmitter } from 'events';
import { Pool as MySQLPool } from 'mysql';
import { Pool as PgPool } from 'pg';
import { Encoding } from './Encoding';

export default class Conversion {
    /**
     * Parsed Nmig's configuration object.
     */
    public readonly _config: any;

    /**
     * An object, representing source (MySQL) db connection details.
     */
    public readonly _sourceConString: any;

    /**
     * An object, representing target (PostgreSQL) db connection details.
     */
    public readonly _targetConString: any;

    /**
     * V8 memory limit of the loader process.
     */
    public _loaderMaxOldSpaceSize: number | string;

    /**
     * Maximal amount of simultaneous connections to your MySQL and PostgreSQL servers each.
     */
    public readonly _maxEachDbConnectionPoolSize: number;

    /**
     * JavaScript encoding type.
     */
    public readonly _encoding: Encoding;

    /**
     * The path to the "all.log" file.
     */
    public readonly _allLogsPath: string;

    /**
     * Default file permissions.
     */
    public readonly _0777: string;

    /**
     * Specifies the character, that separates columns within each record.
     */
    public readonly _delimiter: string;

    /**
     * Indicates if the schema in the target database is preset.
     */
    public readonly _migrateOnlyData: boolean;

    /**
     * A path to the "logs_directory".
     */
    public readonly _logsDirPath: string;

    /**
     * A path to the data types map.
     */
    public readonly _dataTypesMapAddr: string;

    /**
     * A path to the index types map.
     */
    public readonly _indexTypesMapAddr: string;

    /**
     * A path to the "errors-only.log" file.
     */
    public readonly _errorLogsPath: string;

    /**
     * A path to the "not_created_views" folder.
     */
    public readonly _notCreatedViewsPath: string;

    /**
     * List of tables, that will not be migrated.
     */
    public readonly _excludeTables: string[];

    /**
     * List of tables, that will be migrated.
     */
    public readonly _includeTables: string[];

    /**
     * The timestamp, at which the migration began.
     */
    public _timeBegin: Date;

    /**
     * Current version of source (MySQL) db.
     */
    public _mysqlVersion: string | number;

    /**
     * Node-MySQL connections pool.
     */
    public _mysql?: MySQLPool;

    /**
     * Node-Postgres connection pool.
     */
    public _pg?: PgPool;

    /**
     * An object, representing additional configuration options.
     */
    public readonly _extraConfig: any;

    /**
     * A list of tables, that should be migrated.
     */
    public readonly _tablesToMigrate: string[];

    /**
     * A list of views, that should be migrated.
     */
    public readonly _viewsToMigrate: string[];

    /**
     * A name of the schema, that will contain all migrated tables.
     */
    public readonly _schema: string;

    /**
     * A name of source (MySQL) db, that should be migrated.
     */
    public readonly _mySqlDbName: string;

    /**
     * A dictionary of table names, and corresponding metadata.
     */
    public readonly _dicTables: any;

    /**
     * An array of data chunks.
     */
    public readonly _dataPool: any[];

    /**
     * A flag, that indicates if Nmig currently runs in test mode.
     */
    public _runsInTestMode: boolean;

    /**
     * A flag, that indicates if test resources created by Nmig should be removed.
     */
    public readonly _removeTestResources: boolean;

    /**
     * "migrationCompleted" event.
     */
    public readonly _migrationCompletedEvent: string;

    /**
     * An EventEmitter instance.
     */
    public _eventEmitter: EventEmitter | null;

    /**
     * The data types map.
     */
    public _dataTypesMap: any;

    /**
     * The index types map.
     */
    public _indexTypesMap: any;

    /**
     * Buffer level when stream.write() starts returning false.
     * This number is a number of JavaScript objects.
     */
    public readonly _streamsHighWaterMark: number;

    /**
     * Constructor.
     */
    public constructor(config: any) {
        this._config = config;
        this._sourceConString = this._config.source;
        this._targetConString = this._config.target;
        this._logsDirPath = this._config.logsDirPath;
        this._dataTypesMapAddr = this._config.dataTypesMapAddr;
        this._indexTypesMapAddr = this._config.indexTypesMapAddr;
        this._allLogsPath = path.join(this._logsDirPath, 'all.log');
        this._errorLogsPath = path.join(this._logsDirPath, 'errors-only.log');
        this._notCreatedViewsPath = path.join(this._logsDirPath, 'not_created_views');
        this._excludeTables = this._config.exclude_tables === undefined ? [] : this._config.exclude_tables;
        this._includeTables = this._config.include_tables === undefined ? [] : this._config.include_tables;
        this._timeBegin = new Date();
        this._encoding = this._config.encoding === undefined ? 'utf8' : this._config.encoding;
        this._0777 = '0777';
        this._mysqlVersion = '5.6.21'; // Simply a default value.
        this._extraConfig = this._config.extraConfig === undefined ? false : this._config.extraConfig;
        this._tablesToMigrate = [];
        this._viewsToMigrate = [];
        this._dataPool = [];
        this._dicTables = Object.create(null);
        this._mySqlDbName = this._sourceConString.database;
        this._streamsHighWaterMark = this._config.streams_high_water_mark === undefined ? 16384 : +this._config.streams_high_water_mark;

        this._schema = this._config.schema === undefined || this._config.schema === ''
            ? this._mySqlDbName
            : this._config.schema;

        this._maxEachDbConnectionPoolSize = this._config.max_each_db_connection_pool_size !== undefined && Conversion._isIntNumeric(this._config.max_each_db_connection_pool_size)
            ? +this._config.max_each_db_connection_pool_size
            : 20;

        this._runsInTestMode = false;
        this._eventEmitter = null;
        this._migrationCompletedEvent = 'migrationCompleted';
        this._removeTestResources = this._config.remove_test_resources === undefined ? true : this._config.remove_test_resources;
        this._maxEachDbConnectionPoolSize = this._maxEachDbConnectionPoolSize > 0 ? this._maxEachDbConnectionPoolSize : 20;
        this._loaderMaxOldSpaceSize = this._config.loader_max_old_space_size;
        this._loaderMaxOldSpaceSize = Conversion._isIntNumeric(this._loaderMaxOldSpaceSize) ? this._loaderMaxOldSpaceSize : 'DEFAULT';
        this._migrateOnlyData = this._config.migrate_only_data === undefined ? false : this._config.migrate_only_data;
        this._delimiter = this._config.delimiter !== undefined && this._config.delimiter.length === 1
            ? this._config.delimiter
            : ',';
    }

    /**
     * Checks if there are actions to take other than data migration.
     */
    public shouldMigrateOnlyData(): boolean {
        return this._migrateOnlyData;
    }

    /**
     * Checks if given value is integer number.
     */
    private static _isIntNumeric(value: any): boolean {
        return !isNaN(parseInt(value)) && isFinite(value);
    }

    /**
     * Initializes Conversion instance.
     */
    public static initializeConversion(config: any): Promise<Conversion> {
        return Promise.resolve(new Conversion(config));
    }
}
