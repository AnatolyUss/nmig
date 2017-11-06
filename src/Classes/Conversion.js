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
'use strict';

const path = require('path');

module.exports = class Conversion {
    /**
     * Constructor.
     *
     * @param {Object} config
     */
    constructor(config) {
        this._config                  = config;
        this._sourceConString         = this._config.source;
        this._targetConString         = this._config.target;
        this._logsDirPath             = this._config.logsDirPath;
        this._dataTypesMapAddr        = this._config.dataTypesMapAddr;
        this._allLogsPath             = path.join(this._logsDirPath, 'all.log');
        this._errorLogsPath           = path.join(this._logsDirPath, 'errors-only.log');
        this._notCreatedViewsPath     = path.join(this._logsDirPath, 'not_created_views');
        this._noVacuum                = this._config.no_vacuum;
        this._excludeTables           = this._config.exclude_tables;
        this._includeTables           = this._config.include_tables;
        this._timeBegin               = new Date();
        this._encoding                = this._config.encoding === undefined ? 'utf8' : this._config.encoding;
        this._dataChunkSize           = this._config.data_chunk_size === undefined ? 1 : +this._config.data_chunk_size;
        this._dataChunkSize           = this._dataChunkSize <= 0 ? 1 : this._dataChunkSize;
        this._0777                    = '0777';
        this._mysql                   = null;
        this._pg                      = null;
        this._mysqlVersion            = '5.6.21'; // Simply a default value.
        this._extraConfig             = this._config.extraConfig;
        this._tablesToMigrate         = [];
        this._viewsToMigrate          = [];
        this._processedChunks         = 0;
        this._dataPool                = [];
        this._dicTables               = Object.create(null);
        this._mySqlDbName             = this._sourceConString.database;
        this._schema                  = this._config.schema === undefined || this._config.schema === ''
            ? this._mySqlDbName
            : this._config.schema;

        this._maxDbConnectionPoolSize = this._config.max_db_connection_pool_size !== undefined && this.isIntNumeric(this._config.max_db_connection_pool_size)
            ? +this._config.max_db_connection_pool_size
            : 10;

        this._maxDbConnectionPoolSize = this._maxDbConnectionPoolSize > 0 ? this._maxDbConnectionPoolSize : 10;
        this._loaderMaxOldSpaceSize   = this._config.loader_max_old_space_size;
        this._loaderMaxOldSpaceSize   = this.isIntNumeric(this._loaderMaxOldSpaceSize) ? this._loaderMaxOldSpaceSize : 'DEFAULT';
        this._migrateOnlyData         = this._config.migrate_only_data;
        this._delimiter               = this._config.delimiter !== undefined && this._config.delimiter.length === 1
            ? this._config.delimiter
            : ',';
    }

    /**
     * Checks if given value is integer number.
     *
     * @param {String|Number} value
     *
     * @returns {Boolean}
     */
    isIntNumeric(value) {
        return !isNaN(parseInt(value)) && isFinite(value);
    }
};
