/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>
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

const isIntNumeric = require('./IntegerValidator');

/**
 * Constructor.
 *
 * @param {Object} config
 */
module.exports = function Conversion(config) {
    this._config                = config;
    this._sourceConString       = this._config.source;
    this._targetConString       = this._config.target;
    this._tempDirPath           = this._config.tempDirPath;
    this._logsDirPath           = this._config.logsDirPath;
    this._dataTypesMapAddr      = __dirname + '/DataTypesMap.json';
    this._allLogsPath           = this._logsDirPath + '/all.log';
    this._errorLogsPath         = this._logsDirPath + '/errors-only.log';
    this._notCreatedViewsPath   = this._logsDirPath + '/not_created_views';
    this._copyOnly              = this._config.copy_only;
    this._noVacuum              = this._config.no_vacuum;
    this._excludeTables         = this._config.exclude_tables;
    this._timeBegin             = new Date();
    this._encoding              = this._config.encoding === undefined ? 'utf8' : this._config.encoding;
    this._dataChunkSize         = this._config.data_chunk_size === undefined ? 1 : +this._config.data_chunk_size;
    this._dataChunkSize         = this._dataChunkSize < 1 ? 1 : this._dataChunkSize;
    this._0777                  = '0777';
    this._mysql                 = null;
    this._pg                    = null;
    this._tablesToMigrate       = [];
    this._viewsToMigrate        = [];
    this._tablesCnt             = 0;
    this._viewsCnt              = 0;
    this._dataPool              = [];
    this._dicTables             = Object.create(null);
    this._mySqlDbName           = this._sourceConString.database;
    this._schema                = this._config.schema === undefined ||
                                  this._config.schema === ''
                                  ? this._mySqlDbName
                                  : this._config.schema;

    this._maxPoolSizeSource     = this._config.max_pool_size_source !== undefined &&
                                  isIntNumeric(this._config.max_pool_size_source)
                                  ? +this._config.max_pool_size_source
                                  : 10;

    this._maxPoolSizeTarget     = this._config.max_pool_size_target !== undefined &&
                                  isIntNumeric(this._config.max_pool_size_target)
                                  ? +this._config.max_pool_size_target
                                  : 10;

    this._maxPoolSizeSource     = this._maxPoolSizeSource > 0 ? this._maxPoolSizeSource : 10;
    this._maxPoolSizeTarget     = this._maxPoolSizeTarget > 0 ? this._maxPoolSizeTarget : 10;

    this._pipeWidth             = this._config.pipe_width !== undefined &&
                                  isIntNumeric(this._config.pipe_width)
                                  ? +this._config.pipe_width
                                  : this._maxPoolSizeTarget;

    this._pipeWidth             = this._pipeWidth > this._maxPoolSizeTarget ? this._maxPoolSizeTarget : this._pipeWidth;
    this._loaderMaxOldSpaceSize = this._config.loader_max_old_space_size;
    this._loaderMaxOldSpaceSize = isIntNumeric(this._loaderMaxOldSpaceSize) ? this._loaderMaxOldSpaceSize : 'DEFAULT';

    this._convertTinyintToBoolean = this._config.convert_tinyint_to_boolean;
};
