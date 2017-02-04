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

/**
 * Get current table's name.
 *
 * @param {Conversion} self
 * @param {String}     currentTableName
 * @param {Boolean}    shouldGetOriginal
 *
 * @returns {String}
 */
module.exports.getTableName = (self, currentTableName, shouldGetOriginal) => {
    if (self._extraConfig !== null && 'tables' in self._extraConfig) {
        for (let i = 0; i < self._extraConfig.tables.length; ++i) {
            if ((shouldGetOriginal ? self._extraConfig.tables[i].name.new : self._extraConfig.tables[i].name.original) === currentTableName) {
                return shouldGetOriginal ? self._extraConfig.tables[i].name.original : self._extraConfig.tables[i].name.new;
            }
        }
    }

    return currentTableName;
};

/**
 * Get current column's name.
 *
 * @param {Conversion} self
 * @param {String}     originalTableName
 * @param {String}     currentColumnName
 * @param {Boolean}    shouldGetOriginal
 *
 * @returns {String}
 */
module.exports.getColumnName = (self, originalTableName, currentColumnName, shouldGetOriginal) => {
    if (self._extraConfig !== null && 'tables' in self._extraConfig) {
        for (let i = 0; i < self._extraConfig.tables.length; ++i) {
            if (self._extraConfig.tables[i].name.original === originalTableName && 'columns' in self._extraConfig.tables[i]) {
                for (let columnsCount = 0; columnsCount < self._extraConfig.tables[i].columns.length; ++columnsCount) {
                    if (self._extraConfig.tables[i].columns[columnsCount].original === currentColumnName) {
                        return shouldGetOriginal
                            ? self._extraConfig.tables[i].columns[columnsCount].original
                            : self._extraConfig.tables[i].columns[columnsCount].new;
                    }
                }
            }
        }
    }

    return currentColumnName;
};
