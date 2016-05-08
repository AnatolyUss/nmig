/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2016 Anatoly Khaytovich <anatolyuss@gmail.com>
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
 * Arranges columns data before loading.
 *
 * @param   {Array} arrTableColumns
 * @returns {String}
 */
module.exports = function(arrTableColumns) {
    let strRetVal = '';

    for (let i = 0; i < arrTableColumns.length; ++i) {
        if (
            arrTableColumns[i].Type.indexOf('geometry') !== -1
            || arrTableColumns[i].Type.indexOf('point') !== -1
            || arrTableColumns[i].Type.indexOf('linestring') !== -1
            || arrTableColumns[i].Type.indexOf('polygon') !== -1
        ) {
            strRetVal += 'HEX(ST_AsWKB(`' + arrTableColumns[i].Field + '`)),';
        } else if (
            arrTableColumns[i].Type.indexOf('blob') !== -1
            || arrTableColumns[i].Type.indexOf('binary') !== -1
        ) {
            strRetVal += 'HEX(`' + arrTableColumns[i].Field + '`),';
        } else if (
            arrTableColumns[i].Type.indexOf('bit') !== -1
        ) {
            strRetVal += 'BIN(`' + arrTableColumns[i].Field + '`),';
        } else if (
            arrTableColumns[i].Type.indexOf('timestamp') !== -1
            || arrTableColumns[i].Type.indexOf('date') !== -1
        ) {
            strRetVal += 'IF(`' + arrTableColumns[i].Field
                      +  '` IN(\'0000-00-00\', \'0000-00-00 00:00:00\'), \'-INFINITY\', `'
                      +  arrTableColumns[i].Field + '`),';
        } else {
            strRetVal += '`' + arrTableColumns[i].Field + '`,';
        }
    }

    return strRetVal.slice(0, -1);
};
