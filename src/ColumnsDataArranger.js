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
 * Define if given type is one of MySQL spacial types.
 *
 * @param {String} type
 *
 * @returns {Boolean}
 */
const isSpacial = type => {
    return type.indexOf('geometry') !== -1
        || type.indexOf('point') !== -1
        || type.indexOf('linestring') !== -1
        || type.indexOf('polygon') !== -1;
};

/**
 * Define if given type is one of MySQL binary types.
 *
 * @param {String} type
 *
 * @returns {Boolean}
 */
const isBinary = type => {
    return type.indexOf('blob') !== -1 || type.indexOf('binary') !== -1;
};

/**
 * Define if given type is one of MySQL bit types.
 *
 * @param {String} type
 *
 * @returns {Boolean}
 */
const isBit = type => {
    return type.indexOf('bit') !== -1;
};

/**
 * Define if given type is one of MySQL date-time types.
 *
 * @param {String} type
 *
 * @returns {Boolean}
 */
const isDateTime = type => {
    return type.indexOf('timestamp') !== -1 || type.indexOf('date') !== -1;
};

/**
 * Arranges columns data before loading.
 *
 * @param {Array}      arrTableColumns
 * @param {Number}     mysqlVersion
 *
 * @returns {String}
 */
module.exports = (arrTableColumns, mysqlVersion) => {
    let strRetVal               = '';
    const arrTableColumnsLength = arrTableColumns.length;
    const wkbFunc               = mysqlVersion >= 5.76 ? 'ST_AsWKB' : 'AsWKB';

    for (let i = 0; i < arrTableColumnsLength; ++i) {
        const field = arrTableColumns[i].Field;
        const type  = arrTableColumns[i].Type;

        if (isSpacial(type)) {
            strRetVal += 'HEX(' + wkbFunc + '(`' + field + '`)) AS `' + field + '`,';
        } else if (isBinary(type)) {
            strRetVal += 'HEX(`' + field + '`) AS `' + field + '`,';
        } else if (isBit(type)) {
            strRetVal += 'BIN(`' + field + '`) AS `' + field + '`,';
        } else if (isDateTime(type)) {
            strRetVal += 'IF(`' + field +  '` IN(\'0000-00-00\', \'0000-00-00 00:00:00\'), \'-INFINITY\', CAST(`'
                +  field + '` AS CHAR)) AS `' + field + '`,';
        } else {
            strRetVal += '`' + field + '` AS `' + field + '`,';
        }
    }

    return strRetVal.slice(0, -1);
};
