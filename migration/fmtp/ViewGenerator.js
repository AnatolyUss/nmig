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
 * Attempts to convert MySQL view to PostgreSQL view.
 *
 * @param   {String} schema
 * @param   {String} viewName
 * @param   {String} mysqlViewCode
 * @returns {String}
 */
module.exports = function(schema, viewName, mysqlViewCode) {
    mysqlViewCode        = mysqlViewCode.split('`').join('"');
    let queryStart       = mysqlViewCode.indexOf('AS');
    mysqlViewCode        = mysqlViewCode.slice(queryStart);
    let arrMysqlViewCode = mysqlViewCode.split(' ');
    
    for (let i = 0; i < arrMysqlViewCode.length; ++i) {
        if (
            arrMysqlViewCode[i].toLowerCase() === 'from'
            || arrMysqlViewCode[i].toLowerCase() === 'join'
            && i + 1 < arrMysqlViewCode.length
        ) {
            arrMysqlViewCode[i + 1] = '"' + schema + '".' + arrMysqlViewCode[i + 1];
        }
    }

    return 'CREATE OR REPLACE VIEW "' + schema + '"."' + viewName + '" ' + arrMysqlViewCode.join(' ') + ';';
};
