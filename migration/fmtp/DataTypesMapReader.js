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

const fs = require('fs');

/**
 * Reads "./DataTypesMap.json" and converts its json content to js object.
 * Appends this object to "FromMySQL2PostgreSQL" instance.
 *
 * @param   {Conversion} self
 * @returns {Promise}
 */
module.exports = function(self) {
    return new Promise((resolve, reject) => {
        fs.readFile(self._dataTypesMapAddr, (error, data) => {
            if (error) {
                console.log('\t--[readDataTypesMap] Cannot read "DataTypesMap" from ' + self._dataTypesMapAddr);
                reject();
            } else {
                try {
                    self._dataTypesMap = JSON.parse(data.toString());
                    console.log('\t--[readDataTypesMap] Data Types Map is loaded...');
                    if(self._convertTinyintToBoolean) {
                        self._dataTypesMap.tinyint.increased_size = '';
                        self._dataTypesMap.tinyint.type           = 'boolean';
                        console.log('\t--[readDataTypesMap] Will transform tinyint fields to boolean...');
                    }
                    resolve();
                } catch (err) {
                    console.log('\t--[readDataTypesMap] Cannot parse JSON from' + self._dataTypesMapAddr);
                    reject();
                }
            }
        });
    });
};
