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

const fs   = require('fs');
const path = require('path');
const main = require('./migration/fmtp/Main');

const strPathToConfig = path.join(__dirname, 'config.json');

fs.readFile(strPathToConfig, (error, data) => {
    if (error) {
        console.log('\n\t--Cannot run migration\nCannot read configuration info from ' + strPathToConfig);
    } else {
        try {
            const config            = JSON.parse(data.toString());
            config.tempDirPath      = path.join(__dirname, 'temporary_directory');
            config.logsDirPath      = path.join(__dirname, 'logs_directory');
            config.dataTypesMapAddr = path.join(__dirname, 'DataTypesMap.json');
            main(config);
        } catch (err) {
            console.log('\n\t--Cannot parse JSON from ' + strPathToConfig);
        }
    }
});
