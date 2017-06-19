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

const log                = require('./Logger');
const generateError      = require('./ErrorGenerator');
const directoriesManager = require('./DirectoriesManager');

/**
 * Closes DB connections.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
const closeConnections = self => {
    return new Promise(resolve => {
        if (self._mysql) {
            self._mysql.end(error => {
                if (error) {
                    log(self, '\t--[closeConnections] ' + error);
                }

                log(self, '\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
                self._pg = null;
                resolve();
            });
        } else {
            log(self, '\t--[closeConnections] All DB connections to both MySQL and PostgreSQL servers have been closed...');
            self._pg = null;
            resolve();
        }
    });
}

/**
 * Closes DB connections and removes the "./temporary_directory".
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports = self => {
    return new Promise(resolve => {
        log(self, '\t--[cleanup] Cleanup resources...');
        return directoriesManager.removeTemporaryDirectory(self).then(() => {
            return closeConnections(self);
        }).then(() => {
            log(self, '\t--[cleanup] Cleanup finished...');
            resolve();
        });
    });
};
