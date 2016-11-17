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

const fs  = require('fs');
const log = require('./Logger');

/**
 * Creates temporary directory.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.createTemporaryDirectory = function(self) {
    return new Promise((resolve, reject) => {
        log(self, '\t--[DirectoriesManager.createTemporaryDirectory] Creating temporary directory...');
        fs.stat(self._tempDirPath, (directoryDoesNotExist, stat) => {
            if (directoryDoesNotExist) {
                fs.mkdir(self._tempDirPath, self._0777, e => {
                    if (e) {
                        let msg = '\t--[DirectoriesManager.createTemporaryDirectory] Cannot perform a migration due to impossibility to create '
                                + '"temporary_directory": ' + self._tempDirPath;

                        log(self, msg);
                        reject();
                    } else {
                        log(self, '\t--[DirectoriesManager.createTemporaryDirectory] Temporary directory is created...');
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                log(self, '\t--[DirectoriesManager.createTemporaryDirectory] Cannot perform a migration due to unexpected error');
                reject();
            } else {
                resolve();
            }
        });
    });
};

/**
 * Removes temporary directory.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.removeTemporaryDirectory = function(self) {
    return new Promise(resolve => {
        fs.readdir(self._tempDirPath, (err, arrContents) => {
            let msg = '';

            if (err) {
                msg = '\t--[DirectoriesManager.removeTemporaryDirectory] Note, TemporaryDirectory located at "'
                    + self._tempDirPath + '" is not removed \n\t--[DirectoriesManager.removeTemporaryDirectory] ' + err;

                log(self, msg);
                resolve();

            } else {
                let promises = [];

                for (let i = 0; i < arrContents.length; ++i) {
                    promises.push(new Promise(resolveUnlink => {
                        fs.unlink(self._tempDirPath + '/' + arrContents[i], () => resolveUnlink());
                    }));
                }

                Promise.all(promises).then(() => {
                    fs.rmdir(self._tempDirPath, error => {
                        if (error) {
                            msg = '\t--[DirectoriesManager.removeTemporaryDirectory] Note, TemporaryDirectory located at "'
                                + self._tempDirPath + '" is not removed \n\t--[DirectoriesManager.removeTemporaryDirectory] ' + error;
                        } else {
                            msg = '\t--[DirectoriesManager.removeTemporaryDirectory] TemporaryDirectory located at "'
                                + self._tempDirPath + '" is removed';
                        }

                        log(self, msg);
                        resolve();
                    });
                });
            }
        });
    });
};

/**
 * Creates logs directory.
 *
 * @param {Conversion} self
 *
 * @returns {Promise}
 */
module.exports.createLogsDirectory = function(self) {
    return new Promise((resolve, reject) => {
        console.log('\t--[DirectoriesManager.createLogsDirectory] Creating logs directory...');
        fs.stat(self._logsDirPath, (directoryDoesNotExist, stat) => {
            if (directoryDoesNotExist) {
                fs.mkdir(self._logsDirPath, self._0777, e => {
                    if (e) {
                        let msg = '\t--[DirectoriesManager.createLogsDirectory] Cannot perform a migration due to impossibility to create '
                                + '"logs_directory": ' + self._logsDirPath;

                        console.log(msg);
                        reject();
                    } else {
                        log(self, '\t--[DirectoriesManager.createLogsDirectory] Logs directory is created...');
                        resolve();
                    }
                });
            } else if (!stat.isDirectory()) {
                console.log('\t--[DirectoriesManager.createLogsDirectory] Cannot perform a migration due to unexpected error');
                reject();
            } else {
                log(self, '\t--[DirectoriesManager.createLogsDirectory] Logs directory already exists...');
                resolve();
            }
        });
    });
};
