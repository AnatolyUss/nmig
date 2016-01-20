/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
'use strict';
const fs    = require('fs');
const mysql = require('mysql');
const pg    = require('pg');

/**
 * Constructor.
 */
function ViewGenerator() {
    // No code should be put here.
}

/**
 * Attempts to convert mysql view to postgresql view.
 *
 * @param   {String} schema
 * @param   {String} viewName
 * @returns {Promise}
 */
ViewGenerator.prototype.generateView = function(schema, viewName) {
    return new Promise(resolve => {
        resolve();
    });
};

module.exports.ViewGenerator = ViewGenerator;
