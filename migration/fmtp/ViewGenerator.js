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

function ViewGenerator() {
    // No code should be put here.
}

/**
 * Attempts to convert mysql view to postgresql view.
 *
 * @param   {String} schema
 * @param   {String} viewName
 * @returns {String}
 */
ViewGenerator.prototype.generateView = function(schema, viewName) {
    return 'test';
};

module.exports.ViewGenerator = ViewGenerator;
