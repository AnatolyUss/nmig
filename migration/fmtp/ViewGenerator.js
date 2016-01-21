/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
'use strict';

/**
 * Constructor.
 */
function ViewGenerator() {
    // No code should be put here.
}

/**
 * Attempts to convert MySQL view to PostgreSQL view.
 *
 * @param   {String} schema
 * @param   {String} viewName
 * @param   {String} mysqlViewCode
 * @returns {String}
 */
ViewGenerator.prototype.generateView = function(schema, viewName, mysqlViewCode) {
    mysqlViewCode        = mysqlViewCode.split('`').join('"');
    let queryStart       = mysqlViewCode.indexOf('AS');
    mysqlViewCode        = mysqlViewCode.slice(queryStart);
    let arrMysqlViewCode = mysqlViewCode.split(' ');

    for (let i = 0; i < arrMysqlViewCode.length; i++) {
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

module.exports.ViewGenerator = ViewGenerator;
