/* 
 * This file is a part of "NMIG" - the database migration tool.
 * 
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 * 
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>  
 */
'use strict';
var fs   = require('fs');
var fmtp = require('./migration/fmtp/FromMySQL2PostgreSQL');
var nmig = new fmtp.FromMySQL2PostgreSQL();
console.log();

fs.readFile(__dirname + '/config.json', function(error, data) {
    var errMsg;
    
    if (error) {
        errMsg = '\t--Cannot run migration\nCannot read configuration info from ' + __dirname + '/config.json';
        console.log(errMsg);
    } else {
        try {
            var config         = JSON.parse(data.toString());
            config.tempDirPath = __dirname + '/temporary_directory';
            config.logsDirPath = __dirname + '/logs_directory';
            nmig.run(config);
        } catch (err) {
            errMsg = '\t--Cannot parse JSON from' + __dirname + '/config.json';
            console.log(errMsg);
        }
    }
});

