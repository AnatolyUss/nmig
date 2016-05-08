<h3>NMIG - the database migration tool.</h3>

<h3>WHAT IS IT ALL ABOUT?</h3>
<p>NMIG is an app, intended to make a process of migration
from MySQL to PostgreSQL as easy and smooth as possible.</p>

<h3>KEY FEATURES</h3>
<ul>
<li> Ease of use - the only thing needed to run this app is the Node.js runtime.</li>
<li> Accuracy of migration the database structure - NMIG converts
   MySQL data types to corresponding PostgreSQL data types, creates constraints,
   indexes, primary and foreign keys exactly as they were before migration.</li>

<li>Ability to migrate big databases - in order to reduce RAM consumption NMIG will split each table's data into several chunks. <br />
Chunk size can be adjusted easily via configuration file.</li>

<li> Speed of data transfer - in order to migrate data fast NMIG uses PostgreSQL COPY protocol.</li>
   
<li>Ease of monitoring - NMIG will provide detailed output about every step, it takes during the execution.</li>
<li>
 Ease of configuration - all the parameters required for migration should be put in one single JSON document.
 </li>
</ul>

<h3>SYSTEM REQUIREMENTS</h3>
<ul>
<li> <b>Node.js 5.x.x</b></li>
</ul>

<h3>USAGE</h3>
<p><b>1.</b> Create a new database.<br />
   <b>Sample:</b>&nbsp;<code> CREATE DATABASE my_postgresql_database;</code></p>

<p><b>2.</b> Download NMIG package and put it on the machine running your PostgreSQL.<br />
   <b>Sample:</b>&nbsp;<code>/path/to/nmig</code></p>

<p><b>3.</b> Edit configuration file located at <code>/path/to/nmig/config.json</code> with correct details.<br /></p>
<b>Remarks:</b>
   <ul>
   <li> config.json contains brief description of each configuration parameter</li>
   <li>Make sure, that username, you use in your PostgreSQL connection details, defined as superuser (usually "postgres")<br> More info: <a href="http://www.postgresql.org/docs/current/static/app-createuser.html">http://www.postgresql.org/docs/current/static/app-createuser.html</a></li>
   </ul>

<p><b>4.</b> Go to nmig directory, install dependencies, and run the app with <code>--expose-gc</code> flag<br />
    &nbsp;&nbsp;&nbsp;&nbsp;<b>Sample:</b><br />
    <pre>$ cd /path/to/nmig</pre><br />
    <pre>$ npm install</pre><br />
    <pre>$ node --expose-gc nmig.js</pre><br />
</p>
<p>
   &nbsp;&nbsp;
   <b>Remark</b>: you can increase node.js memory limit (RAM usage) using <code>--max-old-space-size</code> flag<br />
</p>
<p>
   &nbsp;&nbsp;
   Following command will increase memory limit to ~2GB and run nmig
   <br />&nbsp;&nbsp;<code>$ node --max-old-space-size=2048 --expose-gc nmig.js</code>
</p>

<p><b>5.</b> At the end of migration check log files, if necessary.<br />&nbsp;&nbsp;&nbsp;
   Log files will be located under "logs_directory" folder in the root of the package.<br />&nbsp;&nbsp;&nbsp;
   <b>Note:</b> "logs_directory" will be created during script execution.</p>


<p><b>6.</b> In case of any remarks, misunderstandings or errors during migration,<br /> &nbsp;&nbsp;&nbsp;
   please feel free to email me
   <a href="mailto:anatolyuss@gmail.com?subject=NMIG">anatolyuss@gmail.com</a></p>

<h3>VERSION</h3>
<p>Current version is 1.2.0<br />
(major version . improvements . bug fixes)</p>


<h3>TEST</h3>
<p>Tested using MySQL Community Server (5.6.21) and PostgreSQL (9.3).<br />
The entire process of migration 59.6 MB database (52 tables, 570754 rows),<br />
which includes data types mapping, creation of tables, constraints, indexes, <br />
PKs, FKs, migration of data, garbage-collection (VACUUM) and analyzing the newly created <br />
PostgreSQL database took 1 minute 18 seconds.</p>
<p>
<b>Remark:</b>&nbsp; it is highly recommended to VACUUM newly created database! <br />
Just keep in mind, that VACUUM is a very time-consuming procedure. <br />
So if you are short in time - disable VACUUM via config.json ("no_vacuum" parameter). <br />
Such step will save you ~25% of migration time. <br />
The migration process described above without VACUUM took 58 seconds only.
</p>

<h3>LICENSE</h3>
<p>NMIG is available under "GNU GENERAL PUBLIC LICENSE" (v. 3) <br />
<a href="http://www.gnu.org/licenses/gpl.txt">http://www.gnu.org/licenses/gpl.txt.</a></p>


<h3>REMARKS</h3>
<p>Errors/Exceptions are not passed silently.<br />
Any error will be immediately written into the error log file.</p>
