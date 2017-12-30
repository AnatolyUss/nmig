<h3>NMIG - the database migration tool.</h3>

<h3>WHAT IS IT ALL ABOUT?</h3>
<p>NMIG is an app, intended to make a process of migration
from MySQL to PostgreSQL as easy and smooth as possible.</p>

<h3>KEY FEATURES</h3>
<ul>
<li> Precise migration of the database structure - NMIG converts
   MySQL data types to corresponding PostgreSQL data types, creates constraints,
   indexes, primary and foreign keys exactly as they were before migration.</li>

<li>Ability to rename tables and columns during migration.</li>
<li>Ability to recover migration process if disaster took place (without restarting from the beginning).</li>
<li>Ability to migrate big databases - in order to eliminate "process out of memory" issues NMIG will split each table's data into several chunks.<br>Each group of chunks will be loaded via separate worker process.</li>

<li> Speed of data transfer - in order to migrate data fast NMIG uses PostgreSQL COPY protocol.</li>
<li>Ease of monitoring - NMIG will provide detailed output about every step, it takes during the execution.</li>
<li>
 Ease of configuration - all the parameters required for migration should be put in one single JSON document.
 </li>
</ul>

<h3>SYSTEM REQUIREMENTS</h3>
<ul>
<li> <b>Node.js 8 or higher</b></li>
</ul>

<h3>USAGE</h3>
<p><b>1.</b> Create a new database.<br />
   <b>Sample:</b>&nbsp;<code> CREATE DATABASE my_postgresql_database;</code><br />
   If you are planning to migrate spatial data (geometry type columns), then <b>PostGIS</b> should be installed and enabled.
</p>

<p><b>2.</b> Download NMIG package and put it on the machine running your PostgreSQL (not mandatory, but preferably).<br />
   <b>Sample:</b>&nbsp;<code>/path/to/nmig</code></p>

<p><b>3.</b> Edit configuration file located at <code>/path/to/nmig/config.json</code> with correct details.<br /></p>
<b>Notes:</b>
   <ul>
   <li> config.json contains brief description of each configuration parameter</li>
   <li>Make sure, that username, you use in your PostgreSQL connection details, defined as superuser (usually "postgres")<br> More info: <a href="http://www.postgresql.org/docs/current/static/app-createuser.html">http://www.postgresql.org/docs/current/static/app-createuser.html</a></li>
   </ul>

<p><b>4.</b> Go to nmig directory, install dependencies, and run the app<br />
    &nbsp;&nbsp;&nbsp;&nbsp;<b>Sample:</b><br />
    <pre>$ cd /path/to/nmig</pre><br />
    <pre>$ npm install</pre><br />
    <pre>$ npm start</pre><br />
</p>

<p><b>5.</b> If a disaster took place during migration (for what ever reason) - simply restart the process
<code>$ npm start</code><br>&nbsp;&nbsp;&nbsp;&nbsp;NMIG will restart from the point it was stopped at.
</p>

<p><b>6.</b> At the end of migration check log files, if necessary.<br />&nbsp;&nbsp;&nbsp;
   Log files will be located under "logs_directory" folder in the root of the package.<br />&nbsp;&nbsp;&nbsp;
   <b>Note:</b> "logs_directory" will be created during script execution.</p>


<p><b>7.</b> In case of any remarks, misunderstandings or errors during migration,<br /> &nbsp;&nbsp;&nbsp;
   please feel free to email me
   <a href="mailto:anatolyuss@gmail.com?subject=NMIG">anatolyuss@gmail.com</a></p>

<h3>VERSION</h3>
<p>Current version is 3.4.0<br />
(major version . improvements . bug fixes)</p>

<h3>REMARKS</h3>
<p>Errors/Exceptions are not passed silently.<br />
Any error will be immediately written into the error log file.</p>

<h3>KNOWN ISSUES</h3>
<ul>
   <li>Empty strings in char/varchar columns may be interpreted as NULL.</li>
</ul>

<h3>LICENSE</h3>
<p>NMIG is available under "GNU GENERAL PUBLIC LICENSE" (v. 3) <br />
<a href="http://www.gnu.org/licenses/gpl.txt">http://www.gnu.org/licenses/gpl.txt.</a></p>
