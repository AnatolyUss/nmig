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
<li>Ability to recover migration process if disaster took place without restarting from the beginning.</li>
<li>Ability to migrate big databases fast - in order to migrate data NMIG utilizes PostgreSQL COPY protocol.</li>
<li>Ease of monitoring - NMIG will provide detailed output about every step, it takes during the execution.</li>
<li>
 Ease of configuration - all the parameters required for migration should be put in one single JSON document.
 </li>
</ul>

<h3>SYSTEM REQUIREMENTS</h3>
<ul>
<li> <b>Node.js 10 or higher</b></li>
</ul>

<h3>USAGE</h3>
<p><b>1.</b> Create a new PostgreSQL database.<br />
   <b>Sample:</b>&nbsp;<code> CREATE DATABASE my_postgresql_database;</code><br />
   If you are planning to migrate spatial data (geometry type columns), then <b>PostGIS</b> should be installed and enabled.
</p>

<p><b>2.</b> Download Nmig package and put it on the machine running your PostgreSQL (not mandatory, but preferably).<br />
   <b>Sample:</b>&nbsp;<code>/path/to/nmig</code></p>

<p><b>3.</b> Edit configuration file located at <code>/path/to/nmig/config/config.json</code> with correct details.<br /></p>
<b>Notes:</b>
   <ul>
   <li> config.json contains brief description of each configuration parameter</li>
   <li>Make sure, that username, you use in your PostgreSQL connection details, defined as superuser (usually "postgres")<br> More info: <a href="http://www.postgresql.org/docs/current/static/app-createuser.html">http://www.postgresql.org/docs/current/static/app-createuser.html</a></li>
   <li>
   <ul>
   <li>As an option, you can move the entire <code>config</code> folder out of Nmig's directory and place it in any location</li>
   <li>As an option, you can store the Nmig's logs in any location. All you need to do is to create the <code>nmig_logs</code> directory</li>
   </ul>
   </li>
   </ul>

<p><b>4.</b> Go to Nmig directory, install dependencies, compile and run the app<br />
    <b>Sample:</b><br />
    <pre>$ cd /path/to/nmig</pre><br />
    <pre>$ npm install</pre><br />
    <pre>$ npm run build</pre><br />
    <pre>$ npm start</pre><br />
    <b>Or, if you have moved <code>config</code> folder out from Nmig's directory:</b><br /><br />
    <pre>npm start -- --conf-dir='/path/to/nmig_config' --logs-dir='/path/to/nmig_logs'</pre><br />

<p><b>5.</b> If a disaster took place during migration (for what ever reason) - simply restart the process
<code>$ npm start</code><br />
Or, if you have moved <code>config</code> folder out from Nmig's directory:<br />
<code>$ npm start -- --conf-dir='/path/to/nmig_config' --logs-dir='/path/to/nmig_logs'</code><br />

&nbsp;&nbsp;&nbsp;&nbsp;NMIG will restart from the point it was stopped at.
</p>

<p><b>6.</b> At the end of migration check log files, if necessary.<br />&nbsp;&nbsp;&nbsp;
   Log files will be located under "logs_directory" folder in the root of the package.<br />&nbsp;&nbsp;&nbsp;
   <b>Note:</b> If you've created <code>nmig_logs</code> folder outside the nmig's directory than "logs_directory" will reside in <code>nmig_logs</code>.
   <br />
   <b>Note:</b> "logs_directory" will be created during script execution.</p>


<p><b>7.</b> In case of any remarks, misunderstandings or errors during migration,<br /> &nbsp;&nbsp;&nbsp;
   please feel free to email me
   <a href="mailto:anatolyuss@gmail.com?subject=NMIG">anatolyuss@gmail.com</a></p>

<h3>RUNNING TESTS</h3>
<p><b>1.</b> Create a new PostgreSQL database.<br />
   <b>Sample:</b>&nbsp;<code> CREATE DATABASE nmig_test_db;</code><br />
</p>
<p><b>2.</b> Download Nmig package.<br/><b>Sample:</b>&nbsp;<code>/path/to/nmig</code></p>
<p><b>3.</b> Edit configuration file located at <code>/path/to/nmig/config/test_config.json</code> with correct details.<br /></p>
<b>Notes:</b>
<ul>
   <li> test_config.json contains brief description of each configuration parameter</li>
   <li>Make sure, that username, you use in your PostgreSQL connection details, defined as superuser (usually "postgres")<br>
        More info:
        <a href="http://www.postgresql.org/docs/current/static/app-createuser.html">http://www.postgresql.org/docs/current/static/app-createuser.html</a>
   </li>
   <li>
      <ul>
      <li>As an option, you can move the entire <code>config</code> folder out of Nmig's directory and place it in any location</li>
      <li>As an option, you can store the Nmig's logs in any location. All you need to do is to create the <code>nmig_logs</code> directory</li>
      </ul>
    </li>
</ul>
<p><b>4.</b> Go to nmig directory, install dependencies, compile and run tests<br />
    <b>Sample:</b><br />
    <pre>$ cd /path/to/nmig</pre><br />
    <pre>$ npm install</pre><br />
    <pre>$ npm run build</pre><br />
    <pre>$ npm test</pre><br />
    <b>Or, if you have moved <code>config</code> folder out from Nmig's directory:</b><br /><br />
    <pre>npm test -- --conf-dir='/path/to/nmig_config' --logs-dir='/path/to/nmig_logs'</pre><br />
</p>
<p><b>5.</b> At the end of migration check log files, if necessary.<br />&nbsp;&nbsp;&nbsp;
   Log files will be located under "logs_directory" folder in the root of the package.<br />&nbsp;&nbsp;&nbsp;
<b>Note:</b> If you've created <code>nmig_logs</code> folder outside the nmig's directory than "logs_directory" will reside in <code>nmig_logs</code>.
<br /><b>Note:</b> "logs_directory" will be created during script execution.</p>

<h3>VERSION</h3>
<p>Current version is 5.5.0<br />

<h3>LICENSE</h3>
<p>NMIG is available under "GNU GENERAL PUBLIC LICENSE" (v. 3) <br />
<a href="http://www.gnu.org/licenses/gpl.txt">http://www.gnu.org/licenses/gpl.txt.</a></p>
