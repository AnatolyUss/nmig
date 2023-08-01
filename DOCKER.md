**Note: Docker support is a work in progress. The following instructions may not work exactly as expected across operating systems and environments.**

<h3>RUNNING WITH DOCKER</h3>

<p>Nmig can run inside a Docker container while connecting to MySQL and PostgreSQL on the host machine.</p>

<p><b>1.</b> Follow instructions in the "USAGE" section in README.md to:
   <ol type=a>
   <li>Download the Nmig repository.</li>
   <li>Create a PostgreSQL database.</li>
   <li>Edit the necessary files in the <code>config</code> directory.</li>
   </ol>
</p>
<p><b>2.</b> Build a Docker image from the <code>nmig</code> directory. <pre>$ docker build --tag my-migration . </pre>

<p><b>3.</b> Mount the edited <code>config</code> directory and run Nmig in a new Docker container.<br/>
<pre>$ docker run --rm \
    --mount type=bind,source=/path/to/nmig_config,target=/usr/src/app/config \
    my-migration \
    npm start
</pre>
</p>
<p>
<b>Notes: </b>
<ul>
   <li>These steps require Docker. Consult <a href="https://www.docker.com">the official Docker site</a> for download and installation instructions.</li>
   <li>In configuration files, the <code>target.host</code> and <code>source.host</code> properties will be <code>"host.docker.internal"</code> instead of <code>"localhost"</code>.</li>
   <li>The examples above use "my-migration" as the image tag, but it can be any name you choose.</p>
</p>
</ul>
