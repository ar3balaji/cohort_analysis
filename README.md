# Cohort Analysis
<p>
    This repository contains a code to analyze how distinct customer order behavior changes
    from their signup date to present.
</p>

## Technologies used:
<ol>
    <li>Python Flask</li>
    <li>Mysql</li/>
    <li>Docker and Docker-compose</li>
</ol>

## How to install docker
<ol>
    <li>For windows use: https://docs.docker.com/docker-for-windows/install/</li>
    <li>For linux use: https://runnable.com/docker/install-docker-on-linux</li/>
</ol>

## How to run application
<ol>
    <li>After docker is installed in the machine, verify it is working correctly</li>
    <li>
        Checkout repository and from root folder of this project run this command
        <code>docker-compose up</code>
    </li/>
    <li>
        After running command, it will launch two docker containers named 
        cohort_analysis_app_1, cohort_analysis_db_1
    </li>
    <li>Application will be accessible at http://0.0.0.0:5000/</li>
    <li>
        Note application will be using port 5000, mysql database will be accessible using
        port 32000. Application uses port 3306 to communicate with database. Additional properties are configurable in 
        docker-compose file, which is located in root directory
    </li>
</ol>

## How to use application
<ol>
    <li>Import customers data using http://0.0.0.0:5000/import/customers</li>
    <li>
        Import orders data using http://0.0.0.0:5000/import/orders</code>
    </li/>
    <li>
        After data is loaded. use this link to generate report http://0.0.0.0:5000/cohort/analysis by choosing required time zone
    </li>
</ol>

## How to run tests
<ol>
    <li>From root folder, change working directory to app and run this command to start unit tests <code>python initiateTests.py</code></li>
</ol>