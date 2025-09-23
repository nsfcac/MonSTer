# MonSTer

## About MonSTer
MonSTer is an “out-of-the-box” monitoring tool for high-performance computing platforms. It uses the evolving specification Redfish to retrieve sensor data from Baseboard Management Controller, and resource management tools such as Slurm to obtain application information and resource usage data. Additionally, it also uses a time-series database (TimeScaleDB implemented in the code) for data storage. MonSTer correlates applications to resource usage and reveals insightful knowledge without having additional overhead on the application and computing nodes. 

For details about MonSTer, please refer to the paper: 
```
@inproceedings{li2020monster,
  title={MonSTer: an out-of-the-box monitoring tool for high performance computing systems},
  author={Li, Jie and Ali, Ghazanfar and Nguyen, Ngan and Hass, Jon and Sill, Alan and Dang, Tommy and Chen, Yong},
  booktitle={2020 IEEE International Conference on Cluster Computing (CLUSTER)},
  pages={119--129},
  year={2020},
  organization={IEEE}
}
```

For examples of visualization of data based on the above please see [https://idatavisualizationlab.github.io/HPCC/](https://idatavisualizationlab.github.io/HPCC/).

## Prerequisite
MonSTer requires that iDRAC nodes (pull model or push model), TimeScaleDB service, and Slurm REST API service can be accessed from the host machine where MonSTer is running.

## Initial Setup

1. Copy the `config.yml.example` file to `config.yml` and edit the file to configure the iDRAC nodes, TimeScaleDB service, and Slurm REST API service.

2. The __usernames__ and __passwords__ should be configured in the environment (edit the `~/.bashrc` or `~/.bash_profile`) instead of hard-coded in the code or in the configuration file.

```bash
# For TimeScaleDB
export tsdb_username=tsdb_user
export tsdb_password=tsdb_pwd

# For iDRAC8
export idrac_username=idrac_user
export idrac_password=idrac_pwd

# For Slurm REST API
export slurm_username=slurm_user
```

3. The database specified in the configuration file should be created and applied the TimeScaleDB extension before run any codes.

```bash
-- Create the database 'demo' for the owner 'monster',
CREATE DATABASE demo WITH OWNER monster;
-- Connect to the database
\c demo
-- Extend the database with TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

# MetricsBuilder

## About Metrics Builder

**Metrics Builder** acts as a middleware between the consumers (i.e. analytic clients or tools) and the producers (i.e. the databases). Its provides APIs for [the web applications](https://idatavisualizationlab.github.io/HPCC/) and accelerates the data query performance.

# Run MonSTer and MetricsBuilder

1. Set up the virtual environment and install the required packages.

```bash
# Create the virtual environment
python3.9 -m venv .venv
# Activate the virtual environment
source .venv/bin/activate
# Install project in editable mode and install the required packages
pip install -e .
```

2. Copy config.yml.example and change the configuration accordingly.
```bash
# Copy config.yml.example and rename it
cp config.yml.example config.yml
``` 

3. Initialize the TimeScaleDB tables by running the `init_db.py` script.

```bash
python ./monster/init_tsdb.py --config=config.yml
```

## Option 1: Run the code directly
1. Run the code to collect the data from iDRAC and Slurm.

```bash
nohup python ./monster/monit_idrac.py --config=config.yml >/dev/null 2>&1 &
nohup python ./monster/monit_slurm.py --config=config.yml >/dev/null 2>&1 &
```

2. Run the MetricsBuilder API server.

```bash
nohup python ./mbuilder/mb_run.py --config=config.yml >./log/mbapi.log 2>&1 &
```

3. Stop the running services.

```bash
kill $(ps aux | grep 'mb_run.py --config=config.yml' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'monit_idrac.py --config=config.yml' | grep -v grep | awk '{print $2}')
kill $(ps aux | grep 'monit_slurm.py --config=config.yml' | grep -v grep | awk '{print $2}')
```

## Option 2: Setup a systemd service

### Step 1. Create a Shell Wrapper Script
This script activates the virtual environment and starts the monster app.

Create:
```bash
/home/username/MonSTer/run_monster.sh
```

Content:
```bash
#!/bin/bash
# Activate virtual environment
source /home/username/MonSTer/.ven/bin/activate
# Start each script in the background
python /home/username/MonSTer/monster/monit_idrac.py --config=config.yml & 
python /home/username/MonSTer/monster/monit_slurm.py --config=config.yml &
# Keep the service running by waiting for all child processes
wait
```

Make it executable:
```bash
chmod +x /home/username/MonSTer/run_monster.sh
```

### Step 2. Create a systemd Service File
Create file:
```bash
sudo vim /etc/systemd/system/monster.service
```

Content:
```ini
[Unit]
Description=Monster Service
After=network.target

[Service]
Environment="tsdb_username=tsdb_user"
Environment="tsdb_password=tsdb_pwd"
Environment="idrac_username=idrac_user"
Environment="idrac_password=idrac_pwd"
Environment="slurm_username=slurm_user"
Type=simple
User=username
WorkingDirectory=/home/username/MonSTer
ExecStart=/home/username/MonSTer/run_monster.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Reload and enable the service:
```bash
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable monster.service
sudo systemctl start monster.service
```

Stop the service:
```bash
sudo systemctl stop monster.service
```

# Serving APIs with Nginx
## SSL Configuration (use hugo.hpcc.ttu.edu as an example)
### Prerequisites
- A valid DNS record for `hugo.hpcc.ttu.edu` pointing to the server’s public IP.
- `nginx` installed and running.
- `certbot` installed
### Step-by-Step SSL Setup
#### 1. Open Firewall Port 443
Allow HTTPS traffic through the firewall:
```bash
firewall-cmd --permanent --add-service=https
firewall-cmd --reload
```
To confirm it's active, try from you local computer:
```bash
nc -zv hugo.hpcc.ttu.edu 443
```
nc -zv hugo.hpcc.ttu.edu 443
#### 2. Ensure Nginx Has a Server Block for the Domain
Create or edit the Nginx configuration at `/etc/nginx/conf.d/hugo.hpcc.ttu.edu.conf`:
```nginx
server {
    listen 80;
    server_name hugo.hpcc.ttu.edu;

    root /usr/share/nginx/html;
    index index.html;
}
```
Open `/etc/nginx/nginx.conf` and make sure it includes the following line:
```nginx
include /etc/nginx/conf/*.conf;
```
Reload Nginx to apply changes:
```bash
nginx -t && systemctl reload nginx
```
#### 3. Issue the SSL Certificate with Certbot
Run the following Certbot command to automatically obtain and configure the certificate:
```bash
certbot --nginx -d hugo.hpcc.ttu.edu
```
This will:
- Obtain an SSL certificate from Let’s Encrypt.
- Modify the Nginx config to add a secure listen 443 ssl block.
- Configure automatic redirection from HTTP to HTTPS (if approved during prompts).

### Verification
- Visit `https://hugo.hpcc.ttu.edu` in your browser.
- Ensure the connection is secure and no *Not Secure* warnings appear.

## Serve APIs
#### Example URLs:
- `https://hugo.hpcc.ttu.edu/api/nocona/` -> FastAPI on port 5000
- `https://hugo.hpcc.ttu.edu/api/quanah/` -> FastAPI on port 5001
#### 1. Update Nginx Configuration
Edit the nginx configuration to include:
```nginx
server {
    listen 443 ssl;
    server_name hugo.hpcc.ttu.edu;
    ssl_certificate /etc/letsencrypt/live/hugo.hpcc.ttu.edu/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/hugo.hpcc.ttu.edu/privkey.pem; # managed by Certbot

    location /api/nocona/ {
        proxy_pass http://127.0.0.1:5000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /api/quanah/ {
        proxy_pass http://127.0.0.1:5001/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        root /usr/share/nginx/html;
        index index.html;
    }

    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;
}
```
#### 2. Update `root_path` in Fast API apps
**For /api/nocona/:**
```python
app = FastAPI(root_path="/api/nocona")
```
**For /api/quanah/:**
```python
app = FastAPI(root_path="/api/quanah")
```
> The partition name is defined in the configuration file; we set `root_path=f"/api/{partition}"` in the source code.
Then restart the service.
#### 3. Verification
Restart Nginx:
```bash
nginx -t && systemctl reload nginx
```
Access via:
- `https://hugo.hpcc.ttu.edu/api/nocona/docs` for the Nocona API
- `https://hugo.hpcc.ttu.edu/api/quanah/docs` for the Quanah API

#### 4. Trouble-shooting
If your system has SELinux enabled, it may block Nginx from making localhost connections.
Test with:
```bash
getenforce
```
If it says Enforcing, try (as root):
```bash
setenforce 0
```
Then reload Nginx and test again. If it works, you need a permanent SELinux policy:
```bash
sudo setsebool -P httpd_can_network_connect 1
```
This enables Nginx (`httpd`) to make outbound connections (like `proxy_pass`) permanently in SELinux policy.

# Troubleshooting Package Installation Issues.

This section lists common errors and their solutions when installing packages required to build or install `psycopg2` and PostgreSQL development libraries.

## 1. Missing Python Headers

**Error message:**

```bash
    In file included from psycopg/adapter_asis.c:28:
    ./psycopg/psycopg.h:35:10: fatal error: Python.h: No such file or directory
       35 | #include <Python.h>
          |          ^~~~~~~~~~
    compilation terminated.

    It appears you are missing some prerequisite to build the package from source
```

**Cause:**

This error occurs because the **Python development headers** (Python.h, etc.) are missing.

**Solution:**

Install the development package for Python:

```bash
sudo dnf install python3-devel
```

## 2. Missing PostgreSQL Client Headers

**Error message:**

```bash
    In file included from psycopg/adapter_asis.c:28:
    ./psycopg/psycopg.h:36:10: fatal error: libpq-fe.h: No such file or directory
       36 | #include <libpq-fe.h>
          |          ^~~~~~~~~~~~
    compilation terminated.

    It appears you are missing some prerequisite to build the package from source.
```

**Cause:**

This indicates that the **PostgreSQL client development headers** (libpq-fe.h) are missing. These are required for building `psycopg2`.

**Solution:**

Install the development libraries for PostgreSQL 17:
```
sudo dnf install postgresql17-devel
```

## 3. Missing Dependency: perl-IPC-Run

**Error message when installing postgresql17-devel:**

```bash
Error: Unable to find a match: perl-IPC-Run
```
**Cause:**

The package `perl-IPC-Run` is a dependency of `postgresql17-devel`, but it is not available in the default repositories.

**Solution:**

Enable the CodeReady Builder (CRB) repository and install `perl-IPC-Run`:

```bash
sudo dnf --enablerepo=crb install perl-IPC-Run
``` 

Then retry installing `postgresql17-devel`:
```bash
sudo dnf install postgresql17-devel
``` 
