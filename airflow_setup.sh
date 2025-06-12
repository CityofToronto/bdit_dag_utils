#!/bin/bash
# ./airflow_setup.sh airflow_setup.ini
# exit upon any error
# set -e

# create a folder if it doesn't exist
touch2() { mkdir -p "$(dirname "$1")" && touch "$1" ; }

# copy file $1 to $2 if $1 exists
cp2() { [[ -e $1 ]] && cp $1 $2; }

insert_if_not_exists() {
    # insert $1 into file $2 if it doesn't exist in the file
    if ! grep -Fxq "$1" $2; then
        echo "$1" >> $2
    fi
}

update_file() {
    # replace pattern $1 with text $2 in file $3
    # update_file "\(^dags_folder = .*$\)" "dags_folder = abcd" "test"
    if ! (sed -i "s/$1/$2/ w /dev/stdout" $3 | grep -q "$2"); then
        echo "Failed to update $1"
    fi
}

vercomp () {
    # Compares two versions $1 and $2
    # Returns 0 if they're equal, 1 if $1 is greater than $2 and 2 if $1 is lower than $2
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 2
        fi
    done
    return 0
}

backup_airflow() {
    if [ ${UPGRADE,,} == 'true' ]; then
        # create the backup folder if it doesn't exist
        [[ ! -d $BACKUP_PATH ]] && mkdir $BACKUP_PATH
        echo "Backing up the old Airflow installation into ${BACKUP_PATH}"

        source $OLD_AIRFLOW_VENV/bin/activate
        old_airflow_version=`airflow version`
        deactivate
        echo "Airflow ${old_airflow_version}: backed up on `date +'%Y_%m_%d_%H_%M'`" > "${BACKUP_PATH}/README"

        # backup the venv folder
        [[ ! -d "${BACKUP_PATH}/venv" ]] && mkdir "${BACKUP_PATH}/venv"
        cp -r $OLD_AIRFLOW_VENV "${BACKUP_PATH}/venv"
        # Airflow home folder (including the configuration file)
        [[ ! -d "${BACKUP_PATH}/home" ]] && mkdir "${BACKUP_PATH}/home"
        cp -r $OLD_AIRFLOW_HOME "${BACKUP_PATH}/home"
        # Airflow env file
        [[ ! -d "${BACKUP_PATH}/files" ]] && mkdir "${BACKUP_PATH}/files"
        cp2 $OLD_ENV_FILE "${BACKUP_PATH}/files"
        # setup config
        cp2 $1 "${BACKUP_PATH}/files"
        # service files
        cp2 /etc/systemd/system/airflow-webserver.service "${BACKUP_PATH}/files"
        cp2 /etc/systemd/system/airflow-scheduler.service "${BACKUP_PATH}/files"
        # backup the database
        echo "Enter the password of the PostgreSQL user: $PG_ADMIN"
        pg_dump -h $PG_HOST_ADDRESS -U $PG_ADMIN --create $OLD_PG_DATABASE > "${BACKUP_PATH}/files/database_`date +'%Y_%m_%d_%H_%M'`"
        # stop Airflow
        ${sudo} systemctl stop airflow-webserver
        ${sudo} systemctl stop airflow-scheduler
    fi
}

setup_venv() {
    if [[ -d "${AIRFLOW_VENV}" ]]; then
        read -p "Do you want to overwrite ${AIRFLOW_VENV}? [Y/n]" overwrite_flag
        if [ ${overwrite_flag,,} == 'n' ]; then
            exit 1
        else
            rm -rf $AIRFLOW_VENV
        fi
    fi
    python3 -m venv $AIRFLOW_VENV
}

setup_airflow() {
    PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    echo "Installing Airflow ${AIRFLOW_VERSION} on ${PYTHON_VERSION}..."
    if [ -z "${CONSTRAINT_URL}" ]; then
        CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    fi
    # TO DO: remove proxy setup. should be configured after creating the venv
    if [ ${USE_PROXY,,} == 'true' ]; then
        python3 -m pip install --proxy=http://$proxy_username:$proxy_pass@$PROXY_URL:8080 --upgrade pip
        python3 -m pip install --proxy=http://$proxy_username:$proxy_pass@$PROXY_URL:8080 "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    else
        python3 -m pip install --upgrade pip
        python3 -m pip install "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    fi
    echo 'Successfully installed Airflow '`airflow version`
}

setup_airflow_db() {
    # supports PostgreSQL only
    read -sp "Enter the password for PostgreSQL user '${PG_USERNAME}':" pg_username_pass
    echo
    if [ ${UPGRADE,,} == 'true' ]; then
        echo "The old database will be upgraded..."
        PG_DATABASE=$OLD_PG_DATABASE
    else
        echo "Creating a new database ${PG_DATABASE}..."
        tmpfile=$(mktemp ./.tmp.XXXXXX)
        echo "CREATE DATABASE ${PG_DATABASE};\
            GRANT ALL PRIVILEGES ON DATABASE ${PG_DATABASE} TO ${PG_USERNAME};\
            ALTER DATABASE ${PG_DATABASE} SET search_path = public;\
            ALTER DATABASE ${PG_DATABASE} OWNER TO ${PG_USERNAME};" > $tmpfile
        psql -h $PG_HOST_ADDRESS -U $PG_ADMIN -f "$tmpfile"
        rm "$tmpfile"

        # set up Airflow-PG connection
        # check if this's needed to avoid duplication
        echo "Updating ~/.pgpass with the credentials of PostgreSQL user ${PG_USERNAME}..."
        echo "${PG_HOST_ADDRESS}:5432:${PG_DATABASE}:${PG_USERNAME}:${pg_username_pass}" >> ~/.pgpass
        chmod 0600 ~/.pgpass
    fi
}

setup_airflow_env() {
    if [[ -f "${INPUT_ENV_FILE}" ]]; then
        if [[ -f "${ENV_FILE}" ]]; then
            read -p "Do you want to overwrite the environment variable file ${ENV_FILE}? [Y/n]" overwrite_flag
            if [ ${overwrite_flag,,} == 'y' ]; then
                cp2 "${INPUT_ENV_FILE}" "${ENV_FILE}"
            fi
        else
            cp2 "${INPUT_ENV_FILE}" "${ENV_FILE}"
        fi
        chmod 600 $ENV_FILE
        set -a; source $ENV_FILE; set +a
    else
        echo "The environment variable file ${INPUT_ENV_FILE} doesn't exist"
        exit 1
    fi
}

initialize_airflow_db() {
    if [ ${UPGRADE,,} == 'true' ]; then
        echo "Upgrading Airflow database..."
        vercomp $AIRFLOW_VERSION '2.7.0'
        if [[ $? == 2 ]]; then
            airflow db upgrade
        else
            airflow db migrate
        fi
        airflow db upgrade
        airflow db check && echo "Upgraded Airflow database successfully..."
    else
        echo "Initializing Airflow database..."
        vercomp $AIRFLOW_VERSION '2.7.0'
        if [[ $? == 2 ]]; then
            airflow db init
        else
            airflow db migrate
        fi
        airflow db check && echo "Initialized Airflow database successfully..."
    fi
}

initialize_services() {
    if [ ${UPGRADE,,} == 'true' ]; then
        echo "Updating service files..."
        ENV_FILE_=`echo $ENV_FILE | sed -r 's/\//\\\\\//g'`
        AIRFLOW_VENV_=`echo $AIRFLOW_VENV | sed -r 's/\//\\\\\//g'`
        AIRFLOW_HOME_=`echo $AIRFLOW_HOME | sed -r 's/\//\\\\\//g'`
        # webserver
        cp "/etc/systemd/system/airflow-webserver.service" "${AIRFLOW_HOME}/temp"
        # update_file "\(^Environment.*DEPLOYMENT.*$\)" "Environment='DEPLOYMENT=${DEPLOYMENT}'" "${AIRFLOW_HOME}/temp"
        update_file "\(^EnvironmentFile.*$\)" "EnvironmentFile=${ENV_FILE_}" "${AIRFLOW_HOME}/temp"
        update_file "\(^ExecStart.*$\)" "ExecStart= /bin/bash -c 'source ${AIRFLOW_VENV_}/bin/activate ; ${AIRFLOW_VENV_}/bin/airflow webserver -p ${WEBSERVER_PORT}  --pid ${AIRFLOW_HOME_}/webserver.pid'" "${AIRFLOW_HOME}/temp"
        cat "${AIRFLOW_HOME}/temp" > "/etc/systemd/system/airflow-webserver.service"
        rm "${AIRFLOW_HOME}/temp"
        # scheduler
        cp "/etc/systemd/system/airflow-scheduler.service" "${AIRFLOW_HOME}/temp"
        # update_file "\(^Environment.*DEPLOYMENT.*$\)" "Environment='DEPLOYMENT=${DEPLOYMENT}'" "${AIRFLOW_HOME}/temp"
        update_file "\(^EnvironmentFile.*$\)" "EnvironmentFile=${ENV_FILE_}" "${AIRFLOW_HOME}/temp"
        update_file "\(^ExecStart.*$\)" "ExecStart= /bin/bash -c 'source ${AIRFLOW_VENV_}/bin/activate ; ${AIRFLOW_VENV_}/bin/airflow scheduler'" "${AIRFLOW_HOME}/temp"
        cat "${AIRFLOW_HOME}/temp" > "/etc/systemd/system/airflow-scheduler.service"
        rm "${AIRFLOW_HOME}/temp"
    else
        echo "Creating service files..."
        service_file=/etc/systemd/system/airflow-webserver.service
        [[ -e $service_file ]] && ${sudo} rm $service_file
        ${sudo} touch $service_file
        ${sudo} chmod 777 $service_file
        ${sudo} cat > $service_file <<-EOF
        [Unit]
        Description=Airflow webserver daemon
        After=network.target postgresql.service mysql.service redis.service rabbitmq-server.service
        Wants=postgresql.service mysql.service redis.service rabbitmq-server.service

        [Service]
        EnvironmentFile=$ENV_FILE
        User=airflow
        Group=airflow
        Type=simple
        ExecStart= bash -c 'source $AIRFLOW_VENV/bin/activate ; $AIRFLOW_VENV/bin/airflow webserver -p $WEBSERVER_PORT --pid $AIRFLOW_HOME/webserver.pid'
        RuntimeDirectory=airflow
        RuntimeDirectoryMode=0775
        Restart=on-failure
        RestartSec=5s
        PrivateTmp=true

        [Install]
        WantedBy=multi-user.target
EOF

        service_file=/etc/systemd/system/airflow-scheduler.service
        [[ -e $service_file ]] && ${sudo} rm $service_file
        ${sudo} touch $service_file
        ${sudo} chmod 777 $service_file
        ${sudo} cat > $service_file <<-EOF
        [Unit]
        Description=Airflow scheduler daemon
        After=network.target postgresql.service mysql.service
        Wants=postgresql.service mysql.service

        [Service]
        EnvironmentFile=#ENV_FILE
        User=airflow
        Group=airflow
        Type=simple
        ExecStart= bash -c 'source $AIRFLOW_VENV/bin/activate ; $AIRFLOW_VENV/bin/airflow scheduler'
        Restart=always
        RestartSec=5s

        [Install]
        WantedBy=multi-user.target
EOF
    fi

    # load service files
    ${sudo} systemctl daemon-reload
    # enable autostart and start the services
    ${sudo} systemctl enable airflow-webserver
    ${sudo} systemctl enable airflow-scheduler
    ${sudo} systemctl start airflow-webserver
    ${sudo} systemctl start airflow-scheduler
}

# read the installation parameters
#=================================
# 1. from config file
if [ $# -eq 0 ]; then
    echo "Please, input a configuration file"
    exit 128
else
    source $1
    export AIRFLOW_HOME=$AIRFLOW_HOME
fi

# 2. prxoy username & password for on-prem servers
if [ ${USE_PROXY,,} == 'true' ]; then
    echo "Enter a username and password to set up the proxy settings"
    read -p "Username:" proxy_username
    read -sp "Password:" proxy_pass
    echo
fi

# 3. replace sudo with whatever command in the config file; otherwise, keep it `sudo`
sudo="${sudo:-sudo}"

# backup old Airflow installation if needed
#==========================================
read -p "Do you want to back up old Airflow files? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    backup_airflow $1
fi

# set up the new Airflow
#=======================
# 1. set up the virtual environment
read -p "(1/7) Do you want to set up a new venv for Airflow? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    setup_venv
fi
[[ -e "${AIRFLOW_VENV}/bin/activate" ]] && source $AIRFLOW_VENV/bin/activate # activate the venv (if it exists)

# 2. install Airflow
read -p "(2/7) Do you want to set up Airflow? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    setup_airflow
fi

# 3. set up a new Airfow database if needed
read -p "(3/7) Do you want to set up Airflow database backend? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    setup_airflow_db
fi

# 4. set up Airflow env vars
read -p "(4/7) Do you want to create/update Airflow environment variables file? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    setup_airflow_env
fi

# 5. add Airflow env vars to .bashrc
read -p "(5/7) Do you want to update the .bashrc file with Airflow environment variables? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    insert_if_not_exists "set -a; source $ENV_FILE; set +a" ~/.bashrc
fi

# 6. initizlize/upgrade the database
read -p "(6/7) Do you want to initialize/upgrade Airflow database? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    initialize_airflow_db
fi

# 7. create/update the service files
read -p "(7/7) Do you want to create/update Airflow systemd files? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    initialize_services
fi

echo "Airflow is successfully installed!"
echo "Try accessing Airflow at http://localhost:${WEBSERVER_PORT}/airflow"
