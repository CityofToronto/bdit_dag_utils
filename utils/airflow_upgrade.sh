
# ./airflow_setup.sh airflow_setup.ini
# exit upon any error
set -e

# copy file $1 to $2 if $1 exists
cp2() { [[ -e $1 ]] && cp $1 $2; }

#
check_os() { cat /etc/os-release | grep '^ID=.*' | cut -d= -f2; }

#this can be used to update config, if necessary (not currently in use)
update_file() {
    # replace pattern $1 with text $2 in file $3
    # update_file "\(^dags_folder = .*$\)" "dags_folder = abcd" "test"
    if ! (sed -i "s/$1/$2/ w /dev/stdout" $3 | grep -q "$2"); then
        echo "Failed to update $1"
    fi
}

backup_airflow() {
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
    cp $OLD_ENV_FILE "${BACKUP_PATH}/files"
    # setup config
    cp $1 "${BACKUP_PATH}/files"
    # service files
    cp /etc/systemd/system/airflow-api-server.service "${BACKUP_PATH}/files"
    cp /etc/systemd/system/airflow-dag-processor.service "${BACKUP_PATH}/files"
    cp /etc/systemd/system/airflow-scheduler.service "${BACKUP_PATH}/files"
    # backup the database
    echo "Enter the password of the PostgreSQL user: $PG_ADMIN"
    pg_dump -h $PG_HOST_ADDRESS -U $PG_ADMIN --create $OLD_PG_DATABASE > "${BACKUP_PATH}/files/database_`date +'%Y_%m_%d_%H_%M'`"
    # stop Airflow
    if (check_os | grep -q 'rhel'); then
        pbrun /bin/systemctl stop airflow-scheduler
        pbrun /bin/systemctl stop airflow-api-server
        pbrun /bin/systemctl stop airflow-dag-processor
    elif (check_os | grep -q 'ubuntu'); then
        sudo /bin/systemctl stop airflow-scheduler
        sudo /bin/systemctl stop airflow-api-server
        sudo /bin/systemctl stop airflow-dag-processor
    fi
}

#check if $1 GB of free space is available in the home folder before backup
check_free_space() {
    if [ $(df $HOME | awk 'NR==2 {print $4}') -lt $(($1 * 1024 * 1024)) ]; then
        echo "Error: Less than $1 GB free in $HOME"
        exit 128
    else
        echo "Found more than $1 GB free in $HOME, continuing."
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

#migrate airflow database
initialize_airflow_db() {
    echo "Migrating Airflow database..."
    airflow db migrate
    echo "Checking if migrations successful..."
    airflow db check-migrations
    airflow db check && echo "Initialized Airflow database successfully..."
}

setup_airflow() {
    PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
    echo "Installing Airflow ${AIRFLOW_VERSION} on ${PYTHON_VERSION}..."
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
    if [ $ON_PREM_SERVER == 'True' ]; then
        python3 -m pip install --proxy=http://$proxy_username:$proxy_pass@proxy.toronto.ca:8080 --upgrade pip
        python3 -m pip install --proxy=http://$proxy_username:$proxy_pass@proxy.toronto.ca:8080 "apache-airflow[${AIRFLOW_EXTRAS}]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
        echo 'Successfully installed Airflow '`airflow version`
    else
        python3 -m pip install --upgrade pip
        echo "Airflow constraints must be manually downloaded and installed from $CONSTRAINT_URL via requirements.in"
    fi
    echo "Successfully installed Airflow ${AIRFLOW_VERSION}"
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
        chmod 777 $ENV_FILE
        set -a; source $ENV_FILE; set +a
    else
        echo "The environment variable file ${INPUT_ENV_FILE} doesn't exist"
        exit 1
    fi
}

# read the installation parameters
#=================================
# 1. from config file
if [ $# -eq 0 ]; then
    echo "Please, input a configuration file"
    exit 128
else
    source $1
fi
# 2. proxy username & password for on-prem servers
if [ ${USE_PROXY,,} == 'true' ]; then
    echo "Enter a Novell username and password to set up the proxy settings"
    read -p "Username:" proxy_username
    read -sp "Password:" proxy_pass
    echo
fi

# 1. Backup old Airflow installation if needed
#==========================================
read -p "(1/7) Do you want to backup Airflow files? [Y/n]" backup_flag
if [ ${backup_flag,,} == 'y' ]; then
    check_free_space 2
    backup_airflow $1
fi

# set up the new Airflow
#=======================
# 2. set up the virtual environment
read -p "(2/7) Do you want to set up a new venv for Airflow? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    check_free_space 1
    setup_venv
fi
# activate the venv (if it exists)
[[ -e "${AIRFLOW_VENV}/bin/activate" ]] && source $AIRFLOW_VENV/bin/activate

# 3. install Airflow
read -p "(3/7) Do you want to set up Airflow? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    setup_airflow
fi

# 4. set up Airflow env vars
read -p "(4/7) Do you want to create/update Airflow environment variables file? [Y/n]" response
if [ ${response,,} == 'y' ]; then
    setup_airflow_env
fi

# 5. initizlize/upgrade the database
read -p "(5/7) Do you want to migrate the database? [Y/n]" migrate_flag
if [ ${migrate_flag,,} == 'y' ]; then
    initialize_airflow_db
fi

# 6. create/update the service files
# We don't have anything in our service files that changes when we upgrade, since we use the same airflow home and airflow_venv locations.
# Need to manually create new service files at this point during Airflow 3 upgrade as root.
read -p "If you to update the service files at /etc/systemd/system/, do so now then enter Y to reload service files." update_services
if [ ${update_services,,} == 'y' ]; then
    if (check_os | grep -q 'rhel'); then
        pbrun systemctl daemon-reload
    elif (check_os | grep -q 'ubuntu'); then
        sudo systemctl daemon-reload
    fi
fi

# 7. restart Airflow services
if (check_os | grep -q 'rhel'); then
    pbrun /bin/systemctl start airflow-scheduler
    pbrun /bin/systemctl start airflow-api-server
    pbrun /bin/systemctl start airflow-dag-processor
elif (check_os | grep -q 'ubuntu'); then
    sudo /bin/systemctl start airflow-scheduler
    sudo /bin/systemctl start airflow-api-server
    sudo /bin/systemctl start airflow-dag-processor
fi