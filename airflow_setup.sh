
# ./airflow_setup.sh airflow_setup.ini
# exit upon any error
set -e

update_file() {
    # replace pattern $1 with text $2 in file $3
    # update_file "\(^dags_folder = .*$\)" "dags_folder = abcd" "test"
    if ! (sed -i "s/$1/$2/ w /dev/stdout" $3 | grep -q "$2"); then
        echo "Failed to update $1"
    fi
}


#Check Airflow home folder has more than 2GB free for backup
required_GB=2
if [ $(df $HOME | awk 'NR==2 {print $4}') -lt $(($required_GB * 1024 * 1024)) ]; then
    echo "Error: Less than $required_GB GB free in $HOME"
    exit 128
else
    echo "Found more than $required_GB GB free in $HOME, continuing."
fi

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
if [ ${ON_PREM_SERVER,,} == 'true' ]; then
    echo "Enter a Novell username and password to set up the proxy settings"
    read -p "Username:" proxy_username
    read -sp "Password:" proxy_pass
    echo
fi

# backup old Airflow installation if needed
#==========================================
read -p "Do you want to backup Airflow files? [Y/n]" backup_flag
if [ ${UPGRADE,,} == 'true' ] && [ ${backup_flag,,} == 'y' ]; then
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
    if (cat /etc/os-release | grep '^ID=.*' | cut -d= -f2 | grep -q 'rhel'); then
        pbrun /bin/systemctl stop airflow-scheduler
        pbrun /bin/systemctl stop airflow-api-server
        pbrun /bin/systemctl stop airflow-dag-processor
    elif (cat /etc/os-release | grep '^ID=.*' | cut -d= -f2 | grep -q 'ubuntu'); then
        sudo /bin/systemctl stop airflow-scheduler
        sudo /bin/systemctl stop airflow-api-server
        sudo /bin/systemctl stop airflow-dag-processor
    fi
fi

# set up the new Airflow
#=======================
# 1. set up the virtual environment
if [[ -d "${AIRFLOW_VENV}" ]]; then
    read -p "Do you want to overwrite ${AIRFLOW_VENV}? [Y/n]" overwrite_flag
    if [ ${overwrite_flag,,} == 'n' ]; then
        exit 1
    else
        rm -rf $AIRFLOW_VENV
    fi
fi
python3 -m venv $AIRFLOW_VENV
source $AIRFLOW_VENV/bin/activate

# 2. install Airflow
echo "Installing Airflow ${AIRFLOW_VERSION}..."
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
if [ $ON_PREM_SERVER == 'True' ]; then
    python3 -m pip install --proxy=http://$proxy_username:$proxy_pass@proxy.toronto.ca:8080 --upgrade pip
    python3 -m pip install --proxy=http://$proxy_username:$proxy_pass@proxy.toronto.ca:8080 "apache-airflow[celery,postgres,slack,http,fab]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
    echo 'Successfully installed Airflow '`airflow version`
else
    python3 -m pip install --upgrade pip
    echo "Airflow constraints must be manually downloaded from $CONSTRAINT_URL"
fi

# 3. set up a new Airfow database if needed
read -sp "Enter the password for PostgreSQL user '${PG_USERNAME}':" pg_username_pass
echo
if [ ${UPGRADE,,} == 'true' ]; then
    echo "The old database will be upgraded..."
    PG_DATABASE=$OLD_PG_DATABASE
else
    echo "Creating a new database ${PG_DATABASE}..."
    tmpfile=$(mktemp ./.tmp.XXXXXX)
    echo "CREATE DATABASE ${PG_DATABASE};GRANT ALL PRIVILEGES ON DATABASE ${PG_DATABASE} TO ${PG_USERNAME};ALTER DATABASE ${PG_DATABASE} SET search_path = public;" > $tmpfile
    psql -h $PG_HOST_ADDRESS -U $PG_ADMIN -f "$tmpfile"
    rm "$tmpfile"

    # set up Airflow-PG connection
    echo "Updating the credentials for PostgreSQL user ${PG_USERNAME}..."
    echo "${PG_HOST_ADDRESS}:5432:${PG_DATABASE}:${PG_USERNAME}:${pg_username_pass}" >> ~/.pgpass
    chmod 0600 ~/.pgpass
fi

# 4. set up Airflow env vars
# 5. initizlize/upgrade the database
if [ ${UPGRADE,,} == 'true' ]; then
    echo "Migrating Airflow database..."
    airflow db migrate
    echo "Checking if migrations successful..."
    airflow db check-migrations
fi
airflow db check && echo "Initialized Airflow database successfully..."

# 6. create/update the service files
# We don't have anything in our service files that changes when we upgrade, since we use the same airflow home and airflow_venv locations.
# Need to manually create new service files at this point during Airflow 3 upgrade as root.
read -p "If you to update the service files at /etc/systemd/system/, do so now then enter Y to reload service files." update_services
if [ ${update_services,,} == 'y' ]; then
    if (cat /etc/os-release | grep '^ID=.*' | cut -d= -f2 | grep -q 'rhel'); then
        pbrun systemctl daemon-reload
    elif (cat /etc/os-release | grep '^ID=.*' | cut -d= -f2 | grep -q 'ubuntu'); then
        sudo systemctl daemon-reload
    fi
fi

# 7. restart Airflow services
if (cat /etc/os-release | grep '^ID=.*' | cut -d= -f2 | grep -q 'rhel'); then
    pbrun /bin/systemctl start airflow-scheduler
    pbrun /bin/systemctl start airflow-api-server
    pbrun /bin/systemctl start airflow-dag-processor
elif (cat /etc/os-release | grep '^ID=.*' | cut -d= -f2 | grep -q 'ubuntu'); then
    sudo /bin/systemctl start airflow-scheduler
    sudo /bin/systemctl start airflow-api-server
    sudo /bin/systemctl start airflow-dag-processor
fi