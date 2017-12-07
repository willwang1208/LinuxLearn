#!/bin/bash
#

MONGODB_HOME=/opt/mongodb-3.2.6

MONGODB_CTL_HOME=$(dirname `readlink -f "$0"`)
MONGODB_CONF_DIR=${MONGODB_CTL_HOME}/conf
PARSE_CONF_SHELL_FILE=${MONGODB_CTL_HOME}/parse_yaml.sh

METHOD=
NODES=
EXEC_FORCE=
LOG_KEPT_DAYS=
BACKUP_DIRS=
BACKUP_KEPT_COUNT=

help(){
    echo -e "
    Usage:
        ./mongodb_ctl.sh [OPTION]...
    Options:
        -m        method (eg. start|stop|log_rotate|physical_backup)
        -n        yaml config file name, *.mcf (eg. mongos|config|shard1)
        -f        if force to do
        -d        retention days of log file
        -b        directory of backups
        -c        retention count of backups
    Examples:
        1.  start
            ./mongodb_ctl.sh -m start -n config
            ./mongodb_ctl.sh -m start -n "config shard1 mongos"
        2.  stop
            ./mongodb_ctl.sh -m stop -n mongos
            ./mongodb_ctl.sh -m stop -n "config shard2"
        3.  log_rotate
            ./mongodb_ctl.sh -m log_rotate -n "config shard1 mongos" -d 15
        4.  physical_backup
            ./mongodb_ctl.sh -m physical_backup -n "shard1 shard2 config" -b "/data/mongodb/backup /data2/mongodb/backup /data/mongodb/backup" -c 5
    Crontab:
        */1 * * * * /opt/mongodb_conf/mongodb_ctl.sh -m start -n "config shard1 shard2"
        0 0 * * * /opt/mongodb_conf/mongodb_ctl.sh -m log_rotate -n "config shard1 shard2" -d 15
        0 9,11,14,16,18,23 * * * /opt/mongodb_conf/mongodb_ctl.sh -m physical_backup -n "shard1" -b "/data/mongodb/backup" -c 5
    "
}

while getopts "h? m: n: f d: b: c:" OPTION
do
    case $OPTION in
        h|\?)
            help
            exit 0
            ;;
        m)
            METHOD=$OPTARG
            ;;
        n)
            NODES="$NODES $OPTARG"
            ;;
        f)
            EXEC_FORCE=1
            ;;
        d)
            LOG_KEPT_DAYS=$OPTARG
            ;;
        b)
            BACKUP_DIRS="$BACKUP_DIRS $OPTARG"
            ;;
        c)
            BACKUP_KEPT_COUNT=$OPTARG
            ;;
    esac
done

load_conf(){
    local mcf_file=$1
    local filename=$(basename $mcf_file)
    local vars=`${PARSE_CONF_SHELL_FILE} -f ${mcf_file}`
    for var in $vars
    do
        eval "$var"
    done
}

start(){
    for NODE in $NODES
    do
        if [ ! -f $MONGODB_CONF_DIR/${NODE}.mcf ]; then
            echo "Config file not found. $MONGODB_CONF_DIR/${NODE}.mcf"
            continue
        fi
        local count=`ps -fe |grep "$MONGODB_HOME" |grep "${NODE}.mcf" | wc -l`
        if [ $count -lt 1 ]; then
            load_conf $MONGODB_CONF_DIR/${NODE}.mcf
            local log_path=`dirname ${systemLog_path}`
            mkdir -p ${log_path}
            if [ "${NODE}" == "mongos" ]; then
                $MONGODB_HOME/bin/mongos --config $MONGODB_CONF_DIR/${NODE}.mcf
            else
                mkdir -p ${storage_dbPath}
                rm -f ${storage_dbPath}/mongod.lock
                $MONGODB_HOME/bin/mongod --config $MONGODB_CONF_DIR/${NODE}.mcf
            fi
        fi
    done
}

stop(){
    for NODE in $NODES
    do
        if [ ! -f $MONGODB_CONF_DIR/${NODE}.mcf ]; then
            echo "Config file not found. $MONGODB_CONF_DIR/${NODE}.mcf"
            continue
        fi
        load_conf $MONGODB_CONF_DIR/${NODE}.mcf
        if [ ! -z $EXEC_FORCE ]; then
            $MONGODB_HOME/bin/mongo admin --port ${net_port} --eval "db.shutdownServer({force:true});"
        else
            $MONGODB_HOME/bin/mongo admin --port ${net_port} --eval "db.shutdownServer();"
        fi
    done
}

log_rotate(){
    if [ -z $LOG_KEPT_DAYS ]; then
        exit 1
    fi
    
    for NODE in $NODES
    do
        if [ ! -f $MONGODB_CONF_DIR/${NODE}.mcf ]; then
            echo "Config file not found. $MONGODB_CONF_DIR/${NODE}.mcf"
            continue
        fi
        load_conf $MONGODB_CONF_DIR/${NODE}.mcf
        $MONGODB_HOME/bin/mongo admin --port ${net_port} --eval "db.runCommand( { logRotate : 1 } );"
        # reserved N days of log
        local log_dirname=`dirname ${systemLog_path}`
        local log_basename=`basename ${systemLog_path}`
        find ${log_dirname}/ -name "${log_basename}.*" -mtime +${LOG_KEPT_DAYS} -delete
    done
}

# it is safe to run on hidden node
physical_backup(){
    local length
    local i
    local ports
    local db_paths
    local backup_dirs
    
    local l_nodes=(${NODES[*]})
    local l_backup_dirs=(${BACKUP_DIRS[*]})
    
    length=${#l_nodes[@]}
    for (( i=0; i<$length; i++ ))
    do
        local node=${l_nodes[$i]}
        if [ ! -f $MONGODB_CONF_DIR/${node}.mcf ]; then
            echo "Config file not found. $MONGODB_CONF_DIR/${node}.mcf"
            continue
        fi
        load_conf $MONGODB_CONF_DIR/${node}.mcf
        local is_secondary=`$MONGODB_HOME/bin/mongo admin --port ${net_port} --eval "db.isMaster().secondary" |tail -n1`
        if [ "${is_secondary}" == "true" ] || [ ! -z $EXEC_FORCE ]; then
            ports="$ports ${net_port}"
            db_paths="$db_paths ${storage_dbPath}"
            backup_dirs="$backup_dirs ${l_backup_dirs[$i]}"
        else
            echo "The source node is not secondary."
        fi
    done
    
    ports=(${ports})
    db_paths=(${db_paths})
    backup_dirs=(${backup_dirs})
    
    local daytime=$(date +%Y%m%d-%H%M%S)
    length=${#ports[@]}
    for (( i=0; i<$length; i++ ))
    do
    {
        local port=${ports[$i]}
        local backup_dir=${backup_dirs[$i]}
        local db_path=${db_paths[$i]}
        if [ ! -z ${backup_dir} ]; then
            $MONGODB_HOME/bin/mongo admin --port ${port} --eval "db.fsyncLock();"
            ls -d ${backup_dir}/* -1 |sort -r -n |tail -n +${BACKUP_KEPT_COUNT} |xargs rm -rf
            mkdir -p ${backup_dir}/${daytime}
            cp -r ${db_path} ${backup_dir}/${daytime}/
            $MONGODB_HOME/bin/mongo admin --port ${port} --eval "db.fsyncUnlock();"
        fi
    }&
    done
    wait
}

case "$METHOD" in
    start)             start
                       ;;
    stop)              stop
                       ;;
    log_rotate)        log_rotate
                       ;;
    physical_backup)   physical_backup
                       ;;
    *)                 help
                       ;;
esac
