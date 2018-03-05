#!/bin/bash
# convention over configuration

#### convention
PGXL_USER=postgres
PGXL_DATA_HOME=/data/pgxl_data
PGXL_LOG_HOME=/log/pgxl_log
PGXL_XLOG_HOME=/log/pgxl_xlog
PGXL_ALOG_HOME=/data/pgxl_alog
PGXL_BACKUP_HOME=/data/pgxl_basebackup
GTM_DIR_NAME=gtm
GTM_PROXY_DIR_NAME=gtm_pxy
COORDINATOR_DIR_NAME=coord
DATANODE_DIR_NAME=dn

PORT_SSH=22
PORT_GTM=20001
PORT_GTM_PXY=20002
PORT_COORD=21001
PORT_COORD_POOL=21101
PORT_DN=23001
PORT_DN_POOL=23101

# control
#CTL_HOME=$(dirname $(readlink -f $0))
CTL_HOME=/data/pgxl_ctl
CTL_LOG_DIR=$CTL_HOME/log
CTL_ETC_DIR=$CTL_HOME/etc
CTL_RUN_DIR=$CTL_HOME/run
CTL_SQL_DIR=$CTL_HOME/sql
INIT_CONFIG=pgxl_init.conf
RUNTIME_CONFIG=pgxl_runtime.conf
REGISTER_SQL=register_nodes.sql
REBALANCE_SQL=rebalance_data.sql
ROLLBACK_SQL=rollback_px.sql
VACUUM_FREEZE_SQL=vacuum_freeze_dbs.sql
ANALYZE_SQL=analyze_dbs.sql
LOG_FILE=pgxl_ctl.log

TRUE=1
FALSE=2
STATUS_RUNNING=1
STATUS_STOPPED=2
STATUS_UNREACHABLE=3
KEEPER_INTERVAL=5s

# template
TEMPLATE_GTM=gtm.conf
TEMPLATE_GTM_PROXY=gtm_proxy.conf
TEMPLATE_HBA=pg_hba.conf
TEMPLATE_COORD=postgresql.coord.conf
TEMPLATE_DN=postgresql.dn.conf
TEMPLATE_DN_RECOVERY=recovery.conf
TEMPLATE_PGPASS=pgpass.conf

# option
METHOD=
MODE=
NAME=
ROLE=
CONDITION=

# variables in INIT_CONFIG file
declare GTM_MASTER_HOST GTM_STANDBY_HOST
declare -a GTM_PROXY_NAMES DATANODE_NAMES COORDINATOR_NAMES
declare -A GTM_PROXY_HOSTS 
declare -A DATANODE_MASTER_HOSTS DATANODE_STANDBY_HOSTS DATANODE_BACKUP_HOSTS 
declare -A COORDINATOR_HOSTS

# variables in RUNTIME_CONFIG file
declare RT_GTM_MASTER_HOST RT_GTM_STANDBY_HOST
declare -a RT_GTM_PROXY_NAMES RT_DATANODE_NAMES RT_COORDINATOR_NAMES RT_DBS
declare -A RT_GTM_PROXY_HOSTS
declare -A RT_DATANODE_MASTER_HOSTS RT_DATANODE_STANDBY_HOSTS RT_DATANODE_BACKUP_HOSTS
declare -A RT_COORDINATOR_HOSTS

# shared variable
VARS=

# aliases
shopt -s expand_aliases
alias ssh="ssh -p $PORT_SSH -o StrictHostKeyChecking=no"
alias scp="scp -P $PORT_SSH -o StrictHostKeyChecking=no"

# read parameters from command line
while getopts m:z:n:r:c: OPTION
do
     case $OPTION in
         m)
             METHOD=$OPTARG 
         ;;
         z)
             MODE=$OPTARG 
         ;;
         n)
             NAME=$OPTARG
         ;;
         r)
             ROLE=$OPTARG
         ;;
         c)
             CONDITION=$OPTARG
         ;;
     esac
done
shift $((OPTIND-1))

log(){
    local lines=$1
    echo "$lines" | awk -v date="$(date '+%Y-%m-%d %H:%M:%S')" '{if(NF){print date" "$0}}' >> $CTL_LOG_DIR/$LOG_FILE
    echo "$lines" | awk '{if(NF){print $0}}'
}
# functions and variables are not accessible in separate programs, e.g. when run "bash -c ...". 
# so "declare -x" or "export" them for the next usage like this, "| xargs -0 -n1 -r bash -c 'log "$@"' _"
declare -x -f log
declare -x CTL_LOG_DIR
declare -x LOG_FILE

run_cmd_on(){
    [ "$1" == "local" ] && eval "$2" | xargs -0 -n1 -r bash -c 'log "$@"' _ || ssh $1 "$2" | xargs -0 -n1 -r bash -c 'log "$@"' _
}

add_node_info_to_rt_conf(){
    local mode=$1
    local name=$2
    local role=$3
    if [ "$mode" == "gtm" ] || [ -z $mode ]; then 
        if [ "$role" == "master" ] || [ -z $role ]; then 
            [ -z $GTM_MASTER_HOST ] || set_rt_conf "RT_GTM_MASTER_HOST=$GTM_MASTER_HOST"
        fi
        if [ "$role" == "standby" ] || [ -z $role ]; then 
            [ -z $GTM_STANDBY_HOST ] || set_rt_conf "RT_GTM_STANDBY_HOST=$GTM_STANDBY_HOST"
        fi
    fi
    if [ "$mode" == "gtm_proxy" ] || [ -z $mode ]; then 
        for nodename in ${GTM_PROXY_NAMES[*]}
        do
            [ ! -z $name ] && [ "$name" != "$nodename" ] && continue
            [[ " ${RT_GTM_PROXY_NAMES[@]} " =~ " $nodename " ]] || set_rt_conf "RT_GTM_PROXY_NAMES=(${RT_GTM_PROXY_NAMES[*]} $nodename)"
            [ -z ${GTM_PROXY_HOSTS[$nodename]} ] || set_rt_conf "RT_GTM_PROXY_HOSTS[$nodename]=${GTM_PROXY_HOSTS[$nodename]}"
        done
    fi
    if [ "$mode" == "coordinator" ] || [ -z $mode ]; then 
        for nodename in ${COORDINATOR_NAMES[*]}
        do
            [ ! -z $name ] && [ "$name" != "$nodename" ] && continue
            [[ " ${RT_COORDINATOR_NAMES[@]} " =~ " $nodename " ]] || set_rt_conf "RT_COORDINATOR_NAMES=(${RT_COORDINATOR_NAMES[*]} $nodename)"
            [ -z ${COORDINATOR_HOSTS[$nodename]} ] || set_rt_conf "RT_COORDINATOR_HOSTS[$nodename]=${COORDINATOR_HOSTS[$nodename]}"
        done
    fi
    if [ "$mode" == "datanode" ] || [ -z $mode ]; then 
        for nodename in ${DATANODE_NAMES[*]}
        do
            [ ! -z $name ] && [ "$name" != "$nodename" ] && continue
            [[ " ${RT_DATANODE_NAMES[@]} " =~ " $nodename " ]] || set_rt_conf "RT_DATANODE_NAMES=(${RT_DATANODE_NAMES[*]} $nodename)"
            if [ "$role" == "master" ] || [ -z $role ]; then 
                [ -z ${DATANODE_MASTER_HOSTS[$nodename]} ] || set_rt_conf "RT_DATANODE_MASTER_HOSTS[$nodename]=${DATANODE_MASTER_HOSTS[$nodename]}"
            fi
            if [ "$role" == "standby" ] || [ -z $role ]; then 
                [ -z ${DATANODE_STANDBY_HOSTS[$nodename]} ] || set_rt_conf "RT_DATANODE_STANDBY_HOSTS[$nodename]=${DATANODE_STANDBY_HOSTS[$nodename]}"
            fi
            if [ "$role" == "backup" ] || [ -z $role ]; then 
                [ -z ${DATANODE_BACKUP_HOSTS[$nodename]} ] || set_rt_conf "RT_DATANODE_BACKUP_HOSTS[$nodename]=${DATANODE_BACKUP_HOSTS[$nodename]}"
            fi
        done
    fi
}

set_rt_conf(){
    eval "$1"
    local pattern=$(echo $1 | cut -d '=' -f 1 | sed "s|\[|\\\[|g" | sed "s|\]|\\\]|g")
    local pattern_replace=$(echo $1 | sed "s|\[|\\\[|g" | sed "s|\]|\\\]|g")
    if grep "^${pattern}" $CTL_RUN_DIR/$RUNTIME_CONFIG; then
        sed -i "s|^$pattern.*|$pattern_replace|g" $CTL_RUN_DIR/$RUNTIME_CONFIG
    else
        echo "$1" >> $CTL_RUN_DIR/$RUNTIME_CONFIG
    fi
    log "Set runtime config : $1"
}

load_config(){
    local file=$1
    while read line
    do
        [ "$line" != "" ] && log "Load config $file : $line" && eval "$line"
    done < $file
}

get_node_status(){
    get_node_status_by_nmap "$1" "$2"
    if [[ $VARS -eq $STATUS_UNREACHABLE ]]; then
        sleep 1s
        get_node_status_by_nmap "$1" "$2"
    fi
}

# user need to run nmap as root. visudo add 'postgres ALL=(ALL)  NOPASSWD: /usr/bin/nmap'
get_node_status_by_nmap(){
    local host=$1
    local port=$2
    if [ -z $host ] || [ -z $port ]; then
        VARS=$STATUS_UNREACHABLE
    else
        local resp=`sudo -n nmap $host -p $port |grep "$port/tcp" |awk '{print $2}'`
        if [ "$resp" == "" ]; then
            VARS=$STATUS_UNREACHABLE
        elif [ "$resp" == "open" ]; then
            VARS=$STATUS_RUNNING
        elif [ "$resp" == "closed" ]; then
            VARS=$STATUS_STOPPED
        else
            VARS=$STATUS_UNREACHABLE
        fi
    fi
}

get_node_status_by_ssh(){
    local host=$1
    local port=$2
    if [ -z $host ] || [ -z $port ]; then
        VARS=$STATUS_UNREACHABLE
    else
        # by the way, do not use telnet to check port of postgres or gtm. it create a complete connection to pg, and will make pg error.
        #local resp=`echo -e "\n" | timeout 1 telnet $host $PORT_SSH 2>/dev/null | grep Connected`
        local resp
        ping -c 1 -w 1 $host &>/dev/null && resp="Connected" || resp=""
        if [ "$resp" == "" ]; then
            VARS=$STATUS_UNREACHABLE
        else
            # it is simpler to use pidof gtm or gtm_proxy. but the server may does not install pidof.
            if [ "$port" == "$PORT_GTM" ]; then
                resp=`ssh $host "ps aux |grep \"gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME\" | grep -v \"grep\""`
            elif [ "$port" == "$PORT_GTM_PXY" ]; then
                resp=`ssh $host "ps aux |grep \"gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME\" | grep -v \"grep\""`
            elif [ "$port" == "$PORT_COORD" ]; then
                resp=`ssh $host "ps aux |grep \"postgres --coordinator\" |grep \" -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME\""`
            elif [ "$port" == "$PORT_DN" ]; then
                resp=`ssh $host "ps aux |grep \"postgres --datanode\" |grep \" -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME\""`
            fi
            [ "$resp" == "" ] && VARS=$STATUS_STOPPED || VARS=$STATUS_RUNNING
        fi
    fi
}

get_node_status_info(){
    local host=$1
    local port=$2
    get_node_status "$host" $port
    local status=$VARS
    if [[ $status -eq $STATUS_RUNNING ]]; then
        echo -e "\e[1;32mRunning\e[0m"
    elif [[ $status -eq $STATUS_STOPPED ]]; then
        echo -e "\e[1;33mStopped\e[0m"
    elif [[ $status -eq $STATUS_UNREACHABLE ]]; then
        echo -e "\e[1;31mHostdown\e[0m"
    else
        echo -e "\e[1;41mERROR\e[0m"
    fi
}

# $1 is array of host to search within, $2 is port to checkout, $3 is host that should be excluded
search_an_active_host(){
    local hosts=($1)
    local port=$2
    local excluded=$3
    local host
    for host in ${hosts[*]}; do
        if [ "$host" != "$excluded" ]; then 
            get_node_status "$host" $port
            local status=$VARS
            [[ $status -eq $STATUS_RUNNING ]] && VARS=$host && break || VARS=""
        fi
    done
}

# $1 is host, $2 is directory
is_remote_dir_exists(){
    [ "`ssh $1 "ls -d $2 2>/dev/null"`" == "$2" ] && VARS=$TRUE || VARS=$FALSE
}

# $1 is host, $2 is file
is_remote_file_exists(){
    [ "`ssh $1 "ls $2 2>/dev/null"`" == "$2" ] && VARS=$TRUE || VARS=$FALSE
}

# lock before add node
is_cluster_locked_for_ddl(){
    search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
    local host=$VARS
    local rs=`ssh $host "psql -p $PORT_COORD -c \"select pgxc_lock_for_backup();\" -t 2>&1" |grep 'lock is already held' |wc -l`
    [[ $rs -gt 0 ]] && VARS=$TRUE || VARS=$FALSE
}

# clean or backup existed data directory
pretreatment_remote_directory(){
    local host=$1
    local dirname=$2
    local condition=$3
    if [ -z $dirname ] || [ -z $condition ]; then 
        log "WARNING: Pretreatment remote directory failed. host: ${host}. dirname: ${dirname}. condition: ${condition}"
        return 1
    fi
    local shell_cmds
    if [ "$condition" == "clean" ]; then 
        log "Clean $host $PGXL_DATA_HOME/$dirname"
        shell_cmds="cd $PGXL_DATA_HOME && rm -rf $dirname;"
        [ -z $PGXL_XLOG_HOME ] || shell_cmds=${shell_cmds}"cd $PGXL_XLOG_HOME && rm -rf $dirname;"
    elif [ "$condition" == "backup" ]; then
        local daytime=`date +%Y%m%d-%H%M%S`
        log "Backup $host $PGXL_DATA_HOME/$dirname backup to $PGXL_DATA_HOME/${dirname}_$daytime"
        shell_cmds="cd $PGXL_DATA_HOME && mv $dirname ${dirname}_$daytime;"
        [ -z $PGXL_XLOG_HOME ] || shell_cmds=${shell_cmds}"cd $PGXL_XLOG_HOME && mv $dirname ${dirname}_$daytime;"
    fi
    [ -z "$shell_cmds" ] || run_cmd_on $host "$shell_cmds"
}

#select * from pgxc_class c where exists (select * from pgxc_node n where n.node_name = 'datanode2' and (c.nodeoids::oid[] @> ARRAY[n.oid]));
#psql -p 21001 -c "select * from pgxc_class c where exists (select * from pgxc_node n where n.node_name = 'datanode2' and (n.oid = c.nodeoids[0] or n.oid = c.nodeoids[1] or n.oid = c.nodeoids[2]));" -t > tjjj
is_datanode_ready_to_be_removed(){
    local name=$1
    search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
    local host=$VARS
    local sql="select count(*) from pgxc_class c where exists (select node_name from pgxc_node n where n.node_name = '$name' and (c.nodeoids::oid[] @> ARRAY[n.oid]));"
    VARS=$TRUE
    load_databases $host $PORT_COORD
    for dbname in ${RT_DBS[*]}; do
        local count=`ssh $host "psql -p $PORT_COORD -d $dbname -t -c \"$sql\" 2>/dev/null |tr -s '\n' |sed 's/^[ \t]*//g'"`
        [[ $count -gt 0 ]] && VARS=$FALSE && break
    done
}

load_databases(){
    local host=$1
    local port=$2
    [[ ${#RT_DBS[@]} -eq 0 ]] && RT_DBS=(`ssh $host "psql -p $port -t -c \"select datname from pg_database;\" 2>/dev/null |tr -s '\n' |sed 's/^[ \t]*//g'"`)
}

prepare_register_sql(){
    local operate=$1
    local node=$2
    local primary=${RT_DATANODE_NAMES[0]}
    local name
    echo "" > $CTL_RUN_DIR/$REGISTER_SQL
    if [ "$operate" == "delete" ] && [ "$node" != "$primary" ]; then
        echo "DROP NODE $node;" >> $CTL_RUN_DIR/$REGISTER_SQL
    else
        for name in ${RT_COORDINATOR_NAMES[*]}; do
            local host=${RT_COORDINATOR_HOSTS[$name]}
            if [ "$host" != "" ]; then
                echo "CREATE NODE $name WITH (TYPE='coordinator', HOST='$host', PORT=$PORT_COORD);" >> $CTL_RUN_DIR/$REGISTER_SQL
                echo "ALTER NODE $name WITH (TYPE='coordinator', HOST='$host', PORT=$PORT_COORD);" >> $CTL_RUN_DIR/$REGISTER_SQL
            fi
        done
        for name in ${RT_DATANODE_NAMES[*]}; do
            local host=${RT_DATANODE_MASTER_HOSTS[$name]}
            if [ "$name" == "$primary" ]; then
                echo "CREATE NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN, PRIMARY);" >> $CTL_RUN_DIR/$REGISTER_SQL
                echo "ALTER NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN, PRIMARY);" >> $CTL_RUN_DIR/$REGISTER_SQL
            else
                echo "CREATE NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN);" >> $CTL_RUN_DIR/$REGISTER_SQL
                echo "ALTER NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN);" >> $CTL_RUN_DIR/$REGISTER_SQL
            fi
        done
    fi
    echo "SELECT pg_reload_conf();" >> $CTL_RUN_DIR/$REGISTER_SQL
    run_cmd_on local "echo \"Register SQL:\";cat $CTL_RUN_DIR/$REGISTER_SQL"
}

prepare_rebalance_sql(){
    local name=$1
    local host=$2
    local condition=$3
    local shell_cmds=""
    echo "SET statement_timeout = 0;" > $CTL_RUN_DIR/$REBALANCE_SQL
    load_databases $host $PORT_COORD
    for dbname in ${RT_DBS[*]}; do
        shell_cmds=${shell_cmds}"echo \"\c $dbname\";"
        if [ "$condition" == "add" ]; then
            shell_cmds=${shell_cmds}"
                psql -p $PORT_COORD -d $dbname -c \"SELECT 'ALTER TABLE \\\"' || pg_class.relname || '\\\" ADD NODE ($name);' FROM pgxc_class INNER JOIN pg_class ON pgxc_class.pcrelid = pg_class.oid;\" -t;
            " 
        elif [ "$condition" == "delete" ]; then
            shell_cmds=${shell_cmds}"
                psql -p $PORT_COORD -d $dbname -c \"SELECT 'ALTER TABLE \\\"' || pg_class.relname || '\\\" DELETE NODE ($name);' FROM pgxc_class INNER JOIN pg_class ON pgxc_class.pcrelid = pg_class.oid;\" -t;
            "
        fi
    done
    [ -z "$shell_cmds" ] || ssh $host "$shell_cmds" >> $CTL_RUN_DIR/$REBALANCE_SQL 2>/dev/null
    run_cmd_on local "echo \"Rebalance SQL:\";cat $CTL_RUN_DIR/$REBALANCE_SQL"
}

prepare_analyze_sql(){
    local mode=$1
    local host=$2
    echo "SET statement_timeout = '1h';" > $CTL_RUN_DIR/$ANALYZE_SQL
    load_databases $host $PORT_COORD
    for dbname in ${RT_DBS[*]}; do
        [ "$dbname" == "template0" ] && continue
        echo "\c $dbname" >> $CTL_RUN_DIR/$ANALYZE_SQL
        if [ -z $mode ] || [ "$mode" == "all" ]; then 
            echo "ANALYZE;" >> $CTL_RUN_DIR/$ANALYZE_SQL
        elif [ "$mode" == "coordinator" ]; then 
            echo "ANALYZE (coordinator);" >> $CTL_RUN_DIR/$ANALYZE_SQL
        fi
    done
    run_cmd_on local "echo \"Rebalance SQL:\";cat $CTL_RUN_DIR/$ANALYZE_SQL"
}

# serial or semi-parallel operation based on $RUNTIME_CONFIG file.
do_runtime_operate(){
    local operate mode name role condition serial arg OPTIND
    while getopts "o:m:n:r:c:s" arg
    do
         case "$arg" in
             o) operate="$OPTARG" ;;
             m) mode="$OPTARG" ;;
             n) name="$OPTARG" ;;
             r) role="$OPTARG" ;;
             c) condition="$OPTARG" ;;
             s) serial=1 ;;
         esac
    done
    shift $((OPTIND-1))
    if [ "$mode" == "gtm" ]; then 
        if [ "$role" == "master" ] || [ "$role" == "" ]; then 
            op_gtm $operate gtm master "$condition"
        fi
        if [ "$role" == "standby" ] || [ "$role" == "" ]; then 
            op_gtm $operate gtm standby "$condition"
        fi
    elif [ "$mode" == "gtm_proxy" ]; then 
        if [ "$name" == "" ]; then 
            for nodename in ${RT_GTM_PROXY_NAMES[*]}; do
                [ -z $serial ] && { op_gtm_proxy $operate $nodename "$condition" & } || op_gtm_proxy $operate $nodename "$condition"
            done
            wait
        else
            op_gtm_proxy $operate $name "$condition"
        fi
    elif [ "$mode" == "coordinator" ]; then 
        if [ "$name" == "" ]; then 
            for nodename in ${RT_COORDINATOR_NAMES[*]}; do
                [ -z $serial ] && { op_coordinator $operate $nodename "$condition" & } || op_coordinator $operate $nodename "$condition"
            done
            wait
        else
            op_coordinator $operate "$name" "$condition"
        fi
    elif [ "$mode" == "datanode" ]; then 
        if [ "$name" == "" ]; then 
            if [ "$role" == "master" ] || [ "$role" == "" ]; then 
                for nodename in ${RT_DATANODE_NAMES[*]}; do
                    [ -z $serial ] && { op_datanode $operate $nodename master "$condition" & } || op_datanode $operate $nodename master "$condition"
                done
                wait
            fi
            if [ "$role" == "standby" ] || [ "$role" == "" ]; then 
                for nodename in ${RT_DATANODE_NAMES[*]}; do
                    [ -z $serial ] && { op_datanode $operate $nodename standby "$condition" & } || op_datanode $operate $nodename standby "$condition"
                done
                wait
            fi
            if [ "$role" == "backup" ] || [ "$role" == "" ]; then 
                for nodename in ${RT_DATANODE_NAMES[*]}; do
                    [ -z $serial ] && { op_datanode $operate $nodename backup "$condition" & } || op_datanode $operate $nodename backup "$condition"
                done
                wait
            fi
        else
            if [ "$role" == "master" ] || [ "$role" == "" ]; then 
                op_datanode $operate $name master "$condition"
            fi
            if [ "$role" == "standby" ] || [ "$role" == "" ]; then 
                op_datanode $operate $name standby "$condition"
            fi
            if [ "$role" == "backup" ] || [ "$role" == "" ]; then 
                op_datanode $operate $name backup "$condition"
            fi
        fi
    fi
}

op_gtm(){
    local op=$1
    local name=$2
    local role=$3
    local condition="$4"
    local host_m=$RT_GTM_MASTER_HOST
    local host_s=$RT_GTM_STANDBY_HOST
    local host
    if [ "$role" == "master" ]; then
        host=$host_m
    elif [ "$role" == "standby" ]; then
        host=$host_s
    fi
    [ -z $host ] && return 1
    log "Operate on gtm: $op $role $host $condition"
    get_node_status $host $PORT_GTM
    local stat_of_host=$VARS
    if [[ $stat_of_host -eq $STATUS_UNREACHABLE ]]; then
        log "WARNING: can not $op on $name($host). Host is unreachable"
        return 1
    elif [[ $stat_of_host -eq $STATUS_RUNNING ]]; then
        if [[ " init rebuild " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is running. Try to stop it before. $0 -m stop -z gtm -n $name -r $role"
            return 1
        elif [[ " start " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already running."
            return 1
        fi
    elif [[ $stat_of_host -eq $STATUS_STOPPED ]]; then
        if [[ " promote " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is stopped. Try to start it before. $0 -m start -z gtm -n $name -r $role"
            return 1
        elif [[ " stop shutdown " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already stopped."
            return 1
        fi
    fi
    if [ "$op" == "init" ]; then 
        gtm_init "$host" "$role" "$condition"
    elif [ "$op" == "kill" ]; then 
        gtm_kill "$host"
    elif [ "$op" == "start" ] || [ "$op" == "stop" ] || [ "$op" == "restart" ] || [ "$op" == "status" ]; then 
        gtm_start_stop_restart_status "$op" "$host"
    elif [ "$op" == "shutdown" ]; then 
        gtm_shutdown "$host"
    elif [ "$op" == "promote" ]; then 
        gtm_promote "$host"
    elif [ "$op" == "rebuild" ] && [ "$role" == "standby" ]; then 
        init_gtm_standby "$host_m" "$host_s" "$condition"
    elif [ "$op" == "syncconfig" ]; then 
        gtm_sync_config "$role"
    elif [ "$op" == "exec" ]; then 
        run_cmd_on "$host" "$condition"
    else 
        log "WARNING: $op $name $role $host failed. $op is not supported by $FUNCNAME"
    fi
}

gtm_init(){
    local host=$1
    local role=$2
    local condition=$3
    pretreatment_remote_directory $host $GTM_DIR_NAME $condition
    is_remote_dir_exists $host $PGXL_DATA_HOME/$GTM_DIR_NAME
    local resp=$VARS
    if [[ $resp -eq $TRUE ]]; then
        log "WARNING: Skip init. Gtm $role already exists"
    elif [[ $resp -eq $FALSE ]]; then
        run_cmd_on $host "initgtm -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME"
        gtm_sync_config $role
    fi
}

gtm_sync_config(){
    local role=$1
    local host_m=$RT_GTM_MASTER_HOST
    local host_s=$RT_GTM_STANDBY_HOST
    local host
    local host_a
    local startup
    if [ "$role" == "master" ]; then
        host=$host_m
        host_a=$host_s
        startup="ACT"
    elif [ "$role" == "standby" ]; then
        host=$host_s
        host_a=$host_m
        startup="STANDBY"
    fi
    scp $CTL_ETC_DIR/$TEMPLATE_GTM $host:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf 1>/dev/null
    run_cmd_on $host "
        mkdir -p $PGXL_LOG_HOME/$GTM_DIR_NAME;
        sed -i \"s|^log_file.*|log_file = \'$PGXL_LOG_HOME/$GTM_DIR_NAME/gtm.log\'|g\" $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf;
        sed -i \"s/^active_host.*/active_host = \'$host_a\'/g\" $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf;
        sed -i \"s/^startup.*/startup = ${startup}/g\" $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf;
    "
}

gtm_start_stop_restart_status(){
    local op=$1
    local host=$2
    run_cmd_on $host "
        cat $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control 2>/dev/null;
        gtm_ctl $op -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME -m fast -w -t 5 > $FUNCNAME.$op.oplog 2>&1;
        cat $FUNCNAME.$op.oplog
    "
}

gtm_shutdown(){
    run_cmd_on $1 "
        cat $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control 2>/dev/null;
        gtm_ctl stop -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME -m fast -w -t 5 > $FUNCNAME.oplog 2>&1 || ps aux |grep \"gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME\" | grep -v \"grep\" | awk '{print \$2}' | xargs -r kill -9;
        cat $FUNCNAME.oplog
    "
}

gtm_kill(){
    # the $ in the $2 argument should be escaped
    run_cmd_on $1 "ps aux |grep \"gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME\" | grep -v \"grep\" | awk '{print \$2}' | xargs -r kill -9"
}

gtm_promote(){
    run_cmd_on $1 "
        gtm_ctl promote -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME -m fast -w -t 5 > $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

op_gtm_proxy(){
    local op=$1
    local name=$2
    local condition=$3
    local host=${RT_GTM_PROXY_HOSTS[$name]}
    [ -z $host ] && return 1
    log "Operate on gtm proxy: $op $name $host $condition"
    get_node_status "$host" $PORT_GTM_PXY
    local stat_of_host=$VARS
    if [[ $stat_of_host -eq $STATUS_UNREACHABLE ]]; then
        log "WARNING: can not $op on $name($host). Host is unreachable"
    elif [[ $stat_of_host -eq $STATUS_RUNNING ]]; then
        if [[ " init rebuild " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is running. Try to stop it before. $0 -m stop -z gtm_proxy -n $name"
            return 1
        elif [[ " start " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already running."
            return 1
        fi
    elif [[ $stat_of_host -eq $STATUS_STOPPED ]]; then
        if [[ " reconnect " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is stopped. Try to start it before. $0 -m start -z gtm_proxy -n $name"
            return 1
        elif [[ " stop shutdown " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already stopped."
            return 1
        fi
    fi
    if [ "$op" == "init" ]; then 
        gtm_proxy_init "$name" "$host" "$RT_GTM_MASTER_HOST" "$condition"
    elif [ "$op" == "kill" ]; then 
        gtm_proxy_kill "$host"
    elif [ "$op" == "start" ] || [ "$op" == "stop" ] || [ "$op" == "restart" ] || [ "$op" == "status" ]; then 
        gtm_proxy_start_stop_restart_status "$op" "$host"
    elif [ "$op" == "shutdown" ]; then 
        gtm_proxy_shutdown "$host"
    elif [ "$op" == "reconnect" ]; then 
        gtm_proxy_reconnect "$host"
    elif [ "$op" == "rebuild" ]; then 
        gtm_proxy_init "$name" "$host" "$RT_GTM_MASTER_HOST" "$condition"
    elif [ "$op" == "syncconfig" ]; then 
        gtm_proxy_sync_config "$name" "$host" "$RT_GTM_MASTER_HOST"
    elif [ "$op" == "exec" ]; then 
        run_cmd_on "$host" "$condition"
    else 
        log "WARNING: $op $name $host failed. $op is not supported by $FUNCNAME"
    fi
}

gtm_proxy_init(){
    local name=$1
    local host=$2
    local host_m=$3
    local condition=$4
    pretreatment_remote_directory $host $GTM_PROXY_DIR_NAME $condition
    is_remote_dir_exists $host $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME
    local resp=$VARS
    if [[ $resp -eq $TRUE ]]; then
        log "WARNING: Skip init. Gtm proxy $name already exists"
    elif [[ $resp -eq $FALSE ]]; then
        run_cmd_on $host "initgtm -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME;"
        gtm_proxy_sync_config $name $host $host_m
    fi
}

gtm_proxy_sync_config(){
    local name=$1
    local host=$2
    local host_m=$3
    scp $CTL_ETC_DIR/$TEMPLATE_GTM_PROXY $host:$PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf 1>/dev/null
    run_cmd_on $host "
        mkdir -p $PGXL_LOG_HOME/$GTM_PROXY_DIR_NAME;
        sed -i \"s/^nodename.*/nodename = \'$name\'/g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
        sed -i \"s/^gtm_host.*/gtm_host = \'$host_m\'/g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
        sed -i \"s|^log_file.*|log_file = \'$PGXL_LOG_HOME/$GTM_PROXY_DIR_NAME/gtm_pxy.log\'|g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
    "
}

# there is a bug when use "gtm_ctl start -Z gtm_proxy" with docker. use "gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME > $op_$name.oplog 2>&1" instead.
gtm_proxy_start_stop_restart_status(){
    local op=$1
    local host=$2
    run_cmd_on $host "
        gtm_ctl $op -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME -m fast -w -t 5 > $FUNCNAME.$op.oplog 2>&1;
        cat $FUNCNAME.$op.oplog
    "
}

# the reconnect operation will create a new file "newgtm" under gtm proxy home, written to the content of "-s ... -t ..." behind "-o". it is not loaded when gtm proxy starts
gtm_proxy_reconnect(){
    run_cmd_on $1 "
        gtm_ctl reconnect -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME -o \"-s $RT_GTM_MASTER_HOST -t $PORT_GTM\" > $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

gtm_proxy_shutdown(){
    run_cmd_on $1 "
        gtm_ctl stop -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME -m fast -w -t 5 > $FUNCNAME.oplog 2>&1 || ps aux |grep \"gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME\" | grep -v \"grep\" | awk '{print \$2}' | xargs -r kill -9;
        cat $FUNCNAME.oplog
    "
}

gtm_proxy_kill(){
    run_cmd_on $1 "ps aux |grep \"gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME\" | grep -v \"grep\" | awk '{print \$2}' | xargs -r kill -9"
}


op_coordinator(){
    local op=$1
    local name=$2
    local condition=$3
    local host=${RT_COORDINATOR_HOSTS[$name]}
    [ -z $host ] && return 1
    log "Operate on coordinator: $op $name $host $condition"
    get_node_status "$host" $PORT_COORD
    local stat_of_host=$VARS
    if [[ $stat_of_host -eq $STATUS_UNREACHABLE ]]; then
        log "WARNING: can not $op on $name($host). Host is unreachable"
    elif [[ $stat_of_host -eq $STATUS_RUNNING ]]; then
        if [[ " init copy " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is running. Try to stop it before. $0 -m stop -z coordinator -n $name"
            return 1
        elif [[ " start " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already running."
            return 1
        fi
    elif [[ $stat_of_host -eq $STATUS_STOPPED ]]; then
        if [[ " reload register reconfig xsql " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is stopped. Try to start it before. $0 -m start -z coordinator -n $name"
            return 1
        elif [[ " stop shutdown " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already stopped."
            return 1
        fi
    fi
    if [ "$op" == "init" ]; then 
        coordinator_init "$name" "$host" "$condition"
    elif [ "$op" == "kill" ]; then 
        coordinator_kill "$host"
    elif [ "$op" == "start" ] || [ "$op" == "stop" ] || [ "$op" == "restart" ] || [ "$op" == "status" ] || [ "$op" == "reload" ]; then 
        coordinator_start_stop_restart_status_reload "$op" "$host"
    elif [ "$op" == "shutdown" ]; then 
        coordinator_shutdown "$host"
    elif [ "$op" == "cleanlog" ]; then 
        coordinator_cleanlog "$host" "$condition"
    elif [ "$op" == "register" ]; then 
        coordinator_register "$host"
    elif [ "$op" == "copy" ]; then 
        coordinator_copy "$name" "$host" "$condition"
    elif [ "$op" == "syncconfig" ]; then 
        coordinator_sync_config "$name" "$host"
    elif [ "$op" == "reconfig" ]; then 
        coordinator_reconfig "$host"
    elif [ "$op" == "analyze" ]; then 
        coordinator_analyze "$host" 
    elif [ "$op" == "xsql" ]; then 
        coordinator_xsql "$host" "$condition"
    elif [ "$op" == "exec" ]; then 
        run_cmd_on "$host" "$condition"
    else 
        log "WARNING: $op $name $role $host failed. It is not supported by $FUNCNAME"
    fi
}

coordinator_init(){
    local name=$1
    local host=$2
    local condition=$3
    pretreatment_remote_directory $host $COORDINATOR_DIR_NAME $condition
    is_remote_dir_exists $host $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME
    local resp=$VARS
    if [[ $resp -eq $TRUE ]]; then
        log "WARNING: Skip init. Coordinator $name exists"
    elif [[ $resp -eq $FALSE ]]; then
        run_cmd_on $host "initdb --nodename $name -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME;"
        coordinator_sync_config $name $host
    fi
}

coordinator_sync_config(){
    local name=$1
    local host=$2
    scp $CTL_ETC_DIR/$TEMPLATE_HBA $host:$PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_hba.conf 1>/dev/null
    scp $CTL_ETC_DIR/$TEMPLATE_COORD $host:$PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf 1>/dev/null
    scp $CTL_ETC_DIR/$TEMPLATE_PGPASS $host:$PGXL_DATA_HOME/$TEMPLATE_PGPASS 1>/dev/null
    shell_cmds="
        chmod 0600 $PGXL_DATA_HOME/$TEMPLATE_PGPASS;
        rm -f ~/.pgpass;
        ln -s $PGXL_DATA_HOME/$TEMPLATE_PGPASS ~/.pgpass;
        mkdir -p $PGXL_LOG_HOME/$COORDINATOR_DIR_NAME;
        sed -i \"s/^pgxc_node_name.*/pgxc_node_name = \'$name\'/g\" $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf;
        sed -i \"s|^log_directory.*|log_directory = \'$PGXL_LOG_HOME/$COORDINATOR_DIR_NAME\'|g\" $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf;
    "
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        shell_cmds=${shell_cmds}"
            mkdir -p $PGXL_XLOG_HOME/$COORDINATOR_DIR_NAME;
            test ! -L $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog && mv $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog $PGXL_XLOG_HOME/$COORDINATOR_DIR_NAME/;
            test ! -L $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog && ln -s $PGXL_XLOG_HOME/$COORDINATOR_DIR_NAME/pg_xlog $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog
        "
    fi
    
    run_cmd_on $host "$shell_cmds"
}

coordinator_start_stop_restart_status_reload(){
    local op=$1
    local host=$2
    run_cmd_on $host "
        pg_ctl $op -Z coordinator -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME -m fast -w -t 5 > $FUNCNAME.$op.oplog 2>&1;
        cat $FUNCNAME.$op.oplog
    "
}

coordinator_shutdown(){
    run_cmd_on $1 "
        pg_ctl stop -Z coordinator -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME -m fast -w -t 8 > $FUNCNAME.oplog 2>&1 || ps aux |grep \"postgres --coordinator\" |grep \" -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME\" | awk '{print \$2}' | xargs -r -I {} pg_ctl kill QUIT {} >> $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

# sometimes, the process maybe have multiple "--coordinator" or "--datanode"
# notice, it has special means of '-' next to '"' in sting ' grep "- " '
coordinator_kill(){
    run_cmd_on $1 "
        ps aux |grep \"postgres --coordinator\" |grep \" -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME\" | awk '{print \$2}' | xargs -r -I {} pg_ctl kill QUIT {} > $FUNCNAME.oplog 2>&1;
        ps aux |grep \"postgres\" |grep \"process\" | awk '{print \$2}' | xargs -r -I {} pg_ctl kill QUIT {} > $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

coordinator_register(){
    local host=$1
    scp $CTL_RUN_DIR/$REGISTER_SQL $host:
    run_cmd_on $host "
        psql -p $PORT_COORD -f $REGISTER_SQL 1>/dev/null 2>&1;
        psql -p $PORT_COORD -c \"SELECT oid, * FROM pgxc_node;\" 2>&1;
    "
}

coordinator_reconfig(){
    run_cmd_on $1 "psql -p $PORT_COORD -c \"SELECT pg_reload_conf();\" 2>&1;"
}

coordinator_analyze(){
    local host=$1
    scp $CTL_RUN_DIR/$ANALYZE_SQL $host:
    run_cmd_on $host "psql -p $PORT_COORD -f $ANALYZE_SQL 1>/dev/null 2>&1;"
}

coordinator_xsql(){
    run_cmd_on $1 "psql -p $PORT_COORD -c \"$2\" 2>&1;"
}

coordinator_cleanlog(){
    local host=$1
    local days=$2
    if [ -z $days ]; then
        days=7
        log "Clean pg_log on $host using default 7 days kept"
    fi
    run_cmd_on $host "find $PGXL_LOG_HOME/$COORDINATOR_DIR_NAME/ -name \"*.log*\" -mtime +$days -delete;"
}

coordinator_copy(){
    local name=$1
    local host=$2
    local condition=$3
    search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD $host
    local src_host=$VARS
    [ -z $src_host ] && log "ERROR: Can not copy node to ${host}. Source is null." && return 1
    coordinator_init "$name" "$host" "$condition"
    run_cmd_on $host "
        pg_dumpall -p $PORT_COORD -h ${src_host} -s --include-nodes --dump-nodes --file=$PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/primary.sql 2>$FUNCNAME.oplog;
        pg_ctl start -Z restoremode -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME -w -t 5 -o -i >> $FUNCNAME.oplog 2>&1;
        psql -p $PORT_COORD -f $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/primary.sql 2>>$FUNCNAME.oplog;
        pg_ctl stop -Z restoremode -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME -w -t 5 >> $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog;
    "
}

            
op_datanode(){
    local op=$1
    local name=$2
    local role=$3
    local condition="$4"
    local host_m=${RT_DATANODE_MASTER_HOSTS[$name]}
    local host_s=${RT_DATANODE_STANDBY_HOSTS[$name]}
    local host_b=${RT_DATANODE_BACKUP_HOSTS[$name]}
    local host
    if [ "$role" == "master" ]; then
        host=$host_m
    elif [ "$role" == "standby" ]; then
        host=$host_s
    elif [ "$role" == "backup" ]; then
        host=$host_b
    fi
    [ -z $host ] && return 1
    log "Operate on datanode: $op $name $role $host $condition"
    get_node_status "$host" $PORT_DN
    local stat_of_host=$VARS
    if [[ $stat_of_host -eq $STATUS_UNREACHABLE ]]; then
        log "WARNING: can not $op on $name($host). Host is unreachable"
    elif [[ $stat_of_host -eq $STATUS_RUNNING ]]; then
        if [[ " init rebuild copy " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is running. Try to stop it before. $0 -m stop -z datanode -n $name -r $role"
            return 1
        elif [[ " start " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already running."
            return 1
        fi
    elif [[ $stat_of_host -eq $STATUS_STOPPED ]]; then
        if [[ " reload register reconfig xsql cleanpx " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host failed. Server is stopped. Try to start it before. $0 -m start -z datanode -n $name -r $role"
            return 1
        elif [[ " stop shutdown " =~ " $op " ]]; then
            log "WARNING: $op $name $role $host skipped. Server is already stopped."
            return 1
        fi
    fi
    if [ "$op" == "init" ]; then 
        if [ "$role" == "backup" ]; then
            datanode_init_backup "$name" "$host" "$condition"
        else
            datanode_init "$name" "$role" "$host" "$condition"
        fi
    elif [ "$op" == "kill" ] && [ "$role" != "backup" ]; then 
        datanode_kill "$host"
    elif [[ ( "$op" == "start" || "$op" == "stop" || "$op" == "restart" || "$op" == "status" || "$op" == "reload" ) && ( "$role" != "backup" ) ]]; then 
        datanode_start_stop_restart_status_reload "$op" "$host"
    elif [ "$op" == "shutdown" ] && [ "$role" != "backup" ]; then 
        datanode_shutdown "$host"
    elif [ "$op" == "rebuild" ] && [ "$role" == "standby" ]; then 
        datanode_init "$name" "$role" "$host" "$condition"
    elif [ "$op" == "promote" ] && [ "$role" == "standby" ]; then 
        datanode_promote "$host"
    elif [ "$op" == "cleanpx" ] && [ "$role" == "master" ]; then 
        datanode_cleanpx "$host" "$condition"
    elif [ "$op" == "cleanlog" ] && [ "$role" != "backup" ]; then 
        datanode_cleanlog "$host" "$condition"
    elif [ "$op" == "register" ] && [ "$role" == "master" ]; then 
        datanode_register "$host"
    elif [ "$op" == "cleanachlog" ] && [ "$role" == "backup" ]; then 
        datanode_cleanachlog "$host" "$condition"
    elif [ "$op" == "copy" ] && [ "$role" == "master" ]; then 
        datanode_copy "$name" "$host" "$condition"
    elif [ "$op" == "basebackup" ] && [ "$role" == "backup" ]; then 
        datanode_basebackup "$name" "$host" "$host_m" "$host_s" "$condition"
    elif [ "$op" == "syncconfig" ] && [ "$role" != "backup" ]; then 
        datanode_sync_config "$name" "$role"
    elif [ "$op" == "reconfig" ] && [ "$role" == "master" ]; then 
        datanode_reconfig "$host"
    elif [ "$op" == "xsql" ] && [ "$role" == "master" ]; then 
        datanode_xsql "$host" "$condition"
    elif [ "$op" == "exec" ]; then 
        run_cmd_on "$host" "$condition"
    else 
        log "WARNING: $op $name $role $host failed. It is not supported by $FUNCNAME"
    fi
}

datanode_init(){
    local name=$1
    local role=$2
    local host=$3
    local condition=$4
    [ "$role" == "backup" ] && return 1
    pretreatment_remote_directory $host $DATANODE_DIR_NAME $condition
    is_remote_dir_exists $host $PGXL_DATA_HOME/$DATANODE_DIR_NAME
    local resp=$VARS
    if [[ $resp -eq $TRUE ]]; then
        log "WARNING: Skip init. Datanode $name $role already exists"
    elif [[ $resp -eq $FALSE ]]; then
        if [ "$role" == "master" ]; then
            run_cmd_on $host "initdb --nodename $name -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME;"
        elif [ "$role" == "standby" ]; then
            # "pg_basebackup ... -c fast" cost too much disk io.
            run_cmd_on $host "pg_basebackup -p $PORT_DN -h ${RT_DATANODE_MASTER_HOSTS[$name]} -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME;"
        fi
        datanode_sync_config "$name" "$role"
    fi
}

datanode_sync_config(){
    local name=$1
    local role=$2
    local host_m=${RT_DATANODE_MASTER_HOSTS[$name]}
    local host_s=${RT_DATANODE_STANDBY_HOSTS[$name]}
    local host_b=${RT_DATANODE_BACKUP_HOSTS[$name]}
    local host
    local host_a
    if [ "$role" == "master" ]; then
        host=$host_m
        host_a=$host_s
    elif [ "$role" == "standby" ]; then
        host=$host_s
        host_a=$host_m
    fi
    local pg_alog_dir=$PGXL_ALOG_HOME/$name
    scp $CTL_ETC_DIR/$TEMPLATE_HBA $host:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf 1>/dev/null
    scp $CTL_ETC_DIR/$TEMPLATE_DN $host:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf 1>/dev/null
    scp $CTL_ETC_DIR/$TEMPLATE_PGPASS $host:$PGXL_DATA_HOME/$TEMPLATE_PGPASS 1>/dev/null
    local shell_cmds="
        chmod 0600 $PGXL_DATA_HOME/$TEMPLATE_PGPASS;
        rm -f ~/.pgpass;
        ln -s $PGXL_DATA_HOME/$TEMPLATE_PGPASS ~/.pgpass;
        mkdir -p $PGXL_LOG_HOME/$DATANODE_DIR_NAME;
        sed -i \"s/^archive_mode.*/archive_mode = on/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s/^pgxc_node_name.*/pgxc_node_name = \'$name\'/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s|^log_directory.*|log_directory = \'$PGXL_LOG_HOME/$DATANODE_DIR_NAME\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.done;
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/backup_label.old;
    "
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        shell_cmds=${shell_cmds}"
            mkdir -p $PGXL_XLOG_HOME/$DATANODE_DIR_NAME;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && mv $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && ln -s $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog;
        "
    fi
    
    if [ ! -z $host_b ]; then 
        shell_cmds=${shell_cmds}"
            sed -i \"s|^archive_command.*|archive_command = \'scp -P $PORT_SSH -o StrictHostKeyChecking=no %p $PGXL_USER@$host_b:$pg_alog_dir/%f\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        "
    elif [ ! -z $host_s ]; then 
        shell_cmds=${shell_cmds}"
            sed -i \"s|^archive_command.*|archive_command = \'scp -P $PORT_SSH -o StrictHostKeyChecking=no %p $PGXL_USER@$host_a:$pg_alog_dir/%f\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
            mkdir -m 700 -p $pg_alog_dir;
        "
    else
        shell_cmds=${shell_cmds}"
            sed -i \"s/^archive_mode.*/archive_mode = off/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
            sed -i \"s|^archive_command.*|archive_command = \'\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        "
    fi
    run_cmd_on $host "$shell_cmds"
    
    if [ "$role" == "standby" ]; then 
        scp $CTL_ETC_DIR/$TEMPLATE_DN_RECOVERY $host:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf 1>/dev/null
        shell_cmds="
            sed -i \"s|^primary_conninfo.*|primary_conninfo = \'host = $host_a port = $PORT_DN user = $PGXL_USER application_name = $name\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        "
        if [ ! -z $host_b ]; then 
            shell_cmds=${shell_cmds}"
                sed -i \"s|^restore_command.*|restore_command = \'scp -P $PORT_SSH -o StrictHostKeyChecking=no $PGXL_USER@$host_b:$pg_alog_dir/%f %p\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
                sed -i \"s|^archive_cleanup_command.*|archive_cleanup_command = \'/bin/date\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
            "
        else
            shell_cmds=${shell_cmds}"
                sed -i \"s|^restore_command.*|restore_command = \'cp $pg_alog_dir/%f %p\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
                sed -i \"s|^archive_cleanup_command.*|archive_cleanup_command = \'pg_archivecleanup $pg_alog_dir %r\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
            "
        fi
    fi
    run_cmd_on $host "$shell_cmds"
}

datanode_init_backup(){
    local name=$1
    local host=$2
    local condition=$3
    local pg_alog_dir=$PGXL_ALOG_HOME/$name
    if [ "$condition" == "clean" ] || [ "$condition" == "backup" ]; then
        run_cmd_on $host "mkdir -m 700 -p $pg_alog_dir;rm -rf $pg_alog_dir/* 2>/dev/null"
    fi
}

datanode_start_stop_restart_status_reload(){
    # TODO check recovery.conf if start or restart standy node
    local op=$1
    local host=$2
    run_cmd_on $host "
        pg_ctl $op -Z datanode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME -m fast -w -t 5 > $FUNCNAME.$op.oplog 2>&1;
        cat $FUNCNAME.$op.oplog
    "
}

datanode_shutdown(){
    run_cmd_on $1 "
        pg_ctl stop -Z datanode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME -m fast -w -t 8 > $FUNCNAME.oplog 2>&1 || ps aux |grep \"postgres --datanode\" |grep \" -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME\" | awk '{print \$2}' | xargs -r -I {} pg_ctl kill QUIT {} >> $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

datanode_kill(){
    run_cmd_on $1 "
        ps aux |grep \"postgres --datanode\" |grep \" -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME\" | awk '{print \$2}' | xargs -r -I {} pg_ctl kill QUIT {} > $FUNCNAME.oplog 2>&1;
        ps aux |grep \"postgres\" |grep \"process\" | awk '{print \$2}' | xargs -r -I {} pg_ctl kill QUIT {} >> $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

datanode_promote(){
    run_cmd_on $1 "
        pg_ctl promote -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME > $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog
    "
}

datanode_register(){
    local host=$1
    scp $CTL_RUN_DIR/$REGISTER_SQL $host:
    run_cmd_on $host "
        psql -p $PORT_DN -f $REGISTER_SQL 1>/dev/null 2>&1;
        psql -p $PORT_DN -c \"SELECT oid, * FROM pgxc_node;\" 2>&1;
    "
}

datanode_reconfig(){
    run_cmd_on $1 "psql -p $PORT_DN -c \"SELECT pg_reload_conf();\" 2>&1;"
}

datanode_xsql(){
    run_cmd_on $1 "psql -p $PORT_DN -c \"$2\" 2>&1;"
}

datanode_cleanpx(){
    local host=$1
    local second_ago=$2
    if [ -z $second_ago ]; then
        second_ago=60
        log "Clean pg_prepared_xacts on $host using default 60 second ago"
    fi
    local dt=`date -d "${second_ago} second ago" +"%Y-%m-%d %H:%M:%S"`
    local shell_cmds=""
    load_databases $host $PORT_DN
    for dbname in ${RT_DBS[*]}; do
        shell_cmds=${shell_cmds}"
            psql -p $PORT_DN -t -c \"SELECT 'ROLLBACK PREPARED '''||gid||''';' FROM pg_prepared_xacts WHERE database = '$dbname' AND prepared < '$dt';\" > ${dbname}.$ROLLBACK_SQL 2>/dev/null;
            psql -p $PORT_DN -d $dbname -f ${dbname}.$ROLLBACK_SQL;
        "
    done
    [ -z "$shell_cmds" ] || run_cmd_on $host "$shell_cmds"
}

datanode_cleanlog(){
    local host=$1
    local days=$2
    if [ -z $days ]; then
        days=7
        log "Clean pg_log on $host using default 7 days kept"
    fi
    run_cmd_on $host "find $PGXL_LOG_HOME/$DATANODE_DIR_NAME/ -name \"*.log*\" -mtime +$days -delete;"
}

datanode_cleanachlog(){
    local host=$1
    local kept_count=$2
    if [ -z $kept_count ]; then
        kept_count=7
        log "Clean pg_alog on $host using default 7 kept"
    fi
    run_cmd_on $host "ls -t ${PGXL_ALOG_HOME}/${name}/* |tail -n +${kept_count} |xargs rm -f;"
}

datanode_copy(){
    local name=$1
    local host=$2
    local condition=$3
    search_an_active_host "${RT_DATANODE_MASTER_HOSTS[*]}" $PORT_DN $host
    local src_host=$VARS
    [ -z $src_host ] && log "ERROR: Can not copy node to ${host}. Source is null." && return 1
    datanode_init "$name" master "$host" "$condition"
    run_cmd_on $host "
        pg_dumpall -p $PORT_DN -h ${src_host} -s --include-nodes --dump-nodes --file=$PGXL_DATA_HOME/$DATANODE_DIR_NAME/primary.sql 2>$FUNCNAME.oplog;
        pg_ctl start -Z restoremode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME -w -t 5 -o -i >> $FUNCNAME.oplog 2>&1;
        psql -p $PORT_DN -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/primary.sql 2>>$FUNCNAME.oplog;
        pg_ctl stop -Z restoremode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME -w -t 5 >> $FUNCNAME.oplog 2>&1;
        cat $FUNCNAME.oplog;
    "
}

datanode_basebackup(){
    local name=$1
    local host=$2
    local host_m=$3
    local host_s=$4
    local kept_count=$5
    if [ -z $kept_count ]; then
        kept_count=3
        log "Clean pg_basebackup on $host using default 3 kept"
    fi
    local dir_datetime="$(date '+%Y%m%d-%H%M%S')"
    # "pg_basebackup ... -c fast" cost too much disk io.
    run_cmd_on $host "
        mkdir -p $PGXL_BACKUP_HOME/$name/${dir_datetime};
        pg_basebackup -p $PORT_DN -h $host_m -D $PGXL_BACKUP_HOME/$name/${dir_datetime} || pg_basebackup -p $PORT_DN -h $host_s -D $PGXL_BACKUP_HOME/$name/${dir_datetime} || exit;
        ls -d $PGXL_BACKUP_HOME/$name/* -1 |sort -r -n |tail -n +${kept_count} |xargs rm -rf;
    "
}


failover_gtm(){
    get_node_status "$RT_GTM_STANDBY_HOST" $PORT_GTM
    local stat_of_standby=$VARS
    if [[ $stat_of_standby -eq $STATUS_UNREACHABLE ]]; then
        log "ERROR: Can not perform failover on gtm. Host standby is unreachable"
        exit 1
    fi
    
    log ">>>>>> Perform failover on gtm to $RT_GTM_STANDBY_HOST"
    
    do_runtime_operate -o shutdown -m coordinator
    
    local tmp_host=$RT_GTM_MASTER_HOST
    set_rt_conf "RT_GTM_MASTER_HOST=$RT_GTM_STANDBY_HOST"
    set_rt_conf "RT_GTM_STANDBY_HOST=$tmp_host"
    
    gtm_sync_config master
    
    for gpname in ${RT_GTM_PROXY_NAMES[*]}; do
    {
        local gphost=${RT_GTM_PROXY_HOSTS[$gpname]}
        get_node_status "$gphost" $PORT_GTM_PXY
        local stat_of_host=$VARS
        [[ $stat_of_host -eq $STATUS_RUNNING ]] && ssh $gphost "sed -i \"s|^gtm_host.*|gtm_host = \'$RT_GTM_MASTER_HOST\'|g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;"
    }&
    done
    wait
    
    do_runtime_operate -o shutdown -m datanode
    do_runtime_operate -o shutdown -m gtm_proxy
    do_runtime_operate -o shutdown -m gtm
    
    ssh $RT_GTM_MASTER_HOST "rm -f $PGXL_DATA_HOME/$GTM_DIR_NAME/register.node;"
    
    get_node_status "$RT_GTM_STANDBY_HOST" $PORT_GTM
    local status=$VARS
    if [[ $status -eq $STATUS_RUNNING ]] || [[ $status -eq $STATUS_STOPPED ]]; then
        scp $RT_GTM_STANDBY_HOST:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control $RT_GTM_MASTER_HOST:$PGXL_DATA_HOME/$GTM_DIR_NAME/
    else
        log "WARNING: Can not sync gtm.control to new gtm master."
    fi
    
    do_runtime_operate -o start -m gtm -r master
    do_runtime_operate -o start -m gtm_proxy
    do_runtime_operate -o start -m datanode
    do_runtime_operate -o start -m coordinator
    log "Failover on gtm is done"
}

failover_datanode(){
    local name=$1
    [ -z $name ] && log "ERROR: Can not perform failover on datanode. Name is null" && return 1
    
    local new_master=${RT_DATANODE_STANDBY_HOSTS[$name]}
    local new_standby=${RT_DATANODE_MASTER_HOSTS[$name]}
    local backup=${RT_DATANODE_BACKUP_HOSTS[$name]}
    
    get_node_status "$new_master" $PORT_DN
    local stat_of_new_master=$VARS
    [[ $stat_of_new_master -ne $STATUS_RUNNING ]] && log "ERROR: Can not perform failover on datanode ${name}. Standby is not running" && return 1
    
    log ">>>>>> Perform failover on datanode $name. New master node is $new_master"
    
    get_node_status "$new_standby" $PORT_DN
    local stat_of_new_standby=$VARS
    [[ $stat_of_new_standby -eq $STATUS_RUNNING ]] && do_runtime_operate -o shutdown -m datanode -n $name -r master
    
    do_runtime_operate -o promote -m datanode -n $name -r standby
    
    set_rt_conf "RT_DATANODE_MASTER_HOSTS[$name]=$new_master"
    set_rt_conf "RT_DATANODE_STANDBY_HOSTS[$name]=$new_standby"
    
    register_nodes
    
    log "Fail over datanode $name done"
}

add_gtm_proxy(){
    local name=$1
    local condition=$2
    [ -z $name ] && return 1
    add_node_info_to_rt_conf gtm_proxy "$name"
    do_runtime_operate -o init -m gtm_proxy -n $name -c "$condition"
    do_runtime_operate -o start -m gtm_proxy -n $name
}

add_coordinator(){
    local name=$1
    local condition=$2
    [ -z $name ] && return 1
    is_cluster_locked_for_ddl
    local resp=$VARS
    if [[ "$resp" -eq $TRUE ]]; then
        add_node_info_to_rt_conf coordinator $name
        do_runtime_operate -o copy -m coordinator -n $name -c "$condition"
        do_runtime_operate -o start -m coordinator -n $name
        register_nodes
    else
        log "WARNING: need lock the cluster before add node. Open a psql client and execute \"select pgxc_lock_for_backup();\""
    fi
}

add_datanode(){
    local name=$1
    local condition=$2
    [ -z $name ] && return 1
    is_cluster_locked_for_ddl
    local resp=$VARS
    if [[ "$resp" -eq $TRUE ]]; then
        add_node_info_to_rt_conf datanode "$name"
        do_runtime_operate -o copy -m datanode -n $name -c "$condition"
        do_runtime_operate -o start -m datanode -n $name -r master
        do_runtime_operate -o rebuild -m datanode -n $name -r standby
        do_runtime_operate -o start -m datanode -n $name -r standby
        register_nodes
    else
        log "WARNING: need lock the cluster before add node. Open a psql client and execute \"select pgxc_lock_for_backup();\""
    fi
}

remove_gtm_proxy(){
    local name=$1
    [ -z $name ] && return 1
    do_runtime_operate -o shutdown -m gtm_proxy -n $name
    set_rt_conf "RT_GTM_PROXY_NAMES=(${RT_GTM_PROXY_NAMES[@]/$name/})"
    set_rt_conf "RT_GTM_PROXY_HOSTS[$name]="
}

remove_coordinator(){
    local name=$1
    [ -z $name ] && return 1
    do_runtime_operate -o shutdown -m coordinator -n $name
    register_nodes delete $name
    set_rt_conf "RT_COORDINATOR_NAMES=(${RT_COORDINATOR_NAMES[@]/$name/})"
    set_rt_conf "RT_COORDINATOR_HOSTS[$name]="
}

remove_datanode(){
    local name=$1
    [ -z $name ] && return 1
    is_datanode_ready_to_be_removed $name
    local resp=$VARS
    if [[ "$resp" -eq $TRUE ]]; then
        do_runtime_operate -o shutdown -m datanode -n $name
        register_nodes delete $name
        set_rt_conf "RT_DATANODE_NAMES=(${RT_DATANODE_NAMES[@]/$name/})"
        set_rt_conf "RT_DATANODE_MASTER_HOSTS[$name]="
        set_rt_conf "RT_DATANODE_STANDBY_HOSTS[$name]="
        set_rt_conf "RT_DATANODE_BACKUP_HOSTS[$name]="
    else
        log "ERROR: Cannot remove datanode $name. Please re-balance data first."
    fi
}

register_nodes(){
    local op=$1
    local name=$2
    [ -z $op ] && prepare_register_sql || prepare_register_sql delete $name
    do_runtime_operate -o register -m datanode -r master -s
    do_runtime_operate -o register -m coordinator -s
}

# It costs a long time and locks table.
rebalance_datanode(){
    local name=$1
    local condition=$2
    [[ ( -z $name ) || ( -z $condition ) ]] && log "ERROR: Can not rebalance datanode. Name: ${name}. Condition: ${condition}" && return 1
    search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
    local host=$VARS
    prepare_rebalance_sql "$name" "$host" "$condition"
    scp $CTL_RUN_DIR/$REBALANCE_SQL $host:
    run_cmd_on $host "psql -p $PORT_COORD -f $REBALANCE_SQL 1>/dev/null 2>&1;"
}

vacuum_freeze_dbs(){
    search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
    local host=$VARS
    run_cmd_on $host "psql -p $PORT_COORD -c \"SELECT age(datfrozenxid), datfrozenxid, oid, datname FROM pg_database;\";"
    do_runtime_operate -o xsql -m coordinator -c "update pg_database set datallowconn='t' where datname='template0';"
    echo "SET statement_timeout = '1h';" > $CTL_RUN_DIR/$VACUUM_FREEZE_SQL
    for nodename in ${RT_DATANODE_NAMES[*]}; do
        echo "EXECUTE DIRECT ON ($nodename) 'update pg_database set datallowconn=''t'' where datname=''template0''';" >> $CTL_RUN_DIR/$VACUUM_FREEZE_SQL
    done
    load_databases $host $PORT_COORD
    for dbname in ${RT_DBS[*]}; do
        echo "\c $dbname" >> $CTL_RUN_DIR/$VACUUM_FREEZE_SQL
        echo "VACUUM FREEZE;" >> $CTL_RUN_DIR/$VACUUM_FREEZE_SQL
    done
    for nodename in ${RT_DATANODE_NAMES[*]}; do
        echo "EXECUTE DIRECT ON ($nodename) 'update pg_database set datallowconn=''f'' where datname=''template0''';" >> $CTL_RUN_DIR/$VACUUM_FREEZE_SQL
    done
    scp $CTL_RUN_DIR/$VACUUM_FREEZE_SQL $host:
    run_cmd_on $host "psql -p $PORT_COORD -f $VACUUM_FREEZE_SQL;"
    do_runtime_operate -o xsql -m coordinator -c "update pg_database set datallowconn='f' where datname='template0';"
    run_cmd_on $host "psql -p $PORT_COORD -c \"SELECT age(datfrozenxid), datfrozenxid, oid, datname FROM pg_database;\";"
}

analyze_dbs(){
    local mode=$1
    local name=$2
    search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
    local host=$VARS
    prepare_analyze_sql "$mode" "$host"
    if [ -z $mode ] || [ "$mode" == "all" ]; then 
        scp $CTL_RUN_DIR/$ANALYZE_SQL $host:
        run_cmd_on $host "psql -p $PORT_COORD -f $ANALYZE_SQL;"
    elif [ "$MODE" == "coordinator" ]; then 
        do_runtime_operate -o analyze -m coordinator -n "$name"
    fi
}

start_keeper(){
    # lock
    exec 7<> .pgxl_keeper.lock
    flock -n 7
    [ $? -eq 1 ] && return 1
    
    log ">>>>>> Start pgxl cluster keeper."
    local no_fo=${CONDITION}
    local dns_frozen
    local need_cleanpx=0
    while [ true ]
    do
        # gtm
        get_node_status "$RT_GTM_MASTER_HOST" $PORT_GTM
        local s_master=$VARS
        get_node_status "$RT_GTM_STANDBY_HOST" $PORT_GTM
        local s_standby=$VARS
        if [[ $s_master -eq $STATUS_STOPPED ]]; then
            need_cleanpx=1
            do_runtime_operate -o start -m gtm -r master
            get_node_status "$RT_GTM_MASTER_HOST" $PORT_GTM
            local s_master=$VARS
            if [[ $s_master -eq $STATUS_RUNNING ]]; then
                search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
                local activehost=$VARS
                local rs=`ssh $activehost "pg_isready --port $PORT_COORD -t 1" | grep accepting |wc -l`
                if [ "$rs" == "0" ]; then
                    do_runtime_operate -o shutdown -m coordinator
                    do_runtime_operate -o shutdown -m datanode
                fi
                do_runtime_operate -o restart -m gtm_proxy
                if [ "$rs" == "0" ]; then
                    do_runtime_operate -o start -m datanode
                    do_runtime_operate -o start -m coordinator
                fi
            elif [ -z $no_fo ] && [[ $s_standby -eq $STATUS_RUNNING ]]; then
                failover_gtm
                sleep 3s
                do_runtime_operate -o cleanpx -m datanode -r master -c 5
                continue
            fi
        elif [ -z $no_fo ] && [[ $s_master -eq $STATUS_UNREACHABLE ]] && [[ $s_standby -eq $STATUS_RUNNING ]]; then
            need_cleanpx=1
            failover_gtm
            continue
        fi
        if [[ $s_standby -eq $STATUS_STOPPED ]]; then
            do_runtime_operate -o rebuild -m gtm -r standby -c clean
            do_runtime_operate -o start -m gtm -r standby
        elif [[ $s_standby -eq $STATUS_RUNNING ]]; then
            # sync gtm.control to gtm standby
            scp $RT_GTM_MASTER_HOST:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control $RT_GTM_STANDBY_HOST:$PGXL_DATA_HOME/$GTM_DIR_NAME/
        fi
        
        # gtm proxy
        for name in ${RT_GTM_PROXY_NAMES[*]}; do
            local host=${RT_GTM_PROXY_HOSTS[$name]}
            get_node_status "$host" $PORT_GTM_PXY
            local stat_of_host=$VARS
            if [[ $stat_of_host -eq $STATUS_STOPPED ]]; then
                get_node_status "$RT_GTM_MASTER_HOST" $PORT_GTM
                local s_gtm_master=$VARS
                [[ $s_gtm_master -ne $STATUS_RUNNING ]] && continue 2
                do_runtime_operate -o rebuild -m gtm_proxy -n "$name" -c clean
                do_runtime_operate -o start -m gtm_proxy -n "$name"
            fi
        done
        
        # datanode
        for name in ${RT_DATANODE_NAMES[*]}; do
            local master=${RT_DATANODE_MASTER_HOSTS[$name]}
            local standby=${RT_DATANODE_STANDBY_HOSTS[$name]}
            get_node_status "$master" $PORT_DN
            local s_master=$VARS
            get_node_status "$standby" $PORT_DN
            local s_standby=$VARS
            if [[ $s_master -eq $STATUS_STOPPED ]]; then
                need_cleanpx=1
                search_an_active_host "${RT_DATANODE_MASTER_HOSTS[*]}" $PORT_DN
                local activehost=$VARS
                local rs=`ssh $activehost "pg_isready --port $PORT_DN -t 1" | grep accepting |wc -l`
                [ "$rs" == "0" ] && continue 2
                get_node_status "$master" $PORT_GTM_PXY
                local s_gtmpxy_host=$VARS
                [[ $s_gtmpxy_host -ne $STATUS_RUNNING ]] && continue 2
                do_runtime_operate -o start -m datanode -n "$name" -r master
                get_node_status "$master" $PORT_DN
                s_master=$VARS
                if [ -z $no_fo ] && [[ $s_master -ne $STATUS_RUNNING ]] && [[ $s_standby -eq $STATUS_RUNNING ]]; then
                    failover_datanode "$name"
                    continue 2
                fi
            elif [ -z $no_fo ] && [[ $s_master -eq $STATUS_UNREACHABLE ]] && [[ $s_standby -eq $STATUS_RUNNING ]]; then
                need_cleanpx=1
                failover_datanode "$name"
                continue 2
            fi
            if [[ $s_standby -eq $STATUS_STOPPED ]]; then
                is_remote_file_exists $standby $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf
                local f_exists=$VARS
                if [[ $f_exists -eq $FALSE ]]; then
                    if [ -z "$dns_frozen" ] || [[ ! " ${dns_frozen[@]} " =~ " $name " ]]; then
                        do_runtime_operate -o rebuild -m datanode -n "$name" -r standby -c backup
                    else
                        continue
                    fi
                fi
                do_runtime_operate -o start -m datanode -n "$name" -r standby
                get_node_status "$standby" $PORT_DN
                s_standby=$VARS
                if [[ $s_standby -eq $STATUS_STOPPED ]]; then
                    if [[ $f_exists -eq $FALSE ]]; then
                        dns_frozen+=("$name")
                    else
                        do_runtime_operate -o rebuild -m datanode -n "$name" -r standby -c clean
                        do_runtime_operate -o start -m datanode -n "$name" -r standby
                    fi
                fi
            fi
        done
        
        # coordinator
        for name in ${RT_COORDINATOR_NAMES[*]}; do
            local host=${RT_COORDINATOR_HOSTS[$name]}
            get_node_status "$host" $PORT_COORD
            local stat_of_host=$VARS
            if [[ $stat_of_host -eq $STATUS_STOPPED ]]; then
                search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
                local activehost=$VARS
                local rs=`ssh $activehost "pg_isready --port $PORT_COORD -t 1" | grep accepting |wc -l`
                [ "$rs" == "0" ] && continue 2
                get_node_status "$host" $PORT_GTM_PXY
                local s_gtmpxy_host=$VARS
                [[ $s_gtmpxy_host -ne $STATUS_RUNNING ]] && continue 2
                do_runtime_operate -o start -m coordinator -n "$name"
                get_node_status "$host" $PORT_COORD
                stat_of_host=$VARS
                if [[ $stat_of_host -ne $STATUS_RUNNING ]]; then
                    log "WARNING: Coordinator $name can not be started. Try to unregister and re-add it"
                fi
            elif [[ $stat_of_host -ne $STATUS_RUNNING ]]; then
                log "WARNING: Coordinator $name can not be connected"
            fi
        done
        
        if [[ $need_cleanpx -eq 1 ]]; then
            need_cleanpx=0
            sleep 3s
            do_runtime_operate -o cleanpx -m datanode -r master -c 5
        fi
        
        sleep $KEEPER_INTERVAL
    done
}

stop_keeper(){
    ps -ef |grep "$0" |grep start |grep keeper |awk '{print $2}' |xargs -r kill -9
    log ">>>>>> Stop pgxl cluster keeper."
}

###############################################################################################################
#     main methods
###############################################################################################################

pgxl_init(){
    mkdir -p $CTL_LOG_DIR $CTL_RUN_DIR $CTL_SQL_DIR;
    load_config "$CTL_HOME/$INIT_CONFIG"
    if [ "$MODE" == "all" ]; then 
        if [ -f "$CTL_RUN_DIR/$RUNTIME_CONFIG" ]; then 
            log "ERROR: $CTL_RUN_DIR/$RUNTIME_CONFIG exists. Please stop all nodes and remove it first"
            log "HINT: $0 -m shutdown -z all; $0 -m kill -z all; rm -f $CTL_RUN_DIR/$RUNTIME_CONFIG"
        else
            add_node_info_to_rt_conf
            
            do_runtime_operate -o init -m gtm -c $CONDITION
            do_runtime_operate -o init -m gtm_proxy -c $CONDITION
            do_runtime_operate -o init -m datanode -r backup -c $CONDITION
            do_runtime_operate -o init -m datanode -r master -c $CONDITION
            do_runtime_operate -o init -m coordinator -c $CONDITION
            
            do_runtime_operate -o start -m gtm
            do_runtime_operate -o start -m gtm_proxy
            # need to start master before init standby
            do_runtime_operate -o start -m datanode -r master
            do_runtime_operate -o start -m coordinator
            
            do_runtime_operate -o init -m datanode -r standby -c $CONDITION
            # need to start master and standby before register nodes if synchronous_standby_names is not empty
            do_runtime_operate -o start -m datanode -r standby
            
            register_nodes
            
            local scripts=`ls $CTL_SQL_DIR/`
            search_an_active_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
            local host=$VARS
            for f in $scripts; do
                scp $CTL_SQL_DIR/$f $host:
                run_cmd_on $host "psql -p $PORT_COORD -f $f;"
            done
        fi
    else 
        add_node_info_to_rt_conf "$MODE" "$NAME" "$ROLE"
        do_runtime_operate -o init -m "$MODE" -n "$NAME" -r "$ROLE" -c "$CONDITION"
    fi
}

pgxl_start(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate -o start -m gtm
        do_runtime_operate -o start -m gtm_proxy
        do_runtime_operate -o start -m datanode
        do_runtime_operate -o start -m coordinator
    elif [ "$MODE" == "keeper" ]; then 
        start_keeper > /dev/null 2>&1 &
    else 
        do_runtime_operate -o start -m "$MODE" -n "$NAME" -r "$ROLE"
    fi
}

pgxl_stop(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate -o stop -m coordinator
        do_runtime_operate -o stop -m datanode
        do_runtime_operate -o stop -m gtm_proxy
        do_runtime_operate -o stop -m gtm
    elif [ "$MODE" == "keeper" ]; then 
        stop_keeper
    else 
        do_runtime_operate -o stop -m "$MODE" -n "$NAME" -r "$ROLE"
    fi
}

pgxl_restart(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate -o shutdown -m coordinator
        do_runtime_operate -o shutdown -m datanode
        do_runtime_operate -o shutdown -m gtm_proxy
        do_runtime_operate -o shutdown -m gtm
        do_runtime_operate -o start -m gtm
        do_runtime_operate -o start -m gtm_proxy
        do_runtime_operate -o start -m datanode
        do_runtime_operate -o start -m coordinator
    else 
        do_runtime_operate -o restart -m "$MODE" -n "$NAME" -r "$ROLE"
    fi
}

pgxl_op_asc(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate -o $METHOD -m gtm -c "$CONDITION"
        do_runtime_operate -o $METHOD -m gtm_proxy -c "$CONDITION"
        do_runtime_operate -o $METHOD -m datanode -c "$CONDITION"
        do_runtime_operate -o $METHOD -m coordinator -c "$CONDITION"
    else 
        do_runtime_operate -o $METHOD -m "$MODE" -n "$NAME" -r "$ROLE" -c "$CONDITION"
    fi
}

pgxl_op_desc(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate -o $METHOD -m coordinator -c "$CONDITION"
        do_runtime_operate -o $METHOD -m datanode -c "$CONDITION"
        do_runtime_operate -o $METHOD -m gtm_proxy -c "$CONDITION"
        do_runtime_operate -o $METHOD -m gtm -c "$CONDITION"
    else 
        do_runtime_operate -o $METHOD -m "$MODE" -n "$NAME" -r "$ROLE" -c "$CONDITION"
    fi
}

pgxl_add(){
    load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "coordinator" ]; then 
        add_coordinator "$NAME" "$CONDITION"
    elif [ "$MODE" == "datanode" ]; then 
        add_datanode "$NAME" "$CONDITION"
    elif [ "$MODE" == "gtm_proxy" ]; then 
        add_gtm_proxy "$NAME" "$CONDITION"
    fi
}

pgxl_remove(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "coordinator" ]; then 
        remove_coordinator "$NAME"
    elif [ "$MODE" == "datanode" ]; then 
        remove_datanode "$NAME"
    elif [ "$MODE" == "gtm_proxy" ]; then 
        remove_gtm_proxy "$NAME"
    fi
}

pgxl_failover(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "gtm" ]; then 
        failover_gtm
    elif [ "$MODE" == "datanode" ]; then 
        failover_datanode "$NAME"
    fi
}

pgxl_register(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    register_nodes
}

pgxl_rebalance(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    rebalance_datanode "$NAME" "$CONDITION"
}

pgxl_freeze(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    vacuum_freeze_dbs
}

pgxl_analyze(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    analyze_dbs "$MODE" "$NAME"
}

pgxl_topology(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    echo -e "[GTM] gtm:master:$RT_GTM_MASTER_HOST:$(get_node_status_info $RT_GTM_MASTER_HOST $PORT_GTM)"
    echo -e "[GTM] gtm:standby:$RT_GTM_STANDBY_HOST:$(get_node_status_info $RT_GTM_STANDBY_HOST $PORT_GTM)"
    for name in ${RT_GTM_PROXY_NAMES[*]}; do
    {
        local host=${RT_GTM_PROXY_HOSTS[$name]}
        [ -z $host ] || echo -e "[GTM_PROXY] ${name}:host:${host}:$(get_node_status_info $host $PORT_GTM_PXY)"
    }&
    done
    wait
    for name in ${RT_DATANODE_NAMES[*]}; do
    {
        local host_m=${RT_DATANODE_MASTER_HOSTS[$name]}
        local host_s=${RT_DATANODE_STANDBY_HOSTS[$name]}
        [ -z $host_m ] || echo -e "[DATANODE] ${name}:master:${host_m}:$(get_node_status_info $host_m $PORT_DN)"
        [ -z $host_s ] || echo -e "[DATANODE] ${name}:standby:${host_s}:$(get_node_status_info $host_s $PORT_DN)"
    }&
    done
    wait
    for name in ${RT_COORDINATOR_NAMES[*]}; do
    {
        local host=${RT_COORDINATOR_HOSTS[$name]}
        [ -z $host ] || echo -e "[COORDINATOR] ${name}:host:${host}:$(get_node_status_info $host $PORT_COORD)"
    }&
    done
    wait
}

pgxl_usage(){
    echo -e "
        Usage:
            ./pgxl_ctl.sh [OPTION]...
        Options:
            -m        method (eg. init|add|remove|start|stop|kill|restart|reload|status|rebuild|failover|register|rebalance|topology)
            -z        mode (eg. all|gtm|gtm_proxy|datanode|coordinator|keeper)
            -n        name of node
            -r        role (eg. master|standby)
            -c        condition (eg. clean|backup)
        Examples:
            <1> Initialize pgxl cluster configured in pgxl_init.conf. Include add all nodes information to ${RUNTIME_CONFIG}, initialize all nodes, register nodes and execute sql under ${CTL_SQL_DIR}.
                ./pgxl_ctl.sh -m init -z all
                ./pgxl_ctl.sh -m init -z all -c clean
            <2> Initialize some node only. Include add these node information to ${RUNTIME_CONFIG}, initialize these nodes.
                ./pgxl_ctl.sh -m init -z gtm -r master
                ./pgxl_ctl.sh -m init -z gtm_proxy
                ./pgxl_ctl.sh -m init -z coordinator -c backup
                ./pgxl_ctl.sh -m init -z datanode -n datanode1 -r standby
            <3> Add new node configured in pgxl_init.conf to a running cluster. Before do this, choose an active coordinator and issue pgxc_lock_for_backup() to block DDL issued to all the active coordinators. After, issue quit to release DDL lock.
                ./pgxl_ctl.sh -m add -z coordinator -n coord5 -c clean
                ./pgxl_ctl.sh -m add -z datanode -n datanode3 -c backup
                ./pgxl_ctl.sh -m add -z gtm_proxy -n gtm_pxy3 -c clean
            <4> Remove node from a running cluster.
                ./pgxl_ctl.sh -m remove -z coordinator -n coord5
                ./pgxl_ctl.sh -m remove -z datanode -n datanode3
                ./pgxl_ctl.sh -m remove -z gtm_proxy -n gtm_pxy3
            <5> Re-balance data. It will lock table.
                ./pgxl_ctl.sh -m rebalance -n datanode5 -c add
                ./pgxl_ctl.sh -m rebalance -n datanode5 -c delete
            <6> Failover master node if it broke down.
                ./pgxl_ctl.sh -m failover -z gtm
                ./pgxl_ctl.sh -m failover -z datanode -n datanode3
            <7> Rebuild standby node or gtm proxy if it broke down. 
                ./pgxl_ctl.sh -m rebuild -z gtm -r standby -c clean
                ./pgxl_ctl.sh -m rebuild -z gtm_proxy -c clean
                ./pgxl_ctl.sh -m rebuild -z datanode -n datanode2 -r standby -c backup
            <8> Some operate on nodes. shutdown is if stop timeout then kill. 
                ./pgxl_ctl.sh -m start -z all
                ./pgxl_ctl.sh -m stop -z gtm -r master
                ./pgxl_ctl.sh -m kill -z gtm_proxy -n gtm_pxy1
                ./pgxl_ctl.sh -m shutdown -z datanode -r standby
                ./pgxl_ctl.sh -m restart -z datanode -n datanode1 -r master
                ./pgxl_ctl.sh -m status -z coordinator
            <9> Sync *.conf file to nodes. 
                ./pgxl_ctl.sh -m syncconfig -z all
                ./pgxl_ctl.sh -m syncconfig -z datanode
                ./pgxl_ctl.sh -m syncconfig -z coordinator
           <10> Reconfig or Reload postgresql.conf on datanode or coordinator
                ./pgxl_ctl.sh -m reconfig -z datanode
                ./pgxl_ctl.sh -m reload -z coordinator
           <11> Register all nodes according to pgxl_runtime.conf.
                ./pgxl_ctl.sh -m register -z all
           <12> Show cluster status
                ./pgxl_ctl.sh -m topology
           <13> Execute sql on datanodes or coordinators
                ./pgxl_ctl.sh -m xsql -z coordinator -c \"CREATE EXTENSION pg_stat_statements;CREATE EXTENSION plpythonu;\"
           <14> Execute bash shell on nodes
                ./pgxl_ctl.sh -m exec -z gtm -r master -c \"uptime\"
           <15> Clean log N days ago.
                ./pgxl_ctl.sh -m cleanlog -z coordinator -c 30
                ./pgxl_ctl.sh -m cleanlog -z datanode -c 7
           <16> Clean data exist in table pg_prepared_xacts on datanode beyond x seconds. Default 60 seconds.
                ./pgxl_ctl.sh -m cleanpx -z datanode -r master -c 10
           <17> Clean achieve xlog on backup node.
                ./pgxl_ctl.sh -m cleanachlog -z datanode -r backup -c 1024
           <18> Make a basebackup of datanode.
                ./pgxl_ctl.sh -m basebackup -z datanode -r backup -c 7
           <19> Execute vacuum freeze on all databases.
                ./pgxl_ctl.sh -m freeze
           <20> Execute analyze on all or coordinator only.
                ./pgxl_ctl.sh -m analyze -z all
                ./pgxl_ctl.sh -m analyze -z coordinator -n coord5
           <21> HA keeper
                ./pgxl_ctl.sh -m start -z keeper
                ./pgxl_ctl.sh -m start -z keeper -c no_failover
                ./pgxl_ctl.sh -m stop -z keeper
           <22> Crontab
                */1 * * * * flock -n /tmp/pgxl_keeper.lock -c '/bin/bash /home/postgres/pgxl_ctl/pgxl_ctl.sh -m start -z keeper'
                0 0 * * * /bin/bash /usr/local/pgxl_ctl/pgxl_ctl.sh -m cleanlog -z all -c 15
                */5 * * * * /bin/bash /usr/local/pgxl_ctl/pgxl_ctl.sh -m cleanachlog -z datanode -r backup -c 3072
                15 */4 * * * /bin/bash /usr/local/pgxl_ctl/pgxl_ctl.sh -m basebackup -z datanode -r backup -c 10
                0 19 * * * /bin/bash /usr/local/pgxl_ctl/pgxl_ctl.sh -m freeze
    "
}

if [[ " init add remove start stop restart rebuild reconnect shutdown kill failover " =~ " $METHOD " ]]; then
    if [ "`ps -ef |grep \"$0\" |grep start |grep keeper |wc -l`" != "0" ]; then
        if [ "$MODE" != "keeper" ]; then
            log "WARNING: HA keeper is running. Please stop it before."
            exit 1
        fi
    fi
fi

case "$METHOD" in
    init)                                          pgxl_init
                                                   ;;
    add)                                           pgxl_add
                                                   ;;
    remove)                                        pgxl_remove
                                                   ;;
    start)                                         pgxl_start
                                                   ;;
    stop)                                          pgxl_stop
                                                   ;;
    restart)                                       pgxl_restart
                                                   ;;
    status | xsql | exec | reconnect)              pgxl_op_asc
                                                   ;;
    syncconfig | reload | reconfig | basebackup)   pgxl_op_asc
                                                   ;;
    cleanachlog | cleanlog | cleanpx | rebuild)    pgxl_op_asc
                                                   ;;
    shutdown | kill)                               pgxl_op_desc
                                                   ;;
    failover)                                      pgxl_failover
                                                   ;;
    register | reg)                                pgxl_register
                                                   ;;
    rebalance | rbl)                               pgxl_rebalance
                                                   ;;
    topology | tplg)                               pgxl_topology
                                                   ;;
    freeze)                                        pgxl_freeze
                                                   ;;
    analyze | analyse)                             pgxl_analyze
                                                   ;;
    *)                                             pgxl_usage
                                                   ;;
esac
