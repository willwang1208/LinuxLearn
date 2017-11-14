#!/bin/bash
# convention over configuration

# convention
PGXL_USER=postgres
PGXL_DATA_HOME=/data/pgxl_data
PGXL_LOG_HOME=/log/pgxl_log
PGXL_XLOG_HOME=/log/pgxl_xlog
PGXL_ALOG_HOME=/data/pgxl_alog
PGXL_BACKUP_HOME=/data/pgxl_basebackup
PGXL_TMP_DIR=/tmp
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
LOG_FILE=pgxl_ctl.log
TRUE=1
FALSE=2
STATUS_RUNNING=1
STATUS_STOPPED=2
STATUS_HOSTDOWN=3
STATUS_ERROR=100
KEEPER_INTERVAL=5s

# template
TEMPLATE_GTM_MASTER=gtm.master.conf
TEMPLATE_GTM_SLAVE=gtm.slave.conf
TEMPLATE_GTM_PROXY=gtm_proxy.conf
TEMPLATE_HBA_COORD=pg_hba.coord.conf
TEMPLATE_HBA_DN=pg_hba.dn.conf
TEMPLATE_COORD=postgresql.coord.conf
TEMPLATE_DN_MASTER=postgresql.dn.master.conf
TEMPLATE_DN_SLAVE=postgresql.dn.slave.conf
TEMPLATE_DN_HIDDEN=postgresql.dn.hidden.conf
TEMPLATE_DN_RECOVERY=recovery.conf
TEMPLATE_PGPASS=pgpass.conf

# option
METHOD=
MODE=
NAME=
ROLE=
CONDITION=

# host setting in init config file
GTM_MASTER_HOST=$GTM_MASTER_HOST
GTM_SLAVE_HOST=$GTM_SLAVE_HOST
GTM_PROXY_NAMES=(${GTM_PROXY_NAMES[*]})
GTM_PROXY_HOSTS=(${GTM_PROXY_HOSTS[*]})
DATANODE_NAMES=(${DATANODE_NAMES[*]})
DATANODE_MASTER_HOSTS=(${DATANODE_MASTER_HOSTS[*]})
DATANODE_SLAVE_HOSTS=(${DATANODE_SLAVE_HOSTS[*]})
DATANODE_HIDDEN_HOSTS=(${DATANODE_HIDDEN_HOSTS[*]})
COORDINATOR_NAMES=(${COORDINATOR_NAMES[*]})
COORDINATOR_HOSTS=(${COORDINATOR_HOSTS[*]})

DATABASES=(${DATABASES[*]})

# host setting in runtime config file
RT_GTM_MASTER_HOST=
RT_GTM_SLAVE_HOST=
RT_GTM_PROXY_NAMES=
RT_GTM_PROXY_HOSTS=
RT_DATANODE_NAMES=
RT_DATANODE_MASTER_HOSTS=
RT_DATANODE_SLAVE_HOSTS=
RT_DATANODE_HIDDEN_HOSTS=
RT_COORDINATOR_NAMES=
RT_COORDINATOR_HOSTS=
RT_TMS=

# variable
VARS=

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

log(){
    local lines=$1
    echo -e "$lines" | awk -v date="$(date '+%Y-%m-%d %H:%M:%S')" '{if(NF){print date" "$0}}' >> $CTL_LOG_DIR/$LOG_FILE
    echo -e "$lines" | awk '{if(NF){print $0}}'
}

find_host_by_name(){
    VARS=""
    local somename=$1
    local names=($2)
    local hosts=($3)
    local length=${#names[@]}
    local i
    if [ "$somename" != "" ]; then 
        for (( i=0; i<$length; i++ ))
        do
            local name=${names[$i]}
            if [ "$somename" == "$name" ]; then 
                VARS=${hosts[$i]}
            fi
        done
    fi
}

find_index_by_name(){
    VARS=""
    local somename=$1
    local names=($2)
    local length=${#names[@]}
    local i
    if [ "$somename" != "" ]; then 
        for (( i=0; i<$length; i++ ))
        do
            local name=${names[$i]}
            if [ "$somename" == "$name" ]; then 
                VARS=$i
            fi
        done
    fi
}

find_alive_host(){
    local hosts=($1)
    local port=$2
    local except=$3
    local length=${#hosts[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local host=${hosts[$i]}
        if [ "$host" != "$except" ] && [ "$host" != "" ]; then 
            get_process_status "$host" $port
            local status=$VARS
            VARS=""
            if [[ $status -eq $STATUS_RUNNING ]]; then
                VARS=$host
                break
            fi
        fi
    done
}

is_remote_dir_exists(){
    local host=$1
    local dir=$2
    local resp=`ssh -p $PORT_SSH $PGXL_USER@$host "ls -d $dir 2>/dev/null"`
    if [ "$resp" == "$dir" ]; then
        VARS=$TRUE
    else
        VARS=$FALSE
    fi
}

is_remote_file_exists(){
    local host=$1
    local file=$2
    local resp=`ssh -p $PORT_SSH $PGXL_USER@$host "ls $file 2>/dev/null"`
    if [ "$resp" == "$file" ]; then
        VARS=$TRUE
    else
        VARS=$FALSE
    fi
}

get_process_status(){
    local host=$1
    local port=$2
    local special=$3
    if [ "$host" != "" ] && [ "$port" != "" ]; then
        #local resp=`echo -e "\n" | timeout 1 telnet $host $PORT_SSH 2>/dev/null | grep Connected`
        local resp
        ping -c 1 -w 1 $host &>/dev/null && resp="Connected" || resp=""
        if [ "$resp" == "" ]; then
            VARS=$STATUS_HOSTDOWN
        else
            if [ "$special" == "gtm" ]; then
                #resp=`ssh -p $PORT_SSH $PGXL_USER@$host "pidof gtm"`
                resp=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME\" | grep -v \"grep\""`
            elif [ "$special" == "gtm_proxy" ]; then
                #resp=`ssh -p $PORT_SSH $PGXL_USER@$host "pidof gtm_proxy"`
                resp=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME\" | grep -v \"grep\""`
            else
                #resp=`echo -e "\n" | timeout 1 telnet $host $port 2>/dev/null | grep Connected`
                if [ "$port" == "$PORT_COORD" ]; then
                    resp=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"postgres --coordinator -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME\" | grep -v \"grep\""`
                elif [ "$port" == "$PORT_DN" ]; then
                    resp=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"postgres --datanode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME\" | grep -v \"grep\""`
                fi
            fi
            if [ "$resp" == "" ]; then
                VARS=$STATUS_STOPPED
            else
                VARS=$STATUS_RUNNING
            fi
        fi
    else
        VARS=$STATUS_ERROR
    fi
}

get_process_status2(){
    local mode=$1
    local name=$2
    local role=$3
    local host
    local port
    local special
    if [ "$mode" == "gtm" ]; then 
        port=$PORT_GTM
        special=gtm
        if [ "$role" == "master" ]; then 
            host=$RT_GTM_MASTER_HOST
        elif [ "$role" == "slave" ]; then 
            host=$RT_GTM_SLAVE_HOST
        fi
    elif [ "$mode" == "gtm_proxy" ]; then 
        port=$PORT_GTM_PXY
        special=gtm_proxy
        find_host_by_name $name "${RT_GTM_PROXY_NAMES[*]}" "${RT_GTM_PROXY_HOSTS[*]}"
        host=$VARS
    elif [ "$mode" == "coordinator" ]; then 
        port=$PORT_COORD
        find_host_by_name $name "${RT_COORDINATOR_NAMES[*]}" "${RT_COORDINATOR_HOSTS[*]}"
        host=$VARS
    elif [ "$mode" == "datanode" ]; then 
        port=$PORT_DN
        if [ "$role" == "master" ]; then 
            find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
            host=$VARS
        elif [ "$role" == "slave" ]; then 
            find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_SLAVE_HOSTS[*]}"
            host=$VARS
        #elif [ "$role" == "hidden" ]; then 
        #    find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_HIDDEN_HOSTS[*]}"
        #    host=$VARS
        fi
    fi
    get_process_status "$host" $port $special
}

get_process_status_info(){
    local host=$1
    local port=$2
    local special=$3
    get_process_status "$host" $port $special
    local status=$VARS
    if [[ $status -eq $STATUS_RUNNING ]]; then
        echo -e "\e[1;32mRunning\e[0m"
    elif [[ $status -eq $STATUS_STOPPED ]]; then
        echo -e "\e[1;33mStopped\e[0m"
    elif [[ $status -eq $STATUS_HOSTDOWN ]]; then
        echo -e "\e[1;31mHostdown\e[0m"
    else
        echo -e "\e[1;41mERROR\e[0m"
    fi
}

load_config(){
    local file=$1
    while read line
    do
        if [ "$line" != "" ]; then
            log "Load config $file : $line"
            eval "$line"
        fi
    done < $file
}

edit_runtime_config(){
    local param=$1
    local value=$2
    local index=$3
    local pattern
    if [ "$index" == "" ]; then
        pattern="$param"
    else
        pattern="${param}\[${index}\]"
        param="${param}[${index}]"
    fi
    local line=`grep "^${pattern}" $CTL_RUN_DIR/$RUNTIME_CONFIG`
    if [ "$line" == "" ] && [ "$value" != "" ]; then
        echo "$param=$value" >> $CTL_RUN_DIR/$RUNTIME_CONFIG;
    #elif [ "$value" != "" ]; then
    else
        sed -i "s|^$pattern.*|$pattern=$value|g" $CTL_RUN_DIR/$RUNTIME_CONFIG;
    #else
    #    sed -i "/^$pattern.*/d" $CTL_RUN_DIR/$RUNTIME_CONFIG;
    fi
    eval "$param=$value"
    log "Edit runtime config : $param=$value"
    # timestamp
    local tms=$(date '+%s')
    eval "RT_TMS=$tms"
    local line=`grep "^RT_TMS" $CTL_RUN_DIR/$RUNTIME_CONFIG`
    if [ "$line" == "" ]; then
        echo "RT_TMS=$tms" >> $CTL_RUN_DIR/$RUNTIME_CONFIG;
    else
        sed -i "s|^RT_TMS.*|RT_TMS=$tms|g" $CTL_RUN_DIR/$RUNTIME_CONFIG;
    fi
}

# pretreatment_remote_directory
prepare_before_init(){
    local host=$1
    local dirname=$2
    local condition=$3
    if [ "$condition" == "clean" ] && [ "$dirname" != "" ]; then 
        log "Clean $host $PGXL_DATA_HOME/$dirname"
        ssh -p $PORT_SSH $PGXL_USER@$host "cd $PGXL_DATA_HOME/;rm -rf $dirname 2>/dev/null"
        if [ "$PGXL_XLOG_HOME" != "" ]; then
            ssh -p $PORT_SSH $PGXL_USER@$host "cd $PGXL_XLOG_HOME/;rm -rf $dirname 2>/dev/null"
        fi
    elif [ "$condition" == "backup" ] && [ "$dirname" != "" ]; then
        local daytime=`date +%Y%m%d-%H%M%S`
        log "Backup $host $PGXL_DATA_HOME/$dirname $PGXL_DATA_HOME/${dirname}_$daytime"
        ssh -p $PORT_SSH $PGXL_USER@$host "cd $PGXL_DATA_HOME/;mv $dirname ${dirname}_$daytime 2>/dev/null"
        if [ "$PGXL_XLOG_HOME" != "" ]; then
            ssh -p $PORT_SSH $PGXL_USER@$host "cd $PGXL_XLOG_HOME/;mv $dirname ${dirname}_$daytime 2>/dev/null"
        fi
    fi
}

#select * from pgxc_class c where exists (select * from pgxc_node n where n.node_name = 'datanode2' and (n.oid = c.nodeoids[0] or n.oid = c.nodeoids[1] or n.oid = c.nodeoids[2]));
#psql -p 21001 -c "select * from pgxc_class c where exists (select * from pgxc_node n where n.node_name = 'datanode2' and (n.oid = c.nodeoids[0] or n.oid = c.nodeoids[1] or n.oid = c.nodeoids[2]));" -t > tjjj
is_datanode_ready_to_be_removed(){
    local name=$1
    local host=$2
    local sql="select * from pgxc_class c where exists (select * from pgxc_node n where n.node_name = '$name' and ("
    local length=${#RT_DATANODE_NAMES[@]}
    local length_end=`expr $length - 1`
    local i
    for (( i=0; i<$length; i++ ))
    do
        sql=$sql"n.oid = c.nodeoids[$i] "
        if [[ $i -lt $length_end ]]; then
            sql=$sql"or "
        fi
    done
    sql=$sql"));"
    local resp=`ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_COORD -c \"$sql\" -t;"`
    local resp_length=${#resp}
    if [[ $resp_length -lt 4 ]]; then 
        VARS=$TRUE
    else
        VARS=$FALSE
    fi
}

# init
do_init_operate(){
    local l_mode=$1
    local l_name=$2
    local l_role=$3
    local l_con=$4
    if [ "$l_mode" == "gtm" ]; then 
        if [ "$l_role" == "master" ] || [ "$l_role" == "" ]; then 
            init_gtm_master "$GTM_MASTER_HOST" "$l_con"
        fi
        if [ "$l_role" == "slave" ] || [ "$l_role" == "" ]; then 
            init_gtm_slave "$GTM_MASTER_HOST" "$GTM_SLAVE_HOST" "$l_con"
        fi
    elif [ "$l_mode" == "gtm_proxy" ]; then 
        if [ "$l_name" == "" ]; then 
            local length=${#GTM_PROXY_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
                init_gtm_proxy "${GTM_PROXY_NAMES[$i]}" "${GTM_PROXY_HOSTS[$i]}" $i "$GTM_MASTER_HOST" "$l_con"
            done
        else
            find_host_by_name $l_name "${GTM_PROXY_NAMES[*]}" "${GTM_PROXY_HOSTS[*]}"
            local host=$VARS
            if [ "$host" != "" ]; then 
                find_index_by_name $l_name "${GTM_PROXY_NAMES[*]}"
                local index=$VARS
                init_gtm_proxy $l_name "$host" $index "$GTM_MASTER_HOST" "$l_con"
            fi
        fi
    elif [ "$l_mode" == "coordinator" ]; then 
        if [ "$l_name" == "" ]; then 
            local length=${#COORDINATOR_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
                init_coordinator_master "${COORDINATOR_NAMES[$i]}" "${COORDINATOR_HOSTS[$i]}" $i "$l_con"
            done
        else
            find_host_by_name $l_name "${COORDINATOR_NAMES[*]}" "${COORDINATOR_HOSTS[*]}"
            local host=$VARS
            if [ "$host" != "" ]; then 
                find_index_by_name $l_name "${COORDINATOR_NAMES[*]}"
                local index=$VARS
                init_coordinator_master $l_name $host $index "$l_con"
            fi
        fi
    elif [ "$l_mode" == "datanode" ]; then 
        if [ "$l_name" == "" ]; then 
            local length=${#DATANODE_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
                local name=${DATANODE_NAMES[$i]}
                local master=${DATANODE_MASTER_HOSTS[$i]}
                local slave=${DATANODE_SLAVE_HOSTS[$i]}
                local hidden=${DATANODE_HIDDEN_HOSTS[$i]}
                if [ "$l_role" == "hidden" ] || [ "$l_role" == "" ]; then 
                    init_datanode_hidden "$name" "$master" "$hidden" "$slave" $i "$l_con"
                fi
                if [ "$l_role" == "master" ] || [ "$l_role" == "" ]; then 
                    init_datanode_master "$name" "$master" "$slave" "$hidden" $i "$l_con"
                fi
                if [ "$l_role" == "slave" ] || [ "$l_role" == "" ]; then 
                    init_datanode_slave "$name" "$master" "$slave" "$hidden" $i "$l_con"
                fi
            done
        else
            find_host_by_name $l_name "${DATANODE_NAMES[*]}" "${DATANODE_MASTER_HOSTS[*]}"
            local master=$VARS
            find_host_by_name $l_name "${DATANODE_NAMES[*]}" "${DATANODE_SLAVE_HOSTS[*]}"
            local slave=$VARS
            find_host_by_name $l_name "${DATANODE_NAMES[*]}" "${DATANODE_HIDDEN_HOSTS[*]}"
            local hidden=$VARS
            find_index_by_name $l_name "${DATANODE_NAMES[*]}"
            local index=$VARS
            if [ "$l_role" == "hidden" ] || [ "$l_role" == "" ]; then 
                init_datanode_hidden $l_name "$master" "$hidden" "$slave" $index "$l_con"
            fi
            if [ "$l_role" == "master" ] || [ "$l_role" == "" ]; then 
                init_datanode_master $l_name "$master" "$slave" "$hidden" $index "$l_con"
            fi
            if [ "$l_role" == "slave" ] || [ "$l_role" == "" ]; then 
                init_datanode_slave $l_name "$master" "$slave" "$hidden" $index "$l_con"
            fi
        fi
    fi
}

init_gtm_master(){
    local host=$1
    local condition=$2
    log ">>>>>> Init gtm master. host: $host . $condition"
    if [ "$host" != "" ]; then
        if [ "$condition" == "config" ]; then
            init_gtm_master_config $host
        else
            prepare_before_init $host $GTM_DIR_NAME $condition
            is_remote_dir_exists $host $PGXL_DATA_HOME/$GTM_DIR_NAME
            local resp=$VARS
            if [[ $resp -eq $TRUE ]]; then
                log "WARNING: Init skip. Gtm master exists"
            elif [[ $resp -eq $FALSE ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "initgtm -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME"`
                log "$rs"
                init_gtm_master_config $host
                edit_runtime_config "RT_GTM_MASTER_HOST" "$host"
                log "Init gtm master done"
            fi
        fi
    fi
}

init_gtm_master_config(){
    local host=$1
    scp -P $PORT_SSH -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_GTM_MASTER $PGXL_USER@$host:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$host "
        mkdir -p $PGXL_LOG_HOME/$GTM_DIR_NAME;
        sed -i \"s|^log_file.*|log_file = \'$PGXL_LOG_HOME/$GTM_DIR_NAME/gtm.log\'|g\" $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf;
        touch $PGXL_DATA_HOME/$GTM_DIR_NAME/pgxl.master
    "
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "cat $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf"`
    log "Gtm master config: \n$rs"
}

init_gtm_slave(){
    local master=$1
    local slave=$2
    local condition=$3
    log ">>>>>> Init gtm slave. host: $slave . $condition"
    if [ "$slave" != "" ]; then
        if [ "$condition" == "config" ]; then
            init_gtm_slave_config $master $slave
        else
            prepare_before_init $slave $GTM_DIR_NAME $condition
            is_remote_dir_exists $slave $PGXL_DATA_HOME/$GTM_DIR_NAME
            local resp=$VARS
            if [[ $resp -eq $TRUE ]]; then
                log "WARNING: Init skip. Gtm slave exists"
            elif [[ $resp -eq $FALSE ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$slave "initgtm -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME"`
                log "$rs"
                init_gtm_slave_config $master $slave
                edit_runtime_config "RT_GTM_SLAVE_HOST" "$slave"
                log "Init gtm slave done"
            fi
        fi
    fi
}

init_gtm_slave_config(){
    local master=$1
    local slave=$2
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_GTM_SLAVE $PGXL_USER@$slave:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$slave "
        mkdir -p $PGXL_LOG_HOME/$GTM_DIR_NAME;
        sed -i \"s/^active_host.*/active_host = \'$master\'/g\" $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf;
        sed -i \"s|^log_file.*|log_file = \'$PGXL_LOG_HOME/$GTM_DIR_NAME/gtm.log\'|g\" $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf;
        rm -f $PGXL_DATA_HOME/$GTM_DIR_NAME/pgxl.master;
    "
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$slave "cat $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.conf"`
    log "Gtm slave config: \n$rs"
}

init_gtm_proxy(){
    local name=$1
    local host=$2
    local index=$3
    local master=$4
    local condition=$5
    log ">>>>>> Init gtm proxy. name: $name . host: $host . $condition"
    if [ "$host" != "" ]; then 
        if [ "$condition" == "config" ]; then
            init_gtm_proxy_config $name $host $master
        else
            prepare_before_init $host $GTM_PROXY_DIR_NAME $condition
            is_remote_dir_exists $host $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME
            local resp=$VARS
            if [[ $resp -eq $TRUE ]]; then
                log "WARNING: Init skip. Gtm proxy $name exists"
            elif [[ $resp -eq $FALSE ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "initgtm -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME"`
                log "$rs"
                init_gtm_proxy_config $name $host $master
                edit_runtime_config "RT_GTM_PROXY_NAMES" "$name" $index
                edit_runtime_config "RT_GTM_PROXY_HOSTS" "$host" $index
                log "Init gtm proxy $name $host done"
            fi
        fi
    fi
}

init_gtm_proxy_config(){
    local name=$1
    local host=$2
    local master=$3
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_GTM_PROXY $PGXL_USER@$host:$PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$host "
        mkdir -p $PGXL_LOG_HOME/$GTM_PROXY_DIR_NAME;
        sed -i \"s/^nodename.*/nodename = \'$name\'/g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
        sed -i \"s/^gtm_host.*/gtm_host = \'$master\'/g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
        sed -i \"s|^log_file.*|log_file = \'$PGXL_LOG_HOME/$GTM_PROXY_DIR_NAME/gtm_pxy.log\'|g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
    "
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "cat $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf"`
    log "Gtm proxy $name config: \n$rs"
}

init_coordinator_master(){
    local name=$1
    local host=$2
    local index=$3
    local condition=$4
    log ">>>>>> Init coordinator name: $name . host: $host . $condition"
    if [ "$host" != "" ]; then 
        if [ "$condition" == "config" ]; then
            init_coordinator_master_config $name $host
        else
            prepare_before_init $host $COORDINATOR_DIR_NAME $condition
            is_remote_dir_exists $host $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME
            local resp=$VARS
            if [[ $resp -eq $TRUE ]]; then
                log "WARNING: Init skip. Coordinator $name exists"
            elif [[ $resp -eq $FALSE ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "initdb --nodename $name -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME"`
                log "$rs"
                init_coordinator_master_config $name $host
                edit_runtime_config "RT_COORDINATOR_NAMES" "$name" $index
                edit_runtime_config "RT_COORDINATOR_HOSTS" "$host" $index
                log "Init coordinator $name $host done"
            fi
        fi
    fi
}

init_coordinator_master_config(){
    local name=$1
    local host=$2
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_HBA_COORD $PGXL_USER@$host:$PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_hba.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_COORD $PGXL_USER@$host:$PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_PGPASS $PGXL_USER@$host:$PGXL_DATA_HOME/$TEMPLATE_PGPASS 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$host "
        chmod 0600 $PGXL_DATA_HOME/$TEMPLATE_PGPASS;
        rm -f ~/.pgpass;
        ln -s $PGXL_DATA_HOME/$TEMPLATE_PGPASS ~/.pgpass;
        mkdir -p $PGXL_LOG_HOME/$COORDINATOR_DIR_NAME;
        sed -i \"s/^pgxc_node_name.*/pgxc_node_name = \'$name\'/g\" $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf;
        sed -i \"s|^log_directory.*|log_directory = \'$PGXL_LOG_HOME/$COORDINATOR_DIR_NAME\'|g\" $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf;
    "
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        ssh -p $PORT_SSH $PGXL_USER@$host "
            mkdir -p $PGXL_XLOG_HOME/$COORDINATOR_DIR_NAME;
            test ! -L $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog && mv $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog $PGXL_XLOG_HOME/$COORDINATOR_DIR_NAME/;
            test ! -L $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog && ln -s $PGXL_XLOG_HOME/$COORDINATOR_DIR_NAME/pg_xlog $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_xlog;
        "
    fi
    
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "cat $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/postgresql.conf"`
    log "Coordinator $name config: \n$rs"
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "cat $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/pg_hba.conf"`
    log "Coordinator $name hba: \n$rs"
}

init_datanode_master(){
    local name=$1
    local master=$2
    local slave=$3
    local hidden=$4
    local index=$5
    local condition=$6
    log ">>>>>> Init datanode master name: $name . host: $master . $condition"
    if [ "$master" != "" ]; then 
        if [ "$condition" == "config" ]; then
            init_datanode_master_config "$name" "$master" "$slave" "$hidden"
        else
            prepare_before_init $master $DATANODE_DIR_NAME $condition
            is_remote_dir_exists $master $PGXL_DATA_HOME/$DATANODE_DIR_NAME
            local resp=$VARS
            if [[ $resp -eq $TRUE ]]; then
                log "WARNING: Init skip. Datanode master $name exists"
            elif [[ $resp -eq $FALSE ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$master "initdb --nodename $name -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME"`
                log "$rs"
                init_datanode_master_config "$name" "$master" "$slave" "$hidden"
                edit_runtime_config "RT_DATANODE_NAMES" "$name" $index
                edit_runtime_config "RT_DATANODE_MASTER_HOSTS" "$master" $index
                log "Init datanode master $name $master done"
            fi
        fi
    fi
}

init_datanode_master_config(){
    local name=$1
    local master=$2
    local slave=$3
    local hidden=$4
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_HBA_DN $PGXL_USER@$master:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_DN_MASTER $PGXL_USER@$master:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_PGPASS $PGXL_USER@$master:$PGXL_DATA_HOME/$TEMPLATE_PGPASS 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$master "
        chmod 0600 $PGXL_DATA_HOME/$TEMPLATE_PGPASS;
        rm -f ~/.pgpass;
        ln -s $PGXL_DATA_HOME/$TEMPLATE_PGPASS ~/.pgpass;
        mkdir -p $PGXL_LOG_HOME/$DATANODE_DIR_NAME;
        sed -i \"s/^archive_mode.*/archive_mode = on/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s/^pgxc_node_name.*/pgxc_node_name = \'$name\'/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s|^log_directory.*|log_directory = \'$PGXL_LOG_HOME/$DATANODE_DIR_NAME\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/backup_label.old
        touch $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pgxl.master
    "
    local pg_alog_dir=$PGXL_ALOG_HOME/$name
    if [ "$hidden" != "" ]; then 
        ssh -p $PORT_SSH $PGXL_USER@$master "
            sed -i \"s|^archive_command.*|archive_command = \'scp -P $PORT_SSH %p $PGXL_USER@$hidden:$pg_alog_dir/%f\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        "
    elif [ "$slave" != "" ]; then 
        ssh -p $PORT_SSH $PGXL_USER@$master "
            sed -i \"s|^archive_command.*|archive_command = \'scp -P $PORT_SSH %p $PGXL_USER@$slave:$pg_alog_dir/%f\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        "
    #elif [ "$hidden" != "" ]; then 
    #    ssh -p $PORT_SSH $PGXL_USER@$master "
    #        sed -i \"s|^archive_command.*|archive_command = \'rsync -z -e \\\"ssh -p $PORT_SSH\\\" %p $PGXL_USER@$hidden:$pg_alog_dir/%f\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
    #    "
    else
        ssh -p $PORT_SSH $PGXL_USER@$master "
            sed -i \"s/^archive_mode.*/archive_mode = off/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
            sed -i \"s|^archive_command.*|archive_command = \'\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        "
    fi
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        ssh -p $PORT_SSH $PGXL_USER@$master "
            mkdir -p $PGXL_XLOG_HOME/$DATANODE_DIR_NAME;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && mv $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && ln -s $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog;
        "
    fi
    
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$master "cat $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf"`
    log "Datanode master $name config: \n$rs"
    local rs=`ssh -p $PORT_SSH $PGXL_USER@$master "cat $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf"`
    log "Datanode master $name hba: \n$rs"
}

init_datanode_slave(){
    local name=$1
    local master=$2
    local slave=$3
    local hidden=$4
    local index=$5
    local condition=$6
    log ">>>>>> Init datanode slave name: $name . slave: $slave"
    if [ "$slave" != "" ]; then
        if [ "$condition" == "config" ]; then
            init_datanode_slave_config "$name" $master $slave $hidden
        else
            prepare_before_init $slave $DATANODE_DIR_NAME $condition
            is_remote_dir_exists $slave $PGXL_DATA_HOME/$DATANODE_DIR_NAME
            local resp=$VARS
            if [[ $resp -eq $TRUE ]]; then
                log "WARNING: Init skip. Datanode slave $name exists"
            elif [[ $resp -eq $FALSE ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$slave "pg_basebackup -p $PORT_DN -h $master -c fast -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME"`
                log "$rs"
                init_datanode_slave_config "$name" $master $slave $hidden
                edit_runtime_config "RT_DATANODE_NAMES" "$name" $index
                edit_runtime_config "RT_DATANODE_SLAVE_HOSTS" "$slave" $index
                log "Init datanode slave $name $slave done"
            fi
        fi
    fi
}

init_datanode_slave_config(){
    local name=$1
    local master=$2
    local slave=$3
    local hidden=$4
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_HBA_DN $PGXL_USER@$slave:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_DN_SLAVE $PGXL_USER@$slave:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_DN_RECOVERY $PGXL_USER@$slave:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_PGPASS $PGXL_USER@$slave:$PGXL_DATA_HOME/$TEMPLATE_PGPASS 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$slave "
        chmod 0600 $PGXL_DATA_HOME/$TEMPLATE_PGPASS;
        rm -f ~/.pgpass;
        ln -s $PGXL_DATA_HOME/$TEMPLATE_PGPASS ~/.pgpass;
        mkdir -p $PGXL_LOG_HOME/$DATANODE_DIR_NAME;
        sed -i \"s/^pgxc_node_name.*/pgxc_node_name = \'$name\'/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s|^log_directory.*|log_directory = \'$PGXL_LOG_HOME/$DATANODE_DIR_NAME\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s|^primary_conninfo.*|primary_conninfo = \'host = $master port = $PORT_DN user = $PGXL_USER application_name = $name\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pgxl.master;
    "
    local pg_alog_dir=$PGXL_ALOG_HOME/$name
    if [ "$hidden" != "" ]; then 
        ssh -p $PORT_SSH $PGXL_USER@$slave "
            sed -i \"s|^restore_command.*|restore_command = \'scp -P $PORT_SSH $PGXL_USER@$hidden:$pg_alog_dir/%f %p\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
            sed -i \"s|^archive_cleanup_command.*|archive_cleanup_command = \'/bin/date\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        "
    else
        ssh -p $PORT_SSH $PGXL_USER@$slave "
            mkdir -m 700 -p $pg_alog_dir;
            sed -i \"s|^restore_command.*|restore_command = \'cp $pg_alog_dir/%f %p\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
            sed -i \"s|^archive_cleanup_command.*|archive_cleanup_command = \'pg_archivecleanup $pg_alog_dir %r\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        "
    fi
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        ssh -p $PORT_SSH $PGXL_USER@$slave "
            mkdir -p $PGXL_XLOG_HOME/$DATANODE_DIR_NAME;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && mv $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && ln -s $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog;
        "
    fi
    rs=`ssh -p $PORT_SSH $PGXL_USER@$slave "cat $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf"`
    log "Datanode slave $name config: \n$rs"
    rs=`ssh -p $PORT_SSH $PGXL_USER@$slave "cat $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf"`
    log "Datanode slave $name hba: \n$rs"
}

init_datanode_hidden(){
    local name=$1
    local master=$2
    local hidden=$3
    local slave=$4
    local index=$5
    local condition=$6
    local host=""
    if [ "$hidden" != "" ]; then
        host=$hidden
    elif [ "$slave" != "" ]; then
        host=$slave
    fi
    if [ "$host" != "" ]; then
        log ">>>>>> Init datanode hidden name: $name . host: $host"
        if [ "$condition" == "clean" ]; then
            log "Clean $host $PGXL_ALOG_HOME/$name"
            ssh -p $PORT_SSH $PGXL_USER@$host "cd $PGXL_ALOG_HOME/;rm -rf $name 2>/dev/null"
        fi
        local pg_alog_dir=$PGXL_ALOG_HOME/$name
        ssh -p $PORT_SSH $PGXL_USER@$host "mkdir -p $pg_alog_dir"
        edit_runtime_config "RT_DATANODE_NAMES" "$name" $index
        edit_runtime_config "RT_DATANODE_HIDDEN_HOSTS" "$host" $index
        log "Init datanode hidden $name $host done"
    fi
}

init_datanode_hidden_config(){
    local name=$1
    local master=$2
    local hidden=$3
    local pg_alog_dir=$PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_alog
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        pg_alog_dir=$PGXL_XLOG_HOME/$DATANODE_DIR_NAME/pg_alog
    fi
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_HBA_DN $PGXL_USER@$hidden:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_DN_HIDDEN $PGXL_USER@$hidden:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_DN_RECOVERY $PGXL_USER@$hidden:$PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf 1>/dev/null
    scp -P $PORT_SSH $CTL_ETC_DIR/$TEMPLATE_PGPASS $PGXL_USER@$hidden:$PGXL_DATA_HOME/$TEMPLATE_PGPASS 1>/dev/null
    ssh -p $PORT_SSH $PGXL_USER@$hidden "
        chmod 0600 $PGXL_DATA_HOME/$TEMPLATE_PGPASS;
        rm -f ~/.pgpass;
        ln -s $PGXL_DATA_HOME/$TEMPLATE_PGPASS ~/.pgpass;
        mkdir -p $PGXL_LOG_HOME/$DATANODE_DIR_NAME;
        mkdir -m 700 -p $pg_alog_dir;
        sed -i \"s/^pgxc_node_name.*/pgxc_node_name = \'$name\'/g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s|^log_directory.*|log_directory = \'$PGXL_LOG_HOME/$DATANODE_DIR_NAME\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf;
        sed -i \"s|^primary_conninfo.*|primary_conninfo = \'host = $master port = $PORT_DN user = $PGXL_USER application_name = $name\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        sed -i \"s|^restore_command.*|restore_command = \'cp $pg_alog_dir/%f %p\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        sed -i \"s|^archive_cleanup_command.*|archive_cleanup_command = \'pg_archivecleanup $pg_alog_dir %r\'|g\" $PGXL_DATA_HOME/$DATANODE_DIR_NAME/recovery.conf;
        rm -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pgxl.master;
    "
    if [ "$PGXL_XLOG_HOME" != "" ]; then
        ssh -p $PORT_SSH $PGXL_USER@$hidden "
            mkdir -p $PGXL_XLOG_HOME/$DATANODE_DIR_NAME;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && mv $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/;
            test ! -L $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog && ln -s $PGXL_XLOG_HOME/$DATANODE_DIR_NAME/pg_xlog $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_xlog;
        "
    fi
    
    rs=`ssh -p $PORT_SSH $PGXL_USER@$hidden "cat $PGXL_DATA_HOME/$DATANODE_DIR_NAME/postgresql.conf"`
    log "Datanode hidden $name config: \n$rs"
    rs=`ssh -p $PORT_SSH $PGXL_USER@$hidden "cat $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pg_hba.conf"`
    log "Datanode hidden $name hba: \n$rs"
}

prepare_register_sql(){
    log ">>>>>> Prepare register sql"
    local operate=$1
    local node=$2
    echo "" > $CTL_RUN_DIR/$REGISTER_SQL
    if [ "$operate" == "delete" ]; then
        echo "DROP NODE $node;" >> $CTL_RUN_DIR/$REGISTER_SQL
    else
        local length=${#RT_COORDINATOR_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_COORDINATOR_NAMES[$i]}
            local host=${RT_COORDINATOR_HOSTS[$i]}
            if [ "$host" != "" ]; then
                echo "CREATE NODE $name WITH (TYPE='coordinator', HOST='$host', PORT=$PORT_COORD);" >> $CTL_RUN_DIR/$REGISTER_SQL
                echo "ALTER NODE $name WITH (TYPE='coordinator', HOST='$host', PORT=$PORT_COORD);" >> $CTL_RUN_DIR/$REGISTER_SQL
            fi
        done
        local length=${#RT_DATANODE_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_DATANODE_NAMES[$i]}
            local host=${RT_DATANODE_MASTER_HOSTS[$i]}
            if [[ $i -eq 0 ]]; then
                echo "CREATE NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN, PRIMARY);" >> $CTL_RUN_DIR/$REGISTER_SQL
                echo "ALTER NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN, PRIMARY);" >> $CTL_RUN_DIR/$REGISTER_SQL
            else
                echo "CREATE NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN);" >> $CTL_RUN_DIR/$REGISTER_SQL
                echo "ALTER NODE $name WITH (TYPE='datanode', HOST='$host', PORT=$PORT_DN);" >> $CTL_RUN_DIR/$REGISTER_SQL
            fi
        done
    fi
    echo "SELECT pg_reload_conf();" >> $CTL_RUN_DIR/$REGISTER_SQL
    while read line
    do
        if [ "$line" != "" ]; then
            log "Register SQL: $line"
        fi
    done < $CTL_RUN_DIR/$REGISTER_SQL
}

execute_register_sql(){
    log ">>>>>> Execute register sql"
    local length=${#RT_COORDINATOR_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local name=${RT_COORDINATOR_NAMES[$i]}
        local host=${RT_COORDINATOR_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            get_process_status "$host" $PORT_COORD
            local s_host=$VARS
            if [[ $s_host -eq $STATUS_RUNNING ]]; then
                log "Execute on $name $host"
                scp -P $PORT_SSH $CTL_RUN_DIR/$REGISTER_SQL $PGXL_USER@$host:$PGXL_TMP_DIR/
                ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_COORD -f $PGXL_TMP_DIR/$REGISTER_SQL 1>/dev/null 2>&1;"
            else
                log "WARNING: can not execute on $name $host. It is not running"
            fi
        fi
    done
    local length=${#RT_DATANODE_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local name=${RT_DATANODE_NAMES[$i]}
        local host=${RT_DATANODE_MASTER_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            get_process_status "$host" $PORT_DN
            local s_host=$VARS
            if [[ $s_host -eq $STATUS_RUNNING ]]; then
                log "Execute on $name $host"
                scp -P $PORT_SSH $CTL_RUN_DIR/$REGISTER_SQL $PGXL_USER@$host:$PGXL_TMP_DIR/
                ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_DN -f $PGXL_TMP_DIR/$REGISTER_SQL 1>/dev/null 2>&1;"
            else
                log "WARNING: can not execute on $name $host. It is not running"
            fi
        fi
    done
    local length=${#RT_COORDINATOR_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local name=${RT_COORDINATOR_NAMES[$i]}
        local host=${RT_COORDINATOR_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            get_process_status "$host" $PORT_COORD
            local s_host=$VARS
            if [[ $s_host -eq $STATUS_RUNNING ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_COORD -c \"SELECT oid, * FROM pgxc_node;\" 2>&1;"`
                log "PGXL nodes on $name $host : \n$rs"
            fi
        fi
    done
    local length=${#RT_DATANODE_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local name=${RT_DATANODE_NAMES[$i]}
        local host=${RT_DATANODE_MASTER_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            get_process_status "$host" $PORT_DN
            local s_host=$VARS
            if [[ $s_host -eq $STATUS_RUNNING ]]; then
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_DN -c \"SELECT oid, * FROM pgxc_node;\" 2>&1;"`
                log "PGXL nodes on $name $host : \n$rs"
            fi
        fi
    done
    log "Execute register sql done"
}

execute_general_sql(){
    log ">>>>>> Execute general sql: $2"
    local mode=$1
    local sql="$2"
    if [ "$mode" == "coordinator" ]; then 
        local length=${#RT_COORDINATOR_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_COORDINATOR_NAMES[$i]}
            local host=${RT_COORDINATOR_HOSTS[$i]}
            if [ "$host" != "" ]; then 
                get_process_status "$host" $PORT_COORD
                local s_host=$VARS
                if [[ $s_host -eq $STATUS_RUNNING ]]; then
                    local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_COORD -c \"$sql\" 2>&1;"`
                    log "PGXL nodes on $name $host : \n$rs"
                fi
            fi
        done
    elif [ "$mode" == "datanode" ]; then 
        local length=${#RT_DATANODE_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_DATANODE_NAMES[$i]}
            local host=${RT_DATANODE_MASTER_HOSTS[$i]}
            if [ "$host" != "" ]; then 
                get_process_status "$host" $PORT_DN
                local s_host=$VARS
                if [[ $s_host -eq $STATUS_RUNNING ]]; then
                    local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_DN -c \"$sql\" 2>&1;"`
                    log "PGXL nodes on $name $host : \n$rs"
                fi
            fi
        done
    fi
    log "Execute general sql done"
}

prepare_rebalance_sql(){
    log ">>>>>> Prepare rebalance sql"
    local operate=$1
    local node=$2
    local host=$3
    ssh -p $PORT_SSH $PGXL_USER@$host "echo \"\" > $PGXL_TMP_DIR/$REBALANCE_SQL"
    local length=${#DATABASES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local dbname=${DATABASES[$i]}
        if [ "$operate" == "add" ]; then
            ssh -p $PORT_SSH $PGXL_USER@$host "
                echo \"\c $dbname\" >> $PGXL_TMP_DIR/$REBALANCE_SQL 2>/dev/null;
                psql -p $PORT_COORD -d $dbname -c \"SELECT 'ALTER TABLE \\\"' || relname || '\\\" ADD NODE ($node);' FROM pg_statio_user_tables;\" -t >> $PGXL_TMP_DIR/$REBALANCE_SQL 2>/dev/null;
            "
        elif [ "$operate" == "delete" ]; then
            ssh -p $PORT_SSH $PGXL_USER@$host "
                echo \"\c $dbname\" >> $PGXL_TMP_DIR/$REBALANCE_SQL 2>/dev/null;
                psql -p $PORT_COORD -d $dbname -c \"SELECT 'ALTER TABLE \\\"' || relname || '\\\" DELETE NODE ($node);' FROM pg_statio_user_tables;\" -t >> $PGXL_TMP_DIR/$REBALANCE_SQL 2>/dev/null;
            "
        fi
    done
    scp -P $PORT_SSH $PGXL_USER@$host:$PGXL_TMP_DIR/$REBALANCE_SQL $CTL_RUN_DIR/$REBALANCE_SQL
    while read line
    do
        if [ "$line" != "" ]; then
            log "Rebalance SQL: $line"
        fi
    done < $CTL_RUN_DIR/$REBALANCE_SQL
}

execute_rebalance_sql(){
    log ">>>>>> Execute rebalance sql"
    local host=$1
    scp -P $PORT_SSH $CTL_RUN_DIR/$REBALANCE_SQL $PGXL_USER@$host:$PGXL_TMP_DIR/
    ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_COORD -f $PGXL_TMP_DIR/$REBALANCE_SQL 1>/dev/null 2>&1;"
    log "Execute rebalance sql done"
}

# start stop restart status kill
do_runtime_operate(){
    local operate=$1
    local l_mode=$2
    local l_name=$3
    local l_role=$4
    if [ "$l_mode" == "gtm" ]; then 
        if [ "$l_role" == "master" ] || [ "$l_role" == "" ]; then 
            op_gtm $operate gtm "$RT_GTM_MASTER_HOST"
            sleep 1
            op_write_log $operate gtm "$RT_GTM_MASTER_HOST"
        fi
        if [ "$l_role" == "slave" ] || [ "$l_role" == "" ]; then 
            if [ "$RT_GTM_SLAVE_HOST" != "" ]; then 
                op_gtm $operate gtm "$RT_GTM_SLAVE_HOST"
                sleep 1
                op_write_log $operate gtm "$RT_GTM_SLAVE_HOST"
            fi
        fi
    elif [ "$l_mode" == "gtm_proxy" ]; then 
        if [ "$l_name" == "" ]; then 
            local length=${#RT_GTM_PROXY_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
            {
                op_gtm_proxy $operate "${RT_GTM_PROXY_NAMES[$i]}" "${RT_GTM_PROXY_HOSTS[$i]}"
            }&
            done
            wait
            sleep 1
            local i
            for (( i=0; i<$length; i++ ))
            do
                op_write_log $operate "${RT_GTM_PROXY_NAMES[$i]}" "${RT_GTM_PROXY_HOSTS[$i]}"
            done
        else
            find_host_by_name $l_name "${RT_GTM_PROXY_NAMES[*]}" "${RT_GTM_PROXY_HOSTS[*]}"
            local host=$VARS
            if [ "$host" != "" ]; then 
                op_gtm_proxy $operate $l_name "$host"
                sleep 1
                op_write_log $operate $l_name "$host"
            fi
        fi
    elif [ "$l_mode" == "coordinator" ]; then 
        if [ "$l_name" == "" ]; then 
            local length=${#RT_COORDINATOR_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
            {
                op_coordinator $operate "${RT_COORDINATOR_NAMES[$i]}" "${RT_COORDINATOR_HOSTS[$i]}"
            }&
            done
            wait
            sleep 1
            local i
            for (( i=0; i<$length; i++ ))
            do
                op_write_log $operate "${RT_COORDINATOR_NAMES[$i]}" "${RT_COORDINATOR_HOSTS[$i]}"
            done
        else
            find_host_by_name $l_name "${RT_COORDINATOR_NAMES[*]}" "${RT_COORDINATOR_HOSTS[*]}"
            local host=$VARS
            if [ "$host" != "" ]; then 
                op_coordinator $operate $l_name "$host"
                sleep 1
                op_write_log $operate $l_name "$host"
            fi
        fi
    elif [ "$l_mode" == "datanode" ]; then 
        if [ "$l_name" == "" ]; then 
            local length=${#RT_DATANODE_NAMES[@]}
            if [ "$l_role" == "master" ] || [ "$l_role" == "" ]; then 
                local i
                for (( i=0; i<$length; i++ ))
                do
                {
                    local name=${RT_DATANODE_NAMES[$i]}
                    local master=${RT_DATANODE_MASTER_HOSTS[$i]}
                    op_datanode $operate "$name" "$master"
                }&
                done
                wait
                sleep 1
                local i
                for (( i=0; i<$length; i++ ))
                do
                    local name=${RT_DATANODE_NAMES[$i]}
                    local master=${RT_DATANODE_MASTER_HOSTS[$i]}
                    op_write_log $operate "$name" "$master"
                done
            fi
            if [ "$l_role" == "slave" ] || [ "$l_role" == "" ]; then 
                local i
                for (( i=0; i<$length; i++ ))
                do
                {
                    local name=${RT_DATANODE_NAMES[$i]}
                    local slave=${RT_DATANODE_SLAVE_HOSTS[$i]}
                    op_datanode $operate "$name" "$slave"
                }&
                done
                wait
                sleep 1
                local i
                for (( i=0; i<$length; i++ ))
                do
                    local name=${RT_DATANODE_NAMES[$i]}
                    local slave=${RT_DATANODE_SLAVE_HOSTS[$i]}
                    op_write_log $operate "$name" "$slave"
                done
            fi
            #if [ "$l_role" == "hidden" ] || [ "$l_role" == "" ]; then 
            #    local i
            #    for (( i=0; i<$length; i++ ))
            #    do
            #    {
            #        local name=${RT_DATANODE_NAMES[$i]}
            #        local hidden=${RT_DATANODE_HIDDEN_HOSTS[$i]}
            #        op_datanode $operate "$name" "$hidden"
            #    }&
            #    done
            #    wait
            #    sleep 1
            #    local i
            #    for (( i=0; i<$length; i++ ))
            #    do
            #        local name=${RT_DATANODE_NAMES[$i]}
            #        local hidden=${RT_DATANODE_HIDDEN_HOSTS[$i]}
            #        op_write_log $operate "$name" "$hidden"
            #    done
            #fi
        else
            if [ "$l_role" == "master" ] || [ "$l_role" == "" ]; then 
                find_host_by_name $l_name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
                local host=$VARS
                if [ "$host" != "" ]; then 
                    op_datanode $operate $l_name "$host"
                    sleep 1
                    op_write_log $operate $l_name "$host"
                fi
            fi
            if [ "$l_role" == "slave" ] || [ "$l_role" == "" ]; then 
                find_host_by_name $l_name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_SLAVE_HOSTS[*]}"
                local host=$VARS
                if [ "$host" != "" ]; then 
                    op_datanode $operate $l_name "$host"
                    sleep 1
                    op_write_log $operate $l_name "$host"
                fi
            fi
            #if [ "$l_role" == "hidden" ] || [ "$l_role" == "" ]; then 
            #    find_host_by_name $l_name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_HIDDEN_HOSTS[*]}"
            #    local host=$VARS
            #    if [ "$host" != "" ]; then 
            #        op_datanode $operate $l_name "$host"
            #        sleep 1
            #        op_write_log $operate $l_name "$host"
            #    fi
            #fi
        fi
    fi
}

op_gtm(){
    local op=$1
    local name=$2
    local host=$3
    if [ "$host" != "" ]; then 
        log "Operate: $op $name $host"
        get_process_status "$host" $PORT_GTM gtm
        local s_host=$VARS
        if [[ $s_host -eq $STATUS_HOSTDOWN ]] || [[ $s_host -eq $STATUS_ERROR ]]; then
            log "WARNING: can not $op $name $host. The host is down"
        else
            if [ "$op" == "kill" ]; then 
                #local pid=`ssh -p $PORT_SSH $PGXL_USER@$host "pidof gtm"`
                local pid=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME\" | grep -v \"grep\"" | awk '{print \$2}'`
                if [ "$pid" != "" ]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$host "kill -9 $pid > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
                fi
            elif [ "$op" == "stop" ] || [ "$op" == "restart" ]; then 
                local rs=`ssh -p $PORT_SSH $PGXL_USER@$host "cat $PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control"`
                log "$rs"
                ssh -p $PORT_SSH $PGXL_USER@$host "gtm_ctl $op -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME -m fast > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            else
                ssh -p $PORT_SSH $PGXL_USER@$host "gtm_ctl $op -Z gtm -D $PGXL_DATA_HOME/$GTM_DIR_NAME > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            fi
        fi
    fi
}

op_gtm_proxy(){
    local op=$1
    local name=$2
    local host=$3
    if [ "$host" != "" ]; then 
        log "Operate: $op $name $host"
        get_process_status "$host" $PORT_GTM_PXY gtm_proxy
        local s_host=$VARS
        if [[ $s_host -eq $STATUS_HOSTDOWN ]] || [[ $s_host -eq $STATUS_ERROR ]]; then
            log "WARNING: can not $op $name $host. The host is down"
        else
            if [ "$op" == "kill" ]; then 
                #local pid=`ssh -p $PORT_SSH $PGXL_USER@$host "pidof gtm_proxy"`
                local pid=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME\" | grep -v \"grep\"" | awk '{print \$2}'`
                if [ "$pid" != "" ]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$host "kill -9 $pid > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
                fi
            elif [ "$op" == "stop" ] || [ "$op" == "restart" ]; then 
                ssh -p $PORT_SSH $PGXL_USER@$host "gtm_ctl $op -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME -m fast > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            elif [ "$op" == "start" ]; then 
                # use gtm_ctl start -Z gtm_proxy in docker has a bug. 
                ssh -p $PORT_SSH $PGXL_USER@$host "gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            else
                ssh -p $PORT_SSH $PGXL_USER@$host "gtm_ctl $op -Z gtm_proxy -D $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            fi
        fi
    fi
}

op_coordinator(){
    local op=$1
    local name=$2
    local host=$3
    if [ "$host" != "" ]; then 
        log "Operate: $op $name $host"
        get_process_status "$host" $PORT_COORD
        local s_host=$VARS
        if [[ $s_host -eq $STATUS_HOSTDOWN ]] || [[ $s_host -eq $STATUS_ERROR ]]; then
            log "WARNING: can not $op $name $host. The host is down"
        else
            if [ "$op" == "kill" ]; then 
                local pid=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"postgres --coordinator -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME\" | grep -v \"grep\"" | awk '{print \$2}'`
                if [ "$pid" != "" ]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$host "pg_ctl kill QUIT $pid > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
                fi
            elif [ "$op" == "stop" ] || [ "$op" == "restart" ]; then 
                ssh -p $PORT_SSH $PGXL_USER@$host "pg_ctl $op -Z coordinator -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME -m fast > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            else
                ssh -p $PORT_SSH $PGXL_USER@$host "pg_ctl $op -Z coordinator -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            fi
        fi
    fi
}

op_datanode(){
    local op=$1
    local name=$2
    local host=$3
    if [ "$host" != "" ]; then 
        log "Operate: $op $name $host"
        get_process_status "$host" $PORT_DN
        local s_host=$VARS
        if [[ $s_host -eq $STATUS_HOSTDOWN ]] || [[ $s_host -eq $STATUS_ERROR ]]; then
            log "WARNING: can not $op $name $host. The host is down"
        else
            if [ "$op" == "kill" ]; then 
                local pid=`ssh -p $PORT_SSH $PGXL_USER@$host "ps aux |grep \"postgres --datanode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME\" | grep -v \"grep\"" | awk '{print \$2}'`
                if [ "$pid" != "" ]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$host "pg_ctl kill QUIT $pid > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
                fi
            elif [ "$op" == "stop" ] || [ "$op" == "restart" ]; then 
                ssh -p $PORT_SSH $PGXL_USER@$host "pg_ctl $op -Z datanode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME -m fast > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            else
                ssh -p $PORT_SSH $PGXL_USER@$host "pg_ctl $op -Z datanode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME > $PGXL_TMP_DIR/$op_$name.oplog 2>&1 &"
            fi
        fi
    fi
}

op_write_log(){
    local op=$1
    local name=$2
    local host=$3
    if [ "$host" != "" ]; then 
        local rs=`timeout 1 ssh -p $PORT_SSH $PGXL_USER@$host "cat $PGXL_TMP_DIR/$op_$name.oplog"`
        log "$rs"
    fi
}

do_runtime_stop_or_kill(){
    local mode=$1
    local name=$2
    local role=$3
    local times=$4
    while [[ $times > 0 ]]
    do
        get_process_status2 "$mode" "$name" "$role"
        local status=$VARS
        if [[ $status -eq $STATUS_RUNNING ]]; then
            do_runtime_operate stop "$mode" "$name" "$role"
        fi
        times=`expr $times - 1`
    done
    do_runtime_operate kill "$mode" "$name" "$role"
}

failover_gtm(){
    if [ "$RT_GTM_SLAVE_HOST" == "" ]; then 
        log "ERROR: Cannot fail over gtm. Not found slave"
    else
        log ">>>>>> Fail over gtm to $RT_GTM_SLAVE_HOST"
        init_gtm_master_config $RT_GTM_SLAVE_HOST
        local tmp_host=$RT_GTM_MASTER_HOST
        edit_runtime_config "RT_GTM_MASTER_HOST" "$RT_GTM_SLAVE_HOST"
        edit_runtime_config "RT_GTM_SLAVE_HOST" "$tmp_host"
        local length=${#RT_GTM_PROXY_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
        {
            local gtmpxy_host=${RT_GTM_PROXY_HOSTS[$i]}
            if [ "$gtmpxy_host" != "" ]; then 
                get_process_status "$gtmpxy_host" $PORT_GTM_PXY gtm_proxy
                local s_host=$VARS
                if [[ $s_host -eq $STATUS_RUNNING ]]; then
                    ssh -p $PORT_SSH $PGXL_USER@$gtmpxy_host "
                        sed -i \"s|^gtm_host.*|gtm_host = \'$RT_GTM_MASTER_HOST\'|g\" $PGXL_DATA_HOME/$GTM_PROXY_DIR_NAME/gtm_proxy.conf;
                    "
                fi
            fi
        }&
        done
        wait
        do_runtime_operate kill coordinator
        do_runtime_operate stop datanode
        do_runtime_operate kill datanode
        do_runtime_operate kill gtm_proxy
        do_runtime_operate kill gtm
        ssh -p $PORT_SSH $PGXL_USER@$RT_GTM_MASTER_HOST "
            rm -f $PGXL_DATA_HOME/$GTM_DIR_NAME/register.node;
        "
        get_process_status "$RT_GTM_SLAVE_HOST" $PORT_GTM gtm
        local status=$VARS
        if [[ $status -eq $STATUS_RUNNING ]] || [[ $status -eq $STATUS_STOPPED ]]; then
            ssh -p $PORT_SSH $PGXL_USER@$RT_GTM_MASTER_HOST "
                scp -P $PORT_SSH $PGXL_USER@$RT_GTM_SLAVE_HOST:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control $PGXL_DATA_HOME/$GTM_DIR_NAME/ 1>/dev/null
            "
        else
            log "WARNING: Cannot sync gtm.control to new gtm master."
        fi
        do_runtime_operate start gtm
        do_runtime_operate start gtm_proxy
        do_runtime_operate start datanode
        do_runtime_operate start coordinator
        log "Fail over gtm done"
    fi
}

failover_datanode(){
    local l_name=$1
    if [ "$l_name" == "" ]; then 
        log "ERROR: Cannot fail over datanode. Name is null"
    else
        find_host_by_name $l_name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_SLAVE_HOSTS[*]}"
        local new_master=$VARS
        find_host_by_name $l_name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
        local new_slave=$VARS
        find_host_by_name $l_name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_HIDDEN_HOSTS[*]}"
        local hidden=$VARS
        if [ "$new_master" == "" ]; then 
            log "ERROR: Cannot fail over datanode ${l_name}. Not found slave"
        else
            log ">>>>>> Fail over datanode $l_name to $new_master"
            get_process_status "$new_slave" $PORT_DN
            local status=$VARS
            if [[ $status -eq $STATUS_RUNNING ]]; then
                do_runtime_stop_or_kill datanode $l_name master 3
            fi
            init_datanode_master_config "$l_name" "$new_master" "$new_slave" "$hidden"
            local length=${#RT_DATANODE_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
                local name=${RT_DATANODE_NAMES[$i]}
                if [ "$l_name" == "$name" ]; then 
                    edit_runtime_config "RT_DATANODE_MASTER_HOSTS" "$new_master" $i
                    edit_runtime_config "RT_DATANODE_SLAVE_HOSTS" "$new_slave" $i
                fi
            done
            do_runtime_operate kill coordinator
            do_runtime_operate stop datanode
            do_runtime_operate kill datanode
            do_runtime_operate kill gtm_proxy
            do_runtime_stop_or_kill gtm gtm master 3
            ssh -p $PORT_SSH $PGXL_USER@$RT_GTM_MASTER_HOST "
                rm -f $PGXL_DATA_HOME/$GTM_DIR_NAME/register.node;
            "
            do_runtime_operate start gtm gtm master
            do_runtime_operate start gtm_proxy
            do_runtime_operate start datanode
            do_runtime_operate start coordinator
            prepare_register_sql
            execute_register_sql
            do_runtime_operate kill coordinator
            do_runtime_operate start coordinator
            log "Fail over datanode $l_name done"
            #do_runtime_operate stop datanode "$l_name" "hidden"
            #rebuild_datanode_hidden $l_name backup
            #do_runtime_operate start datanode "$l_name" "hidden"
        fi
    fi
}

rebuild_gtm_slave(){
    local condition=$1
    if [ "$RT_GTM_SLAVE_HOST" != "" ]; then 
        get_process_status "$RT_GTM_SLAVE_HOST" $PORT_GTM gtm
        local status=$VARS
        if [[ $status -eq $STATUS_STOPPED ]]; then
            log ">>>>>> Rebuild gtm slave $RT_GTM_SLAVE_HOST $condition"
            init_gtm_slave "$RT_GTM_MASTER_HOST" "$RT_GTM_SLAVE_HOST" "$condition"
            log "Rebuild gtm slave $RT_GTM_SLAVE_HOST done"
        else
            log "ERROR: Cannot rebuild gtm slave. Node is not stopped"
        fi
    else
        log "ERROR: Cannot rebuild gtm slave. Host is null"
    fi
}

rebuild_gtm_proxy(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot rebuild gtm proxy. Name is null"
    else
        find_host_by_name $name "${RT_GTM_PROXY_NAMES[*]}" "${RT_GTM_PROXY_HOSTS[*]}"
        local host=$VARS
        if [ "$host" != "" ]; then 
            get_process_status "$host" $PORT_GTM_PXY gtm_proxy
            local status=$VARS
            if [[ $status -eq $STATUS_STOPPED ]]; then
                log ">>>>>> Rebuild gtm proxy $name $condition"
                find_index_by_name $name "${RT_GTM_PROXY_NAMES[*]}"
                local index=$VARS
                init_gtm_proxy "$name" "$host" $index "$RT_GTM_MASTER_HOST" "$condition"
                log "Rebuild gtm proxy $name done"
            else
                log "ERROR: Cannot rebuild gtm proxy ${name}. Node is not stopped"
            fi
        else
            log "ERROR: Cannot rebuild gtm proxy ${name}. Host is null"
        fi
    fi
}

rebuild_datanode_slave(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot rebuild datanode slave. Name is null"
    else
        find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
        local master=$VARS
        find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_SLAVE_HOSTS[*]}"
        local slave=$VARS
        find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_HIDDEN_HOSTS[*]}"
        local hidden=$VARS
        find_index_by_name $name "${RT_DATANODE_NAMES[*]}"
        local index=$VARS
        if [ "$master" != "" ] && [ "$slave" != "" ]; then 
            get_process_status "$slave" $PORT_DN
            local status=$VARS
            if [[ $status -eq $STATUS_STOPPED ]]; then
                log ">>>>>> Rebuild datanode slave $name $master $slave $condition"
                init_datanode_slave $name "$master" "$slave" "$hidden" $index "$condition"
                log "Rebuild datanode slave $name $slave done"
            else
                log "ERROR: Cannot rebuild datanode slave ${name} $slave. Node is not stopped. Try to run $0 -m stop -z datanode -n ${name} -r slave"
            fi
        else
            log "ERROR: Cannot rebuild datanode slave ${name}. Master($slave) or slave($slave) host is null"
        fi
    fi
}

rebuild_datanode_hidden(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot rebuild datanode hidden. Name is null"
    else
        find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
        local master=$VARS
        find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_HIDDEN_HOSTS[*]}"
        local hidden=$VARS
        find_index_by_name $name "${RT_DATANODE_NAMES[*]}"
        local index=$VARS
        if [ "$master" != "" ] && [ "$hidden" != "" ]; then 
            get_process_status "$hidden" $PORT_DN
            local status=$VARS
            if [[ $status -eq $STATUS_STOPPED ]]; then
                log ">>>>>> Rebuild datanode hidden $name $master $hidden $condition"
                init_datanode_hidden $name "$master" "$hidden" "" $index "$condition"
                log "Rebuild datanode hidden $name $hidden done"
            else
                log "ERROR: Cannot rebuild datanode hidden ${name} $hidden. Node is not stopped. Try to run $0 -m stop -z datanode -n ${name} -r hidden"
            fi
        else
            log "ERROR: Cannot rebuild datanode hidden ${name}. Master($hidden) or hidden($hidden) host is null"
        fi
    fi
}

add_gtm_proxy(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot add gtm proxy. Name is null"
    else
        find_index_by_name $name "${RT_GTM_PROXY_NAMES[*]}"
        local index=$VARS
        if [ "$index" == "" ]; then 
            log ">>>>>> Add gtm proxy $name $condition"
            do_init_operate gtm_proxy "$name" "" "$condition"
            do_runtime_operate start gtm_proxy "$name"
            log "Add gtm proxy $name done"
        else
            log "WARNING: Cannot add gtm proxy $name. Already exists"
        fi
    fi
}

add_coordinator(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot add coordinator. Name is null"
    else
        find_host_by_name $name "${RT_COORDINATOR_NAMES[*]}" "${RT_COORDINATOR_HOSTS[*]}"
        local host=$VARS
        get_process_status "$host" $PORT_COORD
        local status=$VARS
        if [[ $status -ne $STATUS_RUNNING ]]; then 
            log ">>>>>> Add coordinator $name $condition"
            do_init_operate coordinator "$name" "" "$condition"
            find_host_by_name $name "${RT_COORDINATOR_NAMES[*]}" "${RT_COORDINATOR_HOSTS[*]}"
            local host=$VARS
            find_alive_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD $host
            local primary=$VARS
            if [ "$host" != "" ] && [ "$primary" != "" ]; then 
                get_process_status "$host" $PORT_COORD
                local status=$VARS
                if [[ $status -eq $STATUS_STOPPED ]]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$host "
                        pg_dumpall -p $PORT_COORD -h $primary -s --include-nodes --dump-nodes --file=$PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/primary.sql;
                        pg_ctl start -Z restoremode -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME -o -i > $PGXL_TMP_DIR/restore_$name.oplog 2>&1 &
                        sleep 1s;
                        psql -p $PORT_COORD -f $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME/primary.sql;
                        pg_ctl stop -Z restoremode -D $PGXL_DATA_HOME/$COORDINATOR_DIR_NAME;
                    "
                    do_runtime_operate start coordinator "$name"
                    prepare_register_sql
                    execute_register_sql
                    log "Add coordinator $name done"
                else
                    log "ERROR: Cannot add coordinator ${name}. Host($host) is $status"
                fi
            else
                log "ERROR: Cannot add coordinator ${name}. Host($host) or primary host($primary) is null"
            fi
        else
            log "WARNING: Cannot add coordinator $name. Already exists and running"
        fi
    fi
}

add_datanode(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot add datanode. Name is null"
    else
        find_index_by_name $name "${RT_DATANODE_NAMES[*]}"
        local index=$VARS
        if [ "$index" == "" ]; then 
            log ">>>>>> Add datanode $name $condition"
            do_init_operate datanode "$name" "master" "$condition"
            find_host_by_name $name "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
            local host=$VARS
            find_alive_host "${RT_DATANODE_MASTER_HOSTS[*]}" $PORT_DN $host
            local primary=$VARS
            if [ "$host" != "" ] && [ "$primary" != "" ]; then 
                get_process_status "$host" $PORT_DN
                local status=$VARS
                if [[ $status -eq $STATUS_STOPPED ]]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$host "
                        pg_dumpall -p $PORT_DN -h $primary -s --include-nodes --dump-nodes --file=$PGXL_DATA_HOME/$DATANODE_DIR_NAME/primary.sql;
                        pg_ctl start -Z restoremode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME -o -i > $PGXL_TMP_DIR/restore_$name.oplog 2>&1 &
                        sleep 1s;
                        psql -p $PORT_DN -f $PGXL_DATA_HOME/$DATANODE_DIR_NAME/primary.sql;
                        pg_ctl stop -Z restoremode -D $PGXL_DATA_HOME/$DATANODE_DIR_NAME;
                    "
                    do_runtime_operate start datanode "$name" "master"
                    do_init_operate datanode "$name" "slave" "$condition"
                    do_runtime_operate start datanode "$name" "slave"      # need to start master and slave before register nodes if synchronous_standby_names is not empty
                    prepare_register_sql
                    execute_register_sql
                    log "Add datanode $name done"
                else
                    log "ERROR: Cannot add datanode master ${name}. Host($host) is $status"
                fi
            else
                log "ERROR: Cannot add datanode master ${name}. Host($host) or primary host($primary) is null"
            fi
        else
            log "WARNING: Cannot add datanode $name. Already exists"
        fi
    fi
}

remove_gtm_proxy(){
    local name=$1
    local condition=$2
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot remove gtm proxy. Name is null"
    else
        find_index_by_name $name "${RT_GTM_PROXY_NAMES[*]}"
        local index=$VARS
        #local length=${#RT_GTM_PROXY_NAMES[@]}
        #local length_end=`expr $length - 1`
        if [ "$index" == "" ]; then 
            log "WARNING: Cannot remove gtm proxy $name. Not found"
        #elif [[ $index -ne $length_end ]]; then 
        #    log "WARNING: Cannot remove gtm proxy $name. Not the last one $index $length"
        else
            log ">>>>>> Remove gtm proxy $name $condition"
            do_runtime_operate stop gtm_proxy $name
            do_runtime_operate kill gtm_proxy $name
            edit_runtime_config "RT_GTM_PROXY_NAMES" "" $index
            edit_runtime_config "RT_GTM_PROXY_HOSTS" "" $index
            log "Remove gtm proxy $name done"
        fi
    fi
}

remove_coordinator(){
    local name=$1
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot remove coordinator. Name is null"
    else
        find_index_by_name $name "${RT_COORDINATOR_NAMES[*]}"
        local index=$VARS
        #local length=${#RT_COORDINATOR_NAMES[@]}
        #local length_end=`expr $length - 1`
        if [ "$index" == "" ]; then 
            log "WARNING: Cannot remove coordinator $name. Not found"
        #elif [[ $index -ne $length_end ]]; then 
        #    log "WARNING: Cannot remove coordinator $name. Not the last one $index $length"
        else
            log ">>>>>> Remove coordinator $name"
            do_runtime_operate stop coordinator $name
            do_runtime_operate kill coordinator $name
            prepare_register_sql delete $name
            execute_register_sql
            edit_runtime_config "RT_COORDINATOR_NAMES" "" $index
            edit_runtime_config "RT_COORDINATOR_HOSTS" "" $index
            log "Remove coordinator $name done"
        fi
    fi
}

remove_datanode(){
    local name=$1
    if [ "$name" == "" ]; then 
        log "ERROR: Cannot remove datanode. Name is null"
    else
        find_index_by_name $name "${RT_DATANODE_NAMES[*]}"
        local index=$VARS
        local length=${#RT_DATANODE_NAMES[@]}
        local length_end=`expr $length - 1`
        if [ "$index" != "" ]; then 
            log "WARNING: Cannot remove datanode $name. Not found"
        elif [[ $index -ne $length_end ]]; then 
            log "WARNING: Cannot remove datanode $name. Not the last one $index $length"
        else
            find_alive_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
            local primary=$VARS
            is_datanode_ready_to_be_removed $name $primary
            local resp=$VARS
            if [[ "$resp" -eq $TRUE ]]; then 
                log ">>>>>> Remove datanode $name"
                do_runtime_operate stop datanode $name
                do_runtime_operate kill datanode $name
                prepare_register_sql delete $name
                execute_register_sql
                edit_runtime_config "RT_DATANODE_NAMES" "" $index
                edit_runtime_config "RT_DATANODE_MASTER_HOSTS" "" $index
                edit_runtime_config "RT_DATANODE_SLAVE_HOSTS" "" $index
                edit_runtime_config "RT_DATANODE_HIDDEN_HOSTS" "" $index
                log "Remove datanode $name done"
            else
                log "ERROR: Cannot remove datanode $name. Please re-balance data first."
            fi
        fi
    fi
}

# It costs a long time and locks table.
rebalance_datanode(){
    local option=$1
    local name=$2
    if [ "$option" == "" ] || [ "$name" == "" ]; then 
        log "ERROR: Cannot rebalance datanode. Option($option) or Name($name) is null"
    else
        find_alive_host "${RT_COORDINATOR_HOSTS[*]}" $PORT_COORD
        local primary=$VARS
        if [ "$primary" != "" ]; then 
            prepare_rebalance_sql "$option" "$name" $primary
            execute_rebalance_sql $primary
        else
            log "ERROR: Cannot rebalance datanode. Primary host($primary) is null"
        fi
    fi
}

clean_coordinator_log(){
    local days=$1
    if [ "$days" != "" ]; then 
        log ">>>>>> Execute clean coordinator log $days"
        local length=${#RT_COORDINATOR_HOSTS[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local host=${RT_COORDINATOR_HOSTS[$i]}
            if [ "$host" != "" ]; then 
                ssh -p $PORT_SSH $PGXL_USER@$host "find $PGXL_LOG_HOME/$COORDINATOR_DIR_NAME/ -name \"*.log*\" -mtime +$days -delete;"
            fi
        done
    fi
}

clean_datanode_log(){
    local days=$1
    if [ "$days" != "" ]; then 
        log ">>>>>> Execute clean datanode log $days"
        local length=${#RT_DATANODE_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_DATANODE_NAMES[$i]}
            local master=${RT_DATANODE_MASTER_HOSTS[$i]}
            local slave=${RT_DATANODE_SLAVE_HOSTS[$i]}
            #local hidden=${RT_DATANODE_HIDDEN_HOSTS[$i]}
            if [ "$master" != "" ]; then 
                ssh -p $PORT_SSH $PGXL_USER@$master "find $PGXL_LOG_HOME/$DATANODE_DIR_NAME/ -name \"*.log*\" -mtime +$days -delete;"
            fi
            if [ "$slave" != "" ]; then 
                ssh -p $PORT_SSH $PGXL_USER@$slave "find $PGXL_LOG_HOME/$DATANODE_DIR_NAME/ -name \"*.log*\" -mtime +$days -delete;"
            fi
            #if [ "$hidden" != "" ]; then 
            #    ssh -p $PORT_SSH $PGXL_USER@$hidden "find $PGXL_LOG_HOME/$DATANODE_DIR_NAME/ -name \"*.log*\" -mtime +$days -delete;"
            #fi
        done
    fi
}

start_keeper(){
    # lock
    exec 7<> .pgxl_keeper.lock
    flock -n 7
    if [ $? -eq 1 ]; then
        return 1
    fi
    
    log ">>>>>> Start pgxl cluster keeper."
    while [ true ]
    do
        # gtm
        get_process_status "$RT_GTM_MASTER_HOST" $PORT_GTM gtm
        local s_master=$VARS
        get_process_status "$RT_GTM_SLAVE_HOST" $PORT_GTM gtm
        local s_slave=$VARS
        if [[ $s_master -eq $STATUS_STOPPED ]]; then
            do_runtime_operate start gtm gtm master
            get_process_status "$RT_GTM_MASTER_HOST" $PORT_GTM gtm
            local s_master=$VARS
            if [[ $s_master -ne $STATUS_RUNNING ]] && [[ $s_slave -eq $STATUS_RUNNING ]]; then
                failover_gtm
                continue
            fi
        elif [[ $s_master -eq $STATUS_HOSTDOWN ]] && [[ $s_slave -eq $STATUS_RUNNING ]]; then
            failover_gtm
            continue
        fi
        if [[ $s_slave -eq $STATUS_STOPPED ]]; then
            is_remote_file_exists $RT_GTM_SLAVE_HOST $PGXL_DATA_HOME/$GTM_DIR_NAME/pgxl.master
            local f_master=$VARS
            if [[ $f_master -eq $TRUE ]]; then
                rebuild_gtm_slave backup
            fi
            do_runtime_operate start gtm gtm slave
            get_process_status "$RT_GTM_SLAVE_HOST" $PORT_GTM gtm
            s_slave=$VARS
            if [[ $s_slave -ne $STATUS_RUNNING ]]; then
                rebuild_gtm_slave clean
                do_runtime_operate start gtm gtm slave
            fi
        elif [[ $s_slave -eq $STATUS_RUNNING ]]; then
            ssh -p $PORT_SSH $PGXL_USER@$RT_GTM_SLAVE_HOST "
                scp -P $PORT_SSH $PGXL_USER@$RT_GTM_MASTER_HOST:$PGXL_DATA_HOME/$GTM_DIR_NAME/gtm.control $PGXL_DATA_HOME/$GTM_DIR_NAME/ 1>/dev/null
            "
        fi
        
        # gtm proxy
        local length=${#RT_GTM_PROXY_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_GTM_PROXY_NAMES[$i]}
            local host=${RT_GTM_PROXY_HOSTS[$i]}
            if [ "$host" != "" ]; then 
                get_process_status "$host" $PORT_GTM_PXY gtm_proxy
                local s_host=$VARS
                if [[ $s_host -eq $STATUS_STOPPED ]]; then
                    get_process_status "$RT_GTM_MASTER_HOST" $PORT_GTM gtm
                    local s_gtm_master=$VARS
                    if [[ $s_gtm_master -ne $STATUS_RUNNING ]]; then
                        continue 2
                    fi
                    rebuild_gtm_proxy "$name" clean
                    do_runtime_operate start gtm_proxy $name
                fi
            fi
        done
        
        # datanode
        local length=${#RT_DATANODE_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_DATANODE_NAMES[$i]}
            local master=${RT_DATANODE_MASTER_HOSTS[$i]}
            local slave=${RT_DATANODE_SLAVE_HOSTS[$i]}
            #local hidden=${RT_DATANODE_HIDDEN_HOSTS[$i]}
            get_process_status "$master" $PORT_DN
            local s_master=$VARS
            get_process_status "$slave" $PORT_DN
            local s_slave=$VARS
            #get_process_status "$hidden" $PORT_DN
            #local s_hidden=$VARS
            if [[ $s_master -eq $STATUS_STOPPED ]]; then
                get_process_status "$RT_GTM_MASTER_HOST" $PORT_GTM gtm
                local s_gtm_master=$VARS
                if [[ $s_gtm_master -ne $STATUS_RUNNING ]]; then
                    continue 2
                fi
                get_process_status "$master" $PORT_GTM_PXY gtm_proxy
                local s_gtmpxy_host=$VARS
                if [[ $s_gtmpxy_host -ne $STATUS_RUNNING ]]; then
                    continue 2
                fi
                do_runtime_operate start datanode $name master
                get_process_status "$master" $PORT_DN
                s_master=$VARS
                if [[ $s_master -ne $STATUS_RUNNING ]] && [[ $s_slave -eq $STATUS_RUNNING ]]; then
                    failover_datanode "$name"
                    continue 2
                fi
            elif [[ $s_master -eq $STATUS_HOSTDOWN ]] && [[ $s_slave -eq $STATUS_RUNNING ]]; then
                failover_datanode "$name"
                continue 2
            fi
            if [[ $s_slave -eq $STATUS_STOPPED ]]; then
                is_remote_file_exists $slave $PGXL_DATA_HOME/$DATANODE_DIR_NAME/pgxl.master
                local f_master=$VARS
                if [[ $f_master -eq $TRUE ]]; then
                    rebuild_datanode_slave $name backup
                fi
                do_runtime_operate start datanode $name slave
                get_process_status "$slave" $PORT_DN
                s_slave=$VARS
                if [[ $s_slave -ne $STATUS_RUNNING ]]; then
                    rebuild_datanode_slave $name clean
                    do_runtime_operate start datanode $name slave
                fi
            fi
            #if [[ $s_hidden -eq $STATUS_STOPPED ]]; then
            #   do_runtime_operate start datanode $name hidden
            #    get_process_status "$hidden" $PORT_DN
            #    s_hidden=$VARS
            #    if [[ $s_hidden -ne $STATUS_RUNNING ]]; then
            #        rebuild_datanode_hidden $name clean
            #        do_runtime_operate start datanode $name hidden
            #    fi
            #fi
        done
        
        # coordinator
        local length=${#RT_COORDINATOR_NAMES[@]}
        local i
        for (( i=0; i<$length; i++ ))
        do
            local name=${RT_COORDINATOR_NAMES[$i]}
            local host=${RT_COORDINATOR_HOSTS[$i]}
            if [ "$host" != "" ]; then 
                get_process_status "$host" $PORT_COORD
                local s_host=$VARS
                if [[ $s_host -eq $STATUS_STOPPED ]]; then
                    get_process_status "$RT_GTM_MASTER_HOST" $PORT_GTM gtm
                    local s_gtm_master=$VARS
                    if [[ $s_gtm_master -ne $STATUS_RUNNING ]]; then
                        continue 2
                    fi
                    get_process_status "$host" $PORT_GTM_PXY gtm_proxy
                    local s_gtmpxy_host=$VARS
                    if [[ $s_gtmpxy_host -ne $STATUS_RUNNING ]]; then
                        continue 2
                    fi
                    do_runtime_operate start coordinator $name
                    get_process_status "$host" $PORT_COORD
                    s_host=$VARS
                    if [[ $s_host -ne $STATUS_RUNNING ]]; then
                        #prepare_register_sql delete $name
                        #execute_register_sql
                        #add_coordinator "$name" "clean"
                        log "WARNING: Coordinator $name can not run. Try to unregister and re-add it"
                    else
                        prepare_register_sql
                        execute_register_sql
                    fi
                elif [[ $s_host -ne $STATUS_RUNNING ]]; then
                    log "WARNING: Coordinator $name can not be connected"
                fi
            fi
        done
        
        sleep $KEEPER_INTERVAL
    done
}

stop_keeper(){
    local pids=`ps -ef | grep "$0" | grep "start" | grep "keeper" | grep -v "grep" | awk '{print $2}'`
    local pidarr=($pids)
    local length=${#pidarr[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local pid=${pidarr[$i]}
        kill -9 $pid
    done
    log ">>>>>> Stop pgxl cluster keeper."
}

###############################################################################################################
# main methods
###############################################################################################################

pgxl_init(){
    mkdir $CTL_LOG_DIR $CTL_RUN_DIR $CTL_SQL_DIR;
    load_config "$CTL_HOME/$INIT_CONFIG"
    if [ "$MODE" == "all" ]; then 
        if [ -f "$CTL_RUN_DIR/$RUNTIME_CONFIG" ]; then 
            log "ERROR: $CTL_RUN_DIR/$RUNTIME_CONFIG exists. Please stop all nodes and remove it first"
            log "HINT: $0 -m stop -z all; rm -f $CTL_RUN_DIR/$RUNTIME_CONFIG"
        else
            do_init_operate gtm "" "" "$CONDITION"
            do_init_operate gtm_proxy "" "" "$CONDITION"
            
            do_init_operate datanode "" "hidden" "$CONDITION"
            
            do_init_operate datanode "" "master" "$CONDITION"
            do_init_operate coordinator "" "" "$CONDITION"
            
            do_runtime_operate start gtm
            do_runtime_operate start gtm_proxy
            do_runtime_operate start datanode "" "master"     # need to start master before init slave
            do_runtime_operate start coordinator
            
            do_init_operate datanode "" "slave" "$CONDITION"
            
            do_runtime_operate start datanode "" "slave"      # need to start master and slave before register nodes if synchronous_standby_names is not empty
            #do_runtime_operate start datanode "" "hidden"
            
            prepare_register_sql
            execute_register_sql
            
            local sql_files=`ls $CTL_SQL_DIR/`
            local host=${RT_COORDINATOR_HOSTS[0]}
            for f in $sql_files; do
                scp $CTL_SQL_DIR/$f $PGXL_USER@$host:$PGXL_TMP_DIR/
                ssh -p $PORT_SSH $PGXL_USER@$host "psql -p $PORT_COORD -f $PGXL_TMP_DIR/$f;"
            done
        fi
    else 
        do_init_operate "$MODE" "$NAME" "$ROLE" "$CONDITION"
    fi
}

pgxl_start(){
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate start gtm
        do_runtime_operate start gtm_proxy
        do_runtime_operate start datanode
        do_runtime_operate start coordinator
    elif [ "$MODE" == "keeper" ]; then 
        start_keeper > /dev/null 2>&1 &
    else 
        do_runtime_operate start "$MODE" "$NAME" "$ROLE"
    fi
}

pgxl_stop(){
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate stop coordinator
        do_runtime_operate stop datanode
        do_runtime_operate stop gtm_proxy
        do_runtime_operate stop gtm
    elif [ "$MODE" == "keeper" ]; then 
        stop_keeper
    else 
        do_runtime_stop_or_kill "$MODE" "$NAME" "$ROLE" 3
    fi
}

pgxl_restart(){
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate stop coordinator
        do_runtime_operate stop datanode
        do_runtime_operate stop gtm_proxy
        do_runtime_operate stop gtm
        do_runtime_operate start gtm
        do_runtime_operate start gtm_proxy
        do_runtime_operate start datanode
        do_runtime_operate start coordinator
    else 
        do_runtime_stop_or_kill "$MODE" "$NAME" "$ROLE" 3
        do_runtime_operate start "$MODE" "$NAME" "$ROLE"
    fi
}

pgxl_reload(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    execute_general_sql "$MODE" "SELECT pg_reload_conf();"
}

pgxl_exec(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    execute_general_sql "$MODE" "$CONDITION"
}

pgxl_kill(){
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate kill coordinator
        do_runtime_operate kill datanode
        do_runtime_operate kill gtm_proxy
        do_runtime_operate kill gtm
    else 
        do_runtime_operate kill "$MODE" "$NAME" "$ROLE"
    fi
}

pgxl_status(){
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ]; then 
        do_runtime_operate status gtm
        do_runtime_operate status gtm_proxy
        do_runtime_operate status datanode
        do_runtime_operate status coordinator
    else 
        do_runtime_operate status "$MODE" "$NAME" "$ROLE"
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
    #load_config "$CTL_HOME/$INIT_CONFIG"
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
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "gtm" ]; then 
        failover_gtm
    elif [ "$MODE" == "datanode" ]; then 
        failover_datanode "$NAME"
    fi
}

pgxl_rebuild(){
    #load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "gtm" ]; then 
        rebuild_gtm_slave "$CONDITION"
    elif [ "$MODE" == "gtm_proxy" ]; then 
        rebuild_gtm_proxy "$NAME" "$CONDITION"
    elif [ "$MODE" == "datanode_slave" ]; then 
        rebuild_datanode_slave "$NAME" "$CONDITION"
    #elif [ "$MODE" == "datanode_hidden" ]; then 
    #    rebuild_datanode_hidden "$NAME" "$CONDITION"
    fi
}

pgxl_register(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    prepare_register_sql
    execute_register_sql
}

pgxl_rebalance(){
    load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    rebalance_datanode "$CONDITION" "$NAME"
}

pgxl_topology(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    echo -e "[GTM] gtm:master:$RT_GTM_MASTER_HOST:$(get_process_status_info $RT_GTM_MASTER_HOST $PORT_GTM gtm)"
    echo -e "[GTM] gtm:slave:$RT_GTM_SLAVE_HOST:$(get_process_status_info $RT_GTM_SLAVE_HOST $PORT_GTM gtm)"
    local length=${#RT_GTM_PROXY_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
    {
        local name=${RT_GTM_PROXY_NAMES[$i]}
        local host=${RT_GTM_PROXY_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            echo -e "[GTM_PROXY] ${name}:host:${host}:$(get_process_status_info $host $PORT_GTM_PXY gtm_proxy)"
        fi
    }&
    done
    wait
    local length=${#RT_DATANODE_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
    {
        local name=${RT_DATANODE_NAMES[$i]}
        local master=${RT_DATANODE_MASTER_HOSTS[$i]}
        local slave=${RT_DATANODE_SLAVE_HOSTS[$i]}
        #local hidden=${RT_DATANODE_HIDDEN_HOSTS[$i]}
        if [ "$master" != "" ]; then 
            echo -e "[DATANODE] ${name}:master:${master}:$(get_process_status_info $master $PORT_DN)"
        fi
        if [ "$slave" != "" ]; then 
            echo -e "[DATANODE] ${name}:slave:${slave}:$(get_process_status_info $slave $PORT_DN)"
        fi
        #if [ "$hidden" != "" ]; then 
        #    echo -e "[DATANODE] ${name}:hidden:${hidden}:$(get_process_status_info $hidden $PORT_DN)"
        #fi
    }&
    done
    wait
    local length=${#RT_COORDINATOR_NAMES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
    {
        local name=${RT_COORDINATOR_NAMES[$i]}
        local host=${RT_COORDINATOR_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            echo -e "[COORDINATOR] ${name}:host:${host}:$(get_process_status_info $host $PORT_COORD)"
        fi
    }&
    done
    wait
}

pgxl_clean_log(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    if [ "$MODE" == "all" ] || [ "$MODE" == "coordinator" ]; then 
        clean_coordinator_log "$CONDITION"
    fi
    if [ "$MODE" == "all" ] || [ "$MODE" == "datanode" ]; then 
        clean_datanode_log "$CONDITION"
    fi
}

pgxl_clean_prepared_xacts(){
    load_config "$CTL_HOME/$INIT_CONFIG"
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    local min_ago=$CONDITION
    if [ "$min_ago" == "" ]; then
        min_ago=5
        echo "use default 5 min ago"
    fi
    local dt=`date -d "${min_ago} min ago" +"%Y-%m-%d %H:%M.%S"`
    local length=${#RT_DATANODE_NAMES[@]}
    local length_db=${#DATABASES[@]}
    local i
    for (( i=0; i<$length; i++ ))
    do
        local name=${RT_DATANODE_NAMES[$i]}
        local host=${RT_DATANODE_MASTER_HOSTS[$i]}
        if [ "$host" != "" ]; then 
            get_process_status "$host" $PORT_DN
            local s_host=$VARS
            if [[ $s_host -eq $STATUS_RUNNING ]]; then
                log "Execute on $name $host"
                local j
                for (( j=0; j<$length_db; j++ ))
                do
                    local dbname=${DATABASES[$j]}
                    ssh -p $PORT_SSH $PGXL_USER@$host "
                        psql -p $PORT_DN -t -c \"SELECT 'ROLLBACK PREPARED '''||gid||''';' FROM pg_prepared_xacts WHERE database = '$dbname' AND prepared < '$dt';\" > $PGXL_TMP_DIR/${dbname}.$ROLLBACK_SQL 2>/dev/null;
                        psql -p $PORT_DN -d $dbname -f $PGXL_TMP_DIR/${dbname}.$ROLLBACK_SQL;
                    "
                done
            else
                log "WARNING: can not execute on $name $host. It is not running"
            fi
        fi
    done
}

pgxl_clean_achieve_log(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    local kept_count=${CONDITION}
    if [ "${kept_count}" != "" ]; then 
        log ">>>>>> Execute clean achieve log on ${MODE}. Keep count ${kept_count}"
        if [ "$MODE" == "all" ] || [ "$MODE" == "coordinator" ]; then 
            local length=${#RT_DATANODE_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
                local name=${RT_COORDINATOR_NAMES[$i]}
                local hidden=${RT_COORDINATOR_HIDDEN_HOSTS[$i]}
                if [ "$hidden" != "" ]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$hidden "ls -t ${PGXL_ALOG_HOME}/${name}/* |tail -n +${kept_count} |xargs rm -f"
                fi
            done
        fi
        if [ "$MODE" == "all" ] || [ "$MODE" == "datanode" ]; then 
            local length=${#RT_DATANODE_NAMES[@]}
            local i
            for (( i=0; i<$length; i++ ))
            do
                local name=${RT_DATANODE_NAMES[$i]}
                local hidden=${RT_DATANODE_HIDDEN_HOSTS[$i]}
                if [ "$hidden" != "" ]; then 
                    ssh -p $PORT_SSH $PGXL_USER@$hidden "ls -t ${PGXL_ALOG_HOME}/${name}/* |tail -n +${kept_count} |xargs rm -f"
                fi
            done
        fi
    fi
}

pgxl_basebackup(){
    load_config "$CTL_RUN_DIR/$RUNTIME_CONFIG"
    local dir_datetime="$(date '+%Y%m%d-%H%M%S')"
    local kept_count=${CONDITION}
    if [ "$kept_count" == "" ]; then 
        kept_count=3
    fi
    log ">>>>>> Execute base backup on ${MODE} ${NAME}. Keep count ${kept_count}"
    if [ "$MODE" == "coordinator" ]; then 
        find_host_by_name "$NAME" "${RT_COORDINATOR_NAMES[*]}" "${RT_COORDINATOR_MASTER_HOSTS[*]}"
        local master=$VARS
        find_host_by_name "$NAME" "${RT_COORDINATOR_NAMES[*]}" "${RT_COORDINATOR_HIDDEN_HOSTS[*]}"
        local hidden=$VARS
        if [ "$hidden" != "" ]; then 
            ssh -p $PORT_SSH $PGXL_USER@$hidden "
                mkdir -p $PGXL_BACKUP_HOME/$NAME/${dir_datetime};
                ls -d $PGXL_BACKUP_HOME/$NAME/* -1 |sort -r -n |tail -n +${kept_count} |xargs rm -rf;
                pg_basebackup -p $PORT_DN -h $master -c fast -D $PGXL_BACKUP_HOME/$NAME/${dir_datetime};
            "
        fi
    elif [ "$MODE" == "datanode" ]; then 
        find_host_by_name "$NAME" "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_MASTER_HOSTS[*]}"
        local master=$VARS
        find_host_by_name "$NAME" "${RT_DATANODE_NAMES[*]}" "${RT_DATANODE_HIDDEN_HOSTS[*]}"
        local hidden=$VARS
        if [ "$hidden" != "" ]; then 
            ssh -p $PORT_SSH $PGXL_USER@$hidden "
                mkdir -p $PGXL_BACKUP_HOME/$NAME/${dir_datetime};
                ls -d $PGXL_BACKUP_HOME/$NAME/* -1 |sort -r -n |tail -n +${kept_count} |xargs rm -rf;
                pg_basebackup -p $PORT_DN -h $master -c fast -D $PGXL_BACKUP_HOME/$NAME/${dir_datetime};
            "
        fi
    fi
}

pgxl_usage(){
    echo -e "
        Usage:
            ./pgxl_ctl.sh [OPTION]...
        Options:
            -m        method (eg. init|add|remove|start|stop|kill|restart|reload|status|rebuild|failover|register|rebalance|topology)
            -z        mode (eg. all|gtm|gtm_proxy|datanode|coordinator|keeper)
            -n        name of node
            -r        role (eg. master|slave)
            -c        condition (eg. clean|backup)
        Examples:
            1.Initialize pgxl cluster configured in pgxl_init.conf. Include initialize all nodes, register nodes and execute sql.
                ./pgxl_ctl.sh -m init -z all
                ./pgxl_ctl.sh -m init -z all -c clean
            2.Initialize some node only. Notice that -c config is based on pgxl_init.conf. Never do this on datanode or gtm after failover.
                ./pgxl_ctl.sh -m init -z gtm -r master
                ./pgxl_ctl.sh -m init -z gtm_proxy
                ./pgxl_ctl.sh -m init -z coordinator -c config
                ./pgxl_ctl.sh -m init -z datanode -n datanode1 -r slave
            3.Operate nodes. 
                ./pgxl_ctl.sh -m start -z all
                ./pgxl_ctl.sh -m stop -z gtm -r master
                ./pgxl_ctl.sh -m kill -z gtm_proxy -n gtm_pxy1
                ./pgxl_ctl.sh -m restart -z datanode -n datanode1 -r slave
                ./pgxl_ctl.sh -m status -z coordinator
            4.Rebuild slave node or gtm proxy if it broke down. 
                ./pgxl_ctl.sh -m rebuild -z gtm -c clean
                ./pgxl_ctl.sh -m rebuild -z gtm_proxy -n gtm_pxy2 -c clean
                ./pgxl_ctl.sh -m rebuild -z datanode_slave -n datanode2 -c backup
            5.Failover master node if it broke down. Before do this, stop keeper.
                ./pgxl_ctl.sh -m failover -z gtm
                ./pgxl_ctl.sh -m failover -z datanode -n datanode3
            6.Add new node configured in pgxl_init.conf to a running cluster. Before do this, stop keeper and choose an active coordinator and issue pgxc_lock_for_backup() to block DDL issued to all the active coordinators. After, issue quit to release DDL lock.
                ./pgxl_ctl.sh -m add -z coordinator -n coord5 -c clean
                ./pgxl_ctl.sh -m add -z datanode -n datanode3 -c backup
                ./pgxl_ctl.sh -m add -z gtm_proxy -n gtm_pxy3 -c clean
            7.Remove node from a running cluster. Before do this, stop keeper. Notice that datanode can only be removed when it is at the end of array.
                ./pgxl_ctl.sh -m remove -z coordinator -n coord5
                ./pgxl_ctl.sh -m remove -z datanode -n datanode3
                ./pgxl_ctl.sh -m remove -z gtm_proxy -n gtm_pxy3
            8.Register all nodes according to pgxl_runtime.conf.
                ./pgxl_ctl.sh -m register
            9.Re-balance data. That will lock table.
                ./pgxl_ctl.sh -m rebalance -n datanode5 -c add
                ./pgxl_ctl.sh -m rebalance -n datanode5 -c delete
            10.Show cluster status
                ./pgxl_ctl.sh -m topology
            11.Execute sql on datanodes or coordinators
                ./pgxl_ctl.sh -m exec -z coordinator -c \"CREATE EXTENSION pg_stat_statements;CREATE EXTENSION plpythonu;\"
            12.Reload postgresql.conf on datanode or coordinator
                ./pgxl_ctl.sh -m reload -z coordinator
            13.HA keeper
                ./pgxl_ctl.sh -m start -z keeper
                ./pgxl_ctl.sh -m stop -z keeper
            14.Clean log out of date
                ./pgxl_ctl.sh -m clean_log -z all -c 14
                ./pgxl_ctl.sh -m clean_log -z coordinator -c 30
                ./pgxl_ctl.sh -m clean_log -z datanode -c 7
            15.Clean data exist in table pg_prepared_xacts beyond x minutes. Default 5 minutes.
                ./pgxl_ctl.sh -m clean_px
                ./pgxl_ctl.sh -m clean_px -c 3
            16.Others
                */1 * * * * flock -n /tmp/pgxl_keeper.lock -c '/bin/bash /home/postgres/pgxl_ctl/pgxl_ctl.sh -m start -z keeper'
                0 0 * * * /bin/bash /home/postgres/pgxl_ctl/pgxl_ctl.sh -m clean_log -z all -c 30
    "
}

case "$METHOD" in
    init)                  pgxl_init
                           ;;
    add)                   pgxl_add
                           ;;
    remove)                pgxl_remove
                           ;;
    start)                 pgxl_start
                           ;;
    stop)                  pgxl_stop
                           ;;
    kill)                  pgxl_kill
                           ;;
    restart)               pgxl_restart
                           ;;
    reload)                pgxl_reload
                           ;;
    status)                pgxl_status
                           ;;
    failover)              pgxl_failover
                           ;;
    rebuild)               pgxl_rebuild
                           ;;
    register)              pgxl_register
                           ;;
    rebalance)             pgxl_rebalance
                           ;;
    exec)                  pgxl_exec
                           ;;
    topology)              pgxl_topology
                           ;;
    clean_log)             pgxl_clean_log
                           ;;
    clean_px)              pgxl_clean_prepared_xacts
                           ;;
    backup)                pgxl_basebackup
                           ;;
    clean_achieve_log)     pgxl_clean_achieve_log
                           ;;
    *)                     pgxl_usage
                           ;;
esac
