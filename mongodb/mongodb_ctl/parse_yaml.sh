#!/bin/bash
#

PY_PREFIX=
PY_YAML_FILE=

help(){
    echo -e "
    >>> eg_yml_file <<<
        systemLog:
           destination: file
           path: "/data/log/mongodb/config/config.log"
           logAppend: false
        storage:
           dbPath: "/data/mongodb/config"
           engine: "wiredTiger"
           journal:
              enabled: false
           wiredTiger:
              engineConfig:
                 cacheSizeGB: 1
                 directoryForIndexes: true
           directoryPerDB: true
        
    >>> Examples <<<
        1.  parse_yaml.sh -f /dir/eg_yml_file
        
            print result:
            systemLog_destination="file"
            systemLog_path="/data/log/mongodb/config/config.log"
            systemLog_logAppend="false"
            storage_dbPath="/data/mongodb/config"
            storage_engine="wiredTiger"
            storage_journal_enabled="false"
            storage_wiredTiger_engineConfig_cacheSizeGB="1"
            storage_wiredTiger_engineConfig_directoryForIndexes="true"
            storage_directoryPerDB="true"
            
        2.  parse_yaml.sh -f /dir/eg_yml_file -p "prefix_"
        
            print result:
            prefix_systemLog_destination="file"
            prefix_systemLog_path="/data/log/mongodb/config/config.log"
            prefix_systemLog_logAppend="false"
            prefix_storage_dbPath="/data/mongodb/config"
            prefix_storage_engine="wiredTiger"
            prefix_storage_journal_enabled="false"
            prefix_storage_wiredTiger_engineConfig_cacheSizeGB="1"
            prefix_storage_wiredTiger_engineConfig_directoryForIndexes="true"
            prefix_storage_directoryPerDB="true"
    "
}

while getopts h?p:f: OPTION
do
    case $OPTION in
        h|\?)
            help
            exit 0
            ;;
        p)
            PY_PREFIX=$OPTARG
            ;;
        f)
            PY_YAML_FILE=$OPTARG
            ;;
    esac
done

do_parse(){
    if [ -z ${PY_YAML_FILE} ]; then
        help
        exit 1
    fi
    
    local dis=`cat ${PY_YAML_FILE} | egrep "^[[:space:]].*" | head -n1 | awk '{
        split($0, chars, "");
        count=0;
        for (i=1; i <= length($0); i++) {
            if (chars[i] == " ") count++; else break;
        } 
        print count;
    }'`
    
    if [ -z ${dis} ]; then
        dis=1
    fi
    
    local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
    
    sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  ${PY_YAML_FILE} |
    awk -F$fs '{
        indent = length($1)/'${dis}';
        vname[indent] = $2;
        for (i in vname) {if (i > indent) {delete vname[i]}}
        if (length($3) > 0) {
            vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
            printf("%s%s%s=\"%s\"\n", "'${PY_PREFIX}'",vn, $2, $3);
        }
    }'
}

do_parse
