#!/bin/bash 
#  用于Nagios通过SNMP读取主机信息的插件
#  支持： CPU使用率，负载，内存，磁盘使用率，网络使用，磁盘IO使用，特定进程数量，TCP数量等
#
#  Author  Wang Hengbin
#  Date    2016-02-02
# 
# -H IP -C COMMUNITY -M METHOD -o OID -w WARNING_THRESHOLD -c CRITICAL_THRESHOLD -s SUBOID
# do not use more than one char as getopts option.
# do not use system parameter as local parameter. eg PATH
METHOD=
IP=
COMMUNITY=
OID=
SUBOID=
WARNING_THRESHOLD=
CRITICAL_THRESHOLD=

while getopts M:H:C:o:w:c:s: OPTION
do
     case $OPTION in
      M)
       METHOD=$OPTARG 
       ;;
      H)
       IP=$OPTARG 
       ;;
      C)
       COMMUNITY=$OPTARG
       ;;
      o)
       OID=$OPTARG
       ;;
      w)
       WARNING_THRESHOLD=$OPTARG
       ;;
      c)
       CRITICAL_THRESHOLD=$OPTARG
       ;;
      s)
       SUBOID=$OPTARG
       ;;
     esac
done
# echo $METHOD $IP $COMMUNITY $OID $WARNING_THRESHOLD $CRITICAL_THRESHOLD $SUBOID

# -C mycommunity -H xxx.xxx.xxx.xxx -o someoid -w 80 -c 100
check_snmp(){
    #FOCUS=$(snmpget -c $COMMUNITY -v1 $IP $OID | awk '{print $4}' )
    FOCUS=$(snmpget -c $COMMUNITY -v2c -OqvtU $IP $OID)
    DETAIL="$FOCUS"
    print_status $FOCUS $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M load -w 400 -c 800
check_load(){
    OID=".1.3.6.1.4.1.2021.10.1.5.1 .1.3.6.1.4.1.2021.10.1.3.1 .1.3.6.1.4.1.2021.10.1.3.2 .1.3.6.1.4.1.2021.10.1.3.3"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    # TODO cpu num
    LOAD_1_INT=${FOCUSES[0]}
    LOAD_1=${FOCUSES[1]}
    LOAD_5=${FOCUSES[2]}
    LOAD_15=${FOCUSES[3]}
    
    DETAIL="load average : "$LOAD_1", "$LOAD_5", "$LOAD_15

    print_status $LOAD_1_INT $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M uptime -w 365 -c 800
check_uptime(){
    # unix only
    OID=".1.3.6.1.2.1.25.1.1.0"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)

    DAY=$(expr $FOCUS / 6000 / 60 / 24)
    HOUR=$(expr $FOCUS / 6000 / 60 % 24)
    MINUTE=$(expr $FOCUS / 6000 % 60)
    
    DETAIL="system uptime : "$DAY" days "$HOUR" hours "$MINUTE" minutes"

    print_status $DAY $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M disk -w 80 -c 95 -s 1
check_disk(){
    OID=".1.3.6.1.4.1.2021.9.1.2.$SUBOID .1.3.6.1.4.1.2021.9.1.3.$SUBOID .1.3.6.1.4.1.2021.9.1.6.$SUBOID .1.3.6.1.4.1.2021.9.1.7.$SUBOID .1.3.6.1.4.1.2021.9.1.8.$SUBOID .1.3.6.1.4.1.2021.9.1.9.$SUBOID"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    DISK_PATH=${FOCUSES[0]}
    DEVICE=${FOCUSES[1]}
    TOTAL=${FOCUSES[2]}
    AVAIL=${FOCUSES[3]}
    USE=${FOCUSES[4]}
    PERCENTAGE_USE=${FOCUSES[5]}
    CANUSE_M=$(expr $AVAIL / 1024)
    TOTAL_M=$(expr $TOTAL / 1024)
    USE_M=$(expr $USE / 1024)
    
    DETAIL="disk  "$DISK_PATH" ("$DEVICE") : total "$TOTAL_M" MB - used "$USE_M" MB ("$PERCENTAGE_USE"%) - free "$CANUSE_M" MB"

    print_status $PERCENTAGE_USE $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M memory -w 90 -c 96
check_memory(){
    OID=".1.3.6.1.4.1.2021.4.5.0 .1.3.6.1.4.1.2021.4.11.0 .1.3.6.1.4.1.2021.4.14.0 .1.3.6.1.4.1.2021.4.15.0"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    TOTAL=${FOCUSES[0]}
    AVAIL=${FOCUSES[1]}
    BUFFER=${FOCUSES[2]}
    CACHE=${FOCUSES[3]}
    CANUSE=$(expr $AVAIL + $BUFFER + $CACHE)
    USE=$(expr $TOTAL - $CANUSE)
    PERCENTAGE_USE=$(expr $USE \* 100 / $TOTAL)
    CANUSE_M=$(expr $CANUSE / 1024)
    TOTAL_M=$(expr $TOTAL / 1024)
    USE_M=$(expr $USE / 1024)
    
    DETAIL="memory usage : total "$TOTAL_M" MB - used "$USE_M" MB ("$PERCENTAGE_USE"%) - free "$CANUSE_M" MB"

    print_status $PERCENTAGE_USE $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M net -w 60000 -c 90000 -s 2
check_net(){
    # OID=".1.3.6.1.2.1.2.2.1.2.$SUBOID .1.3.6.1.2.1.2.2.1.10.$SUBOID .1.3.6.1.2.1.2.2.1.16.$SUBOID"
    # FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    OID=".1.3.6.1.2.1.31.1.1.1.1.$SUBOID .1.3.6.1.2.1.31.1.1.1.6.$SUBOID .1.3.6.1.2.1.31.1.1.1.10.$SUBOID"
    FOCUS=$(snmpget -c $COMMUNITY -v2c -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    NET_INTERFACE=${FOCUSES[0]}
    NET_IN=${FOCUSES[1]}
    NET_OUT=${FOCUSES[2]}
    DATA_FILE=/var/tmp/net_${IP}_${NET_INTERFACE}_${COMMUNITY}.data
    NET_TIMESTAMP=$(date '+%s')
    
    ###
    #  NET_TIMESTAMP_L=
    #  NET_IN_L=
    #  NET_OUT_L=
    ###
    if [ -f $DATA_FILE ]; then
        while read line
        do
            eval "$line"
        done < $DATA_FILE
    fi
    
    echo "NET_TIMESTAMP_L="$NET_TIMESTAMP > $DATA_FILE
    echo "NET_IN_L="$NET_IN >> $DATA_FILE
    echo "NET_OUT_L="$NET_OUT >> $DATA_FILE
    
    if [ "x$NET_TIMESTAMP_L" == "x" ]; then
        echo "OK: init".
        exit 0
    fi
    
    SECONDS=$(expr $NET_TIMESTAMP - $NET_TIMESTAMP_L)
    NET_IN_USE=$(expr $NET_IN - $NET_IN_L)
    NET_OUT_USE=$(expr $NET_OUT - $NET_OUT_L)
    NET_IN_TRAFFIC=$(expr $NET_IN_USE / $SECONDS / 1024)
    NET_OUT_TRAFFIC=$(expr $NET_OUT_USE / $SECONDS / 1024)
    NET_ALL_TRAFFIC=$(expr $NET_IN_TRAFFIC + $NET_OUT_TRAFFIC)
    
    DETAIL="net interface "$NET_INTERFACE" : total "$NET_ALL_TRAFFIC" KB/s - in "$NET_IN_TRAFFIC" KB/s - out "$NET_OUT_TRAFFIC" KB/s"

    print_status $NET_ALL_TRAFFIC $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M cpu -w 80 -c 95
check_cpu(){
    OID=".1.3.6.1.4.1.2021.11.9.0 .1.3.6.1.4.1.2021.11.10.0 .1.3.6.1.4.1.2021.11.11.0"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    PERCENTAGE_USER=${FOCUSES[0]}
    PERCENTAGE_SYS=${FOCUSES[1]}
    PERCENTAGE_IDLE=${FOCUSES[2]}
    PERCENTAGE_USE=$(expr $PERCENTAGE_USER + $PERCENTAGE_SYS)
    
    DETAIL="cpu usage : user "$PERCENTAGE_USER"% - system "$PERCENTAGE_SYS"% - idle "$PERCENTAGE_IDLE"%"

    print_status $PERCENTAGE_USE $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}
check_cpu_idle(){
    OID=".1.3.6.1.4.1.2021.11.9.0 .1.3.6.1.4.1.2021.11.10.0 .1.3.6.1.4.1.2021.11.11.0"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    PERCENTAGE_USER=${FOCUSES[0]}
    PERCENTAGE_SYS=${FOCUSES[1]}
    PERCENTAGE_IDLE=${FOCUSES[2]}
    PERCENTAGE_USE=$(expr $PERCENTAGE_USER + $PERCENTAGE_SYS)
    
    DETAIL="cpu usage : user "$PERCENTAGE_USER"% - system "$PERCENTAGE_SYS"% - idle "$PERCENTAGE_IDLE"%"

    print_status $PERCENTAGE_IDLE $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M io -w 2000 -c 3000 -s 1
check_io(){
    #OID=".1.3.6.1.4.1.2021.13.15.1.1.2.$SUBOID .1.3.6.1.4.1.2021.13.15.1.1.3.$SUBOID .1.3.6.1.4.1.2021.13.15.1.1.4.$SUBOID"
    #FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    OID=".1.3.6.1.4.1.2021.13.15.1.1.2.$SUBOID .1.3.6.1.4.1.2021.13.15.1.1.12.$SUBOID .1.3.6.1.4.1.2021.13.15.1.1.13.$SUBOID"
    FOCUS=$(snmpget -c $COMMUNITY -v2c -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    IO_DISK=${FOCUSES[0]}
    IO_READ=${FOCUSES[1]}
    IO_WRITE=${FOCUSES[2]}
    DATA_FILE=/var/tmp/io_${IP}_${IO_DISK}_${COMMUNITY}.data
    IO_TIMESTAMP=$(date '+%s')
    
    ###
    #  IO_TIMESTAMP_L=
    #  IO_READ_L=
    #  IO_WRITE_L=
    ###
    if [ -f $DATA_FILE ]; then
        while read line
        do
            eval "$line"
        done < $DATA_FILE
    fi
    
    echo "IO_TIMESTAMP_L="$IO_TIMESTAMP > $DATA_FILE
    echo "IO_READ_L="$IO_READ >> $DATA_FILE
    echo "IO_WRITE_L="$IO_WRITE >> $DATA_FILE
    
    if [ "x$IO_TIMESTAMP_L" == "x" ]; then
        echo "OK: init".
        exit 0
    fi
    
    SECONDS=$(expr $IO_TIMESTAMP - $IO_TIMESTAMP_L)
    IO_READ_USE=$(expr $IO_READ - $IO_READ_L)
    IO_WRITE_USE=$(expr $IO_WRITE - $IO_WRITE_L)
    IO_READ_TRAFFIC=$(expr $IO_READ_USE / $SECONDS / 1024)
    IO_WRITE_TRAFFIC=$(expr $IO_WRITE_USE / $SECONDS / 1024)
    IO_ALL_TRAFFIC=$(expr $IO_READ_TRAFFIC + $IO_WRITE_TRAFFIC)
    
    DETAIL="io disk "$IO_DISK" : total "$IO_ALL_TRAFFIC" KB/s - read "$IO_READ_TRAFFIC" KB/s - write "$IO_WRITE_TRAFFIC" KB/s"

    print_status $IO_ALL_TRAFFIC $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M proc -w 10 -c 20 -s 1
check_process(){
    OID=".1.3.6.1.4.1.2021.2.1.2.$SUBOID .1.3.6.1.4.1.2021.2.1.5.$SUBOID"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    PROC_NAME=${FOCUSES[0]}
    PROC_COUNT=${FOCUSES[1]}
    
    DETAIL="process ( "$PROC_NAME" ) : count "$PROC_COUNT

    print_status $PROC_COUNT $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M proc_count -w 120 -c 200
check_process_count(){
    OID="host.hrSystem.hrSystemProcesses.0"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    PROC_COUNT=${FOCUSES[0]}
    
    DETAIL="processes : count "$PROC_COUNT

    print_status $PROC_COUNT $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# -C mycommunity -H xxx.xxx.xxx.xxx -M tcp_open -w 1600 -c 2400
check_tcp_open(){
    OID="tcp.tcpCurrEstab.0"
    FOCUS=$(snmpget -c $COMMUNITY -v1 -OqvtU $IP $OID)
    FOCUSES=($FOCUS)
    
    TCP_CURRENT_ESTABLISH=${FOCUSES[0]}
    
    DETAIL="tcp : current established "$TCP_CURRENT_ESTABLISH

    print_status $TCP_CURRENT_ESTABLISH $WARNING_THRESHOLD $CRITICAL_THRESHOLD "$DETAIL --warn ($WARNING_THRESHOLD)"
}

# 80=gt   :80=gt   80:=lt   70:80=ltgt
print_status(){
    FOCUS=$1
    WARN=$2
    CRITICAL=$3
    DETAIL="$4"
    
    if [ "x$FOCUS" == "x" ] || [ "x$WARN" == "x" ] || [ "x$CRITICAL" == "x" ]; then
    	echo "UNKNOWN: $DETAIL".
    	exit 3
    fi
    
    print_status_sub "$CRITICAL" "CRITICAL" 2 "$FOCUS"
    
    print_status_sub "$WARN" "WARNNING" 1 "$FOCUS"
    
    echo "OK: $DETAIL".
    exit 0
}

print_status_sub(){
	STR_ABC=$1
	HEAD_INFO=$2
	EXIT_CODE=$3
	FOCUS=$4
    
	maohao=$(expr index "$STR_ABC" ":")
	LEFT_RANGE=""
	RIGHT_RANGE=""
	
	if [ $maohao -eq 0 ]; then
		RIGHT_RANGE=$STR_ABC
	elif [ $maohao -eq 1 ]; then
		RIGHT_RANGE=${STR_ABC:$maohao}
	elif [ $maohao -eq ${#STR_ABC} ]; then
		LEFT_RANGE=${STR_ABC:0:$maohao-1}
	else	
		RIGHT_RANGE=${STR_ABC:$maohao}
		LEFT_RANGE=${STR_ABC:0:$maohao-1}
	fi
	
	if [ "x$RIGHT_RANGE" != "x" ] && [ $FOCUS -gt $RIGHT_RANGE ]; then
        echo "$HEAD_INFO: $DETAIL".
        exit $EXIT_CODE
    fi
	
	if [ "x$LEFT_RANGE" != "x" ] && [ $FOCUS -lt $LEFT_RANGE ]; then
        echo "$HEAD_INFO: $DETAIL".
        exit $EXIT_CODE
    fi
}

case "$METHOD" in
    load)          check_load
                   ;;
    uptime)        check_uptime
                   ;;
    disk)          check_disk
                   ;;
    memory)        check_memory
                   ;;
    net)           check_net
                   ;;
    cpu)           check_cpu
                   ;;
    cpu_idle)      check_cpu_idle
                   ;;
    io)            check_io
                   ;;
    proc)          check_process
                   ;;
    proc_count)    check_process_count
                   ;;
    tcp_open)      check_tcp_open
                   ;;
    *)             check_snmp
                   ;;
esac
