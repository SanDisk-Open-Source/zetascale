#!/bin/bash 

check_btree=true
logfile=""

usage()
{
    echo "$0 -a -m -h" 
    echo "   -a Run META, LOGS and BTREE checks (default)" 
    echo "   -m Run only META and LOGS checks" 
    echo "   -h Print help" 
    exit 1
}

while getopts "ahl:m" opt; do

    case $opt in

        a) 
            check_btree=true
            ;;

        l) 
            logfile=${OPTARG}
            ;;
        m) 
            check_btree=false
            ;;

        h)
            usage
            ;;

        *)
            usage
            ;;

    esac
done

if [ -n "$logfile" ]
then
    zsck -l $logfile
else
    zsck
fi

if [ "$?" -ne "0" ]
then
    if [ "$check_btree" = true ]
    then
        echo "META/LOGS check failed. Cannot continue."
    fi
    exit 1
fi

if [ "$check_btree" = true ]
then
    if [ -n "$logfile" ]
    then
        zsck -b -l $logfile
    else
        zsck -b
    fi
fi

