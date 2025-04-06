#! /bin/sh 

LOGFORMAT='^\./flink--taskexecutor-[0-9]+-\w{12}\.log(.[1-9][0-9]*)?$'
if ! ls ./flink--taskexecutor*.log* 2> /dev/null 1>&2
  then
    echo "logEx: No flink--taskexecutor*.log* files found." 1>&2
    exit 1 
  else
    for i in ./flink--taskexecutor*.log*
    do
      if ! (echo $i | grep -E $LOGFORMAT 2> /dev/null 1>&2)
      then
          echo "logEx: File $i doesn't conform to the format $LOGFORMAT" 1>&2
      elif ! [ -f $i ]
      then
        echo "logEx: File $i is not a regular file." 1>&2
      elif ! [ -r $i ]
      then
        echo "logEx: File $i is not readable." 1>&2
      else
        ARGS="$ARGS $i"
        continue
      fi
    done
fi

if [ -z "$ARGS" ]
  then
    echo "logEx: No flink--taskexecutor*.log* files found." 1>&2
    exit 1 
fi

eval cat $ARGS \
  | awk '
  BEGIN { 
    format = ".csv"
    base = "Log-Summary"
    sharedAttributes = "Time,Operator"
    sort = "sort -t, -k2,2 >>" 

    TYPEFILE = base format  
    TIMEFILE = base "-Times" format  
    SHEDFILE = base "-Shedding" format  

    printf "Type,%s,Id\n",sharedAttributes > TYPEFILE
    printf "Time,Real Time,Operator\n",sharedAttributes > TIMEFILE
    printf "IsShedding,%s\n",sharedAttributes > SHEDFILE

    close(TYPEFILE)
    close(TIMEFILE)
    close(SHEDFILE)

    saveType = sort TYPEFILE
    saveTime = sort TIMEFILE
    saveShed = sort SHEDFILE

  }
  /(isShedding|type|ptime)/ {
    gsub(/[{}" ]/,"",$0)
  }
  /^type/ {
    gsub(/(type|time|id|name):/,"",$0)
    print $0 | saveType;
  }
  /ptime/ {
    gsub(/(ptime|time|name):/,"",$0)
    print $0 | saveTime;
  }
  /isShedding/ {
    gsub(/(isShedding|time|name):/,"",$0)
    print $0 | saveShed;
  }

  END {
    close(saveType);
    close(saveTime);
    close(saveShed);
  }
  '
