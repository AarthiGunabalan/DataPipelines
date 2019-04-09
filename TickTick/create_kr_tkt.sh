#!/bin/bash/

#=========================================================================
function check_exit
#=========================================================================
{
    if [ $1 -ne 0 ];  then
        exit_with_return_code $1
    fi
}

#=========================================================================
function exit_with_return_code
#=========================================================================
{
    exit_code=$1
    case $exit_code in
        0)
            # success, exit with value 0
            exit 0
            ;;

        1)
            exit 1
            ;;
        *)
            exit $exit_code
            ;;
    esac
}


#===============================================================================
function kerberos_tckt_creation
#===============================================================================
{
    returncode=0
    count=1
    echo "Creating new Kerberos ticket with kinit command"
    export KRB5_CONFIG=/usr/java/default/jre/lib/security/krb5.conf
    kinit -kt /u/users/$USER/.$USER.keytab $USER@HADOOP_${NODE}.<<domain>>.COM
    if [ $? -ne 0 ];then
        echo "kinit failed"
        while [ ${count} -le ${retryCount} ]
        do
            echo "Retrying ${count} of ${retryCount}"
            export KRB5_CONFIG=/usr/java/default/jre/lib/security/krb5.conf
            kinit -kt /u/users/$USER/.$USER.keytab $USER@HADOOP_${NODE}.<<domain>>.COM
            if [ $? -eq 0 ];then
                echo "kinit successful"
                klist &>> ${LOG_FILE}
                break;
            else
                echo "kinit failed"
                count=`expr ${count} + 1`
                sleep ${sleep_time}
            fi
        done

        if [ ${count} -gt ${retryCount} ];then
            echo "Failed creating new kerberos ticket for $count times, hence exiting ..."
            returncode=1
        fi
    else
        echo "Kerberos ticket created successfully"
    fi
    return $returncode
}

#===============================================================================
function kerberos_tckt_expiry_chk
#===============================================================================
{
    returncode=0
    sys_date=`date +%d`
    sys_next_date=`date +%d --date "next day"`
    syt_time_hrs=`date +%H`
    kerb_exp_dt=`klist |grep "<<domain>>.COM" |grep -iv 'Default principal' |tr -s ' '|cut -d ' ' -f3|cut -d '/' -f2`
    ticket_expiry_in_hrs=`klist |grep "<<domain>>.COM" |grep -iv 'Default principal' |tr -s ' '|cut -d ' ' -f4|cut -d ':' -f1`
    ticket_expiry_left_out=`expr $ticket_expiry_in_hrs - $syt_time_hrs`

    if [ $sys_date -eq $kerb_exp_dt ]; then
        if [ $ticket_expiry_left_out -lt $max_time_in_hrs ]; then
            echo "Existing ticket will expire in $ticket_expiry_left_out. Max time allowed is $max_time_in_hrs. Creating new ticket"
            kerberos_tckt_creation
            if [ $? -ne 0 ]; then
                returncode=1
            fi
        else
            echo "Ticket is valid till $ticket_expiry_left_out hrs, hence not creating new ticket..."
        fi
    elif [ $sys_next_date -eq $kerb_exp_dt ]; then
        echo "Ticket will expire soon, hence checking expiry time.."
        sys_time_hrs_1=`expr 24 - $syt_time_hrs`
        sys_time_hrs_2=`expr $sys_time_hrs_1 + $ticket_expiry_in_hrs`
        if [ $sys_time_hrs_2 -lt $max_time_in_hrs ]; then
            echo "Existing ticket will expire in $sys_time_hrs_2. Max time allowed is $max_time_in_hrs. Creating new ticket"
            kerberos_tckt_creation
            if [ $? -ne 0 ]; then
                returncode=1
            fi
         else
             echo "Ticket is valid till $sys_time_hrs_2 hrs, hence not creating new ticket..."
        fi
    elif [ $kerb_exp_dt -lt $sys_date ]; then
        echo "Kerberos ticket expired, hence creating new ticket"
        kerberos_tckt_creation
            if [ $? -ne 0 ]; then
                returncode=1
            fi
    else
        echo "Kerberos expriry date $kerb_exp_dt is not within next day, hence not proceeding further.."
        returncode=1
    fi
    return $returncode
}
#================MAIN STARTS HERE===============================================


    retryCount=3
    sleep_time=10
    max_time_in_hrs=2
    NODE='PROD'

    klist -s
    if [ $? -eq 0 ]; then
        echo "Creating Kerberos ticket!"
        kerberos_tckt_creation
        check_exit $?
    else
        kerberos_tckt_expiry_chk
        check_exit $?
    fi
  exit_with_return_code 0


