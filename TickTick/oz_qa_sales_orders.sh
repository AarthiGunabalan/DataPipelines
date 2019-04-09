#!/bin/ksh

sh $HOME/qa/create_kr_tkt.sh
oozie job -oozie http://oozie-prod.<<hostname>>.<<domain>>.com:<<port>>/oozie -config $HOME/qa/job.properties -auth KERBEROS -run;
echo $? + "Success";
