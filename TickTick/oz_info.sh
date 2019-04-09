#!/bin/ksh

oozie job -oozie http://oozie-prod.<<hostname>>.<<domain>>.com:<<port>>/oozie -info $1;
