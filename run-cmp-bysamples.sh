#!/bin/bash
mkdir -p run

rm -rf run/cmp-bysamples/log
rm -rf run/cmp-bysamples/nodes
rm -rf run/cmp-bysamples
mkdir -p run/cmp-bysamples
cd run/cmp-bysamples

FILE=../../instances/simple-windowed.su
V_INI=2000.00
V_FIN=6000.00
V_INC=101
WIND=0.032 
APH=600
AZIMUTH=0.0

echo Running cmp-bysamples...

CMD="../../spitz-python/spitz-run.sh ../../bin/cmp-bysamples/cmp-bysamples-module $FILE $V_INI $V_FIN $V_INC $WIND $APH $AZIMUTH"
echo $CMD
$CMD

