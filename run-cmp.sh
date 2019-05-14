#!/bin/bash
mkdir -p run

rm -rf run/cmp/log
rm -rf run/cmp/nodes
rm -rf run/cmp
mkdir -p run/cmp
cd run/cmp

FILE=../../instances/simple-windowed.su
V_INI=2000.00
V_FIN=6000.00
V_INC=101
WIND=0.032 
APH=600
AZIMUTH=0.0

echo Running cmp...

CMD="../../spitz-python/spitz-run.sh ../../bin/cmp/cmp-module $FILE $V_INI $V_FIN $V_INC $WIND $APH $AZIMUTH"
echo $CMD
$CMD

