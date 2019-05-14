#!/bin/bash
mkdir -p bin

export SOURCE_FILES="cmp/seismicunix.cpp cmp/semblance.cpp "
COMPILER="g++" # Change this to your favorite compiler
#export SPITS_INCLUDE = "-Icmp/include/"
ALLFLAGS="-I./spitz-include/ccpp/"
SFLAGS="-DSPITZ_SERIAL_DEBUG" # Flags for serial build
RFLAGS="-fPIC -shared" # Flags for building a shared object

#CMP
mkdir -p bin/cmp
echo Building cmp as module...
$COMPILER $ALLFLAGS -o ./bin/cmp/cmp-module ./cmp/main.cpp $SOURCE_FILES $RFLAGS || exit 1
echo Building cmp as serial...
$COMPILER $ALLFLAGS -o ./bin/cmp/cmp-serial ./cmp/main.cpp $SOURCE_FILES $SFLAGS || exit 1

# Getting Started
#mkdir -p bin/getting-started
#echo Building getting-started as module...
#$COMPILER $ALLFLAGS -o ./bin/getting-started/getting-started-module ./examples/getting-started/main.cpp $RFLAGS || exit 1
#echo Building getting-started as serial...
#$COMPILER $ALLFLAGS -o ./bin/getting-started/getting-started-serial ./examples/getting-started/main.cpp $SFLAGS || exit 1

# spitz-pi
#mkdir -p bin/spitz-pi
#echo Building first pi example as module...
#$COMPILER $ALLFLAGS -o ./bin/spitz-pi/spitz-pi-module ./examples/spitz-pi/main.cpp $RFLAGS || exit 1
#echo Building first pi example as serial...
#$COMPILER $ALLFLAGS -o ./bin/spitz-pi/spitz-pi-serial ./examples/spitz-pi/main.cpp $SFLAGS || exit 1

# spitz-pi-2
#mkdir -p bin/spitz-pi-2
#echo Building second pi example as module...
#$COMPILER $ALLFLAGS -o ./bin/spitz-pi-2/spitz-pi-2-module ./examples/spitz-pi-2/main.cpp $RFLAGS || exit 1
#echo Building second pi example as serial...
#$COMPILER $ALLFLAGS -o ./bin/spitz-pi-2/spitz-pi-2-serial ./examples/spitz-pi-2/main.cpp $SFLAGS || exit 1
