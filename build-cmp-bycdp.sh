#!/bin/bash
mkdir -p bin

export SOURCE_FILES="cmp-bycdp/seismicunix.cpp cmp-bycdp/semblance.cpp "
COMPILER="g++" # Change this to your favorite compiler
ALLFLAGS="-I./spitz-include/ccpp/"
SFLAGS="-DSPITZ_SERIAL_DEBUG" # Flags for serial build
RFLAGS="-fPIC -shared" # Flags for building a shared object

#CMP
mkdir -p bin/cmp-bycdp
echo Building cmp-bycdp as module...
$COMPILER $ALLFLAGS -o ./bin/cmp-bycdp/cmp-bycdp-module ./cmp-bycdp/main.cpp $SOURCE_FILES $RFLAGS || exit 1
echo Building cmp-bycdp as serial...
$COMPILER $ALLFLAGS -o ./bin/cmp-bycdp/cmp-bycdp-serial ./cmp-bycdp/main.cpp $SOURCE_FILES $SFLAGS || exit 1
