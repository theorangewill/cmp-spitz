#!/bin/bash
mkdir -p bin

export SOURCE_FILES="cmp-bysamples/seismicunix.cpp cmp-bysamples/semblance.cpp "
COMPILER="g++" # Change this to your favorite compiler
ALLFLAGS="-I./spitz-include/ccpp/"
SFLAGS="-DSPITZ_SERIAL_DEBUG" # Flags for serial build
RFLAGS="-fPIC -shared" # Flags for building a shared object

#CMP
mkdir -p bin/cmp-bysamples
echo Building cmp-bysamples as module...
$COMPILER $ALLFLAGS -o ./bin/cmp-bysamples/cmp-bysamples-module ./cmp-bysamples/main.cpp $SOURCE_FILES $RFLAGS || exit 1
echo Building cmp-bysamples as serial...
$COMPILER $ALLFLAGS -o ./bin/cmp-bysamples/cmp-bysamples-serial ./cmp-bysamples/main.cpp $SOURCE_FILES $SFLAGS || exit 1
