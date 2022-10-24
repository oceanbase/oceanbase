#!/bin/sh

make -j 4
rm -f test_storage.log
./test_storage
