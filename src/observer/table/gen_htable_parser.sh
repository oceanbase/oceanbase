#!/bin/bash
#
# AUTHOR: Zhifeng YANG
# DATE: 2018-07-11
# DESCRIPTION:
#
export PATH=/usr/local/bin:$PATH
# generate sql_parser
bison -v -Werror --defines=../../../src/observer/table/htable_filter_tab.hxx --output=../../../src/observer/table/htable_filter_tab.cxx ../../../src/observer/table/htable_filter_tab.yxx

if [ $? -ne 0 ]
then
    echo yacc error[$?], abort.
    exit 1
fi
flex --header-file="../../../src/observer/table/htable_filter_lex.hxx" --outfile="../../../src/observer/table/htable_filter_lex.cxx" ../../../src/observer/table/htable_filter_lex.lxx
