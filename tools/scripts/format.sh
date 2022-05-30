#!/bin/bash

CLANG_FORMAT='deps/3rd/usr/local/oceanbase/devtools/bin/clang-format'
EXCLUDE_FILES=('sql_parser_mysql_mode_lex.h' \
                'sql_parser_mysql_mode_lex.c' \
                'sql_parser_mysql_mode_tab.c' \
                'ob_signal_worker.cpp' \
                'ev.c' \
		'sql_parser_oracle_mode_tab.c' \
		'sql_parser_oracle_mode_tab.h' \
		'sql_parser_oracle_mode_lex.c' \
		'pl_parser_oracle_mode_tab.c')

function format_file()
{
    if_format=1
    for exclude_file in ${EXCLUDE_FILES[@]}
    do
        if [[ "$1" = *"$exclude_file"* ]]; then
            echo -e '\033[31mDo not format' $1 'because of configuration\033[0m\r'
            if_format=0
            break
        fi
    done
    if [[ x"$if_format" = x"1" ]]; then
        $CLANG_FORMAT -i -style=file $1
    fi
}

function format_folder()
{
 echo '--------------------------------------------------------------------------'
 echo -e '\033[K\033[32mFormat files in the folder' $1 '\033[0m\r'
 for file in `find $1 -name '*.cpp' -o -name '*.h' -o -name '*.ipp' -o -name '*.c'`  
 do
   echo -ne '\033[K\033[32mFormating' $file '\033[0m\r'
   format_file $file
 done
#echo -e '\033[K\033[32mFormat files in the folder' $1 'successfully\033[0m\r'
}

function format()
{
    if [ -z "$1" ]; then
        echo 'please input a file, folder or all'
    elif [[ x"$1" = x"all" ]]; then
        format_folder deps/easy
        format_folder deps/oblib
        format_folder src
        format_folder unittest
        echo -e '\033[K\033[32mFormat successfully\033[0m'
    elif [ -f "$1" ]; then
        if [[ $1 = *.cpp* ]] || [[ $1 = *.c* ]] || [[ $1 = *.h* ]] || [[ $1 = *.ipp* ]]; then
            echo -e '\033[K\033[32mFormating' $1 '\033[0m\r'
	    format_file $1
        else
            echo "please input a cpp/h/c/ipp file"
        fi
    elif [ -d "$1" ]; then
        format_folder $1
    else
        echo "input is illegal"
    fi
}

if [ ! -f "$CLANG_FORMAT" ]; then
   echo "The clang-format binary is not exist, maybe you can use 'sh build.sh init' to solve it"
   exit 8
fi
format $1
