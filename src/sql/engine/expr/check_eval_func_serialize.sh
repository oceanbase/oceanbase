#!/bin/bash

# print the expr eval function not add in ob_expr_eval_functions.cpp

for f in $(grep -l '\<cg_expr\>' *.cpp) ; do
	funcs=$(grep 'eval_func_\s*=\s*' $f | awk -F = '{print $2}' | tr -d '&;')
	for func in $funcs ; do 
		if ! fgrep -q "$func" <(grep -v 'extern int' ob_expr_eval_functions.cpp) ; then
			if [[ $func == *\(* ]] || [[ $func == *[* ]] ; then
				# lookup from table or get from function
				# echo $func
				:
			else
				echo $f $func
			fi

		fi
	done
done
