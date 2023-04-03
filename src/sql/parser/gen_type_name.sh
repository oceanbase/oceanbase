#!/bin/bash
echo -e '#include "objit/common/ob_item_type.h"'
echo -e "const char* get_type_name(int type)\n{"
echo -e "\tswitch(type){"
sed -rn 's/\s*(T_[_A-Z1-9]+)[ =0-9]*,/\tcase \1 : return \"\1\";/p' $1
echo -e '\tdefault:return "Unknown";\n\t}\n}'
