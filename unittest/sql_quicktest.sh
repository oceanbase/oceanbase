#!/bin/bash
CASES="sql/parser/test_parser \
       test_resolver \
       sql/resolver/test_raw_expr_ \
       sql/rewrite/test_ \
       test_optimizer \
       test_join_order \
       test_code_generator \
       sql/engine/expr/ob_expr_ \
       sql/engine/dml/test_ \
       sql/engine/px/test_gi_pump \
       sql/engine/aggregate/test_ \
       sql/engine/set/test_merge_ \
       sql/engine/subquery/test_subplan_filter \
       sql/engine/test_phy_operator \
       sql/engine/test_exec_context \
       sql/plan_cache/test_plan_ \
       sql/plan_cache/test_sql_id_mgr \
       sql/plan_cache/test_parse_node \
       sql/plan_cache/test_id_manager_allocator \
       sql/common/test_ob_sql_utils"
RET=0
for C in $CASES
do
    ./run_tests.sh -q $* $C
    TMP_RET=$?
    if [ $TMP_RET -ne 0 ]
    then
	RET=$TMP_RET
    fi
done

if [ $RET -eq 0 ]
then
    echo -e "\nALL PASSED\n"
else
    echo -e "\nSOME CASE FAILED\n"
fi

echo "See 'unittest/run_tests.log' for the details."

exit $RET
