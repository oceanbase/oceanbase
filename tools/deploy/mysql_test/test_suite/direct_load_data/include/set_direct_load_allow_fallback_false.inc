alter system set direct_load_allow_fallback=False;
let $__parameter_name__=direct_load_allow_fallback;
let $__parameter_value__=False;
--source mysql_test/test_suite/direct_load_data/include/check_parameter.inc
