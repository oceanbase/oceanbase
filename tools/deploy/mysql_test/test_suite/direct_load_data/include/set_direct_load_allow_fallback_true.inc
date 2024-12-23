alter system set direct_load_allow_fallback=True;
let $__parameter_name__=direct_load_allow_fallback;
let $__parameter_value__=True;
--source mysql_test/test_suite/direct_load_data/include/check_parameter.inc
