#package_name: dbms_plsql_code_coverage
#author: wangbai.wx

create or replace package dbms_plsql_code_coverage
  authid current_user is
  coverage_error exception ;
  pragma exception_init(coverage_error, -9825);

  function  start_coverage(run_comment IN varchar2) return number;

  procedure stop_coverage;

  procedure create_coverage_tables(force_it IN boolean := false);

end dbms_plsql_code_coverage;
//
