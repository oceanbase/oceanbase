#package_name: dbms_profiler
#author: heyongyi.hyy

CREATE OR REPLACE PACKAGE DBMS_PROFILER
AUTHID CURRENT_USER
AS
  success constant binary_integer := 0;

  error_param constant binary_integer := 1;

  error_io constant binary_integer := 2;

  error_version constant binary_integer := -1;

  version_mismatch exception;
  profiler_error exception;

  major_version constant binary_integer := 2;
  minor_version constant binary_integer := 0;


  function start_profiler(run_comment IN varchar2 := sysdate,
                          run_comment1 IN varchar2 := '',
                          run_number  OUT binary_integer)
    return binary_integer;
  procedure  start_profiler(run_comment IN varchar2 := sysdate,
                            run_comment1 IN varchar2 := '',
                            run_number  OUT binary_integer);

  function start_profiler(run_comment IN varchar2 := sysdate,
                          run_comment1 IN varchar2 := '')
    return binary_integer;
  procedure  start_profiler(run_comment IN varchar2 := sysdate,
                            run_comment1 IN varchar2 := '');


  function stop_profiler return binary_integer;
  procedure stop_profiler;


  function pause_profiler return binary_integer;
  procedure pause_profiler;


  function resume_profiler return binary_integer;
  procedure resume_profiler;


  function flush_data return binary_integer;
  procedure flush_data;


  procedure get_version(major out binary_integer,
                        minor out binary_integer);


  function internal_version_check return binary_integer;


  procedure rollup_unit(run_number IN number, unit IN number);


  procedure rollup_run(run_number IN number);

  --
  -- Create sequence and tables that the profiler needs
  -- if force_create is true, then drop and recreate the tables
  -- otherwise only create objects do not exist
  --
  procedure ob_init_objects(force_create IN boolean := FALSE);

  --
  -- Drop sequence and tables that the profiler may have created
  --
  procedure ob_drop_objects;

END;



//
