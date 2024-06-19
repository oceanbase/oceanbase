-- package_name: utl_recomp
-- author: webber.wb

create or replace package body utl_recomp AS
  num_threads number;
  num_batch number;
  current_batch number := 0;

  sorted_table varchar2(50) := 'SYS.UTL_RECOMP_SORTED';
  compiled_table varchar2(50) := 'SYS.UTL_RECOMP_COMPILED';
  errors_table varchar2(50) := 'SYS.UTL_RECOMP_ERRORS';
  skip_list_table varchar2(50) := 'SYS.UTL_RECOMP_SKIP_LIST';

  sorted_table_ddl varchar2(500) := 'create table sys.utl_recomp_sorted (
                                      obj# number not null enable,
                                      owner varchar2(128),
                                      objname varchar2(128),
                                      edition_name  varchar2(128),
                                      namespace number,
                                      depth number,
                                      batch# number)';
  idx_sorted_table_ddl varchar2(500) := 'create index sys.idx_utl_recomp_sorted_1 on sys.utl_recomp_sorted(obj#,depth)';
  compiled_table_ddl varchar2(500) := 'create table sys.utl_recomp_compiled (
                                        obj# number not null enable,
                                        batch# number,
                                        compiled_at timestamp (6),
                                        completed_at timestamp (6),
                                        compiled_by varchar2(64))';
  idx_compiled_table_ddl varchar2(500) := 'create index sys.idx_utl_recomp_compiled_1 on sys.utl_recomp_compiled(obj#)';
  errors_table_ddl varchar2(500) := 'create table sys.utl_recomp_errors(
                                      obj# number,
                                      error_at timestamp (6),
                                      compile_err varchar2(4000))';
  idx_errors_table_ddl varchar2(500) := 'create index sys.idx_errors_table_ddl_1 on sys.utl_recomp_errors(obj#)';
  skip_list_table_ddl varchar2(500) := 'create table sys.utl_recomp_skip_list
                                          (OBJ# NUMBER NOT NULL ENABLE)';


  table_not_exist exception;
  pragma exception_init(table_not_exist, -5019);

  valid_routine_name exception;
  pragma exception_init(valid_routine_name, -5980);

  TYPE str_array is table of varchar2(200);

  TYPE OBJECT_INFO IS RECORD (object_id number,
                              owner varchar2(128),
                              object_name varchar2(128),
                              object_type varchar2(23));
  TYPE OBJECT_INFO_ARRAY IS TABLE OF OBJECT_INFO;

  procedure check_privilege as
  begin
    if user != 'SYS' then
      raise valid_routine_name;
    end if;
  end;

  procedure dynamic_execute(sql_str varchar2) as
  begin
    execute immediate sql_str;
  end;

  procedure check_table_exist(table_name varchar2, is_exist out bool) as
    select_str varchar2(200);
  begin
    is_exist := false;
    select_str := 'select count(1) from ' || table_name || ' where 0 = 1';
    execute immediate select_str;
    is_exist := true;
    exception
      when table_not_exist then
        is_exist := false;
      when others then
        raise;
  end;

  procedure prepare_table(table_name varchar2, ddl varchar2, flags PLS_INTEGER) as
    is_exist boolean := false;
  begin
    check_table_exist(table_name, is_exist);
    if (is_exist) then
      if (flags = USE_EXIST_TABLE) then
        null;
      elsif (flags = DROP_EXIST_TABLE) then
        dynamic_execute('drop table ' || table_name);
        dynamic_execute(ddl);
        if table_name = sorted_table then
          dynamic_execute(idx_sorted_table_ddl);
        elsif table_name = compiled_table then
          dynamic_execute(idx_compiled_table_ddl);
        elsif table_name = errors_table then
          dynamic_execute(idx_errors_table_ddl);
        end if;
      else
        RAISE_APPLICATION_ERROR(-20001,
          'table ' || table_name ||' exists, parameter FLAGS must be set to 512 to continue using the table, or set to 1024 to automatically drop and recreate the table');
      end if;
    else
      dynamic_execute(ddl);
      if table_name = sorted_table then
        dynamic_execute(idx_sorted_table_ddl);
      elsif table_name = compiled_table then
        dynamic_execute(idx_compiled_table_ddl);
      elsif table_name = errors_table then
        dynamic_execute(idx_errors_table_ddl);
      end if;
    end if;
  end;


  PROCEDURE truncate_utl_recomp_skip_list(flags PLS_INTEGER := 0) as
    is_exist boolean := false;
  begin
    check_privilege();
    check_table_exist(skip_list_table, is_exist);
    if is_exist then
      if (flags = USE_EXIST_TABLE) then
        dynamic_execute('truncate table sys.utl_recomp_skip_list');
      elsif (flags = DROP_EXIST_TABLE) then
        dynamic_execute('drop table sys.utl_recomp_skip_list');
        dynamic_execute(skip_list_table_ddl);
      else
        RAISE_APPLICATION_ERROR(-20001,
          'table UTL_RECOMP_SKIP_LIST exists, parameter FLAGS must be set to 512 to continue using the table, or set to 1024 to automatically drop and recreate the table');
      end if;
    else
      dynamic_execute(skip_list_table_ddl);
    end if;
  end;

  PROCEDURE populate_utl_recomp_skip_list(flags PLS_INTEGER := 0) as
    dml_str varchar2(500);
  begin
    check_privilege();
    prepare_table(skip_list_table, skip_list_table_ddl, flags);
    prepare_table(compiled_table, compiled_table_ddl, flags);
    dml_str := 'insert into sys.utl_recomp_skip_list select object_id from sys.all_objects o
                where o.status != ''VALID''
                      and o.object_id not in
                        (select OBJ# from sys.utl_recomp_compiled
                        union all select OBJ# from sys.utl_recomp_skip_list)
                      and o.object_type in (''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TYPE'', ''TRIGGER'')';

    dynamic_execute(dml_str);
  end;


  procedure parallel_slave(flags pls_integer) as
    TYPE compile_info IS RECORD (object_id number,
                                owner varchar2(128),
                                object_name varchar2(128),
                                object_type varchar2(23),
                                compile_time timestamp,
                                complete_time timestamp,
                                compile_by varchar2(64));
    type compile_info_array is table of compile_info;
    compile_arr compile_info_array;
    batch_no int := flags;
    error_msg varchar2(4000);
  begin
    check_privilege();
    execute immediate 'select o.object_id, o.owner, o.object_name, o.object_type, null, null, null
                        from sys.all_objects o
                        join sys.utl_recomp_sorted s on o.object_id = s.obj# and s.batch# = :1
                        where o.object_type in (''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TYPE'', ''TRIGGER'')'
            bulk collect into compile_arr using batch_no;
    for i in 1 .. compile_arr.count() loop
      begin
        compile_arr(i).compile_time := systimestamp;
        sys.dbms_utility.VALIDATE(compile_arr(i).object_id);
        exception when others then
          error_msg := sys.dbms_utility.format_error_stack();
          execute immediate 'insert into sys.utl_recomp_errors values (:1, :2, :3)'
            using compile_arr(i).object_id, systimestamp, error_msg;
      end;
      compile_arr(i).complete_time := systimestamp;
      execute immediate 'insert into sys.utl_recomp_compiled values (:1, :2, :3, :4, :5)'
        using compile_arr(i).object_id, batch_no, compile_arr(i).compile_time,
              compile_arr(i).complete_time, batch_no;
      commit;
    end loop;
    exception when others then
      error_msg := sys.dbms_utility.format_error_stack();
      execute immediate 'insert into sys.utl_recomp_errors values (:1, :2, :3)'
            using 0, systimestamp, error_msg;
      commit;
  end;


  procedure drop_jobs is
    job_names str_array;
  begin
    select job_name bulk collect into job_names from SYS.DBA_SCHEDULER_JOBS where job_action like 'SYS.UTL_RECOMP.PARALLEL_SLAVE(%';
    for i in 1 .. job_names.count() loop
      begin
        dbms_scheduler.drop_job(job_names(i), true);
      exception when others then
        null;
      end;
    end loop;
  end;

  -- 清理utl_recomp_compiled表和utl_recomp_errors表,清理系统中已有的job
  procedure init(schema varchar2) is
  begin
    if (schema is null) then
      dynamic_execute('truncate table sys.utl_recomp_compiled');
      dynamic_execute('truncate table sys.utl_recomp_errors');
    else
      execute immediate 'delete from sys.utl_recomp_compiled where obj# in (select object_id from sys.all_objects where owner
                        = :1 and object_type in (''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TYPE'', ''TRIGGER''))'
              using schema;
      execute immediate 'delete from sys.utl_recomp_errors where obj# in (select object_id from sys.all_objects where owner
                        = :1 and object_type in (''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TYPE'', ''TRIGGER''))'
              using schema;
    end if;
    dynamic_execute('truncate table sys.utl_recomp_sorted');
    drop_jobs();
  end;

  PROCEDURE COMPUTE_NUM_THREADS(threads PLS_INTEGER) AS
  begin
    num_threads := threads;
    if (num_threads is null or num_threads <= 0) then
      select (select nvl(min(TO_NUMBER(value)), 1) from  SYS.GV$OB_PARAMETERS where name = 'cpu_count')
            * (select nvl(min(TO_NUMBER(value)), 1) from SYS.GV$OB_PARAMETERS where name = 'cpu_quota_concurrency') num
      into num_threads from dual;
      if (num_threads is null or num_threads <= 0) then
        num_threads := 1;
      end if;
    end if;
  end;

  procedure collect_and_sort_object(schema varchar2) as
    type object_group is record (obj_cnt int, depth int);
    type object_groups is table of object_group;
    obj_gs object_groups;
    insert_str varchar(2000) :=
      'insert into sys.utl_recomp_sorted (obj#, owner, objname, edition_name, namespace)
        select o.object_id, o.owner, o.object_name, o.edition_name, o.namespace
          from sys.all_objects o
          where o.object_id not in
              (select c.OBJ# from sys.utl_recomp_compiled c
                union all
                select l.OBJ# from sys.utl_recomp_skip_list l)
            and o.object_type in (''PACKAGE'', ''PROCEDURE'', ''FUNCTION'', ''TYPE'', ''TRIGGER'')
            and o.object_id > 500000 ';

    update_depth_str varchar2(2000) :=
        'update sys.utl_recomp_sorted o set depth = :1
            where depth is null and
              not exists (select s.obj# from (select * from sys.ALL_VIRTUAL_TENANT_DEPENDENCY_REAL_AGENT where dep_obj_id != ref_obj_id) d, sys.utl_recomp_sorted s
                          where d.dep_obj_id = o.obj#
                            and d.ref_obj_id = s.obj#
                            and s.depth is null)';
    current_depth int := 0;
    min_batch_size int := 10;
    max_batch_size int := 50;
    batch_size int := 0;
  begin
    if (schema is null) then
      execute immediate insert_str;
    else
      execute immediate insert_str || 'and owner = :1' using schema;
    end if;
    commit;

    loop
      execute immediate update_depth_str using current_depth;
      current_depth := current_depth + 1;
      exit when SQL%ROWCOUNT = 0;
      commit;
    end loop;
    execute immediate 'update sys.utl_recomp_sorted set depth = :1 where depth is null' using current_depth;
    commit;

    execute immediate 'select count(obj#), depth from sys.utl_recomp_sorted group by depth order by depth'
        bulk collect into obj_gs;
    num_batch := 0;
    for i in 1 .. obj_gs.count() loop
      batch_size := least(max_batch_size, obj_gs(i).obj_cnt/num_threads);
      batch_size := greatest(min_batch_size, batch_size);
      execute immediate 'update sys.utl_recomp_sorted set batch# = floor((rownum-1)/(:1)) + :2
                          where depth = :3 and batch# is null'
              using batch_size, num_batch, obj_gs(i).depth;
      num_batch := num_batch + ceil(SQL%ROWCOUNT/batch_size);
      commit;
    end loop;
  end;


  procedure create_jobs as
  begin
    for i in 0 .. num_threads-1 loop
      dbms_scheduler.create_job(JOB_NAME => 'UTL_RECOMP_SLAVE_' || current_batch,
                                JOB_TYPE => 'PLSQL_BLOCK',
                                JOB_ACTION => 'SYS.UTL_RECOMP.PARALLEL_SLAVE(' || current_batch || ')',
                                START_DATE => systimestamp,
                                END_DATE => systimestamp + 1,
                                ENABLED => true,
                                AUTO_DROP => false,
                                MAX_RUN_DURATION => 7200);
      current_batch := current_batch + 1;
      exit when current_batch > num_batch;
    end loop;
  end;


  procedure wait_jobs as
    remain_job_cnt int;
  begin
    loop
      select count(*) into remain_job_cnt
        from sys.ALL_VIRTUAL_TENANT_SCHEDULER_JOB_REAL_AGENT
        where LAST_DATE is NULL and job > 0 and JOB_NAME LIKE 'UTL_RECOMP_SLAVE_%';
      exit when remain_job_cnt = 0;
      DBMS_LOCK.SLEEP(3);
    end loop;
  end;


  procedure recomp_parallel(threads PLS_INTEGER := NULL, schema varchar2 := NULL, flags PLS_INTEGER := 0) as
  begin
    current_batch := 0;
    check_privilege();
    prepare_table(skip_list_table, skip_list_table_ddl, flags);
    prepare_table(sorted_table, sorted_table_ddl, flags);
    prepare_table(compiled_table, compiled_table_ddl, flags);
    prepare_table(errors_table, errors_table_ddl, flags);

    init(schema);
    COMPUTE_NUM_THREADS(threads);
    collect_and_sort_object(schema);

    for i in 1 .. ceil(num_batch/num_threads) loop
      create_jobs();
      wait_jobs();
      drop_jobs();
    end loop;
  end;


  procedure recomp_serial(schema varchar2 := null, flags pls_integer := 0) is
  begin
    recomp_parallel(1, schema, flags);
  end;
end;
//
