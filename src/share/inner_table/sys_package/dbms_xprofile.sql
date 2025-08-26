#package_name : dbms_xprofile
#author : zhouhaiyu.zhy

CREATE OR REPLACE PACKAGE dbms_xprofile AUTHID CURRENT_USER AS

    type dbms_xprofile_type_table is table of varchar2(4000);

    function display_profile(trace_id    varchar2 default null,         -- default value: last trace_id
                             tenant_id   integer default 0,             -- default value: current tenant
                             format      varchar2 default 'AGGREGATED', -- default display profile as aggregated format
                             level       integer default 1,             -- 0:CRITICAL, 1:STANDARD, 2:AD_HOC
                             svr_ip      varchar2 default null,         -- default behavior: get profile in all servers
                             svr_port    integer default 0,             -- default behavior: get profile in all servers
                             op_id       integer default null           -- default behavior: get profile of all operators
                            )
    return dbms_xprofile_type_table;

END dbms_xprofile;
