#package_name:dbms_xprofile
#author:zhouhaiyu.zhy

CREATE OR REPLACE PACKAGE dbms_xprofile  AUTHID CURRENT_USER

    function display_profile(trace_id    VARCHAR(64) DEFAULT NULL,         -- DEFAULT value: last trace_id
                             tenant_id	 DECIMAL DEFAULT 0,                -- DEFAULT value: current tenant
                             format		 VARCHAR(32) DEFAULT 'AGGREGATED', -- default display profile as aggregated format
                             level       DECIMAL DEFAULT 1,                -- 0:CRITICAL, 1:STANDARD, 2:AD_HOC
                             svr_ip      VARCHAR(64) DEFAULT NULL,         -- DEFAULT behavior: get profile in all servers
                             svr_port    DECIMAL DEFAULT 0,                -- DEFAULT behavior: get profile in all servers
                             op_id       DECIMAL DEFAULT NULL              -- DEFAULT behavior: get profile of all operators
                        )
    return text;

END dbms_xprofile;
