/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_CONFIG_H_
#define OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_CONFIG_H_

/*
 * Whitelist of system package subprocedures that can be dispatched by the sys tenant
 * ENTRY FORMAT
 *    Each V(arg1, arg2) macro takes two arguments:
 *    - arg1: name of the system package (e.g. DBMS_SCHEDULER, DBMS_STATS).
 *    - arg2: name of the subprocedure within the package (e.g. SET_PARAM).
 * WILDCARD SUPPORT
 *    The * symbol as the second argument indicates that all subprocedures within the specified
 *    package are allowed.
 * EXAMPLES
 *    V(DBMS_SCHEDULER, *): allows all subprocedures in the DBMS_SCHEDULER package.
 *    V(DBMS_STATS, SET_PARAM): allows the set_param subprocedure in the DBMS_STATS package.
 */
#define SYS_DISPATCH_CALL_WHITELIST(V)      \
  V(DBMS_SCHEDULER, *)                      \
  V(DBMS_RESOURCE_MANAGER, *)               \
  V(DBMS_PARTITION, *)                      \
  V(DBMS_BALANCE, *)                        \
  V(DBMS_STATS, RESET_PARAM_DEFAULTS)       \
  V(DBMS_STATS, SET_PARAM)                  \
  V(DBMS_STATS, RESET_GLOBAL_PREF_DEFAULTS) \
  V(DBMS_STATS, SET_GLOBAL_PREFS)           \
  V(DBMS_STATS, SET_SCHEMA_PREFS)           \
  V(DBMS_STATS, SET_TABLE_PREFS)            \
  V(DBMS_STATS, DELETE_SCHEMA_PREFS)        \
  V(DBMS_STATS, DELETE_TABLE_PREFS)         \
  V(DBMS_UDR, *)

#endif  // OCEANBASE_SRC_SQL_RESOLVER_CMD_OB_SYS_DISPATCH_CALL_CONFIG_H_