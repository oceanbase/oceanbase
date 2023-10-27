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

#ifndef _OB_OCEANBAE_SCHEMA_SCHEMA_SERVICE_H
#define _OB_OCEANBAE_SCHEMA_SCHEMA_SERVICE_H

#include "lib/ob_name_def.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "common/object/ob_object.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "share/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_routine_info.h"

namespace oceanbase
{
namespace common
{
class ObScanHelper;
class ObMySQLTransaction;
class ObMySQLProxy;
class ObDbLinkProxy;
class ObISQLClient;
class ObCommonConfig;
}
namespace share
{
namespace schema
{
class ObUDF;
class ObRoutineInfo;
class ObPackageInfo;
class ObTriggerInfo;
class ObUDTTypeInfo;

enum ObSchemaOperationCategory
{
  OB_DDL_TABLE_OPERATION = 0,
  OB_DDL_TENANT_OPERATION,
  OB_DDL_DATABASE_OPERATION,
};

#define OP_TYPE_DEF(ACT)                                         \
  ACT(OB_INVALID_DDL_OP, = 0)                                    \
  ACT(OB_DDL_TABLE_OPERATION_BEGIN, = 1)                         \
  ACT(OB_DDL_DROP_TABLE, = 2)                                    \
  ACT(OB_DDL_ALTER_TABLE, = 3)                                   \
  ACT(OB_DDL_CREATE_TABLE, = 4)                                  \
  ACT(OB_DDL_ADD_COLUMN, = 5)                                    \
  ACT(OB_DDL_DROP_COLUMN, = 6)                                   \
  ACT(OB_DDL_CHANGE_COLUMN, = 7)                                 \
  ACT(OB_DDL_MODIFY_COLUMN, = 8)                                 \
  ACT(OB_DDL_ALTER_COLUMN, = 9)                                  \
  ACT(OB_DDL_MODIFY_META_TABLE_ID, = 10)                         \
  ACT(OB_DDL_KILL_INDEX,)                                        \
  ACT(OB_DDL_STOP_INDEX_WRITE,)                                  \
  ACT(OB_DDL_MODIFY_INDEX_STATUS,)                               \
  ACT(OB_DDL_MODIFY_TABLE_SCHEMA_VERSION,)                       \
  ACT(OB_DDL_MODIFY_TABLE_OPTION, = 15)                          \
  ACT(OB_DDL_TABLE_RENAME,)                                      \
  ACT(OB_DDL_MODIFY_DATA_TABLE_INDEX,)                           \
  ACT(OB_DDL_DROP_INDEX,)                                        \
  ACT(OB_DDL_DROP_VIEW,)                                         \
  ACT(OB_DDL_CREATE_INDEX, = 20)                                 \
  ACT(OB_DDL_CREATE_VIEW,)                                       \
  ACT(OB_DDL_ALTER_TABLEGROUP_ADD_TABLE,)                        \
  ACT(OB_DDL_TRUNCATE_TABLE_CREATE,)                             \
  ACT(OB_DDL_TRUNCATE_TABLE_DROP,)                               \
  ACT(OB_DDL_DROP_TABLE_TO_RECYCLEBIN, = 25)                     \
  ACT(OB_DDL_DROP_VIEW_TO_RECYCLEBIN,)                           \
  ACT(OB_DDL_FLASHBACK_TABLE,)                                   \
  ACT(OB_DDL_FLASHBACK_VIEW,)                                    \
  ACT(OB_DDL_ADD_PARTITION,)                                     \
  ACT(OB_DDL_DROP_PARTITION, = 30)                               \
  ACT(OB_DDL_TRUNCATE_DROP_TABLE_TO_RECYCLEBIN,)                 \
  ACT(OB_DDL_RENAME_INDEX,)                                      \
  ACT(OB_DDL_DROP_INDEX_TO_RECYCLEBIN,)                          \
  ACT(OB_DDL_FLASHBACK_INDEX,)                                   \
  ACT(OB_DDL_PARTITIONED_TABLE, = 35)                            \
  ACT(OB_DDL_FINISH_SPLIT,)                                      \
  ACT(OB_DDL_ADD_CONSTRAINT,)                                    \
  ACT(OB_DDL_DROP_CONSTRAINT,)                                   \
  ACT(OB_DDL_TRUNCATE_PARTITION, = 39)                           \
  ACT(OB_DDL_CREATE_GLOBAL_INDEX, = 40)                          \
  ACT(OB_DDL_ALTER_GLOBAL_INDEX, )                               \
  ACT(OB_DDL_RENAME_GLOBAL_INDEX,)                               \
  ACT(OB_DDL_DROP_GLOBAL_INDEX,)                                 \
  ACT(OB_DDL_MODIFY_GLOBAL_INDEX_STATUS,)                        \
  ACT(OB_DDL_FINISH_LOGICAL_SPLIT, = 45)                         \
  ACT(OB_DDL_SPLIT_PARTITION, = 46)                              \
  ACT(OB_DDL_STANDBY_REPLAY_CREATE_TABLE, = 47)                  \
  ACT(OB_DDL_DELAY_DELETE_TABLE, = 48)                           \
  ACT(OB_DDL_DELAY_DELETE_TABLE_PARTITION, = 49)                 \
  ACT(OB_DDL_UPDATE_TABLE_SCHEMA_VERSION, = 50)                  \
  ACT(OB_DDL_ADD_SUBPARTITION, = 51)                             \
  ACT(OB_DDL_DROP_SUBPARTITION, = 52)                            \
  ACT(OB_DDL_ALTER_INDEX_PARALLEL, = 53)                         \
  ACT(OB_DDL_ADD_SUB_PARTITION, = 54)                            \
  ACT(OB_DDL_DROP_SUB_PARTITION, = 55)                           \
  ACT(OB_DDL_TRUNCATE_SUB_PARTITION, = 56)                       \
  ACT(OB_DDL_SET_INTERVAL, = 57)                                 \
  ACT(OB_DDL_INTERVAL_TO_RANGE, = 58)                            \
  ACT(OB_DDL_TRUNCATE_TABLE, = 59)                               \
  ACT(OB_DDL_RENAME_PARTITION, = 60)                             \
  ACT(OB_DDL_RENAME_SUB_PARTITION, = 61)                         \
  ACT(OB_DDL_TABLE_OPERATION_END, = 100)                         \
  ACT(OB_DDL_TENANT_OPERATION_BEGIN, = 101)                      \
  ACT(OB_DDL_ADD_TENANT,)                                        \
  ACT(OB_DDL_ALTER_TENANT,)                                      \
  ACT(OB_DDL_DEL_TENANT,)                                        \
  ACT(OB_DDL_DEL_TENANT_START,)                                  \
  ACT(OB_DDL_DEL_TENANT_END,)                                    \
  ACT(OB_DDL_ADD_TENANT_START,)                                  \
  ACT(OB_DDL_ADD_TENANT_END,)                                    \
  ACT(OB_DDL_RENAME_TENANT,)                                     \
  ACT(OB_DDL_DROP_TENANT_TO_RECYCLEBIN,)                         \
  ACT(OB_DDL_FLASHBACK_TENANT,)                                  \
  ACT(OB_DDL_TENANT_OPERATION_END, = 200)                        \
  ACT(OB_DDL_DATABASE_OPERATION_BEGIN, = 201)                    \
  ACT(OB_DDL_ADD_DATABASE,)                                      \
  ACT(OB_DDL_ALTER_DATABASE,)                                    \
  ACT(OB_DDL_DEL_DATABASE,)                                      \
  ACT(OB_DDL_RENAME_DATABASE,)                                   \
  ACT(OB_DDL_DROP_DATABASE_TO_RECYCLEBIN,)                       \
  ACT(OB_DDL_FLASHBACK_DATABASE,)                                \
  ACT(OB_DDL_DELAY_DELETE_DATABASE,)                             \
  ACT(OB_DDL_DATABASE_OPERATION_END, = 300)                      \
  ACT(OB_DDL_TABLEGROUP_OPERATION_BEGIN, = 301)                  \
  ACT(OB_DDL_ADD_TABLEGROUP,)                                    \
  ACT(OB_DDL_DEL_TABLEGROUP,)                                    \
  ACT(OB_DDL_RENAME_TABLEGROUP,)                                 \
  ACT(OB_DDL_ALTER_TABLEGROUP,)                                  \
  ACT(OB_DDL_ALTER_TABLEGROUP_PARTITION,)                        \
  ACT(OB_DDL_FINISH_SPLIT_TABLEGROUP,)                           \
  ACT(OB_DDL_FINISH_LOGICAL_SPLIT_TABLEGROUP, = 308)             \
  ACT(OB_DDL_SPLIT_TABLEGROUP_PARTITION, = 309)                  \
  ACT(OB_DDL_PARTITIONED_TABLEGROUP_TABLE, = 310)                \
  ACT(OB_DDL_DELAY_DELETE_TABLEGROUP, = 311)                     \
  ACT(OB_DDL_DELAY_DELETE_TABLEGROUP_PARTITION, = 312)           \
  ACT(OB_DDL_TABLEGROUP_OPERATION_END, = 400)                    \
  ACT(OB_DDL_USER_OPERATION_BEGIN, = 401)                        \
  ACT(OB_DDL_CREATE_USER,)                                       \
  ACT(OB_DDL_DROP_USER,)                                         \
  ACT(OB_DDL_RENAME_USER,)                                       \
  ACT(OB_DDL_LOCK_USER,)                                         \
  ACT(OB_DDL_SET_PASSWD,)                                        \
  ACT(OB_DDL_GRANT_REVOKE_USER,)                                 \
  ACT(OB_DDL_ALTER_USER_REQUIRE,)                                \
  ACT(OB_DDL_MODIFY_USER_SCHEMA_VERSION,)                        \
  ACT(OB_DDL_ALTER_USER_PROFILE,)                                \
  ACT(OB_DDL_ALTER_USER,)                                        \
  ACT(OB_DDL_ALTER_ROLE,)                                        \
  ACT(OB_DDL_USER_OPERATION_END, = 500)                          \
  ACT(OB_DDL_DB_PRIV_OPERATION_BEGIN, = 501)                     \
  ACT(OB_DDL_GRANT_REVOKE_DB,)                                   \
  ACT(OB_DDL_DEL_DB_PRIV,)                                       \
  ACT(OB_DDL_DB_PRIV_OPERATION_END, = 600)                       \
  ACT(OB_DDL_TABLE_PRIV_OPERATION_BEGIN, = 601)                  \
  ACT(OB_DDL_GRANT_REVOKE_TABLE,)                                \
  ACT(OB_DDL_DEL_TABLE_PRIV,)                                    \
  ACT(OB_DDL_TABLE_PRIV_OPERATION_END, = 700)                    \
  ACT(OB_DDL_OUTLINE_OPERATION_BEGIN, = 701)                     \
  ACT(OB_DDL_CREATE_OUTLINE,)                                    \
  ACT(OB_DDL_REPLACE_OUTLINE,)                                   \
  ACT(OB_DDL_DROP_OUTLINE,)                                      \
  ACT(OB_DDL_ALTER_OUTLINE,)                                     \
  ACT(OB_DDL_OUTLINE_OPERATION_END, = 800)                       \
  ACT(OB_DDL_ZONE_OPERATION_BEGIN, = 801)                        \
  ACT(OB_DDL_ALTER_ZONE,)                                        \
  ACT(OB_DDL_ADD_ZONE,)                                          \
  ACT(OB_DDL_DELETE_ZONE,)                                       \
  ACT(OB_DDL_START_ZONE,)                                        \
  ACT(OB_DDL_STOP_ZONE,)                                         \
  ACT(OB_DDL_ZONE_OPERATION_END, = 900)                          \
  ACT(OB_DDL_SYNONYM_OPERATION_BEGIN, = 901)                     \
  ACT(OB_DDL_CREATE_SYNONYM,)                                    \
  ACT(OB_DDL_DROP_SYNONYM,)                                      \
  ACT(OB_DDL_REPLACE_SYNONYM,)                                   \
  ACT(OB_DDL_SYNONYM_OPERATION_END, = 1000)                      \
  ACT(OB_DDL_PLAN_BASELINE_OPERATION_BEGIN, = 1001)              \
  ACT(OB_DDL_CREATE_PLAN_BASELINE,)                              \
  ACT(OB_DDL_REPLACE_PLAN_BASELINE,)                             \
  ACT(OB_DDL_DROP_PLAN_BASELINE,)                                \
  ACT(OB_DDL_ALTER_PLAN_BASELINE,)                               \
  ACT(OB_DDL_PLAN_BASELINE_OPERATION_END, = 1100)                \
  ACT(OB_DDL_ROUTINE_OPERATION_BEGIN, = 1201)                    \
  ACT(OB_DDL_CREATE_ROUTINE,)                                    \
  ACT(OB_DDL_DROP_ROUTINE,)                                      \
  ACT(OB_DDL_ALTER_ROUTINE,)                                     \
  ACT(OB_DDL_REPLACE_ROUTINE,)                                   \
  ACT(OB_DDL_ROUTINE_OPERATION_END, = 1250)                      \
  ACT(OB_DDL_PACKAGE_OPERATION_BEGIN, = 1251)                    \
  ACT(OB_DDL_CREATE_PACKAGE,)                                    \
  ACT(OB_DDL_ALTER_PACKAGE,)                                     \
  ACT(OB_DDL_DROP_PACKAGE,)                                      \
  ACT(OB_DDL_PACKAGE_OPERATION_END, = 1300)                      \
  ACT(OB_DDL_UDF_OPERATION_BEGIN, = 1301)                        \
  ACT(OB_DDL_CREATE_UDF,)                                        \
  ACT(OB_DDL_DROP_UDF,)                                          \
  ACT(OB_DDL_UDF_OPERATION_END, = 1310)                          \
  ACT(OB_DDL_SEQUENCE_OPERATION_BEGIN, = 1311)                   \
  ACT(OB_DDL_CREATE_SEQUENCE,)                                   \
  ACT(OB_DDL_ALTER_SEQUENCE,)                                    \
  ACT(OB_DDL_DROP_SEQUENCE,)                                     \
  ACT(OB_DDL_SEQUENCE_OPERATION_END, = 1320)                     \
  ACT(OB_DDL_UDT_OPERATION_BEGIN, = 1321)                        \
  ACT(OB_DDL_CREATE_UDT,)                                        \
  ACT(OB_DDL_REPLACE_UDT,)                                       \
  ACT(OB_DDL_DROP_UDT,)                                          \
  ACT(OB_DDL_DROP_UDT_BODY,)                                     \
  ACT(OB_DDL_UDT_OPERATION_END, = 1330)                          \
  ACT(OB_DDL_AUDIT_OPERATION_BEGIN, = 1331)                      \
  ACT(OB_DDL_ADD_AUDIT,)                                         \
  ACT(OB_DDL_UPDATE_AUDIT,)                                      \
  ACT(OB_DDL_DEL_AUDIT,)                                         \
  ACT(OB_DDL_AUDIT_OPERATION_END, = 1340)                        \
  ACT(OB_DDL_SYS_VAR_OPERATION_BEGIN, = 1401)                    \
  ACT(OB_DDL_ALTER_SYS_VAR,)                                     \
  ACT(OB_DDL_SYS_VAR_OPERATION_END, = 1500)                      \
  ACT(OB_DDL_ONLY_SIGNAL_OPERATION_BEGIN, = 1501)                \
  ACT(OB_DDL_NOT_SYNC_OPERATION,)                                \
  ACT(OB_DDL_END_SIGN,)                                          \
  ACT(OB_DDL_SWITCHOVER_TO_LEADER_SUCC,)                         \
  ACT(OB_DDL_START_SWTICHOVER_TO_FOLLOWER,)                      \
  ACT(OB_DDL_FINISH_SCHEMA_SPLIT,)                               \
  ACT(OB_DDL_REFRESH_SCHEMA_VERSION,)                            \
  ACT(OB_DDL_FINISH_BOOTSTRAP,)                                  \
  ACT(OB_DDL_FINISH_SCHEMA_SPLIT_V2,)                            \
  ACT(OB_DDL_FINISH_PHYSICAL_RESTORE_MODIFY_SCHEMA,)             \
  ACT(OB_DDL_ONLY_SIGNAL_OPERATION_END, = 1600)                  \
  ACT(OB_DDL_STANDBY_FINISH_REPLAY_SCHEMA_SNAPSHOT, = 1601)      \
  ACT(OB_DDL_KEYSTORE_OPERATION_BEGIN, = 1650)                   \
  ACT(OB_DDL_CREATE_KEYSTORE,)                                   \
  ACT(OB_DDL_ALTER_KEYSTORE,)                                    \
  ACT(OB_DDL_KEYSTORE_OPERATION_END, = 1700)                     \
  ACT(OB_DDL_LABEL_SE_POLICY_OPERATION_BEGIN, = 1701)            \
  ACT(OB_DDL_CREATE_LABEL_SE_POLICY,)                            \
  ACT(OB_DDL_ALTER_LABEL_SE_POLICY,)                             \
  ACT(OB_DDL_DROP_LABEL_SE_POLICY,)                              \
  ACT(OB_DDL_LABEL_SE_POLICY_OPERATION_END, = 1750)              \
  ACT(OB_DDL_LABEL_SE_COMPONENT_OPERATION_BEGIN, = 1751)         \
  ACT(OB_DDL_CREATE_LABEL_SE_LEVEL,)                             \
  ACT(OB_DDL_ALTER_LABEL_SE_LEVEL,)                              \
  ACT(OB_DDL_DROP_LABEL_SE_LEVEL,)                               \
  ACT(OB_DDL_CREATE_LABEL_SE_COMPARTMENT,)                       \
  ACT(OB_DDL_ALTER_LABEL_SE_COMPARTMENT,)                        \
  ACT(OB_DDL_DROP_LABEL_SE_COMPARTMENT,)                         \
  ACT(OB_DDL_CREATE_LABEL_SE_GROUP,)                             \
  ACT(OB_DDL_ALTER_LABEL_SE_GROUP,)                              \
  ACT(OB_DDL_DROP_LABEL_SE_GROUP,)                               \
  ACT(OB_DDL_LABEL_SE_COMPONENT_OPERATION_END, = 1800)           \
  ACT(OB_DDL_LABEL_SE_LABEL_OPERATION_BEGIN, = 1801)             \
  ACT(OB_DDL_CREATE_LABEL_SE_LABEL,)                             \
  ACT(OB_DDL_ALTER_LABEL_SE_LABEL,)                              \
  ACT(OB_DDL_DROP_LABEL_SE_LABEL,)                               \
  ACT(OB_DDL_LABEL_SE_LABEL_OPERATION_END, = 1850)               \
  ACT(OB_DDL_LABEL_SE_USER_LABEL_OPERATION_BEGIN, = 1851)        \
  ACT(OB_DDL_CREATE_LABEL_SE_USER_LEVELS,)                       \
  ACT(OB_DDL_ALTER_LABEL_SE_USER_LEVELS,)                        \
  ACT(OB_DDL_DROP_LABEL_SE_USER_LEVELS,)                         \
  ACT(OB_DDL_LABEL_SE_USER_LABEL_OPERATION_END, = 1900)          \
  ACT(OB_DDL_TABLESPACE_OPERATION_BEGIN, = 1901)                 \
  ACT(OB_DDL_CREATE_TABLESPACE,)                                 \
  ACT(OB_DDL_ALTER_TABLESPACE,)                                  \
  ACT(OB_DDL_DROP_TABLESPACE,)                                   \
  ACT(OB_DDL_TABLESPACE_OPERATION_END, = 1950)                   \
  ACT(OB_DDL_TRIGGER_OPERATION_BEGIN, = 1951)                    \
  ACT(OB_DDL_CREATE_TRIGGER,)                                    \
  ACT(OB_DDL_ALTER_TRIGGER,)                                     \
  ACT(OB_DDL_DROP_TRIGGER,)                                      \
  ACT(OB_DDL_DROP_TRIGGER_TO_RECYCLEBIN,)                        \
  ACT(OB_DDL_FLASHBACK_TRIGGER,)                                 \
  ACT(OB_DDL_TRIGGER_OPERATION_END, = 1960)                      \
  ACT(OB_DDL_PROFILE_OPERATION_BEGIN, = 1961)                    \
  ACT(OB_DDL_CREATE_PROFILE,)                                    \
  ACT(OB_DDL_ALTER_PROFILE,)                                     \
  ACT(OB_DDL_DROP_PROFILE,)                                      \
  ACT(OB_DDL_PROFILE_OPERATION_END, = 1970)                      \
  ACT(OB_DDL_SYS_PRIV_OPERATION_BEGIN, = 1971)                   \
  ACT(OB_DDL_SYS_PRIV_GRANT_REVOKE,)                             \
  ACT(OB_DDL_SYS_PRIV_DELETE,)                                   \
  ACT(OB_DDL_SYS_PRIV_OPERATION_END, = 1980)                     \
  ACT(OB_DDL_OBJ_PRIV_OPERATION_BEGIN, = 1981)                   \
  ACT(OB_DDL_OBJ_PRIV_GRANT_REVOKE,)                             \
  ACT(OB_DDL_OBJ_PRIV_DELETE,)                                   \
  ACT(OB_DDL_OBJ_PRIV_OPERATION_END, = 1990)                     \
  ACT(OB_DDL_DBLINK_OPERATION_BEGIN, = 1991)                     \
  ACT(OB_DDL_CREATE_DBLINK,)                                     \
  ACT(OB_DDL_DROP_DBLINK,)                                       \
  ACT(OB_DDL_DBLINK_OPERATION_END, = 2000)                       \
  ACT(OB_DDL_DIRECTORY_OPERATION_BEGIN, = 2001)                  \
  ACT(OB_DDL_CREATE_DIRECTORY,)                                  \
  ACT(OB_DDL_ALTER_DIRECTORY,)                                   \
  ACT(OB_DDL_DROP_DIRECTORY,)                                    \
  ACT(OB_DDL_DIRECTORY_OPERATION_END, = 2010)                    \
  ACT(OB_DDL_CONTEXT_OPERATION_BEGIN, = 2011)                    \
  ACT(OB_DDL_CREATE_CONTEXT,)                                    \
  ACT(OB_DDL_ALTER_CONTEXT,)                                     \
  ACT(OB_DDL_DROP_CONTEXT,)                                      \
  ACT(OB_DDL_CONTEXT_OPERATION_END, =2020)                       \
  ACT(OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_BEGIN, = 2021)       \
  ACT(OB_DDL_CREATE_MOCK_FK_PARENT_TABLE, = 2022)                \
  ACT(OB_DDL_ALTER_MOCK_FK_PARENT_TABLE, = 2023)                 \
  ACT(OB_DDL_DROP_MOCK_FK_PARENT_TABLE, = 2024)                  \
  ACT(OB_DDL_MOCK_FK_PARENT_TABLE_OPERATION_END, =2030)          \
  ACT(OB_DDL_RLS_POLICY_OPERATION_BEGIN, = 2031)                 \
  ACT(OB_DDL_CREATE_RLS_POLICY, = 2032)                          \
  ACT(OB_DDL_DROP_RLS_POLICY, = 2033)                            \
  ACT(OB_DDL_ALTER_RLS_POLICY, = 2034)                           \
  ACT(OB_DDL_RLS_POLICY_ADD_ATTRIBUTE, = 2035)                   \
  ACT(OB_DDL_RLS_POLICY_DROP_ATTRIBUTE, = 2036)                  \
  ACT(OB_DDL_RLS_POLICY_OPERATION_END, = 2040)                   \
  ACT(OB_DDL_RLS_GROUP_OPERATION_BEGIN, = 2041)                  \
  ACT(OB_DDL_CREATE_RLS_GROUP, = 2042)                           \
  ACT(OB_DDL_DROP_RLS_GROUP, = 2043)                             \
  ACT(OB_DDL_RLS_GROUP_OPERATION_END, = 2050)                    \
  ACT(OB_DDL_RLS_CONTEXT_OPERATION_BEGIN, = 2051)                \
  ACT(OB_DDL_CREATE_RLS_CONTEXT, = 2052)                         \
  ACT(OB_DDL_DROP_RLS_CONTEXT, = 2053)                           \
  ACT(OB_DDL_RLS_CONTEXT_OPERATION_END, = 2060)                  \
  ACT(OB_DDL_ROUTINE_PRIV_OPERATION_BEGIN, = 2061)               \
  ACT(OB_DDL_GRANT_REVOKE_ROUTINE_PRIV, = 2062)                  \
  ACT(OB_DDL_DEL_ROUTINE_PRIV, = 2063)                           \
  ACT(OB_DDL_ROUTINE_PRIV_OPERATION_END, = 2070)                 \
  ACT(OB_DDL_MAX_OP,)

DECLARE_ENUM(ObSchemaOperationType, op_type, OP_TYPE_DEF);

#define IS_DDL_TYPE(DDL_TYPE, DDL) \
  __attribute__((unused)) static bool is_##DDL##_operation(const ObSchemaOperationType ddl_type) { \
    return ddl_type > OB_DDL_##DDL_TYPE##_OPERATION_BEGIN && ddl_type < OB_DDL_##DDL_TYPE##_OPERATION_END;\
  };


IS_DDL_TYPE(TABLE, table)
IS_DDL_TYPE(TABLEGROUP, tablegroup)
IS_DDL_TYPE(TENANT, tenant)
IS_DDL_TYPE(DATABASE, database)
IS_DDL_TYPE(USER, user)
IS_DDL_TYPE(DB_PRIV, db_priv)
IS_DDL_TYPE(TABLE_PRIV, table_priv)
IS_DDL_TYPE(OUTLINE, outline)
IS_DDL_TYPE(ZONE, zone)
IS_DDL_TYPE(SYNONYM, synonym)
IS_DDL_TYPE(ROUTINE, routine)
IS_DDL_TYPE(PACKAGE, package)
IS_DDL_TYPE(UDF, udf)
IS_DDL_TYPE(UDT, udt)
IS_DDL_TYPE(SEQUENCE, sequence)
IS_DDL_TYPE(SYS_VAR, sys_var)
IS_DDL_TYPE(ONLY_SIGNAL, only_signal)
IS_DDL_TYPE(AUDIT, audit)
IS_DDL_TYPE(KEYSTORE, keystore)
IS_DDL_TYPE(LABEL_SE_POLICY, label_se_policy)
IS_DDL_TYPE(LABEL_SE_COMPONENT, label_se_component)
IS_DDL_TYPE(LABEL_SE_LABEL, label_se_label)
IS_DDL_TYPE(LABEL_SE_USER_LABEL, label_se_user_label)
IS_DDL_TYPE(TABLESPACE, tablespace)
IS_DDL_TYPE(TRIGGER, trigger)
IS_DDL_TYPE(PROFILE, profile)
IS_DDL_TYPE(SYS_PRIV, sys_priv)
IS_DDL_TYPE(OBJ_PRIV, obj_priv)
IS_DDL_TYPE(DBLINK, dblink)
IS_DDL_TYPE(DIRECTORY, directory)
IS_DDL_TYPE(CONTEXT, context)
IS_DDL_TYPE(MOCK_FK_PARENT_TABLE, mock_fk_parent_table)
IS_DDL_TYPE(RLS_POLICY, rls_policy)
IS_DDL_TYPE(RLS_GROUP, rls_group)
IS_DDL_TYPE(RLS_CONTEXT, rls_context)

struct ObSchemaOperation
{
  OB_UNIS_VERSION_V(1);
public:
  ObSchemaOperation();
  virtual ~ObSchemaOperation() = default;
  int64_t  schema_version_;
  uint64_t tenant_id_;
  union {
    uint64_t user_id_;
    uint64_t grantee_id_;
  };
  union {
    uint64_t database_id_;
    uint64_t grantor_id_;
  };
  common::ObString database_name_;
  uint64_t tablegroup_id_;
  union {
    uint64_t table_id_;
    uint64_t outline_id_;
    uint64_t synonym_id_;
    uint64_t routine_id_;
    uint64_t package_id_;
    uint64_t udt_id_;
    uint64_t sequence_id_;
    uint64_t keystore_id_;
    uint64_t label_se_policy_id_;
    uint64_t label_se_component_id_;
    uint64_t label_se_label_id_;
    uint64_t label_se_user_level_id_;
    uint64_t tablespace_id_;
    uint64_t trigger_id_;
    uint64_t profile_id_;
    uint64_t audit_id_;
    uint64_t dblink_id_;
    uint64_t directory_id_;
    uint64_t context_id_;
    uint64_t mock_fk_parent_table_id_;
    uint64_t rls_policy_id_;
    uint64_t rls_group_id_;
    uint64_t rls_context_id_;
  };
  union {
    common::ObString table_name_;
    common::ObString udf_name_;
    common::ObString sequence_name_;
    common::ObString keystore_name_;
    common::ObString tablespace_name_;
    common::ObString context_name_;
    common::ObString mock_fk_parent_table_name_;
  };
  ObSchemaOperationType op_type_;
  common::ObString ddl_stmt_str_;

  bool operator <(const ObSchemaOperation &rv) const { return schema_version_ < rv.schema_version_; }
  void reset();
  bool is_valid() const;
  // Shallow copy
  void set_col_id(uint64_t col_id) { tablegroup_id_ = col_id;}
  void set_obj_id(uint64_t obj_id) { table_id_ = obj_id; }
  void set_grantee_id(uint64_t grantee_id) { user_id_ = grantee_id; }
  void set_grantor_id(uint64_t grantor_id) { database_id_ = grantor_id; }
  uint64_t get_col_id() const { return tablegroup_id_; }
  uint64_t get_obj_id() const { return table_id_; }
  uint64_t get_grantee_id() const { return user_id_; }
  uint64_t get_grantor_id() const { return database_id_; }
  uint64_t get_obj_type() const;

  ObSchemaOperation &operator=(const ObSchemaOperation &other);
  static common::ObString type_semantic_str(ObSchemaOperationType op_type);
  static const char *type_str(ObSchemaOperationType op_type);
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

enum ObClusterSchemaStatus
{
  NORMAL_STATUS = 0,
  //cluster is in bootstrap or schema replay, schema_version is not refer to timestamp
  BOOTSTRAP_STATUS = 1,
  //the status is only in standby cluster, after registe success, it needs schema snapshot access.
  //the schema version is also not refer to timestamp as bootstrap status
  SCHEMA_REPLAY_STATUS = 2,
};

struct AlterColumnSchema : public ObColumnSchemaV2
{
  OB_UNIS_VERSION_V(1);
public:
  AlterColumnSchema()
    : ObColumnSchemaV2(),
      alter_type_(OB_INVALID_DDL_OP),
      origin_column_name_(),
      is_primary_key_(false),
      is_autoincrement_(false),
      is_unique_key_(false),
      is_drop_default_(false),
      is_set_nullable_(false),
      is_set_default_(false),
      check_timestamp_column_order_(false),
      is_no_zero_date_(false),
      next_column_name_(),
      prev_column_name_(),
      is_first_(false)
  {}

  explicit AlterColumnSchema(common::ObIAllocator *allocator)
    : ObColumnSchemaV2(allocator),
      alter_type_(OB_INVALID_DDL_OP),
      origin_column_name_(),
      is_primary_key_(false),
      is_autoincrement_(false),
      is_unique_key_(false),
      is_drop_default_(false),
      is_set_nullable_(false),
      is_set_default_(false),
      check_timestamp_column_order_(false),
      is_no_zero_date_(false),
      next_column_name_(),
      prev_column_name_(),
      is_first_(false)
  {}
  AlterColumnSchema &operator=(const AlterColumnSchema &alter_column_schema);
  int assign(const ObColumnSchemaV2 &other);
  const common::ObString& get_origin_column_name() const { return origin_column_name_;};
  int set_origin_column_name(const common::ObString& origin_column_name)
    { return deep_copy_str(origin_column_name, origin_column_name_); }
  const common::ObString& get_next_column_name() const { return next_column_name_;};
  int set_next_column_name(const common::ObString& next_column_name)
    { return deep_copy_str(next_column_name, next_column_name_); }
  const common::ObString& get_prev_column_name() const { return prev_column_name_;};
  int set_prev_column_name(const common::ObString& prev_column_name)
    { return deep_copy_str(prev_column_name, prev_column_name_); }

  void reset();

  ObSchemaOperationType alter_type_;
  common::ObString origin_column_name_;
  bool is_primary_key_;
  bool is_autoincrement_;
  bool is_unique_key_;
  bool is_drop_default_;
  bool is_set_nullable_;
  bool is_set_default_;
  bool check_timestamp_column_order_;
  bool is_no_zero_date_;
  common::ObString next_column_name_;
  common::ObString prev_column_name_;
  bool is_first_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct AlterTableSchema : public ObTableSchema
{
  OB_UNIS_VERSION_V(1);
public:
  AlterTableSchema()
    : ObTableSchema(),
      alter_type_(OB_INVALID_DDL_OP),
      origin_table_name_(),
      new_database_name_(),
      origin_database_name_(),
      origin_tablegroup_id_(common::OB_INVALID_ID),
      alter_option_bitset_(),
      sql_mode_(SMO_DEFAULT),
      split_partition_name_(),
      split_high_bound_val_(),
      split_list_row_values_(),
      new_part_name_()
  {
  }
  AlterTableSchema(common::ObIAllocator *allocator)
    : ObTableSchema(allocator),
      alter_type_(OB_INVALID_DDL_OP),
      origin_table_name_(),
      new_database_name_(),
      origin_database_name_(),
      origin_tablegroup_id_(common::OB_INVALID_ID),
      alter_option_bitset_(),
      sql_mode_(SMO_DEFAULT),
      split_partition_name_(),
      split_high_bound_val_(),
      split_list_row_values_(),
      new_part_name_()
  {
  }
  inline const common::ObString &get_origin_table_name() const { return origin_table_name_; }
  inline int set_origin_table_name(const common::ObString &origin_table_name);
  inline const common::ObString &get_database_name() const { return new_database_name_; }
  inline int set_database_name(const common::ObString &db_name);
  inline const common::ObString &get_origin_database_name() const { return origin_database_name_;}
  inline int set_origin_database_name(const common::ObString &origin_db_name);
  inline void set_origin_tablegroup_id(const uint64_t origin_tablegroup_id);
  inline void set_sql_mode(ObSQLMode sql_mode) { sql_mode_ = sql_mode; }
  inline ObSQLMode get_sql_mode() const { return sql_mode_; }
  inline int set_split_partition_name(const common::ObString &partition_name);
  inline const common::ObString &get_split_partition_name() const { return split_partition_name_; }
  inline int set_split_high_bound_value(const common::ObRowkey &high_value);
  inline const common::ObRowkey &get_split_high_bound_value() const { return split_high_bound_val_; }
  inline const common::ObRowkey& get_split_list_row_values() const {
    return split_list_row_values_;
  }
  inline const common::ObString &get_new_part_name() const { return new_part_name_; }
  inline int set_new_part_name(const common::ObString &new_part_name);
  int assign_subpartiton_key_info(const common::ObPartitionKeyInfo& src_info);

  int add_alter_column(const AlterColumnSchema &column, const bool need_allocate);
  void reset();

  ObSchemaOperationType alter_type_;
  //original table name
  common::ObString origin_table_name_;
  common::ObString new_database_name_;
  common::ObString origin_database_name_;
  uint64_t origin_tablegroup_id_;
  common::ObBitSet<> alter_option_bitset_;
  ObSQLMode sql_mode_;
  // Record the split source partition_name;
  // If it is a sub-table operation, partition_name is empty;
  // if it is a hash partition repartition, partition_name is empty;
  common::ObString split_partition_name_;
  // for tablegroup
  common::ObRowkey split_high_bound_val_;
  common::ObRowkey split_list_row_values_;
  common::ObString new_part_name_;
  int assign(const ObTableSchema &src_schema);
  //virtual int add_partition(const ObPartition &part);
  //virtual int add_subpartition(const ObSubPartition &sub_part);
  //virtual int alloc_partition(const ObPartition *&partition);
  //virtual int alloc_partition(const ObSubPartition *&subpartition);
  int deserialize_columns(const char *buf, const int64_t data_len, int64_t &pos);

  DECLARE_VIRTUAL_TO_STRING;
};

int AlterTableSchema::set_split_partition_name(const common::ObString &partition_name)
{
  return deep_copy_str(partition_name, split_partition_name_);
}

int AlterTableSchema::set_new_part_name(const common::ObString &new_part_name)
{
  return deep_copy_str(new_part_name, new_part_name_);
}

int AlterTableSchema::set_origin_table_name(const common::ObString &origin_table_name)
{
  return deep_copy_str(origin_table_name, origin_table_name_);
}

int AlterTableSchema::set_database_name(const common::ObString &db_name)
{
  return deep_copy_str(db_name, new_database_name_);
}

int AlterTableSchema::set_origin_database_name(const common::ObString &origin_db_name)
{
  return deep_copy_str(origin_db_name, origin_database_name_);
}

void AlterTableSchema::set_origin_tablegroup_id(const uint64_t origin_tablegroup_id)
{
  origin_tablegroup_id_ = origin_tablegroup_id;
}

int AlterTableSchema::set_split_high_bound_value(const common::ObRowkey &high_value)
{
  return high_value.deep_copy(split_high_bound_val_, *get_allocator());
}

// new cache
struct SchemaKey;
class ObSimpleTenantSchema;
class ObSimpleUserSchema;
class ObSimpleDatabaseSchema;
class ObSimpleTablegroupSchema;
class ObSimpleTableSchemaV2;
class ObSimpleOutlineSchema;
class ObSimpleRoutineSchema;
class ObSimpleSynonymSchema;
class ObSimplePlanBaselineSchema;
class ObSimplePackageSchema;
class ObSimpleTriggerSchema;
class ObSimpleUDFSchema;
class ObSimpleUDTSchema;
class ObSimpleSysVariableSchema;
class ObLabelSePolicySchema;
class ObLabelSeComponentSchema;
class ObLabelSeLabelSchema;
class ObLabelSeUserLevelSchema;
class ObProfileSchema;
class ObDbLinkSchema;
class ObSimpleLinkTableSchema;
class ObDirectorySchema;
class ObSimpleMockFKParentTableSchema;
class ObRlsPolicySchema;
class ObRlsGroupSchema;
class ObRlsContextSchema;

class ObTenantSqlService;
class ObDatabaseSqlService;
class ObTableSqlService;
class ObTablegroupSqlService;
class ObUserSqlService;
class ObPrivSqlService;
class ObOutlineSqlService;
class ObRoutineSqlService;
class ObSynonymSqlService;
class ObPackageSqlService;
class ObTriggerSqlService;
struct VersionHisKey;
struct VersionHisVal;
class ObUDFSqlService;
class ObUDTSqlService;
class ObSequenceSqlService;
class ObSysVariableSqlService;
class ObAuditSqlService;
class ObKeystoreSqlService;
class ObLabelSePolicySqlService;
class ObTablespaceSqlService;
class ObProfileSqlService;
class ObErrorSqlService;
class ObDbLinkSqlService;
class ObDirectorySqlService;
class ObRlsSqlService;
//table schema service interface layer
class ObServerSchemaService;
class ObContextSqlService;
class ObSchemaService
{
public:
  //default false, only use for liboblog to control compatable
  static bool g_ignore_column_retrieve_error_;
  static bool g_liboblog_mode_;
  typedef common::ObArrayImpl<ObSchemaOperation>  ObSchemaOperationSet;
  class SchemaOperationSetWithAlloc: public ObSchemaOperationSet
  {
  public:
    SchemaOperationSetWithAlloc() : string_buf_(common::ObModIds::OB_SCHEMA_OPERATOR_SET_WITH_ALLOC) { }
    virtual ~SchemaOperationSetWithAlloc() { }
    virtual void reset() {
      ObSchemaOperationSet::reset();
      string_buf_.reset();
    }
    int write_string(const common::ObString &str, common::ObString *stroed_str)
    { return string_buf_.write_string(str, stroed_str);}
    virtual void *alloc(const int64_t sz) { return string_buf_.alloc(sz); }
    virtual void free(void *ptr) { string_buf_.free(ptr); ptr = NULL; }

  private:
    common::ObStringBuf string_buf_;//alloc varchar
  };
  virtual ~ObSchemaService() {}
  virtual int init(common::ObMySQLProxy *sql_proxy,
                   common::ObDbLinkProxy *dblink_proxy,
                   const share::schema::ObServerSchemaService *schema_service) = 0;
  virtual void set_common_config(const common::ObCommonConfig *config) = 0;

#define DECLARE_GET_DDL_SQL_SERVICE_FUNC(SCHEMA_TYPE, SCHEMA) \
  virtual Ob##SCHEMA_TYPE##SqlService &get_##SCHEMA##_sql_service() = 0;

  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Tenant, tenant);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Database, database);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Table, table);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Tablegroup, tablegroup);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(User, user);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Priv, priv);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Outline, outline);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Routine, routine);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Trigger, trigger);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Synonym, synonym);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(UDF, udf);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(UDT, udt);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Sequence, sequence);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(SysVariable, sys_variable);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Keystore, keystore);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(LabelSePolicy, label_se_policy);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Tablespace, tablespace);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Profile, profile);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Audit, audit);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(DbLink, dblink);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Directory, directory);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Context, context);
  DECLARE_GET_DDL_SQL_SERVICE_FUNC(Rls, rls);
  //DECLARE_GET_DDL_SQL_SERVICE_FUNC(sys_priv, priv);


  /* sequence_id related */
  virtual int init_sequence_id(const int64_t rootservice_epoch) = 0;
  virtual int inc_sequence_id() = 0;
  virtual uint64_t get_sequence_id() = 0;

  virtual int set_refresh_schema_info(const ObRefreshSchemaInfo &schema_info) = 0;
  virtual int get_refresh_schema_info(ObRefreshSchemaInfo &schema_info) = 0;

  virtual void set_cluster_schema_status(const ObClusterSchemaStatus &schema_status) = 0;
  virtual ObClusterSchemaStatus get_cluster_schema_status() const = 0;
  //get all core table schema
  virtual int get_all_core_table_schema(ObTableSchema &table_schema) = 0;
  //get core table schemas
  virtual int get_core_table_schemas(
              common::ObISQLClient &sql_client,
              const ObRefreshSchemaStatus &schema_status,
              common::ObArray<ObTableSchema> &core_schemas) = 0;

  virtual int get_sys_table_schemas(
              common::ObISQLClient &sql_client,
              const ObRefreshSchemaStatus &schema_status,
              const common::ObIArray<uint64_t> &table_ids,
              common::ObIAllocator &allocator,
              common::ObArray<ObTableSchema *> &sys_schemas) = 0;

  //get table schema of a table id list with schema_version
  virtual int get_batch_table_schema(const ObRefreshSchemaStatus &schema_status,
                                     const int64_t schema_version,
                                     common::ObArray<uint64_t> &table_ids,
                                     common::ObISQLClient &sql_client,
                                     common::ObIAllocator &allocator,
                                     common::ObArray<ObTableSchema *> &table_schema_array) = 0;

#define GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(SCHEMA, SCHEMA_TYPE)                        \
  virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus &schema_status, \
                                    const int64_t schema_version,                             \
                                    common::ObArray<uint64_t> &SCHEMA##_ids,                 \
                                    common::ObISQLClient &sql_client,                         \
                                    common::ObIArray<SCHEMA_TYPE> &SCHEMA##_schema_array) = 0;

  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(database, ObDatabaseSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(outline, ObOutlineInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(routine, ObRoutineInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(synonym, ObSynonymInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(package, ObPackageInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(trigger, ObTriggerInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(udf, ObUDF);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(udt, ObUDTTypeInfo);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(sequence, ObSequenceSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(keystore, ObKeystoreSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_policy, ObLabelSePolicySchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_component, ObLabelSeComponentSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_label, ObLabelSeLabelSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_user_level, ObLabelSeUserLevelSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(tablespace, ObTablespaceSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(profile, ObProfileSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(audit, ObSAuditSchema);
  GET_BATCH_FULL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(mock_fk_parent_table, ObMockFKParentTableSchema);

  virtual int get_batch_users(const ObRefreshSchemaStatus &schema_status,
                              const int64_t schema_version,
                              common::ObArray<uint64_t> &user_ids,
                              common::ObISQLClient &sql_client,
                              common::ObArray<ObUserInfo> &user_array) = 0;

  virtual int get_batch_tenants(common::ObISQLClient &client,
                                const int64_t schema_version,
                                common::ObArray<uint64_t> &tenant_ids,
                                common::ObIArray<ObTenantSchema> &schema_array) = 0;

  virtual int get_tablegroup_schema(const ObRefreshSchemaStatus &schema_status,
                                    const uint64_t tablegroup_id,
                                    const int64_t schema_version,
                                    common::ObISQLClient &sql_client,
                                    common::ObIAllocator &allocator,
                                    ObTablegroupSchema *&tablegroup_schema) = 0;

  virtual int get_sys_variable_schema(common::ObISQLClient &sql_client,
                                      const ObRefreshSchemaStatus &schema_status,
                                      const uint64_t tenant_id,
                                      const int64_t schema_version,
                                      share::schema::ObSysVariableSchema &sys_variable_schema) = 0;

  virtual int get_all_tenants(common::ObISQLClient &sql_client,
                              const int64_t schema_version,
                               common::ObIArray<ObSimpleTenantSchema> &tenant_schema_array) = 0;
  virtual int get_sys_variable(common::ObISQLClient &client,
                               const ObRefreshSchemaStatus &schema_status,
                               const uint64_t tenant_id,
                               const int64_t schema_version,
                               ObSimpleSysVariableSchema &schema) = 0;
  #define GET_ALL_SCHEMA_WITH_ALLOCATOR_FUNC_DECLARE_PURE_VIRTUAL(SCHEMA, SCHEMA_TYPE)    \
    virtual int get_all_##SCHEMA##s(common::ObISQLClient &sql_client, \
                                    common::ObIAllocator &allocator,  \
                                    const ObRefreshSchemaStatus &schema_status,\
                                    const int64_t schema_version,      \
                                    const uint64_t tenant_id,           \
                                    common::ObIArray<SCHEMA_TYPE *> &schema_array) = 0;
  GET_ALL_SCHEMA_WITH_ALLOCATOR_FUNC_DECLARE_PURE_VIRTUAL(table, ObSimpleTableSchemaV2);

  #define GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(SCHEMA, SCHEMA_TYPE)    \
    virtual int get_all_##SCHEMA##s(common::ObISQLClient &sql_client, \
                                    const ObRefreshSchemaStatus &schema_status,\
                                    const int64_t schema_version,      \
                                    const uint64_t tenant_id,           \
                                    common::ObIArray<SCHEMA_TYPE> &schema_array) = 0;
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(user, ObSimpleUserSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(database, ObSimpleDatabaseSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(tablegroup, ObSimpleTablegroupSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(db_priv, ObDBPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(table_priv, ObTablePriv);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(outline, ObSimpleOutlineSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(routine, ObSimpleRoutineSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(synonym, ObSimpleSynonymSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(package, ObSimplePackageSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(trigger, ObSimpleTriggerSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(udf, ObSimpleUDFSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(udt, ObSimpleUDTSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(sequence, ObSequenceSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(keystore, ObKeystoreSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_policy, ObLabelSePolicySchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_component, ObLabelSeComponentSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_label, ObLabelSeLabelSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(label_se_user_level, ObLabelSeUserLevelSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(tablespace, ObTablespaceSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(profile, ObProfileSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(audit, ObSAuditSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(sys_priv, ObSysPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(obj_priv, ObObjPriv);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(dblink, ObDbLinkSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(directory, ObDirectorySchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(context, ObContextSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(rls_policy, ObRlsPolicySchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(rls_group, ObRlsGroupSchema);
  GET_ALL_SCHEMA_FUNC_DECLARE_PURE_VIRTUAL(rls_context, ObRlsContextSchema);

  //get tenant increment schema operation between (base_version, new_schema_version]
  virtual int get_increment_schema_operations(const ObRefreshSchemaStatus &schema_status,
                                              const int64_t base_version,
                                              const int64_t new_schema_version,
                                              common::ObISQLClient &sql_client,
                                              SchemaOperationSetWithAlloc &schema_operations) = 0;

  virtual int check_sys_schema_change(
              common::ObISQLClient &sql_client,
              const ObRefreshSchemaStatus &schema_status,
              const common::ObIArray<uint64_t> &sys_table_ids,
              const int64_t schema_version,
              const int64_t new_schema_version,
              bool &sys_schema_change) = 0;

  virtual int fetch_schema_version(const ObRefreshSchemaStatus &schema_status,
                                   common::ObISQLClient &sql_client,
                                   int64_t &schema_version) = 0;

//  virtual int insert_sys_param(const ObSysParam &sys_param,
//                               common::ObISQLClient *sql_client) = 0;

  virtual int get_core_version(common::ObISQLClient &sql_client,
                               const ObRefreshSchemaStatus &schema_status,
                               int64_t &core_schema_version) = 0;
  virtual int get_baseline_schema_version(common::ObISQLClient &sql_client,
                                          const ObRefreshSchemaStatus &schema_status,
                                          int64_t &baseline_schema_version) = 0;

  virtual int fetch_new_object_ids(
              const uint64_t tenant_id,
              const int64_t object_cnt,
              uint64_t &max_object_id) = 0;
  virtual int fetch_new_partition_ids(
              const uint64_t tenant_id,
              const int64_t partition_num,
              uint64_t &max_partition_id) = 0;
  virtual int fetch_new_tablet_ids(
              const uint64_t tenant_id,
              const bool gen_normal_tablet,
              const uint64_t size,
              uint64_t &min_tablet_id) = 0;
  virtual int fetch_new_table_id(const uint64_t tenant_id, uint64_t &new_table_id) = 0;
  virtual int fetch_new_tenant_id(uint64_t &new_tenant_id) = 0;
  virtual int fetch_new_database_id(const uint64_t tenant_id, uint64_t &new_database_id) = 0;
  virtual int fetch_new_tablegroup_id(const uint64_t tenant_id, uint64_t &new_tablegroup_id) = 0;
  virtual int fetch_new_user_id(const uint64_t tenant_id, uint64_t &new_user_id) = 0;
  virtual int fetch_new_outline_id(const uint64_t tenant_id, uint64_t &new_outline_id) = 0;
  virtual int fetch_new_synonym_id(const uint64_t tenant_id, uint64_t &new_synonym_id) = 0;
  virtual int fetch_new_udf_id(const uint64_t tenant_id, uint64_t &new_udf_id) = 0;
  virtual int fetch_new_constraint_id(const uint64_t tenant_id, uint64_t &new_constraint_id) = 0;
  virtual int fetch_new_sequence_id(const uint64_t tenant_id, uint64_t &new_sequence_id) = 0;
  virtual int fetch_new_udt_id(const uint64_t tenant_id, uint64_t &new_udt_id) = 0;
  virtual int fetch_new_routine_id(const uint64_t tenant_id, uint64_t &new_routine_id) = 0;
  virtual int fetch_new_package_id(const uint64_t tenant_id, uint64_t &new_package_id) = 0;
  virtual int fetch_new_dblink_id(const uint64_t tenant_id, uint64_t &new_dblink_id) = 0;
  virtual int fetch_new_sys_pl_object_id(const uint64_t tenant_id, uint64_t &new_object_id) = 0;

  virtual int fetch_new_keystore_id(const uint64_t tenant_id, uint64_t &keystore_id) = 0;
  virtual int fetch_new_master_key_id(const uint64_t tenant_id, uint64_t &new_master_key_id) = 0;
  virtual int fetch_new_label_se_policy_id(const uint64_t tenant_id, uint64_t &label_se_policy_id) = 0;
  virtual int fetch_new_label_se_component_id(const uint64_t tenant_id, uint64_t &label_se_component_id) = 0;
  virtual int fetch_new_label_se_label_id(const uint64_t tenant_id, uint64_t &label_se_label_id) = 0;
  virtual int fetch_new_label_se_user_level_id(const uint64_t tenant_id, uint64_t &label_se_user_level_id) = 0;
  virtual int fetch_new_tablespace_id(const uint64_t tenant_id, uint64_t &tablespace_id) = 0;
  virtual int fetch_new_trigger_id(const uint64_t tenant_id, uint64_t &new_trigger_id) = 0;
  virtual int fetch_new_profile_id(const uint64_t tenant_id, uint64_t &new_profile_id) = 0;
  virtual int fetch_new_audit_id(const uint64_t tenant_id, uint64_t &new_audit_id) = 0;
  virtual int fetch_new_directory_id(const uint64_t tenant_id, uint64_t &new_directory_id) = 0;
  virtual int fetch_new_context_id(const uint64_t tenant_id, uint64_t &new_context_id) = 0;
  virtual int fetch_new_rls_policy_id(const uint64_t tenant_id, uint64_t &new_rls_policy_id) = 0;
  virtual int fetch_new_rls_group_id(const uint64_t tenant_id, uint64_t &new_rls_group_id) = 0;
  virtual int fetch_new_rls_context_id(const uint64_t tenant_id, uint64_t &new_rls_context_id) = 0;

//------------------For managing privileges-----------------------------//
  #define GET_BATCH_SCHEMAS_WITH_ALLOCATOR_FUNC_DECLARE_PURE_VIRTUAL(SCHEMA, SCHEMA_TYPE)  \
    virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus &schema_status,\
                                      common::ObISQLClient &client,     \
                                      common::ObIAllocator &allocator,  \
                                      const int64_t schema_version,     \
                                      common::ObArray<SchemaKey> &schema_keys, \
                                      common::ObIArray<SCHEMA_TYPE *> &schema_array) = 0;
  #define GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(SCHEMA, SCHEMA_TYPE)  \
    virtual int get_batch_##SCHEMA##s(const ObRefreshSchemaStatus &schema_status,\
                                      common::ObISQLClient &client,     \
                                      const int64_t schema_version,     \
                                      common::ObArray<SchemaKey> &schema_keys, \
                                      common::ObIArray<SCHEMA_TYPE> &schema_array) = 0;
  virtual int get_batch_tenants(common::ObISQLClient &client,
                                const int64_t schema_version,
                                common::ObArray<SchemaKey> &schema_keys,
                                common::ObIArray<ObSimpleTenantSchema> &schema_array) = 0;
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(user, ObSimpleUserSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(database, ObSimpleDatabaseSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(tablegroup, ObSimpleTablegroupSchema);
  GET_BATCH_SCHEMAS_WITH_ALLOCATOR_FUNC_DECLARE_PURE_VIRTUAL(table, ObSimpleTableSchemaV2);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(db_priv, ObDBPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(table_priv, ObTablePriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(outline, ObSimpleOutlineSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(routine, ObSimpleRoutineSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(synonym, ObSimpleSynonymSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(package, ObSimplePackageSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(trigger, ObSimpleTriggerSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(udf, ObSimpleUDFSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(udt, ObSimpleUDTSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(sequence, ObSequenceSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(sys_variable, ObSimpleSysVariableSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(keystore, ObKeystoreSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(label_se_policy, ObLabelSePolicySchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(label_se_component, ObLabelSeComponentSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(label_se_label, ObLabelSeLabelSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(label_se_user_level, ObLabelSeUserLevelSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(tablespace, ObTablespaceSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(profile, ObProfileSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(audit, ObSAuditSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(sys_priv, ObSysPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(obj_priv, ObObjPriv);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(dblink, ObDbLinkSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(directory, ObDirectorySchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(context, ObContextSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(rls_policy, ObRlsPolicySchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(rls_group, ObRlsGroupSchema);
  GET_BATCH_SCHEMAS_FUNC_DECLARE_PURE_VIRTUAL(rls_context, ObRlsContextSchema);


  //--------------For manaing recyclebin -----//
  virtual int insert_recyclebin_object(
      const ObRecycleObject &recycle_obj,
      common::ObISQLClient &sql_client) = 0;
  virtual int fetch_recycle_object(
      const uint64_t tenant_id,
      const common::ObString &object_name,
      const ObRecycleObject::RecycleObjType recycle_obj_type,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObRecycleObject> &recycle_objs) = 0;
  virtual int delete_recycle_object(
      const uint64_t tenant_id,
      const ObRecycleObject &recycle_object,
      common::ObISQLClient &sql_client) = 0;
  virtual int fetch_expire_recycle_objects(
      const uint64_t tenant_id,
      const int64_t expire_time,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObRecycleObject> &recycle_objs) = 0;
  virtual int fetch_recycle_objects_of_db(
      const uint64_t tenant_id,
      const uint64_t database_id,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObRecycleObject> &recycle_objs) = 0;

  // for backup
  virtual int construct_recycle_table_object(
      common::ObISQLClient &sql_client,
      const ObSimpleTableSchemaV2 &table,
      ObRecycleObject &recycle_object) = 0;

  virtual int construct_recycle_database_object(
      common::ObISQLClient &sql_client,
      const ObDatabaseSchema &database,
      ObRecycleObject &recycle_object) = 0;

  virtual int fetch_aux_tables(const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t schema_version,
      common::ObISQLClient &sql_client,
      common::ObIArray<ObAuxTableMetaInfo> &aux_tables) = 0;
  virtual int construct_schema_version_history(
      const ObRefreshSchemaStatus &schema_status,
      common::ObISQLClient &sql_client,
      const int64_t snapshot_version,
      const VersionHisKey &version_his_key,
      VersionHisVal &version_his_val) = 0;
  //-------------------------- for new schema_cache ------------------------------
  virtual int get_table_schema(const ObRefreshSchemaStatus &schema_status,
                               const uint64_t table_id,
                               const int64_t schema_version,
                               common::ObISQLClient &sql_client,
                               common::ObIAllocator &allocator,
                               ObTableSchema *&table_schema) = 0;
  //get table schema of a single table with latest version
  virtual int get_table_schema_from_inner_table(const ObRefreshSchemaStatus &schema_status,
                                                const uint64_t table_id,
                                                common::ObISQLClient &sql_client,
                                                ObTableSchema &table_schema) = 0;
  virtual int get_db_schema_from_inner_table(const ObRefreshSchemaStatus &schema_status,
                                             const uint64_t &database_id,
                                             ObIArray<ObDatabaseSchema> &database_schema,
                                             ObISQLClient &sql_client) = 0;
  virtual int get_full_table_schema_from_inner_table(const ObRefreshSchemaStatus &schema_status,
                                                     const int64_t &table_id,
                                                     ObTableSchema &table_schema,
                                                     ObArenaAllocator &allocator,
                                                     ObMySQLTransaction &trans) = 0;
  // get mock fk parent table schema of a single mock fk parent table
  virtual int get_mock_fk_parent_table_schema_from_inner_table(
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t table_id,
      common::ObISQLClient &sql_client,
      ObMockFKParentTableSchema &mock_fk_parent_table_schema) = 0;
  virtual int get_audits_in_owner(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const ObSAuditType audit_type,
              const uint64_t owner_id,
              common::ObIArray<ObSAuditSchema> &audit_schemas) = 0;
  virtual void set_refreshed_schema_version(const int64_t schema_version) = 0;
  virtual int gen_new_schema_version(const uint64_t tenant_id,
                                     const int64_t refreshed_schema_version,
                                     int64_t &schema_version) = 0;

  // gen schema versions in [start_version, end_version] with specified schema version cnt.
  // @param[out]:
  // - schema_version: end_version
  virtual int gen_batch_new_schema_versions(
              const uint64_t tenant_id,
              const int64_t refreshed_schema_version,
              const int64_t version_cnt,
              int64_t &schema_version) = 0;
  virtual int get_new_schema_version(uint64_t tenant_id, int64_t &schema_version) = 0;

  virtual int get_ori_schema_version(const ObRefreshSchemaStatus &schema_status,
                                     const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     int64_t &last_schema_version) = 0;

  virtual int get_table_latest_schema_versions(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &table_ids,
      common::ObIArray<ObTableLatestSchemaVersion> &table_schema_versions) = 0;

  // whether we can see the expected version or not
  // @return OB_SCHEMA_EAGAIN when not readable
  virtual int can_read_schema_version(const ObRefreshSchemaStatus &schema_status, int64_t expected_version)
  {
    UNUSED(schema_status);
    UNUSED(expected_version);
    return common::OB_SUCCESS;
  }

  virtual int get_drop_tenant_infos(
      common::ObISQLClient &sql_client,
      int64_t schema_version,
      common::ObIArray<ObDropTenantInfo> &drop_tenant_infos) = 0;

  // for liboblog used
  virtual int query_tenant_status(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      TenantStatus &tenant_status) = 0;
  virtual int get_schema_version_by_timestamp(
      common::ObISQLClient &sql_client,
      const ObRefreshSchemaStatus &schema_status,
      const uint64_t tenant_id,
      int64_t timestamp,
      int64_t &schema_version) = 0;
  virtual int get_first_trans_end_schema_version(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      int64_t &schema_version) = 0;

  // link table.
  /**
   * ObMultiVersionSchemaService::get_link_table_schema() need adjust
   * schema_version and link_schema_version, so param table_schema should not be const.
   */
  virtual int get_link_table_schema(const ObDbLinkSchema *dblink_schema,
                                    const common::ObString &database_name,
                                    const common::ObString &table_name,
                                    common::ObIAllocator &allocator,
                                    ObTableSchema *&table_schema,
                                    sql::ObSQLSessionInfo *session_info,
                                    const common::ObString &dblink_name,
                                    bool is_reverse_link,
                                    uint64_t *current_scn) = 0;

  static bool is_formal_version(const int64_t schema_version);
  static bool is_sys_temp_version(const int64_t schema_version);
  static int gen_core_temp_version(const int64_t schema_version,
                                   int64_t &core_temp_version);
  static int gen_sys_temp_version(const int64_t schema_version,
                                  int64_t &sys_temp_version);
  static int alloc_table_schema(const ObTableSchema &table, common::ObIAllocator &allocator,
                                ObTableSchema *&allocated_table_schema);

  /*----------- interfaces for latest schema start -----------*/
  virtual int get_tablegroup_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const ObString &tablegroup_name,
              uint64_t &tablegroup_id) = 0;

  virtual int get_database_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const ObString &database_name,
              uint64_t &database_id) = 0;

  virtual int get_table_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const uint64_t session_id,
              const ObString &table_name,
              uint64_t &table_id,
              ObTableType &table_type,
              int64_t &schema_version) = 0;

  virtual int get_index_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &index_name,
              uint64_t &index_id) = 0;

  virtual int get_mock_fk_parent_table_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &table_name,
              uint64_t &mock_fk_parent_table_id) = 0;

  virtual int get_synonym_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &synonym_name,
              uint64_t &synonym_id) = 0;

  virtual int get_constraint_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &constraint_name,
              uint64_t &constraint_id) = 0;

  virtual int get_foreign_key_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &foreign_key_name,
              uint64_t &foreign_key_id) = 0;

  virtual int get_sequence_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &sequence_name,
              uint64_t &sequence_id,
              bool &is_system_generated) = 0;

  virtual int get_package_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const ObString &package_name,
              const ObPackageType package_type,
              const int64_t compatible_mode,
              uint64_t &package_id) = 0;

  virtual int get_routine_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const uint64_t package_id,
              const uint64_t overload,
              const ObString &routine_name,
              common::ObIArray<std::pair<uint64_t, share::schema::ObRoutineType>> &routine_pairs) = 0;

  virtual int get_udt_id(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const uint64_t database_id,
              const uint64_t package_id,
              const ObString &udt_name,
              uint64_t &udt_id) = 0;

  virtual int get_table_schema_versions(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const common::ObIArray<uint64_t> &table_ids,
              common::ObIArray<ObSchemaIdVersion> &versions) = 0;

  virtual int get_mock_fk_parent_table_schema_versions(
              common::ObISQLClient &sql_client,
              const uint64_t tenant_id,
              const common::ObIArray<uint64_t> &table_ids,
              common::ObIArray<ObSchemaIdVersion> &versions) = 0;

  /*----------- interfaces for latest schema end -------------*/

};
}//namespace schema
}//namespace share
}//namespace oceanbase
#endif /* _OB_OCEANBAE_SCHEMA_SCHEMA_SERVICE_H */
