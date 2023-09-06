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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_BASE_TYPE_
#define OCEANBASE_LOGSERVICE_OB_LOG_BASE_TYPE_

#include "logservice/palf/lsn.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace logservice
{
enum ObLogBaseType
{
  INVALID_LOG_BASE_TYPE = 0,

  TRANS_SERVICE_LOG_BASE_TYPE = 1,

  TABLET_OP_LOG_BASE_TYPE = 2,

  STORAGE_SCHEMA_LOG_BASE_TYPE = 3,

  TABLET_SEQ_SYNC_LOG_BASE_TYPE = 4,

  DDL_LOG_BASE_TYPE = 5,

  KEEP_ALIVE_LOG_BASE_TYPE = 6,

  TIMESTAMP_LOG_BASE_TYPE = 7,

  TRANS_ID_LOG_BASE_TYPE = 8,

  GC_LS_LOG_BASE_TYPE = 9,

  MAJOR_FREEZE_LOG_BASE_TYPE = 10,

  //for primary_ls_service
  PRIMARY_LS_SERVICE_LOG_BASE_TYPE = 11,

  //for recovery_ls_service
  RECOVERY_LS_SERVICE_LOG_BASE_TYPE = 12,

  //for standby timestamp service
  STANDBY_TIMESTAMP_LOG_BASE_TYPE = 13,

  // for global auto increment service
  GAIS_LOG_BASE_TYPE = 14,

  // for das id service
  DAS_ID_LOG_BASE_TYPE = 15,
  //for recovery_ls_service
  RESTORE_SERVICE_LOG_BASE_TYPE = 16,

  RESERVED_SNAPSHOT_LOG_BASE_TYPE = 17,

  MEDIUM_COMPACTION_LOG_BASE_TYPE = 18,

  // for arb garbage collect service, has not been used for now
  ARB_GARBAGE_COLLECT_SERVICE_LOG_BASE_TYPE = 19,
  // for data_dictionary_service
  DATA_DICT_LOG_BASE_TYPE = 20,

  // for arbitration service
  ARBITRATION_SERVICE_LOG_BASE_TYPE = 21,

  // for NET_STANDBY_TNT_SERVICE
  NET_STANDBY_TNT_SERVICE_LOG_BASE_TYPE = 22,

  // for endpoint ingress
  NET_ENDPOINT_INGRESS_LOG_BASE_TYPE = 23,

  HEARTBEAT_SERVICE_LOG_BASE_TYPE = 24,

  // for padding log entry
  PADDING_LOG_BASE_TYPE = 25,

  // for dup table trans
  DUP_TABLE_LOG_BASE_TYPE = 26,

  // for obj lock garbage collect service
  OBJ_LOCK_GARBAGE_COLLECT_SERVICE_LOG_BASE_TYPE = 27,

  // for tenant_transfer_service
  TENANT_TRANSFER_SERVICE_LOG_BASE_TYPE = 28,

  //for tenant balance
  TENANT_BALANCE_SERVICE_LOG_BASE_TYPE = 29,
  //for tenant balance task execute

  BALANCE_EXECUTE_SERVICE_LOG_BASE_TYPE = 30,

  //for backup task scheduler service
  BACKUP_TASK_SCHEDULER_LOG_BASE_TYPE = 31,

  //for backup service
  BACKUP_DATA_SERVICE_LOG_BASE_TYPE = 32,

  //for backup task scheduler service
  BACKUP_CLEAN_SERVICE_LOG_BASE_TYPE = 33,

  //for log archive service
  BACKUP_ARCHIVE_SERVICE_LOG_BASE_TYPE = 34,

  //for transfer handler
  TRANSFER_HANDLER_LOG_BASE_TYPE = 35,

  COMMON_LS_SERVICE_LOG_BASE_TYPE = 36,

  // only use role change service, do not write clog
  LS_BLOCK_TX_SERVICE_LOG_BASE_TYPE = 37,

  // for workload repository service
  WORKLOAD_REPOSITORY_SERVICE_LOG_BASE_TYPE = 38,
  TTL_LOG_BASE_TYPE = 39,
  // pay attention!!!
  // add log type in log_base_type_to_string
  // max value
  MAX_LOG_BASE_TYPE,
};

// Define the maximum length of ObLogBaseType string
static constexpr int64_t OB_LOG_BASE_TYPE_STR_MAX_LEN = 128;
static inline
int log_base_type_to_string(const ObLogBaseType log_type,
                            char *str,
                            const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (log_type == INVALID_LOG_BASE_TYPE) {
    strncpy(str ,"INVALID_TYPE", str_len);
  } else if (log_type == TRANS_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"TRANS_SERVICE", str_len);
  } else if (log_type == TABLET_OP_LOG_BASE_TYPE) {
    strncpy(str ,"TABLET_OP", str_len);
  } else if (log_type == STORAGE_SCHEMA_LOG_BASE_TYPE) {
    strncpy(str ,"STORAGE_SCHEMA", str_len);
  } else if (log_type == TABLET_SEQ_SYNC_LOG_BASE_TYPE) {
    strncpy(str ,"TABLET_SEQ_SYNC", str_len);
  } else if (log_type == DDL_LOG_BASE_TYPE) {
    strncpy(str ,"DDL", str_len);
  } else if (log_type == KEEP_ALIVE_LOG_BASE_TYPE) {
    strncpy(str ,"KEEP_ALIVE", str_len);
  } else if (log_type == TIMESTAMP_LOG_BASE_TYPE) {
    strncpy(str ,"TIMESTAMP", str_len);
  } else if (log_type == TRANS_ID_LOG_BASE_TYPE) {
    strncpy(str ,"TRANS_ID", str_len);
  } else if (log_type == GC_LS_LOG_BASE_TYPE) {
    strncpy(str ,"GC_LS", str_len);
  } else if (log_type == MAJOR_FREEZE_LOG_BASE_TYPE) {
    strncpy(str ,"MAJOR_FREEZE", str_len);
  } else if (log_type == PRIMARY_LS_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"PRIMARY_LS_SERVICE", str_len);
  } else if (log_type == RECOVERY_LS_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"RECOVERY_LS_SERVICE", str_len);
  } else if (log_type == STANDBY_TIMESTAMP_LOG_BASE_TYPE) {
    strncpy(str ,"STANDBY_TIMESTAMP", str_len);
  } else if (log_type == GAIS_LOG_BASE_TYPE) {
    strncpy(str ,"GAIS", str_len);
  } else if (log_type == DAS_ID_LOG_BASE_TYPE) {
    strncpy(str ,"DAS_ID", str_len);
  } else if (log_type == RESTORE_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"RESTORE_SERVICE", str_len);
  } else if (log_type == RESERVED_SNAPSHOT_LOG_BASE_TYPE) {
    strncpy(str ,"RESERVED_SNAPSHOT", str_len);
  } else if (log_type == MEDIUM_COMPACTION_LOG_BASE_TYPE) {
    strncpy(str ,"MEDIUM_COMPACTION", str_len);
  } else if (log_type == ARB_GARBAGE_COLLECT_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"ARB_GARBAGE_COLLECTE_SERVICE", str_len);
  } else if (log_type == DATA_DICT_LOG_BASE_TYPE) {
    strncpy(str ,"DATA_DICTIONARY_SERVICE", str_len);
  } else if (log_type == ARBITRATION_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"ARBITRATION_SERVICE", str_len);
  } else if (log_type == NET_STANDBY_TNT_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"NET_STANDBY_TNT_SERVICE", str_len);
  } else if (log_type == NET_ENDPOINT_INGRESS_LOG_BASE_TYPE){
    strncpy(str ,"NET_ENDPOINT_EGRESS", str_len);
  } else if (log_type == HEARTBEAT_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"HEARTBEAT_SERVICE", str_len);
  } else if (log_type == PADDING_LOG_BASE_TYPE) {
    strncpy(str ,"PADDING_LOG_ENTRY", str_len);
  } else if (log_type == DUP_TABLE_LOG_BASE_TYPE) {
    strncpy(str ,"DUP_TABLE", str_len);
  } else if (log_type == OBJ_LOCK_GARBAGE_COLLECT_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"OBJ_LOCK_GARBAGE_COLLECT_SERVICE", str_len);
  } else if (log_type == TENANT_TRANSFER_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"TENANT_TRANSFER_SERVICE", str_len);
  } else if (log_type == TENANT_BALANCE_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"TENANT_BALANCE", str_len);
  } else if (log_type == BALANCE_EXECUTE_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"BALANCE_EXECUTE_SERVICE", str_len);
  } else if (log_type == BACKUP_DATA_SERVICE_LOG_BASE_TYPE) {
    strncpy(str, "BACKUP_DATA_SERVICE", str_len);
  } else if (log_type == BACKUP_CLEAN_SERVICE_LOG_BASE_TYPE) {
    strncpy(str, "BACKUP_CLEAN_SERVICE", str_len);
  } else if (log_type == BACKUP_ARCHIVE_SERVICE_LOG_BASE_TYPE) {
    strncpy(str, "BACKUP_ARCHIVE_SERVICE", str_len);
  } else if (log_type == BACKUP_TASK_SCHEDULER_LOG_BASE_TYPE) {
    strncpy(str, "BACKUP_TASK_SCHEDULER", str_len);
  } else if (log_type == TRANSFER_HANDLER_LOG_BASE_TYPE) {
    strncpy(str, "TRANSFER_HANDLER", str_len);
  } else if (log_type == COMMON_LS_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"COMMON_LS_SERVICE", str_len);
  } else if (log_type == LS_BLOCK_TX_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"BLOCK_TX_SERVICE", str_len);
  } else if (log_type == WORKLOAD_REPOSITORY_SERVICE_LOG_BASE_TYPE) {
    strncpy(str ,"WORKLOAD_REPOSITORY_SERVICE", str_len);
  } else if (log_type == TTL_LOG_BASE_TYPE) {
    strncpy(str ,"TTL_SERVICE", str_len);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

inline bool is_valid_log_base_type(const ObLogBaseType &type)
{
  return type > INVALID_LOG_BASE_TYPE && type < MAX_LOG_BASE_TYPE;
}

class ObIReplaySubHandler
{
public:
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) = 0;
};

class ObIRoleChangeSubHandler
{
public:
  virtual void switch_to_follower_forcedly() = 0;
  virtual int switch_to_leader() = 0;
  // @retval
  //   OB_SUCCESS, 角色切换执行成功
  //   OB_LS_NEED_REVOKE, 角色切换执行失败, 需要主动卸任
  //   OTHERS , 角色切换执行失败, 不需要主动卸任
  //
  // 新增OB_LS_NEED_REVOKE的原因是, switch_to_follower_gracefully, 包含两步:
  //   a. 执行leader->follower;
  //   b. 执行follower->leader.
  //
  //   a. 执行成功, 表示可以执行后续的切主工作
  //   a. 执行失败, 需要执行1.b
  //   b. 执行成功, 表示这次切主操作执行失败, 但不需要主动卸任
  //   b. 执行失败, 表示这次切主操作执行失败, 但需要主动卸任
  //
  // 经过协商, 决定采用错误码的方式区分上述各种异常.
  virtual int switch_to_follower_gracefully() = 0;
  virtual int resume_leader() = 0;
  VIRTUAL_TO_STRING_KV("ObIRoleChangeSubHandler", "Dummy");
};

// services inherit from ObICheckpointSubHandler
// register in ObCheckpointExecutor
class ObICheckpointSubHandler
{
public:
  virtual share::SCN get_rec_scn() = 0;
  virtual int flush(share::SCN &scn) = 0;
};

#define REGISTER_TO_LOGSERVICE(type, subhandler)                                            \
  if (OB_SUCC(ret)) {                                                                       \
    if (OB_FAIL(replay_handler_.register_handler(type, subhandler))) {                      \
      LOG_WARN("replay_handler_ register failed", K(ret), K(type), K(ls_meta_.ls_id_));     \
    } else if (OB_FAIL(role_change_handler_.register_handler(type, subhandler))) {          \
      LOG_WARN("role_change_handler_ register failed", K(ret), K(type), K(ls_meta_.ls_id_));\
    } else if (OB_FAIL(checkpoint_executor_.register_handler(type, subhandler))) {          \
      LOG_WARN("checkpoint_executor_ register failed", K(ret), K(type), K(ls_meta_.ls_id_));\
    } else {                                                                                \
      LOG_INFO("register to logservice success", K(type), K(ls_meta_.ls_id_));              \
    }                                                                                       \
  }

#define UNREGISTER_FROM_LOGSERVICE(type, subhandler)                                        \
  (void)replay_handler_.unregister_handler(type);                                           \
  (void)role_change_handler_.unregister_handler(type);                                      \
  (void)checkpoint_executor_.unregister_handler(type);                                      \

#define REGISTER_TO_RESTORESERVICE(type, subhandler)                                        \
  if (OB_SUCC(ret)) {                                                                       \
    if (OB_FAIL(restore_role_change_handler_.register_handler(type, subhandler))) {         \
      LOG_WARN("restore_role_change_handler_ register failed",                              \
          K(ret), K(type), K(ls_meta_.ls_id_));                                             \
    } else {                                                                                \
      LOG_INFO("register to restoreservice success", K(type), K(ls_meta_.ls_id_));          \
    }                                                                                       \
  }

#define UNREGISTER_FROM_RESTORESERVICE(type, subhandler)                                     \
  (void)restore_role_change_handler_.unregister_handler(type);
} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_BASE_TYPE_
