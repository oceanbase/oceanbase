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

#define USING_LOG_PREFIX SHARE
#include "ob_alter_system_stmt.h"
#include "lib/encrypt/ob_encrypted_helper.h"

using namespace oceanbase;
using namespace lib;
using namespace common;
using namespace share;
using namespace sql;

ObBackupSetEncryptionStmt::ObBackupSetEncryptionStmt()
    : ObSystemCmdStmt(stmt::T_BACKUP_SET_ENCRYPTION),
      mode_(share::ObBackupEncryptionMode::MAX_MODE),
      encrypted_passwd_()
{
  passwd_buf_[0] = '\0';
}

int ObBackupSetEncryptionStmt::set_param(const int64_t mode, const common::ObString& passwd)
{
  int ret = common::OB_SUCCESS;
  mode_ = static_cast<const share::ObBackupEncryptionMode::EncryptionMode>(mode);
  encrypted_passwd_.assign_ptr(passwd_buf_, sizeof(passwd_buf_));

  if (!share::ObBackupEncryptionMode::is_valid(mode_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(mode), K(mode_));
  } else if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(passwd, encrypted_passwd_))) {
    COMMON_LOG(WARN, "failed to encrypted passwd", K(ret), K(passwd));
  }

  return ret;
}

ObBackupSetDecryptionStmt::ObBackupSetDecryptionStmt() : ObSystemCmdStmt(stmt::T_BACKUP_SET_DECRYPTION)
{
  passwd_array_[0] = '\0';
  pos_ = 0;
}

int ObBackupSetDecryptionStmt::add_passwd(const ObString& passwd)
{
  int ret = OB_SUCCESS;
  char passwd_buf[OB_MAX_PASSWORD_LENGTH];
  ObString encrypted_passwd;
  encrypted_passwd.assign_ptr(passwd_buf, sizeof(passwd_buf));

  if (pos_ != 0) {
    if (FAILEDx(databuff_printf(passwd_array_, sizeof(passwd_array_), pos_, ","))) {
      COMMON_LOG(WARN, "failed to comma", K(ret), K_(pos), K_(passwd_array));
    }
  }

  if (FAILEDx(ObEncryptedHelper::encrypt_passwd_to_stage2(passwd, encrypted_passwd))) {
    COMMON_LOG(WARN, "failed to encrypted passwd", K(ret), K(passwd));
  } else if (OB_FAIL(databuff_printf(passwd_array_,
                 sizeof(passwd_array_),
                 pos_,
                 "%.*s",
                 encrypted_passwd.length(),
                 encrypted_passwd.ptr()))) {
    COMMON_LOG(WARN, "failed to add passwd", K(ret), K_(pos), K_(passwd_array));
  }

  COMMON_LOG(INFO, "add passwd", K(passwd), K(encrypted_passwd), K_(passwd_array));

  return ret;
}
int64_t ObBackupSetDecryptionStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(pos), K_(passwd_array));
  J_OBJ_END();
  return pos;
}
int64_t ObBackupSetEncryptionStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), "mode", share::ObBackupEncryptionMode::to_str(mode_), K_(encrypted_passwd));
  J_OBJ_END();
  return pos;
}
int64_t ObBackupManageStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(type), K_(value));
  J_OBJ_END();
  return pos;
}
int64_t ObBackupDatabaseStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(incremental));
  J_OBJ_END();
  return pos;
}
int64_t ObArchiveLogStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(enable));
  J_OBJ_END();
  return pos;
}
int64_t ObChangeTenantStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id));
  J_OBJ_END();
  return pos;
}
int64_t ObEnableSqlThrottleStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(priority), K_(rt), K_(io), K_(network), K_(cpu), K_(logical_reads),K_(queue_time));
  J_OBJ_END();
  return pos;
}
int64_t ObDropDiskStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));
  J_OBJ_END();
  return pos;
}
int64_t ObAddDiskStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));
  J_OBJ_END();
  return pos;
}
int64_t ObSetDiskValidStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(server));
  J_OBJ_END();
  return pos;
}
int64_t ObRefreshTimeZoneInfoStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id));
  J_OBJ_END();
  return pos;
}
int64_t ObMigrateUnitStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObSetTPStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObSetConfigStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObRefreshMemStatStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObRefreshSchemaStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObClearRoottableStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObAdminMergeStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObRecycleReplicaStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObReportReplicaStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObMigrateReplicaStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObDropReplicaStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObChangeReplicaStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObSwitchRSRoleStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObSwitchReplicaRoleStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObFlushDagWarningsStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_));
  J_OBJ_END();
  return pos;
}
int64_t ObFlushIlogCacheStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(file_id));
  J_OBJ_END();
  return pos;
}
int64_t ObFlushKVCacheStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_name), K_(cache_name));
  J_OBJ_END();
  return pos;
}
int64_t ObLoadBaselineStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(load_baseline_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObFlushCacheStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(flush_cache_arg));
  J_OBJ_END();
  return pos;
}
int64_t ObFreezeStmt::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_STMT_TYPE, ((int)stmt_type_), K_(major_freeze), K(opt_server_list_), K(opt_tenant_ids_), K(opt_partition_key_));
  J_OBJ_END();
  return pos;
}
