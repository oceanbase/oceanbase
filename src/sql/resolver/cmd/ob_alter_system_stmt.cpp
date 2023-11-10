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
  :ObSystemCmdStmt(stmt::T_BACKUP_SET_ENCRYPTION),
    mode_(share::ObBackupEncryptionMode::MAX_MODE),
    encrypted_passwd_()
{
  passwd_buf_[0] = '\0';
}


int ObBackupSetEncryptionStmt::set_param(const int64_t mode, const common::ObString &passwd)
{
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;
  char encrypted_buffer[OB_MAX_PASSWORD_LENGTH] = {0};
  ObString encrypted_str(sizeof(encrypted_buffer), encrypted_buffer);
  mode_ = static_cast<const share::ObBackupEncryptionMode::EncryptionMode>(mode);

  if (!share::ObBackupEncryptionMode::is_valid(mode_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(mode), K(mode_));
  } else if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage2(passwd, encrypted_str))) {
    COMMON_LOG(WARN, "failed to encrypted passwd", K(ret), K(passwd));
  } else if (OB_FAIL(databuff_printf(passwd_buf_,
                                     sizeof(passwd_buf_),
                                     pos,
                                     "%.*s",
                                     encrypted_str.length(),
                                     encrypted_str.ptr()))) {
    COMMON_LOG(WARN, "failed to format passwd_buf", K(ret), K(encrypted_str));
  } else {
    encrypted_passwd_.assign_ptr(passwd_buf_, pos);
  }

  return ret;
}


ObBackupSetDecryptionStmt::ObBackupSetDecryptionStmt()
  : ObSystemCmdStmt(stmt::T_BACKUP_SET_DECRYPTION)
{
  passwd_array_[0] = '\0';
  pos_ = 0;
}


int ObBackupSetDecryptionStmt::add_passwd(const ObString &passwd)
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
  } else if (OB_FAIL(databuff_printf(passwd_array_, sizeof(passwd_array_), pos_, "%.*s",
      encrypted_passwd.length(), encrypted_passwd.ptr()))) {
    COMMON_LOG(WARN, "failed to add passwd", K(ret), K_(pos), K_(passwd_array));
  }

  COMMON_LOG(INFO, "add passwd", K(passwd), K(encrypted_passwd), K_(passwd_array));

  return ret;
}

ObSetRegionBandwidthStmt::ObSetRegionBandwidthStmt()
  : ObSystemCmdStmt(stmt::T_SET_REGION_NETWORK_BANDWIDTH)
{
  max_bw_ = -1;
}

int ObSetRegionBandwidthStmt::set_param(const char *src_region, const char *dst_region, const int64_t max_bw)
{
  int ret = common::OB_SUCCESS;
  snprintf(src_region_, MAX_REGION_LENGTH, "%s", src_region);
  snprintf(dst_region_, MAX_REGION_LENGTH, "%s", dst_region);
  max_bw_ = max_bw;
  return ret;
}

ObAddRestoreSourceStmt::ObAddRestoreSourceStmt()
  : ObSystemCmdStmt(stmt::T_ADD_RESTORE_SOURCE)
{
  restore_source_array_[0] = '\0';
  pos_ = 0;
}

int ObAddRestoreSourceStmt::add_restore_source(const common::ObString &source)
{
  int ret = OB_SUCCESS;
  if (pos_ != 0) {
    if (OB_FAIL(databuff_printf(restore_source_array_, sizeof(restore_source_array_), pos_, ","))) {
      COMMON_LOG(WARN, "failed to add comma", KR(ret), K(pos_), K(restore_source_array_));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(databuff_printf(restore_source_array_, sizeof(restore_source_array_),
    pos_, "%.*s", source.length(), source.ptr()))) {
    COMMON_LOG(WARN, "failed to add restore source", KR(ret), K(pos_), K(restore_source_array_));
  }
  COMMON_LOG(INFO, "add restore source", KR(ret), K(source), K(restore_source_array_));
  return ret;
}
