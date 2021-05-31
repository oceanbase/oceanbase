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
#include "ob_encryption_util.h"
#include "lib/alloc/alloc_assist.h"
#include "share/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/random/ob_random.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
using namespace common;
namespace share {

bool ObEncryptionUtil::need_encrypt(int64_t encrypt_id)
{
  return encrypt_id > static_cast<int64_t>(ObAesOpMode::ob_invalid_mode);
}

int ObEncryptionUtil::parse_encryption_algorithm(const ObString& str, ObAesOpMode& encryption_algorithm)
{
  int ret = OB_SUCCESS;
  UNUSED(str);
  encryption_algorithm = ObAesOpMode::ob_invalid_mode;
  return ret;
}

int ObEncryptionUtil::parse_encryption_algorithm(const char* str, ObAesOpMode& encryption_algorithm)
{
  ObString ob_str(str);
  return parse_encryption_algorithm(ob_str, encryption_algorithm);
}

int ObEncryptionUtil::parse_encryption_id(const char* str, int64_t& encrypt_id)
{
  ObString ob_str(str);
  return parse_encryption_id(ob_str, encrypt_id);
}

int ObEncryptionUtil::parse_encryption_id(const ObString& str, int64_t& encrypt_id)
{
  int ret = OB_SUCCESS;
  ObAesOpMode encryption_algorithm = ObAesOpMode::ob_invalid_mode;
  if (OB_FAIL(parse_encryption_algorithm(str, encryption_algorithm))) {
    LOG_WARN("failed to parse_encryption_algorithm", KR(ret), K(str));
  } else {
    encrypt_id = static_cast<int64_t>(encryption_algorithm);
  }
  return ret;
}

bool ObBackupEncryptionMode::is_valid(const EncryptionMode& mode)
{
  return mode >= NONE && mode < MAX_MODE;
}

bool ObBackupEncryptionMode::is_valid_for_log_archive(const EncryptionMode& mode)
{
  return (NONE == mode || TRANSPARENT_ENCRYPTION == mode);
}

const char* backup_encryption_strs[] = {
    "NONE",
    "PASSWORD",
    "PASSWORD_ENCRYPTION",
    "TRANSPARENT_ENCRYPTION",
    "DUAL_MODE_ENCRYPTION",
};

const char* ObBackupEncryptionMode::to_str(const EncryptionMode& mode)
{
  const char* str = "UNKNOWN";

  if (is_valid(mode)) {
    str = backup_encryption_strs[mode];
  }
  return str;
}

ObBackupEncryptionMode::EncryptionMode ObBackupEncryptionMode::parse_str(const char* str)
{
  ObString obstr(str);
  return parse_str(obstr);
}

ObBackupEncryptionMode::EncryptionMode ObBackupEncryptionMode::parse_str(const common::ObString& str)
{
  EncryptionMode mode = MAX_MODE;
  const int64_t count = ARRAYSIZEOF(backup_encryption_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObBackupEncryptionMode::MAX_MODE) == count, "encryption mode count mismatch");

  if (str.empty()) {
    mode = NONE;
  } else {
    for (int64_t i = 0; i < count; ++i) {
      if (0 == str.case_compare(backup_encryption_strs[i])) {
        mode = static_cast<EncryptionMode>(i);
        break;
      }
    }
  }
  return mode;
}

}  // namespace share
}  // namespace oceanbase
