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

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"

#ifndef OCEANBASE_SHARE_OB_ENCRYPTION_UTIIL_H
#define OCEANBASE_SHARE_OB_ENCRYPTION_UTIIL_H

namespace oceanbase {
namespace common {
class ObString;
}
namespace share {

enum ObAesOpMode {
  ob_invalid_mode = 0,
  // attention:remember to modify compare_aes_mod_safety when add new mode
  ob_max_mode
};

const int64_t OB_ORIGINAL_TABLE_KEY_LEN = 15;
const int64_t OB_ENCRYPTED_TABLE_KEY_LEN = 16;
const int64_t OB_MAX_MASTER_KEY_LENGTH = 16;
const int64_t OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH = 16;

const int64_t OB_CLOG_ENCRYPT_RANDOM_LEN = 16;
const int64_t OB_CLOG_ENCRYPT_MASTER_KEY_LEN = 32;
const int64_t OB_CLOG_ENCRYPT_TABLE_KEY_LEN = 32;

class ObEncryptionUtil {
public:
  static bool need_encrypt(int64_t encrypt_id);
  static int parse_encryption_algorithm(const common::ObString& str, ObAesOpMode& encryption_algorithm);
  static int parse_encryption_algorithm(const char* str, ObAesOpMode& encryption_algorithm);
  static int parse_encryption_id(const char* str, int64_t& encrypt_id);
  static int parse_encryption_id(const common::ObString& str, int64_t& encrypt_id);
};

struct ObBackupEncryptionMode final {
  enum EncryptionMode {
    NONE = 0,
    PASSWORD = 1,
    PASSWORD_ENCRYPTION = 2,
    TRANSPARENT_ENCRYPTION = 3,
    DUAL_MODE_ENCRYPTION = 4,
    MAX_MODE
  };
  static bool is_valid(const EncryptionMode& mode);
  static bool is_valid_for_log_archive(const EncryptionMode& mode);
  static const char* to_str(const EncryptionMode& mode);
  static EncryptionMode parse_str(const char* str);
  static EncryptionMode parse_str(const common::ObString& str);
};

}  // namespace share
}  // namespace oceanbase
#endif
