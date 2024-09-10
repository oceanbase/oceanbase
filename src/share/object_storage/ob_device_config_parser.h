/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONFIG_PARSER_H_
#define OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONFIG_PARSER_H_

#include "share/object_storage/ob_object_storage_struct.h"

namespace oceanbase
{
namespace share
{
class ObDeviceConfigParser
{
public:
  static const char USED_FOR[];
  static const char PATH[];
  static const char ENDPOINT[];
  static const char ACCESS_MODE[];
  static const char ACCESS_ID[];
  static const char ACCESS_KEY[];
  static const char ENCRYPT_KEY[];
  static const char ENCRYPT_INFO[];
  static const char EXTENSION[];
  static const char OLD_ACCESS_MODE[];
  static const char OLD_ACCESS_ID[];
  static const char OLD_ACCESS_KEY[];
  static const char OLD_ENCRYPT_KEY[];
  static const char OLD_ENCRYPT_INFO[];
  static const char OLD_EXTENSION[];
  static const char RAM_URL[];
  static const char OLD_RAM_URL[];
  static const char STATE[];
  static const char STATE_INFO[];
  static const char CREATE_TIMESTAMP[];
  static const char LAST_CHECK_TIMESTAMP[];
  static const char OP_ID[];
  static const char SUB_OP_ID[];
  static const char STORAGE_ID[];
  static const char MAX_IOPS[];
  static const char MAX_BANDWIDTH[];
  static const char INVALID_OP_ID[];

public:
  static int parse_one_device_config(char *opt_str, ObDeviceConfig &config);
  static int parse_config_type_int(const char *key_name, const char *token, int64_t &value);
  static int parse_config_type_uint(const char *key_name, const char *token, uint64_t &value);
  static int parse_config_type_char_array(const char *key_name,
                                          const char *token,
                                          const int64_t max_value_len,
                                          char *value);
  static int convert_access_info_to_old(char *access_info, char *old_access_info);

private:
  enum class FieldType
  {
    TYPE_CHAR_ARRAY = 0,
    TYPE_INT = 1,
    TYPE_UINT = 2,
  };

  // @max_field_len is used for TYPE_CHAR_ARRAY, not used for TYPE_INT and TYPE_UINT
  static int parse_common_field_(const char *token,
                                 void *field,
                                 const FieldType &field_type,
                                 const char *field_name,
                                 const int64_t max_field_len = 0);
  static int parse_access_info_(const char *token, char *access_info, const bool is_old = false);
  static int parse_encrypt_info_(const char *token, char *encrypt_info, const bool is_old = false);
  static int parse_extension_(const char *token, char *extension, const bool is_old = false);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_DEVICE_OB_DEVICE_CONFIG_PARSER_H_
