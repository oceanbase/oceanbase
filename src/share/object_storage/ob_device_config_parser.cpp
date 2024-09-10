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

#define USING_LOG_PREFIX SHARE

#include "share/object_storage/ob_device_config_parser.h"

namespace oceanbase
{
namespace share
{
using namespace share;
using namespace common;
const char ObDeviceConfigParser::USED_FOR[] = "used_for=";
const char ObDeviceConfigParser::PATH[] = "path=";
const char ObDeviceConfigParser::ENDPOINT[] = "endpoint=";
const char ObDeviceConfigParser::ACCESS_MODE[] = "access_mode=";
const char ObDeviceConfigParser::ACCESS_ID[] = "access_id=";
const char ObDeviceConfigParser::ACCESS_KEY[] = "access_key=";
const char ObDeviceConfigParser::ENCRYPT_KEY[] = "encrypt_key=";
const char ObDeviceConfigParser::ENCRYPT_INFO[] = "encrypt_info=";
const char ObDeviceConfigParser::EXTENSION[] = "extension=";
const char ObDeviceConfigParser::OLD_ACCESS_MODE[] = "old_access_mode=";
const char ObDeviceConfigParser::OLD_ACCESS_ID[] = "old_access_id=";

const char ObDeviceConfigParser::OLD_ACCESS_KEY[] = "old_access_key=";
const char ObDeviceConfigParser::OLD_ENCRYPT_KEY[] = "old_encrypt_key=";
const char ObDeviceConfigParser::OLD_ENCRYPT_INFO[] = "old_encrypt_info=";
const char ObDeviceConfigParser::OLD_EXTENSION[] = "old_extension=";
const char ObDeviceConfigParser::RAM_URL[] = "ram_url=";
const char ObDeviceConfigParser::OLD_RAM_URL[] = "old_ram_url=";
const char ObDeviceConfigParser::STATE[] = "state=";
const char ObDeviceConfigParser::STATE_INFO[] = "state_info=";
const char ObDeviceConfigParser::CREATE_TIMESTAMP[] = "create_timestamp=";
const char ObDeviceConfigParser::LAST_CHECK_TIMESTAMP[] = "last_check_timestamp=";
const char ObDeviceConfigParser::OP_ID[] = "op_id=";
const char ObDeviceConfigParser::SUB_OP_ID[] = "sub_op_id=";
const char ObDeviceConfigParser::STORAGE_ID[] = "storage_id=";
const char ObDeviceConfigParser::MAX_IOPS[] = "max_iops=";
const char ObDeviceConfigParser::MAX_BANDWIDTH[] = "max_bandwidth=";
const char ObDeviceConfigParser::INVALID_OP_ID[] = "-1";  // because op_id in __all_zone_storage is int(20), but in DeviceConfig is uint64_t,
                                                          // op_id is init UINT64_MAX, so when ObDeviceManifest::write_head_ to manifest op_id is change as -1

int ObDeviceConfigParser::parse_one_device_config(char *opt_str, ObDeviceConfig &config)
{
  int ret = OB_SUCCESS;
  char *token = opt_str;
  char *saved_ptr = NULL;
  config.reset();

  for (char *str = token; OB_SUCC(ret); str = NULL) {
    token = ::strtok_r(str, "&", &saved_ptr);
    if (OB_ISNULL(token)) {
      break;
    } else {
      // e.g., used_for=clog
      if (OB_SUCC(parse_common_field_(token, config.used_for_, FieldType::TYPE_CHAR_ARRAY, USED_FOR, OB_MAX_STORAGE_USED_FOR_LENGTH))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse used_for", KR(ret), K(token));
      // e.g., path=oss://xxx/yyy
      } else if (OB_SUCC(parse_common_field_(token, config.path_, FieldType::TYPE_CHAR_ARRAY, PATH, OB_MAX_BACKUP_DEST_LENGTH))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse path", KR(ret), K(token));
      // e.g., endpoint=xxx
      } else if (OB_SUCC(parse_common_field_(token, config.endpoint_, FieldType::TYPE_CHAR_ARRAY, ENDPOINT, OB_MAX_BACKUP_ENDPOINT_LENGTH))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse endpoint", KR(ret), K(token));
      // e.g., access_mode=access_by_id or access_id=xxx or ram_url=xxx
      } else if (OB_SUCC(parse_access_info_(token, config.access_info_))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse access_info", KR(ret), K(token));
      // e.g., encrypt_info=xxx
      } else if (OB_SUCC(parse_encrypt_info_(token, config.encrypt_info_))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse encrypt_info", KR(ret), K(token));
      // e.g., extension=xxx
      } else if (OB_SUCC(parse_extension_(token, config.extension_))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse extension", KR(ret), K(token));
      // e.g., old_access_mode=access_by_id or old_access_id=xxx
      } else if (OB_SUCC(parse_access_info_(token, config.old_access_info_, true/*is_old*/))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse access_info", KR(ret), K(token));
      // e.g., old_encrypt_info=xxx
      } else if (OB_SUCC(parse_encrypt_info_(token, config.old_encrypt_info_, true/*is_old*/))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse encrypt_info", KR(ret), K(token));
      // e.g., old_extension=xxx
      } else if (OB_SUCC(parse_extension_(token, config.old_extension_, true/*is_old*/))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse extension", KR(ret), K(token));
      // e.g., state=ADDING
      } else if (OB_SUCC(parse_common_field_(token, config.state_, FieldType::TYPE_CHAR_ARRAY, STATE, OB_MAX_STORAGE_STATE_LENGTH))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse state", KR(ret), K(token));
      // e.g., state_info=xxx
      } else if (OB_SUCC(parse_common_field_(token, config.state_info_, FieldType::TYPE_CHAR_ARRAY, STATE_INFO, OB_MAX_STORAGE_STATE_INFO_LENGTH))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse state_info", KR(ret), K(token));
      // e.g., create_timestamp=1679579590457
      } else if (OB_SUCC(parse_common_field_(token, &(config.create_timestamp_), FieldType::TYPE_INT, CREATE_TIMESTAMP))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse create_timestamp_", KR(ret), K(token));
      // e.g., last_check_timestamp=1679579590457
      } else if (OB_SUCC(parse_common_field_(token, &(config.last_check_timestamp_), FieldType::TYPE_INT, LAST_CHECK_TIMESTAMP))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse last_check_timestamp", KR(ret), K(token));
      // e.g., op_id=2
      } else if (OB_SUCC(parse_common_field_(token, &(config.op_id_), FieldType::TYPE_UINT, OP_ID))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse op_id", KR(ret), K(token));
      // e.g., sub_op_id=1
      } else if (OB_SUCC(parse_common_field_(token, &(config.sub_op_id_), FieldType::TYPE_UINT, SUB_OP_ID))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse sub_op_id", KR(ret), K(token));
      // e.g., storage_id=10
      } else if (OB_SUCC(parse_common_field_(token, &(config.storage_id_), FieldType::TYPE_UINT, STORAGE_ID))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse storage_id", KR(ret), K(token));
      // e.g., iops=10000
      } else if (OB_SUCC(parse_common_field_(token, &(config.max_iops_), FieldType::TYPE_INT, MAX_IOPS))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse iops", KR(ret), K(token));
      // e.g., bandwidth=1000000000
      } else if (OB_SUCC(parse_common_field_(token, &(config.max_bandwidth_), FieldType::TYPE_INT, MAX_BANDWIDTH))) {
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse bandwidth", KR(ret), K(token));
      } else {
        FLOG_WARN("all item not match, unknown token", KR(ret), K(token));
      }
    }
  }
  return ret;
}

int ObDeviceConfigParser::parse_common_field_(
    const char *token,
    void *field,
    const FieldType &field_type,
    const char *field_name,
    const int64_t max_field_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token) || OB_ISNULL(field) || OB_ISNULL(field_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(token), KP(field), KP(field_name));
  } else if (FieldType::TYPE_CHAR_ARRAY == field_type) {
    if (OB_FAIL(parse_config_type_char_array(field_name, token, max_field_len,
                static_cast<char *>(field))) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(field_name), K(token));
    }
  } else if (FieldType::TYPE_INT == field_type) {
    if (OB_FAIL(parse_config_type_int(field_name, token, *(static_cast<int64_t *>(field))))
                && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_int", KR(ret), K(field_name), K(token));
    }
  } else if (FieldType::TYPE_UINT == field_type) {
    if (OB_FAIL(parse_config_type_uint(field_name, token, *(static_cast<uint64_t *>(field))))
                && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_uint", KR(ret), K(field_name), K(token));
    }
  }
  return ret;
}

int ObDeviceConfigParser::parse_access_info_(
    const char *token,
    char *access_info,
    const bool is_old)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_TDE_SECURITY
  const char *access_info_keys[] = {ACCESS_MODE, ACCESS_ID, ACCESS_KEY, RAM_URL};
  const char *old_access_info_keys[] = {OLD_ACCESS_MODE, OLD_ACCESS_ID, OLD_ACCESS_KEY, OLD_RAM_URL};
#else
  const char *access_info_keys[] = {ACCESS_MODE, ACCESS_ID, ENCRYPT_KEY, RAM_URL};
  const char *old_access_info_keys[] = {OLD_ACCESS_MODE, OLD_ACCESS_ID, OLD_ENCRYPT_KEY, OLD_RAM_URL};
#endif
  STATIC_ASSERT(ARRAYSIZEOF(access_info_keys) == ARRAYSIZEOF(old_access_info_keys), "key count mismatch");
  const int64_t key_cnt = ARRAYSIZEOF(access_info_keys);
  const char *key_name = nullptr;
  bool is_item_match = false;

  if (OB_ISNULL(token) || OB_ISNULL(access_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(token), KP(access_info));
  }

  for (int64_t i = 0; OB_SUCC(ret) && (i < key_cnt); ++i) {
    key_name = is_old ? old_access_info_keys[i] : access_info_keys[i];
    if (0 == STRNCMP(key_name, token, STRLEN(key_name))) {
      const int64_t access_info_len = STRLEN(access_info);
      const char *value_str = token + STRLEN(key_name);
      if (0 == STRLEN(value_str)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("value is empty", KR(ret), K(key_name), K(token));
      } else if (0 == access_info_len) { // first field of access_info
        STRCPY(access_info, token);
      } else { // not first field of access_info, need to concat '&'
        access_info[access_info_len] = '&';
        STRCPY(access_info + access_info_len + 1, token);
      }
      is_item_match = true;
      break;
    }
  }
  if (OB_SUCC(ret) && !is_item_match) {
    ret = OB_ITEM_NOT_MATCH;
  }
  return ret;
}

int ObDeviceConfigParser::parse_encrypt_info_(
    const char *token,
    char *encrypt_info,
    const bool is_old)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(token));
  } else if (is_old) { // old_encrypt_info=xxx
    if (OB_FAIL(parse_config_type_char_array(OLD_ENCRYPT_INFO, token, OB_MAX_STORAGE_ENCRYPT_INFO_LENGTH,
                encrypt_info)) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(OLD_ENCRYPT_INFO), K(token));
    }
  } else { // encrypt_info=xxx
    if (OB_FAIL(parse_config_type_char_array(ENCRYPT_INFO, token, OB_MAX_STORAGE_ENCRYPT_INFO_LENGTH,
                encrypt_info)) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(ENCRYPT_INFO), K(token));
    }
  }
  return ret;
}

int ObDeviceConfigParser::parse_extension_(
    const char *token,
    char *extension,
    const bool is_old)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(token));
  } else if (is_old) { // old_extension=xxx
    if (OB_FAIL(parse_config_type_char_array(OLD_EXTENSION, token, OB_MAX_BACKUP_EXTENSION_LENGTH,
                extension)) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(OLD_EXTENSION), K(token));
    }
  } else { // extension=xxx
    if (OB_FAIL(parse_config_type_char_array(EXTENSION, token, OB_MAX_BACKUP_EXTENSION_LENGTH,
                extension)) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(EXTENSION), K(token));
    }
  }
  return ret;
}

int ObDeviceConfigParser::parse_config_type_char_array(
    const char *key_name,
    const char *token,
    const int64_t max_value_len,
    char *value)
{
  int ret = OB_SUCCESS;
  int64_t digit_len = 0;
  if (OB_ISNULL(key_name) || OB_ISNULL(token) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(key_name), KP(token), KP(value));
  } else if (0 == STRNCMP(key_name, token, STRLEN(key_name))) {
    const char *value_str = token + STRLEN(key_name);
    if (STRLEN(value_str) >= max_value_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value str is too long", KR(ret), K(key_name), "value_len",
               STRLEN(value_str), K(max_value_len));
    // when restart server, because manifest's head.shared_storage_info_ is '\0', so head.shared_storage_info_ must be overwritten
    // TODO:当沧溟新的bootstrap共享存储集群的代码合入了后，这里删除掉 @xiaotao.ht
    } else if (STRLEN(value_str) == 0) {
      MEMSET(value, 0, max_value_len);
    } else {
      STRCPY(value, value_str);
    }
  } else {
    ret = OB_ITEM_NOT_MATCH;
  }
  return ret;
}

int ObDeviceConfigParser::parse_config_type_int(
    const char *key_name,
    const char *token,
    int64_t &value)
{
  int ret = OB_SUCCESS;
  int64_t digit_len = 0;
  if (OB_ISNULL(key_name) || OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(key_name), KP(token));
  } else if (0 == STRNCMP(key_name, token, STRLEN(key_name))) {
    const char *value_str = token + STRLEN(key_name);
    const ObString value_ob_str(value_str);
    if (0 == STRLEN(value_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value str is empty", KR(ret), K(key_name));
    } else if (value_ob_str.is_numeric()) {
      errno = 0; // clear errno generated before
      char *endptr = NULL;
      value = strtoll(value_str, &endptr, 10);
      if (((errno != 0) && (0 == value))
          || ((errno == ERANGE) && ((value == LLONG_MAX) || (value == LLONG_MIN)))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("strtoll fail", KR(ret), K(value_str), K(value), K(errno), KERRMSG);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value is not legal digit", KR(ret), K(key_name), K(token));
    }
  } else {
    ret = OB_ITEM_NOT_MATCH;
  }
  return ret;
}

int ObDeviceConfigParser::parse_config_type_uint(
    const char *key_name,
    const char *token,
    uint64_t &value)
{
  int ret = OB_SUCCESS;
  int64_t digit_len = 0;
  if (OB_ISNULL(key_name) || OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(key_name), KP(token));
  } else if (0 == STRNCMP(key_name, token, STRLEN(key_name))) {
    const char *value_str = token + STRLEN(key_name);
    const ObString value_ob_str(value_str);
    if (0 == STRLEN(value_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value str is empty", KR(ret), K(key_name));
    } else if (0 == STRNCMP(value_str, INVALID_OP_ID, STRLEN(value_str))) {
      value = UINT64_MAX;
    } else if (value_ob_str.is_numeric()) {
      errno = 0; // clear errno generated before
      char *endptr = NULL;
      value = strtoull(value_str, &endptr, 10);
      if (((errno != 0) && (0 == value))
          || ((errno == ERANGE) && (value == ULLONG_MAX))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("strtoull fail", KR(ret), K(value_str), K(value), K(errno), KERRMSG);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value is not legal digit", KR(ret), K(key_name), K(token));
    }
  } else {
    ret = OB_ITEM_NOT_MATCH;
  }
  return ret;
}

// 1. convert from "access_mode=access_by_id&access_id=xxx" to
// "old_access_mode=access_by_id&old_access_id=xxx"
// 2. convert from "access_mode=access_by_ram_url&ram_url=xxx" to
// "old_access_mode=access_by_ram_url&old_ram_url=xxx"
int ObDeviceConfigParser::convert_access_info_to_old(char *access_info, char *old_access_info)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_TDE_SECURITY
  const char *access_info_keys[] = {ACCESS_MODE, ACCESS_ID, ACCESS_KEY, RAM_URL};
#else
  const char *access_info_keys[] = {ACCESS_MODE, ACCESS_ID, ENCRYPT_KEY, ACCESS_KEY, RAM_URL};
#endif
  const int64_t key_cnt = ARRAYSIZEOF(access_info_keys);
  const char *key_name = nullptr;
  char *token = access_info;
  char *saved_ptr = NULL;
  if (OB_ISNULL(access_info) || OB_ISNULL(old_access_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(access_info), KP(old_access_info));
  } else {
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "&", &saved_ptr);
      if (OB_ISNULL(token)) {
        break;
      } else {
        bool is_item_match = false;
        for (int64_t i = 0; OB_SUCC(ret) && (i < key_cnt); ++i) {
          key_name = access_info_keys[i];
          if (0 == STRNCMP(key_name, token, STRLEN(key_name))) {
            const int64_t old_access_info_len = STRLEN(old_access_info);
            const char *value_str = token + STRLEN(key_name);
            if (0 == STRLEN(value_str)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("value is empty", KR(ret), K(key_name), K(token));
            } else if (0 == old_access_info_len) { // first field of old_access_info
              STRCPY(old_access_info, "old_");
              STRCPY(old_access_info + STRLEN("old_"), token);
            } else { // not first field of old_access_info, need to concat '&'
              old_access_info[old_access_info_len] = '&';
              STRCPY(old_access_info + old_access_info_len + 1, "old_");
              STRCPY(old_access_info + old_access_info_len + 1 + STRLEN("old_"), token);
            }
            is_item_match = true;
            break;
          }
        }
        if (OB_SUCC(ret) && !is_item_match) {
          ret = OB_ITEM_NOT_MATCH;
        }
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
