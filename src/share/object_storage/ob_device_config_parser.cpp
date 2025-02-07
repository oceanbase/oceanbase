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
const char ObDeviceConfigParser::ACCESS_INFO[] = "access_info=";
const char ObDeviceConfigParser::ENCRYPT_INFO[] = "encrypt_info=";
const char ObDeviceConfigParser::EXTENSION[] = "extension=";
const char ObDeviceConfigParser::OLD_ACCESS_INFO[] = "old_access_info=";
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

int ObDeviceConfigParser::parse_device_config_field(const char *buf, ObDeviceConfig &device_config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is nullptr", KR(ret));
  // e.g., used_for=clog
  } else if (OB_SUCC(parse_common_field_(buf, device_config.used_for_, FieldType::TYPE_CHAR_ARRAY, USED_FOR, OB_MAX_STORAGE_USED_FOR_LENGTH))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse used_for", KR(ret), K(buf));
  // e.g., path=oss://xxx/yyy
  } else if (OB_SUCC(parse_common_field_(buf, device_config.path_, FieldType::TYPE_CHAR_ARRAY, PATH, OB_MAX_BACKUP_DEST_LENGTH))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse path", KR(ret), K(buf));
  // e.g., endpoint=xxx
  } else if (OB_SUCC(parse_common_field_(buf, device_config.endpoint_, FieldType::TYPE_CHAR_ARRAY, ENDPOINT, OB_MAX_BACKUP_ENDPOINT_LENGTH))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse endpoint", KR(ret), K(buf));
  // e.g., access_info=access_id=xxx&encrypt_key=xxx
  } else if (OB_SUCC(parse_access_info_(buf, device_config.access_info_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse access_info", KR(ret), K(buf));
  // e.g., encrypt_info=xxx
  } else if (OB_SUCC(parse_encrypt_info_(buf, device_config.encrypt_info_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse encrypt_info", KR(ret), K(buf));
  // e.g., extension=xxx
  } else if (OB_SUCC(parse_extension_(buf, device_config.extension_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse extension", KR(ret), K(buf));
  // e.g., old_access_info=access_id=xxx&encrypt_key=xxx
  } else if (OB_SUCC(parse_access_info_(buf, device_config.old_access_info_, true/*is_old*/))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse access_info", KR(ret), K(buf));
  // e.g., old_encrypt_info=xxx
  } else if (OB_SUCC(parse_encrypt_info_(buf, device_config.old_encrypt_info_, true/*is_old*/))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse encrypt_info", KR(ret), K(buf));
  // e.g., old_extension=xxx
  } else if (OB_SUCC(parse_extension_(buf, device_config.old_extension_, true/*is_old*/))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse extension", KR(ret), K(buf));
  // e.g., state=ADDING
  } else if (OB_SUCC(parse_common_field_(buf, device_config.state_, FieldType::TYPE_CHAR_ARRAY, STATE, OB_MAX_STORAGE_STATE_LENGTH))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse state", KR(ret), K(buf));
  // e.g., state_info=xxx
  } else if (OB_SUCC(parse_common_field_(buf, device_config.state_info_, FieldType::TYPE_CHAR_ARRAY, STATE_INFO, OB_MAX_STORAGE_STATE_INFO_LENGTH))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse state_info", KR(ret), K(buf));
  // e.g., create_timestamp=1679579590457
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.create_timestamp_), FieldType::TYPE_INT, CREATE_TIMESTAMP))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse create_timestamp_", KR(ret), K(buf));
  // e.g., last_check_timestamp=1679579590457
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.last_check_timestamp_), FieldType::TYPE_INT, LAST_CHECK_TIMESTAMP))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse last_check_timestamp", KR(ret), K(buf));
  // e.g., op_id=2
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.op_id_), FieldType::TYPE_UINT, OP_ID))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse op_id", KR(ret), K(buf));
  // e.g., sub_op_id=1
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.sub_op_id_), FieldType::TYPE_UINT, SUB_OP_ID))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse sub_op_id", KR(ret), K(buf));
  // e.g., storage_id=10
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.storage_id_), FieldType::TYPE_UINT, STORAGE_ID))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse storage_id", KR(ret), K(buf));
  // e.g., iops=10000
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.max_iops_), FieldType::TYPE_INT, MAX_IOPS))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse iops", KR(ret), K(buf));
  // e.g., bandwidth=1000000000
  } else if (OB_SUCC(parse_common_field_(buf, &(device_config.max_bandwidth_), FieldType::TYPE_INT, MAX_BANDWIDTH))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse bandwidth", KR(ret), K(buf));
  } else {
    LOG_WARN("all item not match", KR(ret), K(buf));
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
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(token));
  } else if (is_old) { // old_access_info=xxx
    if (OB_FAIL(parse_config_type_char_array(OLD_ACCESS_INFO, token, OB_MAX_BACKUP_AUTHORIZATION_LENGTH,
                access_info)) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(OLD_ENCRYPT_INFO), K(token));
    }
  } else { // access_info=xxx
    if (OB_FAIL(parse_config_type_char_array(ACCESS_INFO, token, OB_MAX_BACKUP_AUTHORIZATION_LENGTH,
                access_info)) && (OB_ITEM_NOT_MATCH != ret)) {
      LOG_WARN("fail to parse_config_type_char_array", KR(ret), K(ENCRYPT_INFO), K(token));
    }
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

}  // namespace share
}  // namespace oceanbase
