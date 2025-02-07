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
#include "share/restore/ob_restore_type.h"

using namespace oceanbase;
using namespace share;

static const char *OB_RESTORE_TYPE_STR[] = {"FULL", "QUICK"};

ObRestoreType::ObRestoreType(const ObString &str)
{
  type_ = Type::RESTORE_TYPE_MAX;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(OB_RESTORE_TYPE_STR); i++) {
      if (0 == str.case_compare(OB_RESTORE_TYPE_STR[i])) {
        type_ = static_cast<Type>(i);
        break;
      }
    }
  }
  if (Type::RESTORE_TYPE_MAX <= type_) {
     LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid restore type", K_(type), K(str));
  }
}

ObRestoreType &ObRestoreType::operator=(const ObRestoreType &restore_type)
{
  if (this != &restore_type) {
    type_ = restore_type.type_;
  }
  return *this;
}

ObRestoreType &ObRestoreType::operator=(const Type &type)
{
  type_ = type;
  return *this;
}

int ObRestoreType::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(type_)))) {
    LOG_WARN("failed to serialize restore type", K(ret), K(len), K_(type));
  } else {
    pos = new_pos;
  }
  return ret;
}

int ObRestoreType::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&type_)))) {
    LOG_WARN("failed to decode restore type", K(ret), K(len), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObRestoreType::get_serialize_size() const
{
  return serialization::encoded_length_i8(static_cast<int8_t>(type_));
}

const char* ObRestoreType::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(OB_RESTORE_TYPE_STR) == RESTORE_TYPE_MAX, "array size mismatch");
  const char *str = "UNKNOWN";
  if (OB_UNLIKELY(type_ >= ARRAYSIZEOF(OB_RESTORE_TYPE_STR)
                  || type_ < Type::FULL)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown restore type", K_(type));
  } else {
    str = OB_RESTORE_TYPE_STR[type_];
  }
  return str;
}