/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SQL_QRR
#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(FixedParamValue, idx_, raw_text_);
OB_SERIALIZE_MEMBER(DynamicParamInfo, raw_param_idx_, question_mark_idx_);

DEFINE_SERIALIZE(QuestionMarkDefNameCtx)
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), KP(buf), K(serialize_size), "remain", buf_len - pos);
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count_))) {
    LOG_WARN("failed to decode count", K(ret));
  } else {
    for (int64_t i = 0; i < count_ && OB_SUCC(ret); ++i) {
      char *ptr = name_[i];
      const int64_t len = strlen(ptr);
      if (OB_FAIL(serialization::encode_vstr(buf, buf_len, pos, ptr, len))) {
        LOG_WARN("string serialize failed", K(ret));
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(QuestionMarkDefNameCtx)
{
  int ret = OB_SUCCESS;
  const int64_t min_needed_size = 1;
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < min_needed_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), "remain", data_len - pos);
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count_))) {
    LOG_WARN("failed to encode count", K(data_len), K(pos), K(count_), K(ret));
  } else if (OB_ISNULL(name_ = (char **)allocator_.alloc(sizeof(char*) * count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    for (int64_t i = 0; i < count_ && OB_SUCC(ret); ++i) {
      char *ptr = nullptr;
      int64_t len = 0;
      ptr = const_cast<char *>(common::serialization::decode_vstr(buf, data_len, pos, &len));
      if (OB_ISNULL(ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("decode NULL string", K(ret));
      } else {
        name_[i] = ptr;
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(QuestionMarkDefNameCtx)
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(count_);
  for (int64_t i = 0; i < count_; ++i) {
    const int64_t len = strlen(name_[i]);
    size += serialization::encoded_length_vstr(len);
  }
  return size;
}

int QuestionMarkDefNameCtx::assign(const char **name, const int64_t count)
{
  int ret = OB_SUCCESS;
  count_ = count;
  if (OB_ISNULL(name_ = (char **)allocator_.alloc(sizeof(char*) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret));
  } else {
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      char *ptr = nullptr;
      const int32_t len = sizeof(strlen(name[i]));
      if (OB_ISNULL(ptr = (char *)allocator_.alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else {
        MEMCPY(ptr, name[i], len);
        ptr[len] = '\0';
        name_[i] = ptr;
      }
    }
  }
  return ret;
}

int ObUDRInfo::assign(const ObUDRInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  pattern_digest_ = other.pattern_digest_;
  rule_id_ = other.rule_id_;
  rule_version_ = other.rule_version_;
  rule_status_ = other.rule_status_;
  coll_type_ = other.coll_type_;
  db_name_ = other.db_name_;
  rule_name_ = other.rule_name_;
  pattern_ = other.pattern_;
  replacement_ = other.replacement_;
  normalized_pattern_ = other.normalized_pattern_;
  fixed_param_infos_str_ = other.fixed_param_infos_str_;
  dynamic_param_infos_str_ = other.dynamic_param_infos_str_;
  question_mark_ctx_str_ = other.question_mark_ctx_str_;
  return ret;
}

} // namespace sql end
} // namespace oceanbase end
