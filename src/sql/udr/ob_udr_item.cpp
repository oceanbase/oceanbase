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
#include "lib/utility/ob_print_utils.h"
#include "sql/udr/ob_udr_item.h"

namespace oceanbase
{
namespace sql
{

ObUDRItem::~ObUDRItem()
{
  reset();
}

void ObUDRItem::reset()
{
#define SAFE_FREE_STR(name)     \
if (NULL != name.ptr()) {       \
  allocator_.free(name.ptr());  \
  name.reset();                 \
}
#define SAFE_FREE_PTR(ptr)      \
if (NULL != ptr) {              \
  allocator_.free(ptr);         \
  ptr = nullptr;                \
}
  tenant_id_ = OB_INVALID_ID;
  pattern_digest_ = 0;
  rule_id_ = OB_INVALID_ID;
  rule_version_ = OB_INVALID_VERSION;
  rule_status_ = INVALID_STATUS;
  coll_type_ = common::CS_TYPE_INVALID;
  SAFE_FREE_STR(db_name_);
  SAFE_FREE_STR(rule_name_);
  SAFE_FREE_STR(pattern_);
  SAFE_FREE_STR(replacement_);
  SAFE_FREE_STR(normalized_pattern_);
  SAFE_FREE_STR(fixed_param_infos_str_);
  SAFE_FREE_STR(dynamic_param_infos_str_);
  SAFE_FREE_STR(question_mark_ctx_str_);
  fixed_param_info_array_.reset();
  dynamic_param_info_array_.reset();
  question_mark_def_name_ctx_.reset();
  SAFE_FREE_PTR(fixed_param_deserialize_buf_);
  SAFE_FREE_PTR(dynamic_param_deserialize_buf_);
  SAFE_FREE_PTR(question_mark_deserialize_buf_);
#undef SAFE_FREE_PTR
#undef SAFE_FREE_STR
}

int64_t ObUDRItem::inc_ref_count()
{
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObUDRItem::dec_ref_count()
{
  int64_t ref_count = ATOMIC_SAF(&ref_count_, 1);
  if (ref_count > 0) {
    // do nothing
  } else if (0 == ref_count) {
    LOG_DEBUG("remove rule item", K(ref_count), K(this));
    this->~ObUDRItem();
    allocator_.free(this);// I'm sure this is the last line, so it's safe here
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid ref count", K(ref_count));
  }
  return ref_count;
}

int ObUDRItem::deep_copy(const ObUDRInfo &rule_info)
{
#define DEEP_COPY_STR(name)                                     \
if (OB_SUCC(ret)) {                                             \
  if (OB_FAIL(set_##name(rule_info.name##_))) {                 \
    LOG_WARN("failed to deep copy str", K(rule_info.name##_));  \
  }                                                             \
}
  int ret = OB_SUCCESS;
  tenant_id_ = rule_info.tenant_id_;
  pattern_digest_ = rule_info.pattern_digest_;
  rule_id_ = rule_info.rule_id_;
  rule_version_ = rule_info.rule_version_;
  rule_status_ = rule_info.rule_status_;
  coll_type_ = rule_info.coll_type_;
  DEEP_COPY_STR(db_name);
  DEEP_COPY_STR(rule_name);
  DEEP_COPY_STR(pattern);
  DEEP_COPY_STR(replacement);
  DEEP_COPY_STR(normalized_pattern);
  DEEP_COPY_STR(fixed_param_infos_str);
  DEEP_COPY_STR(dynamic_param_infos_str);
  DEEP_COPY_STR(question_mark_ctx_str);
  return ret;
#undef DEEP_COPY_STR
}

template<typename T>
int ObUDRItem::deserialize_with_hex_str(const common::ObString &str, T &infos, char *&deserialize_buf)
{
  int ret = OB_SUCCESS;
  infos.reset();
  deserialize_buf = nullptr;
  const int64_t str_size = str.length();
  const int64_t deserialize_size = str.length() / 2 + 1;
  int64_t deserialize_pos = 0;
  if (str.empty()) {
    // do nothing
    LOG_DEBUG("str is empty", K(ret));
  } else if (OB_ISNULL(deserialize_buf = static_cast<char*>(allocator_.alloc(deserialize_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(deserialize_size));
  } else if (OB_FAIL(hex_to_cstr(str.ptr(), str_size, deserialize_buf, deserialize_size))) {
    LOG_WARN("fail to get cstr from hex", K(ret), K(str_size), K(deserialize_size), K(str));
  } else if (OB_FAIL(infos.deserialize(deserialize_buf, deserialize_size, deserialize_pos))) {
    LOG_WARN("fail to deserialize", K(ret), K(deserialize_pos), K(deserialize_size), K(str));
  } else if (OB_UNLIKELY(deserialize_pos > deserialize_size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deserialize error", K(ret), K(deserialize_pos), K(deserialize_size));
  } else {
    LOG_DEBUG("succ to deserialize", K(infos));
  }
  return ret;
}

int ObUDRItem::deserialize_fixed_param_info_array(const common::ObString str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_with_hex_str<FixedParamValueArray>(str, fixed_param_info_array_,
                                                             fixed_param_deserialize_buf_))) {
    LOG_WARN("failed to deserialize", K(ret), K(str));
  }
  return ret;
}

int ObUDRItem::deserialize_dynamic_param_info_array(const common::ObString str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_with_hex_str<DynamicParamInfoArray>(str, dynamic_param_info_array_,
                                                              dynamic_param_deserialize_buf_))) {
    LOG_WARN("failed to deserialize", K(ret), K(str));
  }
  return ret;
}

int ObUDRItem::deserialize_question_mark_by_name_ctx(const common::ObString str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_with_hex_str<QuestionMarkDefNameCtx>(str, question_mark_def_name_ctx_,
                                                              question_mark_deserialize_buf_))) {
    LOG_WARN("failed to deserialize", K(ret), K(str));
  }
  return ret;
}

} // namespace sql end
} // namespace oceanbase end
