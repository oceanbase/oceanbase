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


#ifndef OB_SQL_UDR_OB_UDR_STRUCT_H_
#define OB_SQL_UDR_OB_UDR_STRUCT_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{

struct FixedParamValue
{
  OB_UNIS_VERSION(1);
public:
  int64_t idx_;
  common::ObString raw_text_;

  FixedParamValue()
    :idx_(common::OB_INVALID_ID)
  {}

  void reset()
  {
    idx_ = common::OB_INVALID_ID;
    raw_text_.reset();
  }

  TO_STRING_KV(K_(idx), K_(raw_text));
};

// Information about the question mark, e.g:
// select 1, ? from dual; -> {{rw_param_idx: 1, question_mark_idx: 1}}
struct DynamicParamInfo
{
  OB_UNIS_VERSION(1);
public:
  int64_t raw_param_idx_;
  int64_t question_mark_idx_;

  DynamicParamInfo()
    :raw_param_idx_(common::OB_INVALID_ID),
      question_mark_idx_(common::OB_INVALID_ID)
  {}

  void reset()
  {
    raw_param_idx_ = common::OB_INVALID_ID;
    question_mark_idx_ = common::OB_INVALID_ID;
  }

  TO_STRING_KV(K_(raw_param_idx), K_(question_mark_idx));
};

struct QuestionMarkDefNameCtx
{
  common::ObIAllocator &allocator_;
  char **name_;
  int64_t count_;

  QuestionMarkDefNameCtx(common::ObIAllocator &allocator)
    : allocator_(allocator),
      name_(nullptr),
      count_(0)
  {}
  int assign(const char **name, const int64_t count);
  void reset()
  {
    if (nullptr != name_) {
      allocator_.free(name_);
      name_ = nullptr;
    }
    count_ = 0;
  }

  TO_STRING_KV(K_(name), K_(count));
  NEED_SERIALIZE_AND_DESERIALIZE;
};

typedef common::ObSEArray<FixedParamValue, 8> FixedParamValueArray;
typedef common::ObSEArray<DynamicParamInfo, 8> DynamicParamInfoArray;
typedef common::ObSEArray<FixedParamValueArray, 8> PatternConstConsList;

struct ObUDRInfo
{
public:
  enum RuleStatus
  {
    INVALID_STATUS = 0,
    ENABLE_STATUS,
    DISABLE_STATUS,
    DELETE_STATUS,
    MAX_STATUS,
  };

  ObUDRInfo()
  : tenant_id_(OB_INVALID_ID),
    pattern_digest_(0),
    rule_id_(OB_INVALID_ID),
    rule_version_(OB_INVALID_VERSION),
    rule_status_(INVALID_STATUS),
    coll_type_(common::CS_TYPE_INVALID),
    db_name_(),
    rule_name_(),
    pattern_(),
    replacement_(),
    normalized_pattern_(),
    fixed_param_infos_str_(),
    dynamic_param_infos_str_(),
    question_mark_ctx_str_() {}
  virtual ~ObUDRInfo() {}
  int assign(const ObUDRInfo &other);

  VIRTUAL_TO_STRING_KV(K_(tenant_id),
                       K_(pattern_digest),
                       K_(rule_id),
                       K_(rule_version),
                       K_(rule_status),
                       K_(coll_type),
                       K_(db_name),
                       K_(rule_name),
                       K_(pattern),
                       K_(replacement),
                       K_(normalized_pattern),
                       K_(fixed_param_infos_str),
                       K_(dynamic_param_infos_str),
                       K_(question_mark_ctx_str));

  uint64_t tenant_id_;
  uint64_t pattern_digest_;
  int64_t rule_id_;
  int64_t rule_version_;
  RuleStatus rule_status_;
  ObCollationType coll_type_;
  common::ObString db_name_;
  common::ObString rule_name_;
  common::ObString pattern_;
  common::ObString replacement_;
  common::ObString normalized_pattern_;
  common::ObString fixed_param_infos_str_;
  common::ObString dynamic_param_infos_str_;
  common::ObString question_mark_ctx_str_;
};

} // namespace sql end
} // namespace oceanbase end

#endif