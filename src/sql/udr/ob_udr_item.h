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


#ifndef OB_SQL_UDR_OB_UDR_ITEM_H_
#define OB_SQL_UDR_OB_UDR_ITEM_H_
#include "sql/udr/ob_udr_struct.h"
#include "sql/udr/ob_udr_context.h"

namespace oceanbase
{
namespace sql
{

class ObUDRItem : protected ObUDRInfo
{
public:
  ObUDRItem(common::ObIAllocator &allocator)
  : ObUDRInfo(),
    allocator_(allocator),
    ref_count_(0),
    fixed_param_deserialize_buf_(nullptr),
    dynamic_param_deserialize_buf_(nullptr),
    question_mark_deserialize_buf_(nullptr),
    fixed_param_info_array_(),
    dynamic_param_info_array_(),
    question_mark_def_name_ctx_(allocator) {}
  virtual ~ObUDRItem();

#define DEFINE_SIMPLE_SETTER(name, type) \
OB_INLINE void set_##name(type name) { name##_ = name; }
#define DEFINE_SIMPLE_GETTER(ret_type, name) \
OB_INLINE ret_type get_##name() const { return name##_; }
#define DEFINE_STR_SETTER(name) \
OB_INLINE int set_##name(const common::ObString &name) \
{                                                      \
  return ob_write_string(allocator_, name, name##_);   \
}
#define DEFINE_STR_GETTER(name) \
OB_INLINE const common::ObString& get_##name() const { return name##_; }

  void reset();
  DEFINE_SIMPLE_SETTER(tenant_id, uint64_t);
  DEFINE_SIMPLE_SETTER(pattern_digest, uint64_t);
  DEFINE_SIMPLE_SETTER(rule_id, int64_t);
  DEFINE_SIMPLE_SETTER(rule_version, int64_t);
  DEFINE_SIMPLE_SETTER(coll_type, ObCollationType);
  DEFINE_STR_SETTER(db_name);
  DEFINE_STR_SETTER(rule_name);
  DEFINE_STR_SETTER(pattern);
  DEFINE_STR_SETTER(replacement);
  DEFINE_STR_SETTER(normalized_pattern);
  DEFINE_STR_SETTER(fixed_param_infos_str);
  DEFINE_STR_SETTER(dynamic_param_infos_str);
  DEFINE_STR_SETTER(question_mark_ctx_str);

  DEFINE_SIMPLE_GETTER(uint64_t, tenant_id);
  DEFINE_SIMPLE_GETTER(uint64_t, pattern_digest);
  DEFINE_SIMPLE_GETTER(int64_t, rule_id);
  DEFINE_SIMPLE_GETTER(int64_t, rule_version);
  DEFINE_SIMPLE_GETTER(ObCollationType, coll_type);
  DEFINE_STR_GETTER(db_name);
  DEFINE_STR_GETTER(rule_name);
  DEFINE_STR_GETTER(pattern);
  DEFINE_STR_GETTER(replacement);
  DEFINE_STR_GETTER(normalized_pattern);
  DEFINE_STR_GETTER(fixed_param_infos_str);
  DEFINE_STR_GETTER(dynamic_param_infos_str);
  DEFINE_STR_GETTER(question_mark_ctx_str);

  inline const FixedParamValueArray &get_fixed_param_value_array() const { return fixed_param_info_array_; }
  inline const DynamicParamInfoArray &get_dynamic_param_info_array() const { return dynamic_param_info_array_; }
  inline const QuestionMarkDefNameCtx *get_question_mark_def_name_ctx() const { return &question_mark_def_name_ctx_; }

  inline int64_t get_ref_count() const { return ref_count_; }
  inline bool is_enable_status() const { return ENABLE_STATUS == rule_status_; }
  void set_rule_status(const RuleStatus status) { ATOMIC_STORE(&rule_status_, status); }
  RuleStatus get_rule_status() const { return ATOMIC_LOAD(&rule_status_); }
  int64_t inc_ref_count();
  int64_t dec_ref_count();
  int deep_copy(const ObUDRInfo &rule_info);
  template<typename T>
  int deserialize_with_hex_str(const common::ObString &str, T &infos, char *&deserialize_buf);
  int deserialize_fixed_param_info_array(const common::ObString str);
  int deserialize_dynamic_param_info_array(const common::ObString str);
  int deserialize_question_mark_by_name_ctx(const common::ObString str);

#undef DEFINE_STR_SETTER
#undef DEFINE_STR_GETTER
#undef DEFINE_SIMPLE_SETTER
#undef DEFINE_SIMPLE_GETTER

  VIRTUAL_TO_STRING_KV(K_(ref_count),
                       K_(fixed_param_info_array),
                       K_(dynamic_param_info_array),
                       K_(question_mark_def_name_ctx));

private:
  common::ObIAllocator &allocator_;
  int64_t ref_count_;
  char *fixed_param_deserialize_buf_;
  char *dynamic_param_deserialize_buf_;
  char *question_mark_deserialize_buf_;
  FixedParamValueArray fixed_param_info_array_;
  DynamicParamInfoArray dynamic_param_info_array_;
  QuestionMarkDefNameCtx question_mark_def_name_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDRItem);
};

} // namespace sql end
} // namespace oceanbase end

#endif