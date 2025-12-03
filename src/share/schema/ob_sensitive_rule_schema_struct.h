
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

#ifndef _OB_OCEANBASE_SCHEMA_SENSITIVE_RULE_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_SENSITIVE_RULE_SCHEMA_STRUCT_H

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObSensitiveFieldItem final
{
  OB_UNIS_VERSION(1);
public:
  ObSensitiveFieldItem()
    : table_id_(common::OB_INVALID_ID),
      column_id_(common::OB_INVALID_ID)
  {}

  ObSensitiveFieldItem(const uint64_t table_id, const uint64_t column_id)
    : table_id_(table_id),
      column_id_(column_id)
  {}

  bool operator==(const ObSensitiveFieldItem &other) const {
    return table_id_ == other.table_id_ && column_id_ == other.column_id_;
  }

  ~ObSensitiveFieldItem() {}

  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    column_id_ = common::OB_INVALID_ID;
  }

  DECLARE_TO_STRING;

  uint64_t table_id_;
  uint64_t column_id_;
};

class ObTenantSensitiveRuleId : public ObTenantCommonSchemaId
{
  OB_UNIS_VERSION(1);
public:
  ObTenantSensitiveRuleId(): ObTenantCommonSchemaId() {}
  ObTenantSensitiveRuleId(const uint64_t tenant_id, const uint64_t sensitive_rule_id)
    : ObTenantCommonSchemaId(tenant_id, sensitive_rule_id) {}
};

class ObSensitiveColumnSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObSensitiveColumnSchema();
  ObSensitiveColumnSchema(const ObSensitiveColumnSchema &src_schema);
  explicit ObSensitiveColumnSchema(common::ObIAllocator *allocator);
  ~ObSensitiveColumnSchema();
  ObSensitiveColumnSchema &operator=(const ObSensitiveColumnSchema &src_schema);
  int assign(const ObSensitiveColumnSchema &src);
  // set methods
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_sensitive_rule_id(const uint64_t sensitive_rule_id) { sensitive_rule_id_ = sensitive_rule_id; }
  OB_INLINE void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE void set_column_id(const uint64_t column_id) { column_id_ = column_id; }
  OB_INLINE void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  // get methods
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_sensitive_rule_id() const { return sensitive_rule_id_; }
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE uint64_t get_column_id() const { return column_id_; }
  OB_INLINE uint64_t get_schema_version() const { return schema_version_; }
  // virtual methods
  void reset() override;
  bool is_valid() const override;
  int64_t get_convert_size() const override;
  // other methods
  TO_STRING_KV(K_(tenant_id), K_(sensitive_rule_id), K_(table_id), K_(column_id));
private:
  uint64_t tenant_id_;
  uint64_t sensitive_rule_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  uint64_t schema_version_;
};

class ObSensitiveRuleSchema : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObSensitiveRuleSchema();
  ObSensitiveRuleSchema(const ObSensitiveRuleSchema &src_schema);
  explicit ObSensitiveRuleSchema(common::ObIAllocator *allocator);
  ~ObSensitiveRuleSchema();
  ObSensitiveRuleSchema &operator=(const ObSensitiveRuleSchema &src_schema);
  int assign(const ObSensitiveRuleSchema &src);
  // set methods
  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_sensitive_rule_id(const uint64_t sensitive_rule_id) { sensitive_rule_id_ = sensitive_rule_id; }
  OB_INLINE void set_protection_policy(const uint64_t policy) { protection_policy_ = policy; }
  OB_INLINE void set_enabled(const bool enabled) { enabled_ = enabled; }
  OB_INLINE void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_name_case_mode(const common::ObNameCaseMode name_case_mode) { name_case_mode_ = name_case_mode; }
  OB_INLINE int set_sensitive_rule_name(const char *sensitive_rule_name) { return deep_copy_str(sensitive_rule_name, sensitive_rule_name_); }
  OB_INLINE int set_sensitive_rule_name(const common::ObString &sensitive_rule_name) { return deep_copy_str(sensitive_rule_name, sensitive_rule_name_); }
  OB_INLINE int set_method(const char *method) { return deep_copy_str(method, method_); }
  OB_INLINE int set_method(const common::ObString &method) { return deep_copy_str(method, method_); }
  OB_INLINE int set_sensitive_field_items(const ObIArray<ObSensitiveFieldItem> &sensitive_field_items) { return sensitive_field_items_.assign(sensitive_field_items); }
  OB_INLINE int add_sensitive_field_item(ObSensitiveFieldItem &item) { return sensitive_field_items_.push_back(item); }
  OB_INLINE int add_sensitive_field_item(uint64_t table_id, uint64_t column_id)
  {
    ObSensitiveFieldItem item(table_id, column_id);
    return sensitive_field_items_.push_back(item);
  }
  // get methods
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_sensitive_rule_id() const { return sensitive_rule_id_; }
  OB_INLINE ObTenantSensitiveRuleId get_tenant_sensitive_rule_id() const { return ObTenantSensitiveRuleId(tenant_id_, sensitive_rule_id_); }
  OB_INLINE uint64_t get_protection_policy() const { return protection_policy_; }
  OB_INLINE uint64_t get_schema_version() const { return schema_version_; }
  OB_INLINE common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  OB_INLINE const char *get_sensitive_rule_name() const { return extract_str(sensitive_rule_name_); }
  OB_INLINE const common::ObString &get_sensitive_rule_name_str() const { return sensitive_rule_name_; }
  OB_INLINE const char *get_method() const { return extract_str(method_); }
  OB_INLINE const common::ObString &get_method_str() const { return method_; }
  OB_INLINE bool get_enabled() const { return enabled_; }
  OB_INLINE const ObIArray<ObSensitiveFieldItem> &get_sensitive_field_items() const { return sensitive_field_items_; }
  OB_INLINE void reset_sensitive_field_items() { sensitive_field_items_.reuse(); }
  // virtual methods
  void reset() override;
  bool is_valid() const override;
  int64_t get_convert_size() const override;

  // other methods
  TO_STRING_KV(K_(tenant_id),
               K_(sensitive_rule_id),
               K_(protection_policy),
               K_(schema_version),
               K_(name_case_mode),
               K_(sensitive_rule_name),
               K_(method),
               K_(enabled),
               K_(sensitive_field_items));
private:
  bool enabled_;
  uint64_t tenant_id_;
  uint64_t sensitive_rule_id_;
  uint64_t protection_policy_;
  uint64_t schema_version_;
  common::ObNameCaseMode name_case_mode_; // no need to serialize
  common::ObString sensitive_rule_name_;
  common::ObString method_;
  common::ObSArray<ObSensitiveFieldItem> sensitive_field_items_;
};

struct ObSensitiveRulePrivSortKey
{
  ObSensitiveRulePrivSortKey() : tenant_id_(OB_INVALID_ID), user_id_(OB_INVALID_ID) {}
  ObSensitiveRulePrivSortKey(const uint64_t tenant_id,
                             const uint64_t user_id,
                             const common::ObString &sensitive_rule)
    : tenant_id_(tenant_id), user_id_(user_id), sensitive_rule_(sensitive_rule) {}

  bool operator==(const ObSensitiveRulePrivSortKey &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
           && (user_id_ == rhs.user_id_)
           && (sensitive_rule_ == rhs.sensitive_rule_);
  }

  bool operator!=(const ObSensitiveRulePrivSortKey &rhs) const
  {
    return !(*this == rhs);
  }

  bool operator<(const ObSensitiveRulePrivSortKey &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (false == bret && tenant_id_ == rhs.tenant_id_) {
      bret = user_id_ < rhs.user_id_;
      if (false == bret && user_id_ == rhs.user_id_) {
        bret = sensitive_rule_ < rhs.sensitive_rule_;
      }
    }
    return bret;
  }

  OB_INLINE uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
    hash_ret = common::murmurhash(sensitive_rule_.ptr(), sensitive_rule_.length(), hash_ret);
    return hash_ret;
  }

  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_) && (user_id_ != common::OB_INVALID_ID);
  }

  int deep_copy(const ObSensitiveRulePrivSortKey &src, common::ObIAllocator &allocator) {
    int ret = OB_SUCCESS;
    tenant_id_ = src.tenant_id_;
    user_id_ = src.user_id_;
    if (OB_FAIL(common::ob_write_string(allocator, src.sensitive_rule_, sensitive_rule_))) {
      SHARE_SCHEMA_LOG(WARN, "fail to deep copy sensitive rule", KR(ret), K(src.sensitive_rule_));
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(sensitive_rule));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString sensitive_rule_;
};

class ObSensitiveRulePriv : public ObSchema, public ObPriv
{
  OB_UNIS_VERSION(1);
public:
  ObSensitiveRulePriv() : ObSchema(), ObPriv() {}
  explicit ObSensitiveRulePriv(common::ObIAllocator *allocator) : ObSchema(allocator), ObPriv() {}
  ObSensitiveRulePriv(const ObSensitiveRulePriv &other) : ObSchema(), ObPriv() { *this = other; }
  virtual ~ObSensitiveRulePriv() {}

  ObSensitiveRulePriv& operator=(const ObSensitiveRulePriv &other);
  bool operator==(const ObSensitiveRulePriv &other);

  // for sort
  ObSensitiveRulePrivSortKey get_sort_key() const
  { return ObSensitiveRulePrivSortKey(tenant_id_, user_id_, sensitive_rule_); }
  static bool cmp(const ObSensitiveRulePriv *lhs, const ObSensitiveRulePriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() < rhs->get_sort_key() : false; }
  static bool cmp_sort_key(const ObSensitiveRulePriv *lhs, const ObSensitiveRulePrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() < sort_key : false; }
  static bool equal(const ObSensitiveRulePriv *lhs, const ObSensitiveRulePriv *rhs)
  { return (NULL != lhs && NULL != rhs) ? lhs->get_sort_key() == rhs->get_sort_key() : false; }
  static bool equal_sort_key(const ObSensitiveRulePriv *lhs, const ObSensitiveRulePrivSortKey &sort_key)
  { return NULL != lhs ? lhs->get_sort_key() == sort_key : false; }

  // set methods
  OB_INLINE int set_sensitive_rule_name(const char *sensitive_rule) { return deep_copy_str(sensitive_rule, sensitive_rule_); }
  OB_INLINE int set_sensitive_rule_name(const common::ObString &sensitive_rule) { return deep_copy_str(sensitive_rule, sensitive_rule_); }

  // get methods
  OB_INLINE const char *get_sensitive_rule_name() const { return extract_str(sensitive_rule_); }
  OB_INLINE const common::ObString &get_sensitive_rule_name_str() const { return sensitive_rule_; }

  TO_STRING_KV(K_(tenant_id), K_(sensitive_rule), "privileges", ObPrintPrivSet(priv_set_));

  // virtual methods
  bool is_valid() const override { return ObSchema::is_valid() && ObPriv::is_valid(); }
  void reset() override;
  int64_t get_convert_size() const override;

private:
  common::ObString sensitive_rule_;
};

template<class T, class V>
struct ObGetSensitiveRulePrivKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetSensitiveRulePrivKey<ObSensitiveRulePrivSortKey, ObSensitiveRulePriv *>
{
  ObSensitiveRulePrivSortKey operator()(const ObSensitiveRulePriv *sensitive_rule_priv) const
  {
    ObSensitiveRulePrivSortKey key;
    return NULL != sensitive_rule_priv ? sensitive_rule_priv->get_sort_key() : key;
  }
};

} // end namespace schema
} // end namespace share
} // end namespace oceanbase

#endif // _OB_OCEANBASE_SCHEMA_SENSITIVE_RULE_SCHEMA_STRUCT_H