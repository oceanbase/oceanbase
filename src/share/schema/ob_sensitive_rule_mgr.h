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

#ifndef OB_SENSITIVE_RULE_MGR_H
#define OB_SENSITIVE_RULE_MGR_H

#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_vector.h"
#include "share/schema/ob_sensitive_rule_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSensitiveRuleNameHashKey
{
public:
  ObSensitiveRuleNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      name_case_mode_(common::OB_NAME_CASE_INVALID),
      name_()
  {}
  ObSensitiveRuleNameHashKey(uint64_t tenant_id,
                             common::ObNameCaseMode mode,
                             common::ObString name)
    : tenant_id_(tenant_id),
      name_case_mode_(mode),
      name_(name)
  {}

  ~ObSensitiveRuleNameHashKey() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, name_, hash_ret);
    return hash_ret;
  }
  inline bool operator == (const ObSensitiveRuleNameHashKey &rv) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
    return (tenant_id_ == rv.tenant_id_)
           && (name_case_mode_ == rv.name_case_mode_)
           && (0 == name_cmp.compare(name_ ,rv.name_));
  }
private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString name_;
};

template<class T, class V>
struct ObGetSensitiveRuleKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetSensitiveRuleKey<ObSensitiveRuleNameHashKey, ObSensitiveRuleSchema *>
{
  ObSensitiveRuleNameHashKey operator() (const ObSensitiveRuleSchema *schema) const {
    return OB_ISNULL(schema)
           ? ObSensitiveRuleNameHashKey()
           : ObSensitiveRuleNameHashKey(schema->get_tenant_id(),
                                        schema->get_name_case_mode(),
                                        schema->get_sensitive_rule_name_str());
  }
};


template<>
struct ObGetSensitiveRuleKey<uint64_t, ObSensitiveRuleSchema *>
{
  uint64_t operator()(const ObSensitiveRuleSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_sensitive_rule_id();
  }
};

class ObSensitiveColumnHashKey
{
public:
  ObSensitiveColumnHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      table_id_(common::OB_INVALID_ID),
      column_id_(common::OB_INVALID_ID)
  {}
  ObSensitiveColumnHashKey(uint64_t tenant_id, uint64_t table_id, uint64_t column_id)
    : tenant_id_(tenant_id),
      table_id_(table_id),
      column_id_(column_id)
  {}
  ~ObSensitiveColumnHashKey() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&column_id_, sizeof(uint64_t), hash_ret);
    return hash_ret;
  }
  inline bool operator == (const ObSensitiveColumnHashKey &rv) const
  {
    return (tenant_id_ == rv.tenant_id_)
           && (table_id_ == rv.table_id_)
           && (column_id_ == rv.column_id_);
  }
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t column_id_;
};

template<class T, class V>
struct ObGetSensitiveColumnKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetSensitiveColumnKey<ObSensitiveColumnHashKey, ObSensitiveColumnSchema *>
{
  ObSensitiveColumnHashKey operator() (const ObSensitiveColumnSchema *schema) const {
    return OB_ISNULL(schema)
           ? ObSensitiveColumnHashKey()
           : ObSensitiveColumnHashKey(schema->get_tenant_id(),
                                      schema->get_table_id(),
                                      schema->get_column_id());
  }
};


template<>
struct ObGetSensitiveColumnKey<uint64_t, ObSensitiveColumnSchema *>
{
  uint64_t operator()(const ObSensitiveColumnSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_sensitive_rule_id();
  }
};

class ObSensitiveRuleMgr
{
public:
  typedef common::ObSortedVector<ObSensitiveRuleSchema *> SensitiveRuleInfos;
  typedef common::hash::ObPointerHashMap<ObSensitiveRuleNameHashKey, ObSensitiveRuleSchema *,
                                         ObGetSensitiveRuleKey, 128> ObSensitiveRuleNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSensitiveRuleSchema *,
                                         ObGetSensitiveRuleKey, 128> ObSensitiveRuleIdMap;
  typedef common::hash::ObPointerHashMap<ObSensitiveColumnHashKey, ObSensitiveColumnSchema *,
                                         ObGetSensitiveColumnKey, 128> ObSensitiveColumnMap;
  typedef SensitiveRuleInfos::iterator SensitiveRuleIter;
  typedef SensitiveRuleInfos::const_iterator ConstSensitiveRuleIter;
  ObSensitiveRuleMgr();
  explicit ObSensitiveRuleMgr(common::ObIAllocator &allocator);
  virtual ~ObSensitiveRuleMgr();
  int init();
  void reset();
  ObSensitiveRuleMgr &operator = (const ObSensitiveRuleMgr &other);
  int assign(const ObSensitiveRuleMgr &other);
  int deep_copy(const ObSensitiveRuleMgr &other);
  int add_sensitive_rule(const ObSensitiveRuleSchema &schema, const common::ObNameCaseMode mode);
  int del_sensitive_rule(const ObTenantSensitiveRuleId &id);
  int get_schema_by_id(const uint64_t sensitive_rule_id,
                       const ObSensitiveRuleSchema *&schema) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const common::ObNameCaseMode mode,
                         const common::ObString &name,
                         const ObSensitiveRuleSchema *&schema) const;
  int get_schema_by_column(const uint64_t tenant_id,
                           const uint64_t table_id,
                           const uint64_t column_id,
                           const ObSensitiveRuleSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObSensitiveRuleSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  inline static bool schema_cmp(const ObSensitiveRuleSchema *lhs,
                                const ObSensitiveRuleSchema *rhs) {
    return lhs->get_tenant_id() != rhs->get_tenant_id()
           ? lhs->get_tenant_id() < rhs->get_tenant_id()
           : lhs->get_sensitive_rule_id() < rhs->get_sensitive_rule_id();
  }
  inline static bool schema_equal(const ObSensitiveRuleSchema *lhs,
                                  const ObSensitiveRuleSchema *rhs) {
    return lhs->get_tenant_id() == rhs->get_tenant_id()
           && lhs->get_sensitive_rule_id() == rhs->get_sensitive_rule_id();
  }
  static bool compare_with_tenant_sensitive_rule_id(const ObSensitiveRuleSchema *lhs,
                                                    const ObTenantSensitiveRuleId &id);
  static bool equal_to_tenant_sensitive_rule_id(const ObSensitiveRuleSchema *lhs,
                                                const ObTenantSensitiveRuleId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int update_column_map(ObSensitiveRuleSchema *new_schema);
  int erase_from_column_map(const uint64_t sensitive_rule_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  SensitiveRuleInfos schema_infos_;
  ObSensitiveRuleNameMap name_map_;
  ObSensitiveRuleIdMap id_map_;
  ObSensitiveColumnMap column_map_;
};

}  // ene namespace schema
}  // end namespace share
}  // end namespace oceanbase

#endif // OB_SENSITIVE_RULE_MGR_H
