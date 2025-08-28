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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_CCL_RULE_MGR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_CCL_RULE_MGR_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_define.h"
#include "share/schema/ob_ccl_schema_struct.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
enum CclRuleContainsInfo
{
  NONE,               //ccl rule without database, table, dml info
  DML,                //ccl rule without database and table, with dml info
  DATABASE_AND_TABLE  //ccl rule with database, table and dml info
};

class ObCCLRuleNameHashKey
{
public:
  ObCCLRuleNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID), name_case_mode_(common::OB_NAME_CASE_INVALID), ccl_rule_name_()
  {
  }
  ObCCLRuleNameHashKey(uint64_t tenant_id, const common::ObNameCaseMode mode, common::ObString ccl_rule_name)
    : tenant_id_(tenant_id), name_case_mode_(mode), ccl_rule_name_(ccl_rule_name)
  {
  }
  ~ObCCLRuleNameHashKey()
  {
  }
  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, ccl_rule_name_, hash_ret);
    return hash_ret;
  }
  bool operator == (const ObCCLRuleNameHashKey &other) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
    return (tenant_id_ == other.tenant_id_) &&
           (name_case_mode_ == other.name_case_mode_) &&
           (0 == name_cmp.compare(ccl_rule_name_, other.ccl_rule_name_));
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_ccl_rule_name(const common::ObString &ccl_rule_name) { ccl_rule_name_ = ccl_rule_name;}
  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObString &get_ccl_rule_name() const { return ccl_rule_name_; }
private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString ccl_rule_name_;
};

template<class T, class V>
struct ObGetCCLRuleKey
{
  void operator()(const T &t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetCCLRuleKey<ObCCLRuleNameHashKey, ObSimpleCCLRuleSchema *>
{
  ObCCLRuleNameHashKey operator()(const ObSimpleCCLRuleSchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObCCLRuleNameHashKey()
        : ObCCLRuleNameHashKey(schema->get_tenant_id(),
                               schema->get_name_case_mode(),
                               schema->get_ccl_rule_name());
  }
};

template<>
struct ObGetCCLRuleKey<uint64_t, ObSimpleCCLRuleSchema *>
{
  uint64_t operator()(const ObSimpleCCLRuleSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_ccl_rule_id();
  }
};

class ObCCLRuleMgr
{
public:
  typedef common::ObSortedVector<ObSimpleCCLRuleSchema *> CCLRuleInfos;
  typedef common::hash::ObPointerHashMap<ObCCLRuleNameHashKey,
                                         ObSimpleCCLRuleSchema *,
                                         ObGetCCLRuleKey, 128> ObCCLRuleNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t,
                                         ObSimpleCCLRuleSchema *,
                                         ObGetCCLRuleKey, 128> ObCCLRuleIdMap;
  typedef CCLRuleInfos::iterator CCLRuleIter;
  typedef CCLRuleInfos::const_iterator ConstCCLRuleIter;
public:
  ObCCLRuleMgr();
  explicit ObCCLRuleMgr(common::ObIAllocator &allocator);
  virtual ~ObCCLRuleMgr();
  ObCCLRuleMgr &operator=(const ObCCLRuleMgr &other);
  int init();

  inline bool is_inited() { return is_inited_; }
  void reset();
  int assign(const ObCCLRuleMgr &other);
  int deep_copy(const ObCCLRuleMgr &other);
  int add_ccl_rule(const ObSimpleCCLRuleSchema &schema, const ObNameCaseMode mode);
  int get_schema_by_id(const uint64_t ccl_rule_id,
                       const ObSimpleCCLRuleSchema *&schema) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const ObNameCaseMode mode,
                         const common::ObString &name,
                         const ObSimpleCCLRuleSchema *&schema) const;
  inline int get_ccl_rule_count() const
  {
    return is_inited_ ? ccl_rules_.size() + ccl_rules_specified_by_dml_.size()
           + ccl_rules_specified_by_database_table_dml_.size() : 0;
  }
  int del_ccl_rule(const ObTenantCCLRuleId &id);

  CCLRuleInfos * get_ccl_rule_belong_ccl_rule_infos(CclRuleContainsInfo contians_info);

  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObSimpleCCLRuleSchema *> &schemas) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObSimpleCCLRuleSchema *> &schemas,
                            const CCLRuleInfos &ccl_rule_infos) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

private:
  int add_ccl_rule(const ObSimpleCCLRuleSchema &schema, const ObNameCaseMode mode, CCLRuleInfos &ccl_rule_infos);
  int del_ccl_rule_from_ccl_rule_infos(const ObTenantCCLRuleId &id, CCLRuleInfos &ccl_rule_infos, ObSimpleCCLRuleSchema *&schema);
  inline static bool compare_ccl_rule(const ObSimpleCCLRuleSchema *lhs,
                                     const ObSimpleCCLRuleSchema *rhs);
  inline static bool equal_ccl_rule(const ObSimpleCCLRuleSchema *lhs,
                                   const ObSimpleCCLRuleSchema *rhs);
  inline static bool compare_with_tenant_ccl_rule_id(const ObSimpleCCLRuleSchema *lhs,
                                                    const ObTenantCCLRuleId &tenant_ccl_rule_id);
  inline static bool equal_with_tenant_ccl_rule_id(const ObSimpleCCLRuleSchema *lhs,
                                                  const ObTenantCCLRuleId &tenant_ccl_rule_id);
  
private:
  static const char *CCL_RULE_MGR;
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  //There are 3-level CCLRuleInfos
  //ccl_rules_ store rules that ars CclRuleContainsInfo::NONE, used before sql get into sql engine
  //ccl_rules_specified_by_dml_ store rules that ars CclRuleContainsInfo::DML, used after parse sql
  //ccl_rules_specified_by_database_table_dml_ store rules that ars CclRuleContainsInfo::DATABASE_AND_TABLE, used after resolve sql
  CCLRuleInfos ccl_rules_;
  CCLRuleInfos ccl_rules_specified_by_dml_;
  CCLRuleInfos ccl_rules_specified_by_database_table_dml_;
  ObCCLRuleNameMap ccl_rule_name_map_;
  ObCCLRuleIdMap ccl_rule_id_map_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_CCL_RULE_MGR_H_
