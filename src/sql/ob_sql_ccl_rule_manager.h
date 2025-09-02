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

#ifndef OCEANBASE_SQL_OB_SQL_CCL_RULE_MANAGER_H_
#define OCEANBASE_SQL_OB_SQL_CCL_RULE_MANAGER_H_

#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_rwlock.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_ccl_rule_mgr.h"
#include "sql/ob_sql_context.h"


using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
class ObCCLDatabaseTableHashWrapper {
public:
  ObCCLDatabaseTableHashWrapper()
      : tenant_id_(common::OB_INVALID_ID),
        name_case_mode_(common::OB_NAME_CASE_INVALID) {}
  ObCCLDatabaseTableHashWrapper(uint64_t tenant_id,
                                const common::ObNameCaseMode mode,
                                const common::ObString &name)
      : tenant_id_(tenant_id), name_case_mode_(mode), name_(name) {}
  ~ObCCLDatabaseTableHashWrapper() {}

  inline uint64_t hash() const {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    common::ObCollationType cs_type =
        ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, name_, hash_ret);
    return hash_ret;
  }
  int hash(uint64_t &hash_val) const {
    hash_val = hash();
    return OB_SUCCESS;
  }
  inline bool operator==(const ObCCLDatabaseTableHashWrapper &rv) const {
    ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
    return (tenant_id_ == rv.tenant_id_) &&
           (name_case_mode_ == rv.name_case_mode_) &&
           (0 == name_cmp.compare(name_, rv.name_));
  }

  TO_STRING_KV(K_(tenant_id), K_(name_case_mode), K_(name));

private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString name_; // database name or table name
};

class ObFormatSQLIDCCLRuleKey
{
public:
  ObFormatSQLIDCCLRuleKey() : ccl_rule_id_(0)
  {}
  ObFormatSQLIDCCLRuleKey(const uint64_t ccl_rule_id, const ObString &format_sqlid) :
    ccl_rule_id_(ccl_rule_id),
    format_sqlid_(format_sqlid)
  {}

  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&ccl_rule_id_, sizeof(ccl_rule_id_), 0);
    hash_ret = common::murmurhash(format_sqlid_.ptr(), format_sqlid_.length(), hash_ret);
    return hash_ret;
  };

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  };

  int compare(const ObFormatSQLIDCCLRuleKey &r) const
  {
    int cmp = 0;
    if (ccl_rule_id_ < r.ccl_rule_id_) {
      cmp = -1;
    } else if (ccl_rule_id_ == r.ccl_rule_id_) {
      cmp = format_sqlid_.compare(r.format_sqlid_);
    } else {
      cmp = 1;
    }
    return cmp;
  }
  bool operator==(const ObFormatSQLIDCCLRuleKey &other) const
  {
    return 0 == compare(other);
  }
  bool operator!=(const ObFormatSQLIDCCLRuleKey &other) const
  {
    return !operator==(other);
  }
  bool operator<(const ObFormatSQLIDCCLRuleKey &other) const
  {
    return -1 == compare(other);
  }
  TO_STRING_KV(K_(ccl_rule_id), K_(format_sqlid));

public:
  uint64_t ccl_rule_id_{0};
  ObString format_sqlid_;
};
class ObCCLRuleConcurrencyValueWrapper
{
public:
  ObCCLRuleConcurrencyValueWrapper() : ccl_rule_id_(0), format_sqlid_(), max_concurrency_(0), cur_concurrency_(0)
  {}
  ObCCLRuleConcurrencyValueWrapper(uint64_t ccl_rule_id, const ObString& format_sqlid, uint64_t max_concurrency) :
    ccl_rule_id_(ccl_rule_id),
    format_sqlid_(format_sqlid),
    max_concurrency_(max_concurrency),
    cur_concurrency_(0)
  {}

  TO_STRING_KV(K_(ccl_rule_id), K_(format_sqlid), K_(max_concurrency), K_(cur_concurrency));

  inline bool reach_concurrent_limit() { return max_concurrency_ == cur_concurrency_; }
  uint64_t ccl_rule_id_{0};
  ObString format_sqlid_;
  uint64_t max_concurrency_{0};
  uint64_t cur_concurrency_{0};
};

class ObSQLCCLRuleLevelConcurrencyMapWrapper
{
public:
  ObSQLCCLRuleLevelConcurrencyMapWrapper() {};
  ~ObSQLCCLRuleLevelConcurrencyMapWrapper();

  int init(const ObMemAttr &bucket_attr);

  int inc_concurrency(const ObFormatSQLIDCCLRuleKey & key,
                      int max_concurrency,
                      ObIArray<ObCCLRuleConcurrencyValueWrapper*>& matched_ccl_value_wrappers);
  int dec_concurrency(ObCCLRuleConcurrencyValueWrapper *p_value_wrapper);

  common::hash::ObHashMap<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*>& get_concurrency_map() {
    return concurrency_map_;
  }

private:
  int insert(const ObFormatSQLIDCCLRuleKey &key, 
             int max_concurrency,
             ObCCLRuleConcurrencyValueWrapper *&p_concurrency);

private:
  ObMemAttr bucket_attr_;
  ObFIFOAllocator alloc_;
  common::hash::ObHashMap<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*> concurrency_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSQLCCLRuleLevelConcurrencyMapWrapper);
};

class ObCCLRuleIncRefAtomicOp
{
public:
  ObCCLRuleIncRefAtomicOp(): value_(nullptr) {}
  
  void operator()(common::hash::HashMapPair<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*> &entry);

  bool try_inc_ref_count(ObCCLRuleConcurrencyValueWrapper *&value);

  ObCCLRuleConcurrencyValueWrapper *get_value() const { return value_; }
  ObCCLRuleConcurrencyValueWrapper*& get_value_for_update() { return value_; }
protected:
  ObCCLRuleConcurrencyValueWrapper *value_;
};

class ObCCLRuleDelAtomicOp
{
public:
  ObCCLRuleDelAtomicOp() = default;
  ~ObCCLRuleDelAtomicOp() = default;
  
  bool operator()(common::hash::HashMapPair<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*> &entry);
};

// Different from src/share/schema/ob_ccl_rule_mgr.h, which is used in schema module
// SQL need get ccl rule schema info by above class
// then fit them into below class to do future real concurrent sql rate
// This class will maintain 2 map:
// rule_level_concurrency_map_wrapper_: Key: ObFormatSQLIDCCLRuleKey(ccl_rule_id, "") -> Value: ObCCLRuleConcurrencyValueWrapper
// format_sqlid_level_concurrency_map_wrapper_: Key: ObFormatSQLIDCCLRuleKey(ccl_rule_id, format_sql_id) -> Value: ObCCLRuleConcurrencyValueWrapper
class ObSQLCCLRuleManager
{
public:
  ObSQLCCLRuleManager();
  virtual ~ObSQLCCLRuleManager() = default;

  struct ObCCLWhitelistRule {
    const char* words[3]; // single whitelist rule
  };

  static constexpr ObCCLWhitelistRule CCL_WHITELIST_RULES[] = {
    {{"DROP", "CONCURRENT_LIMITING_RULE", nullptr}},
    // {{"ALTER", "CONCURRENT_LIMITING_RULE", nullptr}},
    {{nullptr}} // end tag
  };

  int init(uint64_t tenant_id);
  inline bool is_inited() { return inited_; }

  static int mtl_new(ObSQLCCLRuleManager* &sql_ccl_rule_mgr);
  static int mtl_init(ObSQLCCLRuleManager* &sql_ccl_rule_mgr);
  static void mtl_destroy(ObSQLCCLRuleManager* &sql_ccl_rule_mgr);

  int match_ccl_rule(ObIAllocator &alloc, const ObString &username, const ObString &sql,
                     ObCCLAffectDMLType sql_dml_type,
                     const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_databases,
                     const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_tables,
                     ObCCLRuleSchema &ccl_rule, bool &match) const;

  int match_ccl_rule_with_sql(ObIAllocator &alloc,
                              const ObString& user_name,
                              ObSqlCtx &sql_ctx,
                              const ObString &sql,
                              bool is_ps_mode,
                              const ObIArray<const common::ObObjParam *> &param_store,
                              const ObString &format_sqlid,
                              CclRuleContainsInfo contians_info,
                              ObCCLAffectDMLType sql_dml_type,
                              const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_databases,
                              const common::hash::ObHashSet<ObCCLDatabaseTableHashWrapper> &sql_relate_tables,
                              uint64_t &limited_by_ccl_rule_id,
                              ObSqlString &rconstruct_sql);

  void dec_rule_level_concurrency(ObCCLRuleConcurrencyValueWrapper* p_value_wrapper);
  void dec_format_sqlid_level_concurrency(ObCCLRuleConcurrencyValueWrapper* p_value_wrapper);

  int is_whitelist_sql(const ObString &sql, bool &match) const;

  common::hash::ObHashMap<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*>& get_rule_level_concurrency_map() {
    return rule_level_concurrency_map_wrapper_.get_concurrency_map();
  }

  common::hash::ObHashMap<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*>& get_format_sqlid_level_concurrency_map() {
    return format_sqlid_level_concurrency_map_wrapper_.get_concurrency_map();
  }

private:
  int match_keywords_in_sql(const ObString &sql, 
                            const ObIArray<ObString> &ccl_keywords_array, bool &match, bool case_sensitive = true) const;
  int init_whitelist();
  inline int
  inc_rule_level_concurrency(const ObFormatSQLIDCCLRuleKey &key,
                             int max_concurrency,
                             ObIArray<ObCCLRuleConcurrencyValueWrapper *>
                                 &matched_ccl_value_wrappers) {
    return rule_level_concurrency_map_wrapper_.inc_concurrency(
      key, max_concurrency, matched_ccl_value_wrappers);
  }
  inline int inc_format_sqlid_level_concurrency(
      const ObFormatSQLIDCCLRuleKey &key,
      int max_concurrency,
      ObIArray<ObCCLRuleConcurrencyValueWrapper *>
          &matched_ccl_value_wrappers) {
    return format_sqlid_level_concurrency_map_wrapper_.inc_concurrency(
      key, max_concurrency, matched_ccl_value_wrappers);
  }

private:
  bool inited_{false};
  uint64_t tenant_id_{0};
  ObSQLCCLRuleLevelConcurrencyMapWrapper rule_level_concurrency_map_wrapper_;
  ObSQLCCLRuleLevelConcurrencyMapWrapper format_sqlid_level_concurrency_map_wrapper_;
  ObSEArray<ObSEArray<ObString, 2>, 2> whitelist_keywords_array_;
  DISALLOW_COPY_AND_ASSIGN(ObSQLCCLRuleManager);
};

}
}
#endif
