
/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef _OB_OCEANBASE_SCHEMA_CCL_SCHEMA_STRUCT_H
#define _OB_OCEANBASE_SCHEMA_CCL_SCHEMA_STRUCT_H

#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {
namespace schema {

enum ObCCLAffectScope : uint8_t {
  RULE_LEVEL = 0,
  FORMAT_SQLID_LEVEL = 1,
};

enum ObCCLAffectDMLType : uint8_t {
  ALL = 0,
  SELECT = 1,
  UPDATE = 2,
  INSERT = 3,
  DELETE = 4,
};

struct ObTenantCCLRuleId {
  OB_UNIS_VERSION(1);

public:
  ObTenantCCLRuleId()
      : tenant_id_(common::OB_INVALID_ID), ccl_rule_id_(common::OB_INVALID_ID) {
  }
  ObTenantCCLRuleId(const uint64_t tenant_id, const uint64_t ccl_rule_id)
      : tenant_id_(tenant_id), ccl_rule_id_(ccl_rule_id) {}
  bool operator==(const ObTenantCCLRuleId &rhs) const {
    return (tenant_id_ == rhs.tenant_id_) && (ccl_rule_id_ == rhs.ccl_rule_id_);
  }
  bool operator!=(const ObTenantCCLRuleId &rhs) const {
    return !(*this == rhs);
  }
  bool operator<(const ObTenantCCLRuleId &rhs) const {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = ccl_rule_id_ < rhs.ccl_rule_id_;
    }
    return bret;
  }
  inline uint64_t hash() const {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret =
        common::murmurhash(&ccl_rule_id_, sizeof(ccl_rule_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const {
    return (tenant_id_ != common::OB_INVALID_ID) &&
           (ccl_rule_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(ccl_rule_id));
  uint64_t tenant_id_;
  uint64_t ccl_rule_id_;
};

class ObSimpleCCLRuleSchema : public ObSchema { // simple schema
  OB_UNIS_VERSION(1);

public:
  ObSimpleCCLRuleSchema();
  explicit ObSimpleCCLRuleSchema(common::ObIAllocator *allocator);
  ObSimpleCCLRuleSchema(const ObSimpleCCLRuleSchema &src_schema);
  virtual ~ObSimpleCCLRuleSchema();

  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

  ObSimpleCCLRuleSchema &operator=(const ObSimpleCCLRuleSchema &other);
  int assign(const ObSimpleCCLRuleSchema &other);
  bool operator==(const ObSimpleCCLRuleSchema &other) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_ccl_rule_id(uint64_t ccl_rule_id) {
    ccl_rule_id_ = ccl_rule_id;
  }
  inline void set_schema_version(uint64_t schema_version) {
    schema_version_ = schema_version;
  }
  inline int set_ccl_rule_name(const common::ObString &ccl_rule_name) {
    return deep_copy_str(ccl_rule_name, ccl_rule_name_);
  }
  inline void set_affect_for_all_databases(bool affect_for_all_databases) {
    affect_for_all_databases_ = affect_for_all_databases;
  }
  inline void set_affect_for_all_tables(bool affect_for_all_tables) {
    affect_for_all_tables_ = affect_for_all_tables;
  }
  inline void set_affect_dml(ObCCLAffectDMLType affect_dml) {
    affect_dml_ = affect_dml;
  }
  inline void set_name_case_mode(common::ObNameCaseMode mode) { name_case_mode_ = mode; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_ccl_rule_id() const { return ccl_rule_id_; }
  inline const common::ObString &get_ccl_rule_name() const {
    return ccl_rule_name_;
  }
  inline ObCCLAffectDMLType get_affect_dml() const { return affect_dml_; }
  inline bool affect_for_all_databases() const {
    return affect_for_all_databases_;
  }
  inline bool affect_for_all_tables() const { return affect_for_all_tables_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }

  inline ObTenantCCLRuleId get_sort_key() const {
    return ObTenantCCLRuleId(tenant_id_, ccl_rule_id_);
  }

  TO_STRING_KV(K_(tenant_id), K_(ccl_rule_id), K_(ccl_rule_name),
               K_(schema_version), K_(affect_for_all_databases),
               K_(affect_for_all_tables), K_(affect_dml), K_(name_case_mode));

private:
  uint64_t tenant_id_;
  uint64_t ccl_rule_id_;
  ObString ccl_rule_name_;
  int64_t schema_version_;
  bool affect_for_all_databases_;
  bool affect_for_all_tables_;
  ObCCLAffectDMLType affect_dml_; // 0: all, 1: select, 2: update, 3: insert, 4: delete
  common::ObNameCaseMode name_case_mode_; // default:OB_NAME_CASE_INVALID
};

class ObCCLRuleSchema : public ObSimpleCCLRuleSchema { // full schema
  OB_UNIS_VERSION(1);

public:
  ObCCLRuleSchema();
  explicit ObCCLRuleSchema(common::ObIAllocator *allocator);
  ObCCLRuleSchema(const ObCCLRuleSchema &src_schema);
  virtual ~ObCCLRuleSchema();
  ObCCLRuleSchema &operator=(const ObCCLRuleSchema &other);
  int assign(const ObCCLRuleSchema &other);
  bool operator==(const ObCCLRuleSchema &other) const;
  inline void set_affect_scope(ObCCLAffectScope affect_scope) {
    affect_scope_ = affect_scope;
  }
  inline void set_max_concurrency(uint64_t max_concurrency) {
    max_concurrency_ = max_concurrency;
  }
  inline int set_affect_user_name(const common::ObString &affect_user_name) {
    return deep_copy_str(affect_user_name, affect_user_name_);
  }
  inline int set_affect_host(const common::ObString &affect_host) {
    return deep_copy_str(affect_host, affect_host_);
  }
  inline int set_affect_database(const common::ObString &affect_database) {
    return deep_copy_str(affect_database, affect_database_);
  }
  inline int set_affect_table(const common::ObString &affect_table) {
    return deep_copy_str(affect_table, affect_table_);
  }
  inline int set_ccl_keywords(const ObString &ccl_keywords) {
    return deep_copy_str(ccl_keywords, ccl_keywords_);
  }
  inline const common::ObString &get_affect_database() const {
    return affect_database_;
  }
  inline const common::ObString &get_affect_table() const {
    return affect_table_;
  }
  inline const ObString &get_affect_user_name() const {
    return affect_user_name_;
  }
  inline const ObString &get_affect_host() const { return affect_host_; }
  inline ObCCLAffectScope get_affect_scope() const { return affect_scope_; }
  inline const common::ObString &get_ccl_keywords() const {
    return ccl_keywords_;
  }
  inline const ObIArray<common::ObString> &get_ccl_keywords_array() const {
    return ccl_keywords_array_;
  }
  inline uint64_t get_max_concurrency() const { return max_concurrency_; }
  /**
   * @brief 将合并后的关键字字符串按照指定的分隔符和转义符进行分割。
   *        ccl_keywords_ -> ccl_keywords_array_
   * example1: 
   *        c1;c2 = abc;asd -> [c1, c2 = abc, asd]
   * example2: 
   *        t\\\;1\;;c1 = -> [t\;1;, c1 =]
   *
   * @param separator 用于拼接字符串的分隔符: ';'
   * @param escape_char 用于转义特殊字符的字符 '\'
   */
  int split_strings_with_escape(char separator, char escape_char);

  VIRTUAL_TO_STRING_KV(K(get_tenant_id()), K(get_ccl_rule_id()),
                       K(get_ccl_rule_name()), K(get_schema_version()),
                       K(affect_for_all_databases()),
                       K(affect_for_all_tables()), K_(affect_database),
                       K_(affect_table), K_(affect_user_name), K_(affect_host),
                       K_(affect_scope), K_(ccl_keywords), K_(max_concurrency), K_(ccl_keywords_array));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;

private:
  ObString affect_database_;
  ObString affect_table_;
  ObString affect_user_name_;
  ObString affect_host_;
  ObCCLAffectScope affect_scope_; // 0: rule level, 1: format sqlid level
  common::ObString ccl_keywords_; // 3 keywords => ['c1', 'c2 = abc', 'asd'],
                                  // will be stored like 'c1;c2 = abc;asd' here
  uint64_t max_concurrency_;
  ObSEArray<ObString, 2> ccl_keywords_array_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H */