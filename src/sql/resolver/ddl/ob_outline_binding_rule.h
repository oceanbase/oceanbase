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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_OUTLINE_BINDING_RULE_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_OUTLINE_BINDING_RULE_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "share/ob_rpc_struct.h"
#include "sql/outline/ob_pattern_matcher.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief ObOutlineRuleMapping - Single mapping rule for BINDING_RULE MAP clause
 *
 * Each mapping specifies: real_table_name TO 'pattern_string'
 * Supports wildcard table/db names with variable syntax: ${VAR:regex}
 * Each part (db_name or table_name) can have at most one variable.
 */
struct ObOutlineRuleMapping
{
public:
  ObOutlineRuleMapping()
    : allocator_("OutlineMap"),
      original_db_name_(),
      original_table_name_(),
      db_fixed_prefix_(),
      db_fixed_suffix_(),
      table_fixed_prefix_(),
      table_fixed_suffix_(),
      db_var_name_(),
      db_var_regex_(),
      table_var_name_(),
      table_var_regex_(),
      ast_position_(common::OB_INVALID_ID),
      db_obj_id_(common::OB_INVALID_ID),
      tb_obj_id_(common::OB_INVALID_ID),
      db_placeholder_(),
      tb_placeholder_(),
      db_var_info_(),
      table_var_info_(),
      patterns_parsed_(false)
  {}

  // Explicit copy ctor / copy assign: deep-copy all ObString fields into own arena allocator_.
  // Required because this type is stored by value in ObSEArray — a default memberwise copy
  // would shallow-copy ObString pointers into the source's arena and cause use-after-free
  // when the source is destroyed.  Derived caches (var_info_) are NOT copied; the copy must
  // re-run parse_patterns() if pattern views are needed.
  ObOutlineRuleMapping(const ObOutlineRuleMapping &other);
  ObOutlineRuleMapping &operator=(const ObOutlineRuleMapping &other);

  ~ObOutlineRuleMapping() { reset(); }

  void reset();
  bool is_valid() const;
  int assign(const ObOutlineRuleMapping &other);
  // Note: strings are owned by the internal arena allocator_; lifetime equals the object's.
  // The external allocator previously accepted here was never used — removed to avoid misleading callers.
  int deep_copy(const ObOutlineRuleMapping &other);

  // Check if this mapping has db.table form (cross-database)
  bool has_db_prefix() const { return !original_db_name_.empty(); }

  // Check if db_name part has a variable
  bool has_db_var() const { return !db_var_name_.empty(); }

  // Check if table_name part has a variable
  bool has_table_var() const { return !table_var_name_.empty(); }

  // Distinguish wildcard MAP items from fixed non-MAP slots persisted for exact matching.
  bool has_db_wildcard() const;
  bool has_table_wildcard() const;
  bool is_fixed_mapping() const;
  bool needs_placeholder() const { return !is_fixed_mapping(); }

  // JSON serialization for pattern_rules persistence
  int to_json_string(char *buf, int64_t buf_len, int64_t &pos) const;
  // JSON deserialization from pattern_rules column
  int from_json_kv(const common::ObString &key, const common::ObString &value,
                   common::ObIAllocator &allocator);

  // Pre-parsed pattern views
  int parse_patterns();       // Assemble ObPatternVarInfo from prefix/suffix/var fields
  bool are_patterns_parsed() const { return patterns_parsed_; }
  const ObPatternVarInfo &get_db_var_info() const { return db_var_info_; }
  const ObPatternVarInfo &get_table_var_info() const { return table_var_info_; }

  // Setters
  int set_original_db_name(const common::ObString &name);
  int set_original_table_name(const common::ObString &name);
  int set_db_fixed_prefix(const common::ObString &s);
  int set_db_fixed_suffix(const common::ObString &s);
  int set_table_fixed_prefix(const common::ObString &s);
  int set_table_fixed_suffix(const common::ObString &s);
  int set_db_var_name(const common::ObString &name);
  int set_db_var_regex(const common::ObString &regex);
  int set_table_var_name(const common::ObString &name);
  int set_table_var_regex(const common::ObString &regex);
  void set_ast_position(int64_t pos) { ast_position_ = pos; }
  void set_db_obj_id(uint64_t obj_id) { db_obj_id_ = obj_id; }
  void set_tb_obj_id(uint64_t obj_id) { tb_obj_id_ = obj_id; }
  int set_db_placeholder(const common::ObString &ph);
  int set_tb_placeholder(const common::ObString &ph);

  // Getters
  const common::ObString &get_original_db_name() const { return original_db_name_; }
  const common::ObString &get_original_table_name() const { return original_table_name_; }
  const common::ObString &get_db_fixed_prefix() const { return db_fixed_prefix_; }
  const common::ObString &get_db_fixed_suffix() const { return db_fixed_suffix_; }
  const common::ObString &get_table_fixed_prefix() const { return table_fixed_prefix_; }
  const common::ObString &get_table_fixed_suffix() const { return table_fixed_suffix_; }
  const common::ObString &get_db_var_name() const { return db_var_name_; }
  const common::ObString &get_db_var_regex() const { return db_var_regex_; }
  const common::ObString &get_table_var_name() const { return table_var_name_; }
  const common::ObString &get_table_var_regex() const { return table_var_regex_; }

  // True iff some db-pattern form (prefix / suffix / var) is recorded; used by
  // the runtime matcher to decide between pattern matching and exact matching.
  // db_var_regex_ is implied by db_var_name_ (regex is always set when name is
  // set, and is never set without name), so it need not be checked separately.
  bool has_db_pattern_form() const {
    return !db_fixed_prefix_.empty() || !db_fixed_suffix_.empty()
        || !db_var_name_.empty();
  }
  bool has_table_pattern_form() const {
    return !table_fixed_prefix_.empty() || !table_fixed_suffix_.empty()
        || !table_var_name_.empty();
  }
  int64_t get_ast_position() const { return ast_position_; }
  uint64_t get_db_obj_id() const { return db_obj_id_; }
  uint64_t get_tb_obj_id() const { return tb_obj_id_; }
  const common::ObString &get_db_placeholder() const { return db_placeholder_; }
  const common::ObString &get_tb_placeholder() const { return tb_placeholder_; }

  TO_STRING_KV(K_(original_db_name),
               K_(original_table_name),
               K_(db_fixed_prefix),
               K_(db_fixed_suffix),
               K_(table_fixed_prefix),
               K_(table_fixed_suffix),
               K_(db_var_name),
               K_(db_var_regex),
               K_(table_var_name),
               K_(table_var_regex),
               K_(ast_position),
               K_(db_obj_id),
               K_(tb_obj_id),
               K_(db_placeholder),
               K_(tb_placeholder),
               K_(patterns_parsed));

private:
  common::ObArenaAllocator allocator_;   // Owns deep-copied string memory for all ObString fields
  common::ObString original_db_name_;    // Left side db name (empty if only table form)
  common::ObString original_table_name_; // Left side table name
  // Pattern parts are stored already unescaped — backslash escapes (\$ \{ \} \\)
  // and the surrounding ${VAR:regex} are resolved at CREATE time by
  // ObPatternParser, so the matcher reads these strings as literal bytes.
  common::ObString db_fixed_prefix_;     // Literal text before db variable (empty when no db part)
  common::ObString db_fixed_suffix_;     // Literal text after db variable
  common::ObString table_fixed_prefix_;  // Literal text before table variable
  common::ObString table_fixed_suffix_;  // Literal text after table variable
  common::ObString db_var_name_;         // Variable name for db part (empty if no var)
  common::ObString db_var_regex_;        // Variable regex for db part (empty if no var or reference)
  common::ObString table_var_name_;      // Variable name for table part (empty if no var)
  common::ObString table_var_regex_;     // Variable regex for table part (empty if no var or reference)
  int64_t ast_position_;                 // Position in FROM clause parse tree (filled by Resolver)
  uint64_t db_obj_id_;                   // Database object ID for placeholder (DB_[objid]$N)
  uint64_t tb_obj_id_;                   // Table object ID for placeholder (TB_[objid]$N)
  common::ObString db_placeholder_;      // DB_[objid]$N placeholder for outline_content
  common::ObString tb_placeholder_;      // TB_[objid]$N placeholder for outline_content

  // Derived cache, not serialized.  parse_patterns() rebuilds the var_info_
  // structs from the prefix/suffix/var fields above without re-parsing pattern
  // text — these views just hand the same ObString pointers to the matcher.
  ObPatternVarInfo db_var_info_;
  ObPatternVarInfo table_var_info_;
  bool patterns_parsed_;
};

/**
 * @brief ObOutlineBindingRule - Complete BINDING_RULE structure
 *
 * Contains SCOPE setting and MAP rules for outline wildcard binding.
 * Supports SaaS scenarios: cross-database binding and table name wildcards.
 */
class ObOutlineBindingRule
{
public:
  ObOutlineBindingRule()
    : scope_(obrpc::OUTLINE_SCOPE_DATABASE),
      map_items_(),
      is_set_(false)
  {}

  ~ObOutlineBindingRule() { reset(); }

  void reset();
  bool is_valid() const;
  int assign(const ObOutlineBindingRule &other);
  int deep_copy(common::ObIAllocator &allocator, const ObOutlineBindingRule &other);

  // Scope methods
  void set_scope(int64_t scope) { scope_ = scope; is_set_ = true; }
  int64_t get_scope() const { return scope_; }
  bool is_set() const { return is_set_; }
  void set_is_set(bool v) { is_set_ = v; }
  bool is_tenant_scope() const { return scope_ == obrpc::OUTLINE_SCOPE_TENANT; }
  bool is_database_scope() const { return scope_ == obrpc::OUTLINE_SCOPE_DATABASE; }

  // MAP items methods
  int add_map_item(const ObOutlineRuleMapping &mapping);
  int set_map_items(const common::ObIArray<ObOutlineRuleMapping> &items);
  const common::ObSEArray<ObOutlineRuleMapping, 16> &get_map_items() const { return map_items_; }
  common::ObSEArray<ObOutlineRuleMapping, 16> &get_map_items() { return map_items_; }
  int64_t get_map_item_count() const { return map_items_.count(); }
  const ObOutlineRuleMapping &get_map_item(int64_t idx) const { return map_items_.at(idx); }
  ObOutlineRuleMapping &get_map_item(int64_t idx) { return map_items_.at(idx); }

  // Check if any mapping has db.table form (requires TENANT scope)
  bool has_db_table_mapping() const;

  // Validate: MAP with db.table must have SCOPE=TENANT
  bool validate_scope_consistency() const;

  // JSON serialization for pattern_rules persistence (self-contained with expanded regex)
  int to_json_string(char *buf, int64_t buf_len, int64_t &pos) const;

  // Serialize map_items_ as JSON array for pattern_rules column storage
  // Output format: [{...}, {...}] (no scope wrapper, scope is stored via database_id)
  int serialize_pattern_rules(char *buf, int64_t buf_len, int64_t &pos) const;

  // Deserialize pattern_rules column JSON array into map_items_
  static int deserialize_pattern_rules(const common::ObString &json,
                                       common::ObIAllocator &allocator,
                                       common::ObIArray<ObOutlineRuleMapping> &items);

  // Unified deserializer: handles both {"scope":N,"items":[...]} and [...] formats
  static int deserialize_from_json(const common::ObString &json,
                                   common::ObIAllocator &allocator,
                                   ObOutlineBindingRule &rule);

  TO_STRING_KV(K_(scope),
               K_(map_items));

private:
  int64_t scope_;
  common::ObSEArray<ObOutlineRuleMapping, 16> map_items_;
  bool is_set_;  // true if BINDING_RULE clause was specified
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_OUTLINE_BINDING_RULE_H_