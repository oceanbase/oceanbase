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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_create_outline_resolver.h"

#include "share/ob_version.h"
#include "sql/resolver/ddl/ob_create_outline_stmt.h"
#include "sql/resolver/ddl/ob_outline_binding_rule.h"
#include "share/schema/ob_outline_sql_service.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/ob_stmt.h"
#include "lib/hash/ob_hashset.h"
#include "sql/parser/ob_pattern_parser.h"
#include "sql/parser/ob_parser.h"
#include "sql/outline/ob_outline_template_matcher.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObCreateOutlineResolver::resolve_sql_id(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt, bool is_format_sql)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || (node->type_ != T_CHAR && node->type_ != T_VARCHAR)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql id");
  } else {
    if (!is_format_sql) {
      create_outline_stmt.get_sql_id() = ObString::make_string(node->str_value_);
    } else {
      create_outline_stmt.get_format_sql_id() = ObString::make_string(node->str_value_);
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve_hint(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt)
{
  int ret = OB_SUCCESS;
  if (node == NULL) {
    ret = OB_INVALID_OUTLINE;
    LOG_USER_ERROR(OB_INVALID_OUTLINE, "Hint is not correct, please check");
    LOG_WARN("hint is not correct");
  }
  if (OB_SUCC(ret)) {
    if (node->type_ != T_HINT_OPTION_LIST) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      char *buf = (char *)allocator_->alloc(node->str_len_ + 4);
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("cannot alloc mem");
      } else {
        MEMCPY(buf, "/*+", 3);
        MEMCPY(buf + 3, node->str_value_, node->str_len_);
        buf[node->str_len_ + 3] = '\0';
        create_outline_stmt.get_hint() = ObString::make_string(buf);
        if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
            *allocator_, session_info_->get_dtc_params(), create_outline_stmt.get_hint()))) {
          LOG_WARN("fail to convert sql text", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < node->num_child_; i ++) {
      ParseNode *hint_node = node->children_[i];
      if (!hint_node) {
       continue;
      }
      if (hint_node->type_ == T_MAX_CONCURRENT) {
        if (OB_ISNULL(hint_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child of max concurrent node should not be NULL", K(ret));
        } else if (hint_node->children_[0]->value_ >= 0) {
          create_outline_stmt.set_max_concurrent(hint_node->children_[0]->value_);
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve_binding_rule_scope(const ParseNode *node, ObOutlineBindingRule &binding_rule)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scope node is NULL", K(ret));
  } else if (node->type_ != T_BINDING_RULE_SCOPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scope node type", K(ret), K(node->type_));
  } else if (OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scope value node is NULL", K(ret));
  } else {
    int64_t scope_value = node->children_[0]->value_;
    if (scope_value == obrpc::OUTLINE_SCOPE_DATABASE) {
      binding_rule.set_scope(obrpc::OUTLINE_SCOPE_DATABASE);
    } else if (scope_value == obrpc::OUTLINE_SCOPE_TENANT) {
      binding_rule.set_scope(obrpc::OUTLINE_SCOPE_TENANT);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid scope value", K(ret), K(scope_value));
    }
  }
  return ret;
}

// Extract pattern string from a STRING_VALUE ParseNode, preferring raw_text_
// (which preserves the full quoted form from the source SQL) and falling back to
// str_value_/str_len_. raw_text_ includes surrounding quotes, so we strip them.
static int extract_pattern_text(const ParseNode *str_node, ObString &out)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str_node)
      || (str_node->type_ != T_CHAR && str_node->type_ != T_VARCHAR)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pattern node should be STRING_VALUE", K(ret),
             KP(str_node), K(str_node ? str_node->type_ : T_INVALID));
  } else if (OB_NOT_NULL(str_node->raw_text_) && str_node->text_len_ >= 2) {
    out.assign_ptr(str_node->raw_text_ + 1,
                   static_cast<ObString::obstr_size_t>(str_node->text_len_ - 2));
  } else {
    out.assign_ptr(str_node->str_value_, str_node->str_len_);
  }
  return ret;
}

int ObCreateOutlineResolver::parse_and_set_db_pattern(ObPatternParser &pp,
    const ObString &pat, ObOutlineRuleMapping &mapping)
{
  int ret = OB_SUCCESS;
  PatternParseResult result;
  if (OB_FAIL(pp.parse_pattern_string(pat, result))) {
    if (OB_ERR_REGEXP_ERROR == ret && !result.regex_error_.empty()) {
      LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, result.regex_error_.ptr());
    } else if (OB_NOT_SUPPORTED != ret && OB_ERR_REGEXP_ERROR != ret) {
      LOG_USER_ERROR(OB_ERR_PARSER_SYNTAX, "invalid db pattern syntax");
    }
    LOG_WARN("failed to parse db pattern", K(ret), K(pat));
  } else if (OB_FAIL(mapping.set_db_fixed_prefix(result.fixed_prefix_))) {
    LOG_WARN("fail to set db fixed prefix", K(ret), K(result.fixed_prefix_));
  } else if (OB_FAIL(mapping.set_db_fixed_suffix(result.fixed_suffix_))) {
    LOG_WARN("fail to set db fixed suffix", K(ret), K(result.fixed_suffix_));
  } else if (result.has_variable()) {
    if (OB_FAIL(mapping.set_db_var_name(result.var_info_.var_name_))) {
      LOG_WARN("fail to set db var name", K(ret));
    } else if (OB_FAIL(mapping.set_db_var_regex(result.var_info_.var_regex_))) {
      LOG_WARN("fail to set db var regex", K(ret));
    }
  }
  return ret;
}

int ObCreateOutlineResolver::parse_and_set_table_pattern(ObPatternParser &pp,
    const ObString &pat, ObOutlineRuleMapping &mapping)
{
  int ret = OB_SUCCESS;
  PatternParseResult result;
  if (OB_FAIL(pp.parse_pattern_string(pat, result))) {
    if (OB_ERR_REGEXP_ERROR == ret && !result.regex_error_.empty()) {
      LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, result.regex_error_.ptr());
    } else if (OB_NOT_SUPPORTED != ret && OB_ERR_REGEXP_ERROR != ret) {
      LOG_USER_ERROR(OB_ERR_PARSER_SYNTAX, "invalid table pattern syntax");
    }
    LOG_WARN("failed to parse table pattern", K(ret), K(pat));
  } else if (OB_FAIL(mapping.set_table_fixed_prefix(result.fixed_prefix_))) {
    LOG_WARN("fail to set table fixed prefix", K(ret), K(result.fixed_prefix_));
  } else if (OB_FAIL(mapping.set_table_fixed_suffix(result.fixed_suffix_))) {
    LOG_WARN("fail to set table fixed suffix", K(ret), K(result.fixed_suffix_));
  } else if (result.has_variable()) {
    if (OB_FAIL(mapping.set_table_var_name(result.var_info_.var_name_))) {
      LOG_WARN("fail to set table var name", K(ret));
    } else if (OB_FAIL(mapping.set_table_var_regex(result.var_info_.var_regex_))) {
      LOG_WARN("fail to set table var regex", K(ret));
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve_binding_rule_map_item(const ParseNode *node, ObOutlineRuleMapping &mapping)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("map item node is NULL", K(ret));
  } else if (node->type_ != T_BINDING_RULE_MAP_ITEM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid map item node type", K(ret), K(node->type_));
  } else if (node->num_child_ != 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("map item should have 3 children",
             K(ret), K(node->num_child_));
  } else if (OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("map item children are NULL", K(ret));
  } else {
    // children_[0] is T_RELATION_FACTOR (source table or db.table)
    // children_[2] NULL: children_[1] is the table-pattern STRING_VALUE (table-only RIGHT)
    // children_[2] non-NULL: children_[1] is db-pattern, children_[2] is table-pattern (dotted RIGHT)
    ParseNode *relation_node = node->children_[0];
    const bool right_is_dotted = (NULL != node->children_[2]);

    if (relation_node->type_ != T_RELATION_FACTOR) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relation node should be T_RELATION_FACTOR",
               K(ret), K(relation_node->type_));
    } else {
      // Parse T_RELATION_FACTOR: children_[0] = db_name (NULL if only table), children_[1] = table_name
      ParseNode *db_node = relation_node->children_[0];
      ParseNode *table_node = relation_node->children_[1];

      if (OB_NOT_NULL(db_node)) {
        if (db_node->type_ == T_IDENT || db_node->type_ == T_CHAR || db_node->type_ == T_VARCHAR) {
          if (OB_FAIL(mapping.set_original_db_name(ObString(db_node->str_len_, db_node->str_value_)))) {
            LOG_WARN("fail to set original db name", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(table_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_node in T_RELATION_FACTOR is NULL", K(ret));
        } else if (OB_FAIL(mapping.set_original_table_name(ObString(table_node->str_len_, table_node->str_value_)))) {
          LOG_WARN("fail to set original table name", K(ret));
        }
      }

      // Structural symmetry: LEFT db.table <=> RIGHT 'db_pat'.'tbl_pat';
      // LEFT table-only <=> RIGHT 'tbl_pat'. Reject mismatches at resolve time.
      const bool left_has_db = mapping.has_db_prefix();
      if (OB_SUCC(ret)) {
        if (left_has_db && !right_is_dotted) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "BINDING_RULE MAP LEFT is db.table but RIGHT is single pattern; "
              "use 'db_pat'.'tbl_pat' form is");
          LOG_WARN("LEFT is db.table but RIGHT is single pattern", K(ret));
        } else if (!left_has_db && right_is_dotted) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              "BINDING_RULE MAP LEFT is table-only but RIGHT is dotted; "
              "remove the db pattern is");
          LOG_WARN("LEFT is table-only but RIGHT is dotted", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObPatternParser pattern_parser(*allocator_);
        if (right_is_dotted) {
          ObString db_pat;
          ObString tbl_pat;
          if (OB_FAIL(extract_pattern_text(node->children_[1], db_pat))) {
            LOG_WARN("failed to extract db pattern text", K(ret));
          } else if (OB_FAIL(extract_pattern_text(node->children_[2], tbl_pat))) {
            LOG_WARN("failed to extract table pattern text", K(ret));
          } else if (db_pat.empty()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE MAP RIGHT db pattern must not be empty is");
            LOG_WARN("empty db pattern", K(ret));
          } else if (tbl_pat.empty()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE MAP RIGHT table pattern must not be empty is");
            LOG_WARN("empty table pattern", K(ret));
          } else if (OB_FAIL(parse_and_set_db_pattern(pattern_parser, db_pat, mapping))) {
            LOG_WARN("failed to parse db pattern", K(ret), K(db_pat));
          } else if (OB_FAIL(parse_and_set_table_pattern(pattern_parser, tbl_pat, mapping))) {
            LOG_WARN("failed to parse table pattern", K(ret), K(tbl_pat));
          }
        } else {
          // table-only form
          ObString tbl_pat;
          if (OB_FAIL(extract_pattern_text(node->children_[1], tbl_pat))) {
            LOG_WARN("failed to extract table pattern text", K(ret));
          } else if (tbl_pat.empty()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE MAP RIGHT table pattern must not be empty is");
            LOG_WARN("empty table pattern", K(ret));
          } else if (OB_FAIL(parse_and_set_table_pattern(pattern_parser, tbl_pat, mapping))) {
            LOG_WARN("failed to parse table pattern", K(ret), K(tbl_pat));
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve_binding_rule_map(const ParseNode *node, ObOutlineBindingRule &binding_rule)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("map node is NULL", K(ret));
  } else if (node->type_ != T_BINDING_RULE_MAP) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid map node type", K(ret), K(node->type_));
  } else {
    // Iterate through map items
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode *item_node = node->children_[i];
      ObOutlineRuleMapping mapping;
      if (OB_ISNULL(item_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("map item node is NULL", K(ret), K(i), K(node->num_child_));
      } else if (OB_FAIL(resolve_binding_rule_map_item(item_node, mapping))) {
        LOG_WARN("failed to resolve map item", K(ret), K(i));
      } else if (OB_FAIL(binding_rule.add_map_item(mapping))) {
        LOG_WARN("failed to add map item", K(ret));
      }
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve_binding_rule(const ParseNode *node, ObCreateOutlineStmt &create_outline_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("binding_rule node is NULL, caller should have guarded", K(ret));
  } else if (node->type_ != T_BINDING_RULE_CLAUSE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid binding rule node type", K(ret), K(node->type_));
  } else {
    ObOutlineBindingRule &binding_rule = create_outline_stmt.get_binding_rule();

    // Iterate through binding items (SCOPE and MAP)
    // Note: duplicate SCOPE/MAP is already prevented by parser grammar
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      ParseNode *item_node = node->children_[i];
      // T_BINDING_RULE_CLAUSE always has 2 children for structural uniformity; the
      // absent slot (SCOPE-only or MAP-only form) is NULL and skipped here.
      if (OB_NOT_NULL(item_node)) {
        if (item_node->type_ == T_BINDING_RULE_SCOPE) {
          if (OB_FAIL(resolve_binding_rule_scope(item_node, binding_rule))) {
            LOG_WARN("failed to resolve scope", K(ret));
          }
        } else if (item_node->type_ == T_BINDING_RULE_MAP) {
          if (OB_FAIL(resolve_binding_rule_map(item_node, binding_rule))) {
            LOG_WARN("failed to resolve map", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected binding item type", K(ret), K(item_node->type_));
        }
      }
    }

    // Validate: MAP with db.table form requires SCOPE=TENANT
    if (OB_SUCC(ret)) {
      if (!binding_rule.validate_scope_consistency()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "BINDING_RULE MAP with db.table form without SCOPE=TENANT is");
        LOG_WARN("MAP with db.table form requires SCOPE=TENANT", K(ret));
      }
    }

    // Validate: variable must be defined (with regex) BEFORE being referenced
    // AND a re-definition with a different regex across map items is rejected.
    // Single-pass forward scan: walk map items in declaration order, recording defined
    // variable names + their regex as we go. A reference (empty regex) is legal only
    // if the name was recorded by a prior item. A re-definition (same name, different
    // regex) is rejected with OB_NOT_SUPPORTED so the user notices the conflict
    // instead of getting a silently divergent rule.
    if (OB_SUCC(ret) && binding_rule.get_map_item_count() > 0) {
      ObSEArray<std::pair<ObString, ObString>, 8> defined_vars;
      auto find_def = [&defined_vars](const ObString &name) -> int64_t {
        int64_t found = -1;
        for (int64_t j = 0; -1 == found && j < defined_vars.count(); ++j) {
          if (defined_vars.at(j).first == name) {
            found = j;
          }
        }
        return found;
      };
      for (int64_t i = 0; OB_SUCC(ret) && i < binding_rule.get_map_item_count(); ++i) {
        const ObOutlineRuleMapping &item = binding_rule.get_map_item(i);
        const ObString &tb_name = item.get_table_var_name();
        const ObString &tb_regex = item.get_table_var_regex();
        const ObString &db_name = item.get_db_var_name();
        const ObString &db_regex = item.get_db_var_regex();

        // Check db reference first (within an item, both come from the same map entry,
        // so neither side is "before" the other; treat them symmetrically).
        if (!db_name.empty() && db_regex.empty()) {
          if (find_def(db_name) < 0) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE variable referenced before defined is");
            LOG_WARN("variable referenced before defined in MAP",
                     K(ret), K(db_name), K(i));
          }
        }
        // Check table reference
        if (OB_SUCC(ret) && !tb_name.empty() && tb_regex.empty()) {
          if (find_def(tb_name) < 0) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE variable referenced before defined is");
            LOG_WARN("variable referenced before defined in MAP",
                     K(ret), K(tb_name), K(i));
          }
        }
        // Register definitions from this item. Same name with a different regex
        // text is rejected; same name with identical regex is tolerated
        // so users can repeat the binding to two different tables.
        if (OB_SUCC(ret) && !db_name.empty() && !db_regex.empty()) {
          int64_t idx = find_def(db_name);
          if (idx < 0) {
            if (OB_FAIL(defined_vars.push_back(std::make_pair(db_name, db_regex)))) {
              LOG_WARN("fail to push defined db var", K(ret));
            }
          } else if (defined_vars.at(idx).second != db_regex) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE variable defined with conflicting regex is");
            LOG_WARN("variable redefined with conflicting regex",
                     K(ret), K(db_name), K(db_regex),
                     "prev", defined_vars.at(idx).second);
          }
        }
        if (OB_SUCC(ret) && !tb_name.empty() && !tb_regex.empty()) {
          int64_t idx = find_def(tb_name);
          if (idx < 0) {
            if (OB_FAIL(defined_vars.push_back(std::make_pair(tb_name, tb_regex)))) {
              LOG_WARN("fail to push defined table var", K(ret));
            }
          } else if (defined_vars.at(idx).second != tb_regex) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "BINDING_RULE variable defined with conflicting regex is");
            LOG_WARN("variable redefined with conflicting regex",
                     K(ret), K(tb_name), K(tb_regex),
                     "prev", defined_vars.at(idx).second);
          }
        }
      }
    }
  }
  return ret;
}

int ObCreateOutlineResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *node = const_cast<ParseNode *>(&parse_tree);
  ObCreateOutlineStmt *create_outline_stmt = NULL;
  uint64_t compat_version = 0;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ or allocator_ is NULL",
             KP(session_info_), K(allocator_), K(ret));
  } else if (OB_UNLIKELY(is_external_catalog_id(session_info_->get_current_default_catalog()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create outline in catalog is");
  } else if (OB_ISNULL(node)
      || OB_UNLIKELY(node->type_ != T_CREATE_OUTLINE)
      || OB_UNLIKELY(node->num_child_ != OUTLINE_CHILD_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(node), K(node->children_));
  } else if (OB_UNLIKELY(NULL == (create_outline_stmt = create_stmt<ObCreateOutlineStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_outline_stmt", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(MTL_ID()));
  } else {
    stmt_ = create_outline_stmt;
    //set is_replace
    if (node->children_[0] != NULL) {
      create_outline_stmt->set_replace();
    }
    //set owner
    create_outline_stmt->set_owner(session_info_->get_user_name());
    create_outline_stmt->set_owner_id(session_info_->get_user_id());
    //set server version
    ObString server_version;
    if (OB_FAIL(ob_write_string(*allocator_, ObString(build_version()), server_version))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      create_outline_stmt->set_server_version(server_version);
    }

    // resovle outline type
    bool is_format_otl = false;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(node->children_[5])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node children", K(node->children_[5]), K(node->children_));
    } else {
      is_format_otl = (node->children_[5]->value_
                        == ObOutlineType::OUTLINE_TYPE_FORMAT);
      create_outline_stmt->set_format_outline(is_format_otl);
    }

    if (OB_SUCC(ret) && is_format_otl && !oceanbase::share::schema::ObOutlineSqlService::is_formatoutline_compat(compat_version)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "format outline not supported under oceanbase 4.3.4");
      LOG_WARN("format outline not supported under oceanbase 4.3.4", K(ret));
    }

    //resolve database_name and outline_name
    if (OB_SUCC(ret)) {
      ObString db_name;
      ObString outline_name;
      if (OB_FAIL(resolve_outline_name(node->children_[1], db_name, outline_name))) {
        LOG_WARN("fail to resolve outline name", K(ret));
      } else {
        create_outline_stmt->set_database_name(db_name);
        create_outline_stmt->set_outline_name(outline_name);
      }
    }

    if (node->children_[2]->value_ == 1) {
      //resolve outline_stmt
      if (OB_SUCC(ret)) {
        if (!is_format_otl && OB_FAIL(resolve_outline_stmt(node->children_[3],
                                         create_outline_stmt->get_outline_stmt(),
                                         create_outline_stmt->get_outline_sql()))) {
          LOG_WARN("fail to resolve outline stmt", K(ret));
        } else if (is_format_otl && OB_FAIL(resolve_outline_stmt(node->children_[3],
                                         create_outline_stmt->get_outline_stmt(),
                                         create_outline_stmt->get_format_outline_sql()))) {
          LOG_WARN("fail to resolve outline stmt", K(ret));
        }
      }
      //set outline_target
      if (OB_SUCC(ret)) {
        if (OB_FAIL(resolve_outline_target(node->children_[4], create_outline_stmt->get_target_sql()))) {
          LOG_WARN("fail to resolve outline target", K(ret));
        }
      }
    } else {
      if (OB_FAIL(resolve_hint(node->children_[3], *create_outline_stmt))) {
        LOG_WARN("fail to resolve hint", K(ret));
      } else if (OB_FAIL(resolve_sql_id(node->children_[4],
                                        *create_outline_stmt,
                                        is_format_otl))) {
        LOG_WARN("fail to resolve sql id", K(ret));
      }
    }
    // Resolve BINDING_RULE (child[6]) — only if the clause is present
    if (OB_SUCC(ret)) {
      ParseNode *binding_rule_node = node->children_[6];
      if (OB_NOT_NULL(binding_rule_node)) {
        // Check data version compatibility for BINDING_RULE first
        // All BINDING_RULE features require data version >= DATA_VERSION_4_4_2_2
        if (!oceanbase::share::schema::ObOutlineSqlService::is_binding_rule_compat(compat_version)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "BINDING_RULE is not supported under current data version");
          LOG_WARN("BINDING_RULE is not supported under current data version", K(ret), K(compat_version));
        } else if (OB_FAIL(resolve_binding_rule(binding_rule_node, *create_outline_stmt))) {
          LOG_WARN("failed to resolve binding rule", K(ret));
        }
      }
    }

    // BINDING_RULE requires ON stmt syntax (not USING hint)
    if (OB_SUCC(ret) && create_outline_stmt->has_binding_rule()) {
      if (node->children_[2]->value_ != 1) {
        // "USING hint" path: no parsed statement available, BINDING_RULE not supported
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "BINDING_RULE with USING hint syntax is");
        LOG_WARN("BINDING_RULE requires ON stmt syntax", K(ret));
      }
    }

    // MAX_CONCURRENT-limited outlines do not support BINDING_RULE (mutually exclusive).
    // max_concurrent can be set via two paths:
    //   - USING hint path: create_outline_stmt->get_max_concurrent() (set in resolve_hint, default -1)
    //   - ON stmt path:    outline_stmt->query_ctx->global_hint.max_concurrent_
    //                      (set by parser when /*+max_concurrent(N)*/ appears inside the SELECT)
    if (OB_SUCC(ret) && create_outline_stmt->has_binding_rule()) {
      bool has_max_concurrent = (create_outline_stmt->get_max_concurrent() >= 0);
      if (!has_max_concurrent) {
        ObDMLStmt *outline_stmt = static_cast<ObDMLStmt *>(create_outline_stmt->get_outline_stmt());
        if (OB_NOT_NULL(outline_stmt) && OB_NOT_NULL(outline_stmt->get_query_ctx())) {
          const int64_t mc =
              outline_stmt->get_query_ctx()->get_global_hint().max_concurrent_;
          if (mc > ObGlobalHint::UNSET_MAX_CONCURRENT) {
            has_max_concurrent = true;
          }
        }
      }
      if (has_max_concurrent) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
            "MAX_CONCURRENT outline with BINDING_RULE is");
        LOG_WARN("MAX_CONCURRENT and BINDING_RULE are mutually exclusive", K(ret));
      }
    }

    // Collect ast_position from parse tree and match with MAP items
    if (OB_SUCC(ret) && create_outline_stmt->has_binding_rule()) {
      ObDMLStmt *outline_stmt = static_cast<ObDMLStmt *>(create_outline_stmt->get_outline_stmt());
      ObOutlineBindingRule &binding_rule = create_outline_stmt->get_binding_rule();
      ParseNode *stmt_parse_node = node->children_[3];  // statement parse tree node

      // SCOPE=DATABASE without MAP: no template matching needed, skip template path
      // and fall through to the normal (exact signature) outline creation path.
      const bool need_template = binding_rule.is_tenant_scope()
                                 || binding_rule.get_map_item_count() > 0;
      if (need_template && OB_NOT_NULL(outline_stmt)) {
        if (binding_rule.get_map_item_count() == 0) {
          // SCOPE=TENANT without MAP: auto-generate map_items from SQL's tables via parse tree DFS
          if (OB_FAIL(expand_auto_map_items(stmt_parse_node, outline_stmt, binding_rule))) {
            LOG_WARN("failed to expand auto map items", K(ret));
          }
        } else {
          // Collect table names via DFS once for both match_ast_position and expand_fixed_mappings
          ObSEArray<ObTableDbName, 16> table_db_names;
          if (OB_SUCC(ret) && OB_FAIL(ObOutlineTemplateMatcher::collect_table_names_dfs(
                  stmt_parse_node, table_db_names))) {
            LOG_WARN("failed to collect table names via DFS", K(ret));
          }
          // Match MAP items with ast_position
          else if (OB_FAIL(match_ast_position(outline_stmt, binding_rule, table_db_names))) {
            LOG_WARN("failed to match ast position", K(ret));
          }
          // Expand fixed mappings for tables not covered by MAP rules
          else if (OB_FAIL(expand_fixed_mappings(outline_stmt, binding_rule, table_db_names))) {
            LOG_WARN("failed to expand fixed mappings", K(ret));
          }
        }
      }

      // Compute template signature only when template path is needed
      if (OB_SUCC(ret) && need_template) {
        if (OB_FAIL(generate_template_signature(*create_outline_stmt))) {
          LOG_WARN("failed to generate template signature in resolver", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK (OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          ObString(""),
          stmt::T_CREATE_OUTLINE,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
  }


  return ret;
}

// Match MAP items with table positions using parse tree DFS order (symmetric with matching phase)
int ObCreateOutlineResolver::match_ast_position(
    ObDMLStmt *outline_stmt, ObOutlineBindingRule &binding_rule,
    const ObIArray<ObTableDbName> &table_db_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(outline_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outline_stmt is NULL", K(ret));
  } else if (OB_ISNULL(schema_checker_) || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_checker or allocator or session_info is NULL", K(ret));
  } else {
    // Assign ast_position to MAP items based on DFS order
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    for (int64_t pos = 0; OB_SUCC(ret) && pos < table_db_names.count(); ++pos) {
      int64_t position = pos + 1;  // 1-based
      const ObString &tbl_name = table_db_names.at(pos).first;
      const ObString &db_name = table_db_names.at(pos).second;

      for (int64_t j = 0; OB_SUCC(ret) && j < binding_rule.get_map_item_count(); ++j) {
        ObOutlineRuleMapping &mapping = binding_rule.get_map_item(j);
        // Skip already matched mappings (avoid duplicate matching)
        if (mapping.get_ast_position() != OB_INVALID_ID) {
          continue;
        }
        const ObString &map_db = mapping.get_original_db_name();
        const ObString &map_table = mapping.get_original_table_name();

        bool matched = false;
        if (map_db.empty()) {
          matched = (0 == map_table.case_compare(tbl_name));
        } else {
          matched = (0 == map_db.case_compare(db_name) &&
                     0 == map_table.case_compare(tbl_name));
        }
        if (matched) {
          mapping.set_ast_position(position);
          // Fill object IDs from the resolved statement
          if (OB_FAIL(fill_obj_ids_from_resolved_stmt(outline_stmt, mapping,
                                                       tbl_name, db_name, tenant_id))) {
            LOG_WARN("failed to fill obj ids from resolved stmt", K(ret), K(tbl_name));
          } else {
            LOG_DEBUG("matched ast position via DFS", K(map_table), K(position),
                      K(mapping.get_tb_obj_id()), K(mapping.get_db_obj_id()));
          }
          break;  // One parse tree position matches at most one MAP item
        }
      }
    }

    // Verify all MAP items have ast_position assigned
    for (int64_t j = 0; OB_SUCC(ret) && j < binding_rule.get_map_item_count(); ++j) {
      const ObOutlineRuleMapping &mapping = binding_rule.get_map_item(j);
      if (mapping.get_ast_position() == OB_INVALID_ID) {
        ret = OB_INVALID_OUTLINE;
        LOG_USER_ERROR(OB_INVALID_OUTLINE, "MAP item references table not found in SQL");
        LOG_WARN("MAP item has no matching table in SQL", K(ret), K(mapping.get_original_table_name()));
      }
    }
  }
  return ret;
}

// Fill object IDs by looking up the resolved statement's table items
int ObCreateOutlineResolver::fill_obj_ids_from_resolved_stmt(
    ObDMLStmt *outline_stmt, ObOutlineRuleMapping &mapping,
    const ObString &tbl_name, const ObString &db_name, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObIArray<TableItem*> &table_items = outline_stmt->get_table_items();
  bool found = false;

  for (int64_t i = 0; !found && i < table_items.count(); ++i) {
    TableItem *table_item = table_items.at(i);
    if (OB_ISNULL(table_item)) {
      continue;
    }
    // Match by table_name and optionally database_name
    bool name_match = (0 == tbl_name.case_compare(table_item->table_name_));
    bool db_match = db_name.empty() || (0 == db_name.case_compare(table_item->database_name_));
    if (name_match && db_match) {
      found = true;
      // ref_id_ is the real schema table ID for BASE_TABLE and ALIAS_TABLE types
      mapping.set_tb_obj_id(table_item->ref_id_);
      // Resolve database object ID from schema_checker
      if (!table_item->database_name_.empty()) {
        uint64_t db_obj_id = OB_INVALID_ID;
        if (OB_FAIL(schema_checker_->get_database_id(tenant_id, table_item->database_name_, db_obj_id))) {
          LOG_WARN("failed to get database id", K(ret), K(table_item->database_name_));
        } else {
          mapping.set_db_obj_id(db_obj_id);
        }
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    LOG_WARN("table not found in resolved stmt for obj_id lookup", K(tbl_name), K(db_name));
    // Not a hard error: the table might be in a subquery's resolved stmt
  }
  return ret;
}

// Expand binding rules without MAP: auto-generate exact-match map_items from parse tree DFS.
// Uses DFS order for symmetry with the matching phase. Does NOT set db info so that
// cross-database matching works correctly (no db-specific placeholders in outline_content).
int ObCreateOutlineResolver::expand_auto_map_items(
    const ParseNode *stmt_node, ObDMLStmt *outline_stmt,
    ObOutlineBindingRule &binding_rule)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableDbName, 16> table_db_names;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();

  if (OB_FAIL(ObOutlineTemplateMatcher::collect_table_names_dfs(
          stmt_node, table_db_names))) {
    LOG_WARN("failed to collect table names via DFS", K(ret));
  }

  for (int64_t pos = 0; OB_SUCC(ret) && pos < table_db_names.count(); ++pos) {
    int64_t position = pos + 1;  // 1-based
    const ObString &tbl_name = table_db_names.at(pos).first;
    const ObString &db_name = table_db_names.at(pos).second;

    ObOutlineRuleMapping mapping;
    if (OB_FAIL(mapping.set_original_table_name(tbl_name))) {
      LOG_WARN("fail to set original table name", K(ret));
    } else if (OB_FAIL(mapping.set_table_fixed_prefix(tbl_name))) {
      // Auto-generated fixed mapping: the table name is the whole literal
      // pattern (no variable), so it lives entirely in fixed_prefix.
      LOG_WARN("fail to set table fixed prefix", K(ret));
    } else {
      mapping.set_ast_position(position);
      // Fill tb_obj_id from resolved stmt (ref_id_); deliberately skip db info
      if (OB_FAIL(fill_obj_ids_from_resolved_stmt(outline_stmt, mapping,
                                                  tbl_name, db_name, tenant_id))) {
        LOG_WARN("fail to fill obj ids for auto-generated item", K(ret));
      }
    }

    // IMPORTANT: Do NOT set db info. Setting db info causes has_db_prefix()=true,
    // leading to "DB_xxx$N"."TB_yyy$N" placeholders, which break cross-database matching.
    // Only tb_obj_id is set for object-ID-based placeholder generation.

    if (OB_SUCC(ret) && OB_FAIL(binding_rule.add_map_item(mapping))) {
      LOG_WARN("fail to add auto-generated map item", K(ret), K(tbl_name));
    }
  }

  if (OB_SUCC(ret)) {
    binding_rule.set_is_set(true);
  }
  return ret;
}

// Expand fixed mappings: for each table position (from parse tree DFS) NOT covered by
// a MAP item, insert an exact-match mapping. This ensures every table slot has a
// map_item so template signature generation covers all positions.
int ObCreateOutlineResolver::expand_fixed_mappings(
    ObDMLStmt *outline_stmt, ObOutlineBindingRule &binding_rule,
    const ObIArray<ObTableDbName> &table_db_names)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();

  for (int64_t pos = 0; OB_SUCC(ret) && pos < table_db_names.count(); ++pos) {
    int64_t position = pos + 1;  // 1-based
    const ObString &tbl_name = table_db_names.at(pos).first;
    const ObString &db_name = table_db_names.at(pos).second;

    // Check if any existing map_item already covers this position
    bool has_item = false;
    for (int64_t j = 0; !has_item && j < binding_rule.get_map_item_count(); ++j) {
      has_item = (binding_rule.get_map_item(j).get_ast_position() == position);
    }

    if (!has_item) {
      ObOutlineRuleMapping fixed_item;
      if (OB_FAIL(fixed_item.set_original_table_name(tbl_name))) {
        LOG_WARN("fail to set original table name for fixed item", K(ret));
      } else if (OB_FAIL(fixed_item.set_table_fixed_prefix(tbl_name))) {
        // Fixed slot for a table not in MAP: literal table name as prefix.
        LOG_WARN("fail to set table fixed prefix for fixed item", K(ret));
      } else {
        fixed_item.set_ast_position(position);
        if (OB_FAIL(fill_obj_ids_from_resolved_stmt(outline_stmt, fixed_item,
                                                    tbl_name, db_name, tenant_id))) {
          LOG_WARN("fail to fill obj ids for fixed item", K(ret));
        }
      }

      // Do NOT set db info on fixed items — same rationale as SCOPE=TENANT without MAP.

      if (OB_SUCC(ret) && OB_FAIL(binding_rule.add_map_item(fixed_item))) {
        LOG_WARN("failed to add fixed binding item for non-MAP table",
                 K(ret), K(position), K(tbl_name));
      }
    }
  }
  return ret;
}

// Compute template signature from the pristine outline AST (before transform_stmt)
// and store it on the stmt for the executor to later carry into ObOutlineInfo.
int ObCreateOutlineResolver::generate_template_signature(ObCreateOutlineStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid context for template signature generation",
             K(ret), KP(session_info_), KP(allocator_));
  } else {
    // The ON-clause SELECT raw text (normal or format outline).
    const ObString &outline_sql = !stmt.get_outline_sql().empty()
        ? stmt.get_outline_sql() : stmt.get_format_outline_sql();
    if (outline_sql.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("outline sql text is empty for template signature", K(ret));
    } else {
      // Re-parse the ON-clause so identifier sql_str_off_ are relative to
      // outline_sql, then run the same parse-tree worker as the matching side
      // (byte-symmetric, no resolve).
      ObParser parser(*allocator_, session_info_->get_sql_mode(),
                      session_info_->get_charsets4parser());
      ParseResult parse_result;
      ObString sig;
      if (OB_FAIL(parser.parse(outline_sql, parse_result))) {
        LOG_WARN("failed to parse outline sql for template signature", K(ret), K(outline_sql));
      } else if (OB_ISNULL(parse_result.result_tree_)
                 || parse_result.result_tree_->num_child_ < 1
                 || OB_ISNULL(parse_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parse result for outline sql", K(ret), K(outline_sql));
      } else if (OB_FAIL(ObOutlineTemplateMatcher::generate_template_signature_from_parse_tree(
                     outline_sql, parse_result.result_tree_->children_[0], session_info_,
                     stmt.get_stmt_allocator(), false/*need_format*/, sig))) {
        LOG_WARN("failed to generate template signature from parse tree", K(ret));
      } else if (sig.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("template signature is empty", K(ret));
      } else {
        stmt.set_template_signature(sig);
        LOG_DEBUG("[OUTLINE] resolver precomputed template signature (parse-tree)",
                  "outline_name", stmt.get_create_outline_arg().outline_info_.get_name_str(),
                  K(sig));
      }
    }
  }
  return ret;
}

}//sql
}//oceanbase
