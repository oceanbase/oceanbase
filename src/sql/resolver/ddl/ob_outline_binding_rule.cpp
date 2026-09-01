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

#include "sql/resolver/ddl/ob_outline_binding_rule.h"
#include "sql/outline/ob_pattern_matcher.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/string/ob_string.h"
#include "lib/worker.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{

using namespace common;

// ==================== ObOutlineRuleMapping ====================
// Note: ObOutlineRuleMapping serialization is handled via JSON (to_json_string)
// OB_SERIALIZE_MEMBER is not needed for pattern_rules persistence

void ObOutlineRuleMapping::reset()
{
  original_db_name_.reset();
  original_table_name_.reset();
  db_fixed_prefix_.reset();
  db_fixed_suffix_.reset();
  table_fixed_prefix_.reset();
  table_fixed_suffix_.reset();
  db_var_name_.reset();
  db_var_regex_.reset();
  table_var_name_.reset();
  table_var_regex_.reset();
  ast_position_ = OB_INVALID_ID;
  db_obj_id_ = OB_INVALID_ID;
  tb_obj_id_ = OB_INVALID_ID;
  db_placeholder_.reset();
  tb_placeholder_.reset();
  db_var_info_.reset();
  table_var_info_.reset();
  patterns_parsed_ = false;
  allocator_.reset();
}

bool ObOutlineRuleMapping::is_valid() const
{
  // Need an original table name and either a variable or a non-trivial fixed
  // pattern that differs from the table name.
  return !original_table_name_.empty() && has_table_pattern_form();
}

bool ObOutlineRuleMapping::has_db_wildcard() const
{
  bool has_wildcard = false;
  if (!has_db_prefix()) {
    has_wildcard = false;
  } else if (has_db_var()) {
    has_wildcard = true;
  } else if (!has_db_pattern_form()) {
    has_wildcard = false;
  } else {
    // Fixed pattern: a wildcard exists iff the literal prefix (suffix is empty
    // when there is no variable) differs from original_db_name_ — that means
    // the user wrote a different literal for the right side.
    has_wildcard = 0 != db_fixed_prefix_.case_compare(original_db_name_);
  }
  return has_wildcard;
}

bool ObOutlineRuleMapping::has_table_wildcard() const
{
  bool has_wildcard = false;
  if (has_table_var()) {
    has_wildcard = true;
  } else if (!has_table_pattern_form()) {
    has_wildcard = false;
  } else {
    has_wildcard = 0 != table_fixed_prefix_.case_compare(original_table_name_);
  }
  return has_wildcard;
}

bool ObOutlineRuleMapping::is_fixed_mapping() const
{
  return !has_db_wildcard() && !has_table_wildcard();
}


ObOutlineRuleMapping::ObOutlineRuleMapping(const ObOutlineRuleMapping &other)
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
{
  int ret = assign(other);
  if (OB_SUCCESS != ret) {
    LOG_WARN_RET(ret, "ObOutlineRuleMapping copy ctor failed, object left in reset state");
    reset();
  }
}

ObOutlineRuleMapping &ObOutlineRuleMapping::operator=(const ObOutlineRuleMapping &other)
{
  if (this != &other) {
    int ret = assign(other);
    if (OB_SUCCESS != ret) {
      LOG_WARN_RET(ret, "ObOutlineRuleMapping copy assign failed, object left in reset state");
      reset();
    }
  }
  return *this;
}

int ObOutlineRuleMapping::assign(const ObOutlineRuleMapping &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // Self-assignment guard: reset() would free strings before copying
    return ret;
  }
  // Reset own state including allocator memory
  reset();

  if (!other.original_db_name_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.original_db_name_, original_db_name_))) {
    LOG_WARN("fail to deep copy original_db_name in assign", K(ret));
  } else if (!other.original_table_name_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.original_table_name_, original_table_name_))) {
    LOG_WARN("fail to deep copy original_table_name in assign", K(ret));
  } else if (!other.db_fixed_prefix_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.db_fixed_prefix_, db_fixed_prefix_))) {
    LOG_WARN("fail to deep copy db_fixed_prefix in assign", K(ret));
  } else if (!other.db_fixed_suffix_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.db_fixed_suffix_, db_fixed_suffix_))) {
    LOG_WARN("fail to deep copy db_fixed_suffix in assign", K(ret));
  } else if (!other.table_fixed_prefix_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.table_fixed_prefix_, table_fixed_prefix_))) {
    LOG_WARN("fail to deep copy table_fixed_prefix in assign", K(ret));
  } else if (!other.table_fixed_suffix_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.table_fixed_suffix_, table_fixed_suffix_))) {
    LOG_WARN("fail to deep copy table_fixed_suffix in assign", K(ret));
  } else if (!other.db_var_name_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.db_var_name_, db_var_name_))) {
    LOG_WARN("fail to deep copy db_var_name in assign", K(ret));
  } else if (!other.db_var_regex_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.db_var_regex_, db_var_regex_))) {
    LOG_WARN("fail to deep copy db_var_regex in assign", K(ret));
  } else if (!other.table_var_name_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.table_var_name_, table_var_name_))) {
    LOG_WARN("fail to deep copy table_var_name in assign", K(ret));
  } else if (!other.table_var_regex_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.table_var_regex_, table_var_regex_))) {
    LOG_WARN("fail to deep copy table_var_regex in assign", K(ret));
  } else if (!other.db_placeholder_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.db_placeholder_, db_placeholder_))) {
    LOG_WARN("fail to deep copy db_placeholder in assign", K(ret));
  } else if (!other.tb_placeholder_.empty()
      && OB_FAIL(ob_write_string(allocator_, other.tb_placeholder_, tb_placeholder_))) {
    LOG_WARN("fail to deep copy tb_placeholder in assign", K(ret));
  }
  if (OB_SUCC(ret)) {
    ast_position_ = other.ast_position_;
    db_obj_id_ = other.db_obj_id_;
    tb_obj_id_ = other.tb_obj_id_;
  }
  // Derived var_info_ views NOT copied — caller must re-run parse_patterns() if needed.
  return ret;
}

int ObOutlineRuleMapping::deep_copy(const ObOutlineRuleMapping &other)
{
  int ret = OB_SUCCESS;
  // All string fields are owned by the internal arena allocator_ — no external allocator needed.

  if (this == &other) {
    return ret;
  }
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for deep copy", K(ret), K(other));
  } else {
    reset();

    if (OB_SUCC(ret) && !other.original_db_name_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.original_db_name_, original_db_name_))) {
        LOG_WARN("failed to deep copy original_db_name", K(ret), K(other.original_db_name_));
      }
    }

    if (OB_SUCC(ret) && !other.original_table_name_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.original_table_name_, original_table_name_))) {
        LOG_WARN("failed to deep copy original_table_name", K(ret), K(other.original_table_name_));
      }
    }

    if (OB_SUCC(ret) && !other.db_fixed_prefix_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.db_fixed_prefix_, db_fixed_prefix_))) {
        LOG_WARN("failed to deep copy db_fixed_prefix", K(ret), K(other.db_fixed_prefix_));
      }
    }

    if (OB_SUCC(ret) && !other.db_fixed_suffix_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.db_fixed_suffix_, db_fixed_suffix_))) {
        LOG_WARN("failed to deep copy db_fixed_suffix", K(ret), K(other.db_fixed_suffix_));
      }
    }

    if (OB_SUCC(ret) && !other.table_fixed_prefix_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.table_fixed_prefix_, table_fixed_prefix_))) {
        LOG_WARN("failed to deep copy table_fixed_prefix", K(ret), K(other.table_fixed_prefix_));
      }
    }

    if (OB_SUCC(ret) && !other.table_fixed_suffix_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.table_fixed_suffix_, table_fixed_suffix_))) {
        LOG_WARN("failed to deep copy table_fixed_suffix", K(ret), K(other.table_fixed_suffix_));
      }
    }

    if (OB_SUCC(ret) && !other.db_var_name_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.db_var_name_, db_var_name_))) {
        LOG_WARN("failed to deep copy db_var_name", K(ret), K(other.db_var_name_));
      }
    }

    if (OB_SUCC(ret) && !other.db_var_regex_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.db_var_regex_, db_var_regex_))) {
        LOG_WARN("failed to deep copy db_var_regex", K(ret), K(other.db_var_regex_));
      }
    }

    if (OB_SUCC(ret) && !other.table_var_name_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.table_var_name_, table_var_name_))) {
        LOG_WARN("failed to deep copy table_var_name", K(ret), K(other.table_var_name_));
      }
    }

    if (OB_SUCC(ret) && !other.table_var_regex_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.table_var_regex_, table_var_regex_))) {
        LOG_WARN("failed to deep copy table_var_regex", K(ret), K(other.table_var_regex_));
      }
    }

    if (OB_SUCC(ret)) {
      ast_position_ = other.ast_position_;
      db_obj_id_ = other.db_obj_id_;
      tb_obj_id_ = other.tb_obj_id_;
    }

    if (OB_SUCC(ret) && !other.db_placeholder_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.db_placeholder_, db_placeholder_))) {
        LOG_WARN("failed to deep copy db_placeholder", K(ret), K(other.db_placeholder_));
      }
    }

    if (OB_SUCC(ret) && !other.tb_placeholder_.empty()) {
      if (OB_FAIL(ob_write_string(allocator_, other.tb_placeholder_, tb_placeholder_))) {
        LOG_WARN("failed to deep copy tb_placeholder", K(ret), K(other.tb_placeholder_));
      }
    }

    // Re-parse patterns from the deep-copied strings (pointers must point to new memory)
    if (OB_SUCC(ret)) {
      int tmp_ret = parse_patterns();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("parse_patterns failed during deep_copy, non-fatal", K(tmp_ret));
        // Non-fatal: patterns_parsed_ stays false, runtime falls back to plain prefix/suffix match
      }
    }
  }
  return ret;
}

int ObOutlineRuleMapping::set_original_db_name(const common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (name.empty()) {
    original_db_name_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, name, original_db_name_))) {
    LOG_WARN("fail to deep copy original_db_name", K(ret), K(name));
  }
  return ret;
}

int ObOutlineRuleMapping::set_original_table_name(const common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (name.empty()) {
    original_table_name_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, name, original_table_name_))) {
    LOG_WARN("fail to deep copy original_table_name", K(ret), K(name));
  }
  return ret;
}

int ObOutlineRuleMapping::set_db_fixed_prefix(const common::ObString &s)
{
  int ret = OB_SUCCESS;
  if (s.empty()) {
    db_fixed_prefix_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, s, db_fixed_prefix_))) {
    LOG_WARN("fail to deep copy db_fixed_prefix", K(ret), K(s));
  }
  return ret;
}

int ObOutlineRuleMapping::set_db_fixed_suffix(const common::ObString &s)
{
  int ret = OB_SUCCESS;
  if (s.empty()) {
    db_fixed_suffix_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, s, db_fixed_suffix_))) {
    LOG_WARN("fail to deep copy db_fixed_suffix", K(ret), K(s));
  }
  return ret;
}

int ObOutlineRuleMapping::set_table_fixed_prefix(const common::ObString &s)
{
  int ret = OB_SUCCESS;
  if (s.empty()) {
    table_fixed_prefix_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, s, table_fixed_prefix_))) {
    LOG_WARN("fail to deep copy table_fixed_prefix", K(ret), K(s));
  }
  return ret;
}

int ObOutlineRuleMapping::set_table_fixed_suffix(const common::ObString &s)
{
  int ret = OB_SUCCESS;
  if (s.empty()) {
    table_fixed_suffix_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, s, table_fixed_suffix_))) {
    LOG_WARN("fail to deep copy table_fixed_suffix", K(ret), K(s));
  }
  return ret;
}

int ObOutlineRuleMapping::set_db_var_name(const common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (name.empty()) {
    db_var_name_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, name, db_var_name_))) {
    LOG_WARN("fail to deep copy db_var_name", K(ret), K(name));
  }
  return ret;
}

int ObOutlineRuleMapping::set_db_var_regex(const common::ObString &regex)
{
  int ret = OB_SUCCESS;
  if (regex.empty()) {
    db_var_regex_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, regex, db_var_regex_))) {
    LOG_WARN("fail to deep copy db_var_regex", K(ret), K(regex));
  }
  return ret;
}

int ObOutlineRuleMapping::set_table_var_name(const common::ObString &name)
{
  int ret = OB_SUCCESS;
  if (name.empty()) {
    table_var_name_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, name, table_var_name_))) {
    LOG_WARN("fail to deep copy table_var_name", K(ret), K(name));
  }
  return ret;
}

int ObOutlineRuleMapping::set_table_var_regex(const common::ObString &regex)
{
  int ret = OB_SUCCESS;
  if (regex.empty()) {
    table_var_regex_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, regex, table_var_regex_))) {
    LOG_WARN("fail to deep copy table_var_regex", K(ret), K(regex));
  }
  return ret;
}

int ObOutlineRuleMapping::set_db_placeholder(const common::ObString &ph)
{
  int ret = OB_SUCCESS;
  if (ph.empty()) {
    db_placeholder_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, ph, db_placeholder_))) {
    LOG_WARN("fail to deep copy db_placeholder", K(ret), K(ph));
  }
  return ret;
}

int ObOutlineRuleMapping::set_tb_placeholder(const common::ObString &ph)
{
  int ret = OB_SUCCESS;
  if (ph.empty()) {
    tb_placeholder_.reset();
  } else if (OB_FAIL(ob_write_string(allocator_, ph, tb_placeholder_))) {
    LOG_WARN("fail to deep copy tb_placeholder", K(ret), K(ph));
  }
  return ret;
}

int ObOutlineRuleMapping::parse_patterns()
{
  int ret = OB_SUCCESS;
  // Assemble ObPatternVarInfo views from the resolver-populated
  // prefix/suffix/var fields. Escapes and ${VAR:regex} are already
  // resolved — no text parsing needed at this point.

  table_var_info_.reset();
  table_var_info_.prefix_    = table_fixed_prefix_;
  table_var_info_.suffix_    = table_fixed_suffix_;
  table_var_info_.var_name_  = table_var_name_;
  table_var_info_.var_regex_ = table_var_regex_;
  table_var_info_.has_var_   = !table_var_name_.empty();

  db_var_info_.reset();
  db_var_info_.prefix_    = db_fixed_prefix_;
  db_var_info_.suffix_    = db_fixed_suffix_;
  db_var_info_.var_name_  = db_var_name_;
  db_var_info_.var_regex_ = db_var_regex_;
  db_var_info_.has_var_   = !db_var_name_.empty();

  patterns_parsed_ = true;
  return ret;
}

// Helper: write a JSON-escaped string value (with surrounding quotes)
static int append_json_escaped_string(char *buf, int64_t buf_len, int64_t &pos,
                                       const ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\""))) { return ret; }
  for (int64_t i = 0; OB_SUCC(ret) && i < str.length(); ++i) {
    unsigned char c = static_cast<unsigned char>(str.ptr()[i]);
    if (c == '"' || c == '\\') {
      ret = databuff_printf(buf, buf_len, pos, "\\%c", c);
    } else if (c < 0x20) {
      ret = databuff_printf(buf, buf_len, pos, "\\u%04x", (unsigned)(unsigned char)c);
    } else {
      ret = databuff_printf(buf, buf_len, pos, "%c", c);
    }
  }
  if (OB_SUCC(ret)) { ret = databuff_printf(buf, buf_len, pos, "\""); }
  return ret;
}

// Helper: write a JSON key-value pair with escaped string value
static int append_json_kv(char *buf, int64_t buf_len, int64_t &pos,
                           const char *key, const ObString &val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%s\":", key))) {
  } else if (OB_FAIL(append_json_escaped_string(buf, buf_len, pos, val))) {
  }
  return ret;
}

int ObOutlineRuleMapping::to_json_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{"))) {
    LOG_WARN("fail to print json start", K(ret));
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "original_db", original_db_name_))) {
    LOG_WARN("fail to print original_db", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "original_table", original_table_name_))) {
    LOG_WARN("fail to print original_table", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "db_fixed_prefix", db_fixed_prefix_))) {
    LOG_WARN("fail to print db_fixed_prefix", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "db_fixed_suffix", db_fixed_suffix_))) {
    LOG_WARN("fail to print db_fixed_suffix", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "table_fixed_prefix", table_fixed_prefix_))) {
    LOG_WARN("fail to print table_fixed_prefix", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "table_fixed_suffix", table_fixed_suffix_))) {
    LOG_WARN("fail to print table_fixed_suffix", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\"db_var\":{\"name\":"))) {
  } else if (OB_FAIL(append_json_escaped_string(buf, buf_len, pos, db_var_name_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\"regex\":"))) {
  } else if (OB_FAIL(append_json_escaped_string(buf, buf_len, pos, db_var_regex_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "},\"table_var\":{\"name\":"))) {
  } else if (OB_FAIL(append_json_escaped_string(buf, buf_len, pos, table_var_name_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",\"regex\":"))) {
  } else if (OB_FAIL(append_json_escaped_string(buf, buf_len, pos, table_var_regex_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "},"))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"ast_position\":%ld,", ast_position_))) {
    LOG_WARN("fail to print ast_position", K(ret));
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "db_placeholder", db_placeholder_))) {
    LOG_WARN("fail to print db_placeholder", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
  } else if (OB_FAIL(append_json_kv(buf, buf_len, pos, "tb_placeholder", tb_placeholder_))) {
    LOG_WARN("fail to print tb_placeholder", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "}"))) {
    LOG_WARN("fail to print json end", K(ret));
  }
  return ret;
}

// ==================== ObOutlineBindingRule ====================


void ObOutlineBindingRule::reset()
{
  scope_ = obrpc::OUTLINE_SCOPE_DATABASE;
  map_items_.reset();
  is_set_ = false;
}

bool ObOutlineBindingRule::is_valid() const
{
  bool valid = is_set_;
  for (int64_t i = 0; valid && i < map_items_.count(); ++i) {
    valid = map_items_.at(i).is_valid();
  }
  return valid;
}


int ObOutlineBindingRule::assign(const ObOutlineBindingRule &other)
{
  int ret = OB_SUCCESS;
  scope_ = other.scope_;
  is_set_ = other.is_set_;
  if (OB_FAIL(map_items_.assign(other.map_items_))) {
    LOG_WARN("failed to assign map_items", K(ret));
  }
  return ret;
}

int ObOutlineBindingRule::deep_copy(common::ObIAllocator &allocator, const ObOutlineBindingRule &other)
{
  int ret = OB_SUCCESS;
  // The per-mapping arena owns its own strings; external allocator is not used.
  UNUSED(allocator);
  reset();
  scope_ = other.scope_;
  is_set_ = other.is_set_;

  for (int64_t i = 0; OB_SUCC(ret) && i < other.map_items_.count(); ++i) {
    ObOutlineRuleMapping mapping;
    if (OB_FAIL(mapping.deep_copy(other.map_items_.at(i)))) {
      LOG_WARN("failed to deep copy mapping item", K(ret), K(i));
    } else if (OB_FAIL(map_items_.push_back(mapping))) {
      LOG_WARN("failed to push back mapping item", K(ret), K(i));
    }
  }
  return ret;
}

int ObOutlineBindingRule::add_map_item(const ObOutlineRuleMapping &mapping)
{
  int ret = OB_SUCCESS;
  if (!mapping.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mapping item", K(ret), K(mapping));
  } else if (OB_FAIL(map_items_.push_back(mapping))) {
    LOG_WARN("failed to add mapping item", K(ret));
  } else {
    // Mark as having BINDING_RULE when MAP items are added
    // This is needed for MAP-only outlines (without explicit SCOPE=TENANT)
    is_set_ = true;
  }
  return ret;
}

int ObOutlineBindingRule::set_map_items(const common::ObIArray<ObOutlineRuleMapping> &items)
{
  int ret = OB_SUCCESS;
  map_items_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
    if (OB_FAIL(map_items_.push_back(items.at(i)))) {
      LOG_WARN("failed to add mapping item", K(ret), K(i));
    }
  }
  // Mark as having BINDING_RULE when MAP items are set
  if (OB_SUCC(ret) && items.count() > 0) {
    is_set_ = true;
  }
  return ret;
}

bool ObOutlineBindingRule::has_db_table_mapping() const
{
  bool has_db_table = false;
  for (int64_t i = 0; !has_db_table && i < map_items_.count(); ++i) {
    has_db_table = map_items_.at(i).has_db_prefix();
  }
  return has_db_table;
}

bool ObOutlineBindingRule::validate_scope_consistency() const
{
  // If any mapping has db.table form, SCOPE must be TENANT
  if (has_db_table_mapping() && scope_ != obrpc::OUTLINE_SCOPE_TENANT) {
    return false;
  }
  return true;
}

int ObOutlineBindingRule::to_json_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "{\"scope\":%d,\"items\":[", static_cast<int>(scope_)))) {
    LOG_WARN("fail to print json header", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < map_items_.count(); ++i) {
      if (i > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
        LOG_WARN("fail to print comma", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(map_items_.at(i).to_json_string(buf, buf_len, pos))) {
        LOG_WARN("fail to print map item json", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, "]}"))) {
      LOG_WARN("fail to print json footer", K(ret));
    }
  }
  return ret;
}

// ==================== JSON deserialization helpers ====================

// Helper: skip whitespace
static const char *skip_ws(const char *p, const char *end)
{
  while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) {
    ++p;
  }
  return p;
}

// Helper: extract a JSON quoted string value (between double quotes)
// Returns pointer after closing quote, or NULL on error
static const char *extract_json_string(const char *p, const char *end,
                                       ObString &out)
{
  if (OB_ISNULL(p) || p >= end || *p != '"') {
    return NULL;
  }
  ++p; // skip opening "
  const char *start = p;
  while (p < end && *p != '"') {
    if (*p == '\\' && p + 1 < end) {
      p += 2; // skip escaped char
    } else {
      ++p;
    }
  }
  if (p >= end) {
    return NULL;
  }
  out.assign_ptr(start, static_cast<int32_t>(p - start));
  ++p; // skip closing "
  return p;
}

// Helper: unescape JSON string in-place if it contains backslash sequences
// Allocates new buffer only if unescaping is needed
static int unescape_json_value(ObIAllocator &allocator, ObString &str)
{
  int ret = OB_SUCCESS;
  if (str.empty()) { return ret; }
  // Quick scan: does str contain '\'?
  bool needs_unescape = false;
  for (int64_t i = 0; i < str.length(); ++i) {
    if (str.ptr()[i] == '\\') { needs_unescape = true; break; }
  }
  if (!needs_unescape) { return ret; }
  // Allocate and unescape
  char *buf = static_cast<char *>(allocator.alloc(str.length()));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc for JSON unescape", K(ret), K(str.length()));
  } else {
    int64_t out = 0;
    for (int64_t i = 0; i < str.length(); ++i) {
      if (str.ptr()[i] == '\\' && i + 1 < str.length()) {
        char next = str.ptr()[i + 1];
        if (next == '"' || next == '\\' || next == '/') { buf[out++] = next; ++i; }
        else if (next == 'n') { buf[out++] = '\n'; ++i; }
        else if (next == 'r') { buf[out++] = '\r'; ++i; }
        else if (next == 't') { buf[out++] = '\t'; ++i; }
        else { buf[out++] = str.ptr()[i]; } // keep unknown escape as-is
      } else {
        buf[out++] = str.ptr()[i];
      }
    }
    str.assign_ptr(buf, static_cast<int32_t>(out));
  }
  return ret;
}

// Helper: extract JSON integer value
static const char *extract_json_int(const char *p, const char *end,
                                    int64_t &out)
{
  if (OB_ISNULL(p) || p >= end) {
    return NULL;
  }
  bool negative = false;
  if (*p == '-') {
    negative = true;
    ++p;
  }
  if (p >= end || *p < '0' || *p > '9') {
    return NULL;
  }
  int64_t val = 0;
  while (p < end && *p >= '0' && *p <= '9') {
    val = val * 10 + (*p - '0');
    ++p;
  }
  out = negative ? -val : val;
  return p;
}

int ObOutlineRuleMapping::from_json_kv(const ObString &key, const ObString &value,
                                       ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  struct KeyField {
    const char *key_;
    ObString ObOutlineRuleMapping::*field_;
  };
  static const KeyField kKeyFields[] = {
    { "original_db",        &ObOutlineRuleMapping::original_db_name_ },
    { "original_table",     &ObOutlineRuleMapping::original_table_name_ },
    { "db_fixed_prefix",    &ObOutlineRuleMapping::db_fixed_prefix_ },
    { "db_fixed_suffix",    &ObOutlineRuleMapping::db_fixed_suffix_ },
    { "table_fixed_prefix", &ObOutlineRuleMapping::table_fixed_prefix_ },
    { "table_fixed_suffix", &ObOutlineRuleMapping::table_fixed_suffix_ },
    { "db_placeholder",     &ObOutlineRuleMapping::db_placeholder_ },
    { "tb_placeholder",     &ObOutlineRuleMapping::tb_placeholder_ },
  };
  bool matched = false;
  for (int64_t i = 0; OB_SUCC(ret) && !matched && i < ARRAYSIZEOF(kKeyFields); ++i) {
    const KeyField &kf = kKeyFields[i];
    const int32_t kf_len = static_cast<int32_t>(STRLEN(kf.key_));
    if (key.length() == kf_len
        && 0 == MEMCMP(key.ptr(), kf.key_, kf_len)) {
      matched = true;
      if (OB_FAIL(ob_write_string(allocator_, value, this->*(kf.field_)))) {
        LOG_WARN("fail to copy field", K(ret), K(key));
      }
    }
  }
  // db_var/table_var nested objects and ast_position handled by caller
  return ret;
}

int ObOutlineBindingRule::serialize_pattern_rules(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) {
    LOG_WARN("fail to print array start", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < map_items_.count(); ++i) {
      if (i > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {
        LOG_WARN("fail to print comma", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(map_items_.at(i).to_json_string(buf, buf_len, pos))) {
        LOG_WARN("fail to print map item json", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
      LOG_WARN("fail to print array end", K(ret));
    }
  }
  return ret;
}

int ObOutlineBindingRule::deserialize_pattern_rules(const ObString &json,
                                                    ObIAllocator &allocator,
                                                    ObIArray<ObOutlineRuleMapping> &items)
{
  int ret = OB_SUCCESS;
  items.reset();

  if (json.empty()) {
    // Empty pattern_rules means exact match outline, not a template
    return ret;
  }

  const char *p = json.ptr();
  const char *end = p + json.length();

  p = skip_ws(p, end);
  if (p >= end || *p != '[') {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("expected '[' at start of pattern_rules", K(ret), K(json));
    return ret;
  }
  ++p; // skip '['

  while (OB_SUCC(ret)) {
    p = skip_ws(p, end);
    if (p >= end) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("unexpected end of pattern_rules", K(ret));
      break;
    }
    if (*p == ']') {
      break; // end of array
    }
    if (*p != '{') {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("expected '{' for mapping object", K(ret));
      break;
    }
    ++p; // skip '{'

    ObOutlineRuleMapping mapping;
    ObString db_var_name;
    ObString db_var_regex;
    ObString table_var_name;
    ObString table_var_regex;

    // Parse key-value pairs within object
    while (OB_SUCC(ret)) {
      p = skip_ws(p, end);
      if (p >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        break;
      }
      if (*p == '}') {
        ++p;
        break;
      }
      if (*p == ',') {
        ++p;
        p = skip_ws(p, end);
      }

      // Extract key
      ObString key;
      p = extract_json_string(p, end, key);
      if (OB_ISNULL(p)) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("fail to parse JSON key", K(ret));
        break;
      }
      p = skip_ws(p, end);
      if (p >= end || *p != ':') {
        ret = OB_ERR_PARSER_SYNTAX;
        break;
      }
      ++p; // skip ':'
      p = skip_ws(p, end);

      if (key == ObString::make_string("ast_position")) {
        // Integer value
        int64_t val = 0;
        p = extract_json_int(p, end, val);
        if (OB_ISNULL(p)) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to parse ast_position", K(ret));
        } else {
          mapping.set_ast_position(val);
        }
      } else if (key == ObString::make_string("db_var") ||
                 key == ObString::make_string("table_var")) {
        // Nested object: {"name":"...", "regex":"..."}
        if (p >= end || *p != '{') {
          ret = OB_ERR_PARSER_SYNTAX;
          break;
        }
        ++p;
        ObString var_name;
        ObString var_regex;
        while (OB_SUCC(ret) && p < end && *p != '}') {
          if (*p == ',') { ++p; }
          p = skip_ws(p, end);
          ObString sub_key;
          p = extract_json_string(p, end, sub_key);
          if (OB_ISNULL(p)) { ret = OB_ERR_PARSER_SYNTAX; break; }
          p = skip_ws(p, end);
          if (p >= end || *p != ':') { ret = OB_ERR_PARSER_SYNTAX; break; }
          ++p;
          p = skip_ws(p, end);
          ObString sub_val;
          p = extract_json_string(p, end, sub_val);
          if (OB_ISNULL(p)) { ret = OB_ERR_PARSER_SYNTAX; break; }
          if (OB_SUCC(ret) && OB_FAIL(unescape_json_value(allocator, sub_val))) {
            LOG_WARN("fail to unescape var sub_val", K(ret));
            break;
          }
          p = skip_ws(p, end);
          if (sub_key == ObString::make_string("name")) {
            var_name = sub_val;
          } else if (sub_key == ObString::make_string("regex")) {
            var_regex = sub_val;
          }
        }
        if (OB_SUCC(ret) && p < end && *p == '}') {
          ++p;
        }
        if (OB_SUCC(ret)) {
          if (key == ObString::make_string("db_var")) {
            db_var_name = var_name;
            db_var_regex = var_regex;
          } else {
            table_var_name = var_name;
            table_var_regex = var_regex;
          }
        }
      } else {
        // String value for other keys
        ObString val;
        p = extract_json_string(p, end, val);
        if (OB_ISNULL(p)) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to parse JSON string value", K(ret), K(key));
        } else if (OB_FAIL(unescape_json_value(allocator, val))) {
          LOG_WARN("fail to unescape JSON value", K(ret), K(key));
        } else if (OB_FAIL(mapping.from_json_kv(key, val, allocator))) {
          LOG_WARN("fail to set mapping field", K(ret), K(key), K(val));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Setters deep-copy via internal allocator
      if (!db_var_name.empty() && OB_FAIL(mapping.set_db_var_name(db_var_name))) {
        LOG_WARN("fail to set db_var_name", K(ret));
      } else if (!db_var_regex.empty() && OB_FAIL(mapping.set_db_var_regex(db_var_regex))) {
        LOG_WARN("fail to set db_var_regex", K(ret));
      } else if (!table_var_name.empty() && OB_FAIL(mapping.set_table_var_name(table_var_name))) {
        LOG_WARN("fail to set table_var_name", K(ret));
      } else if (!table_var_regex.empty() && OB_FAIL(mapping.set_table_var_regex(table_var_regex))) {
        LOG_WARN("fail to set table_var_regex", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(items.push_back(mapping))) {
        LOG_WARN("fail to push back mapping", K(ret));
      }
    }

    // Skip comma between array items
    p = skip_ws(p, end);
    if (p < end && *p == ',') {
      ++p;
    }
  }

  return ret;
}

int ObOutlineBindingRule::deserialize_from_json(
    const ObString &json, ObIAllocator &allocator, ObOutlineBindingRule &rule)
{
  int ret = OB_SUCCESS;
  rule.reset();
  if (json.empty()) {
    return ret;
  }

  const char *p = skip_ws(json.ptr(), json.ptr() + json.length());
  const char *end = json.ptr() + json.length();
  if (p >= end) {
    return ret;
  }

  if (*p == '{') {
    // Wrapped format: {"scope":N,"items":[...]}
    ++p; // skip '{'
    int64_t scope_val = obrpc::OUTLINE_SCOPE_DATABASE;
    ObString items_json;
    while (OB_SUCC(ret) && p < end && *p != '}') {
      if (*p == ',') { ++p; }
      p = skip_ws(p, end);
      if (p >= end || *p == '}') { break; }
      ObString key;
      p = extract_json_string(p, end, key);
      if (OB_ISNULL(p)) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("fail to parse key in wrapped JSON", K(ret));
        break;
      }
      p = skip_ws(p, end);
      if (p >= end || *p != ':') {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("expected ':' in wrapped JSON", K(ret));
        break;
      }
      ++p; // skip ':'
      p = skip_ws(p, end);

      if (key == ObString::make_string("scope")) {
        p = extract_json_int(p, end, scope_val);
        if (OB_ISNULL(p)) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("fail to parse scope value", K(ret));
          break;
        }
      } else if (key == ObString::make_string("items")) {
        // Find the matching ']' for the items array
        if (p >= end || *p != '[') {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("expected '[' for items array", K(ret));
          break;
        }
        const char *arr_start = p;
        int depth = 1;
        ++p;
        while (p < end && depth > 0) {
          if (*p == '"') {
            ++p; // skip opening quote
            while (p < end && *p != '"') {
              if (*p == '\\' && p + 1 < end) { p += 2; } else { ++p; }
            }
            if (p < end) { ++p; } // skip closing quote
          } else {
            if (*p == '[') { ++depth; }
            else if (*p == ']') { --depth; }
            ++p;
          }
        }
        items_json.assign_ptr(arr_start, static_cast<int32_t>(p - arr_start));
      } else {
        // Skip unknown value (string or number)
        if (*p == '"') {
          ObString dummy;
          p = extract_json_string(p, end, dummy);
          if (OB_ISNULL(p)) {
            ret = OB_ERR_PARSER_SYNTAX;
            break;
          }
        } else {
          // Skip numeric or other simple value
          while (p < end && *p != ',' && *p != '}') { ++p; }
        }
      }
      p = skip_ws(p, end);
    }
    if (OB_SUCC(ret)) {
      rule.scope_ = scope_val;
      if (!items_json.empty()) {
        if (OB_FAIL(deserialize_pattern_rules(items_json, allocator, rule.map_items_))) {
          LOG_WARN("fail to deserialize items array", K(ret));
        }
      }
    }
  } else if (*p == '[') {
    // Direct array format: [{...}]
    if (OB_FAIL(deserialize_pattern_rules(json, allocator, rule.map_items_))) {
      LOG_WARN("fail to deserialize pattern rules array", K(ret));
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("unexpected JSON start char for pattern rules", K(ret));
  }
  if (OB_SUCC(ret)) {
    rule.is_set_ = true;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase