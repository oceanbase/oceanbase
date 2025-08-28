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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_ccl_schema_struct.h"

namespace oceanbase {
namespace share {
namespace schema {

ObSimpleCCLRuleSchema::ObSimpleCCLRuleSchema() : ObSchema() { reset(); }

ObSimpleCCLRuleSchema::ObSimpleCCLRuleSchema(common::ObIAllocator *allocator)
    : ObSchema(allocator) {
  reset();
}

ObSimpleCCLRuleSchema::~ObSimpleCCLRuleSchema() {}

ObSimpleCCLRuleSchema::ObSimpleCCLRuleSchema(const ObSimpleCCLRuleSchema &other) : ObSchema() {
  *this = other;
}

ObSimpleCCLRuleSchema &ObSimpleCCLRuleSchema::operator=(const ObSimpleCCLRuleSchema &other) {
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    ccl_rule_id_ = other.ccl_rule_id_;
    schema_version_ = other.schema_version_;

    affect_for_all_databases_ = other.affect_for_all_databases_;
    affect_for_all_tables_ = other.affect_for_all_tables_;
    affect_dml_ = other.affect_dml_;
    name_case_mode_= other.name_case_mode_;

    // str
    if (OB_FAIL(set_ccl_rule_name(other.ccl_rule_name_))) {
      LOG_WARN("Fail to deep copy ccl_rule name");
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObSimpleCCLRuleSchema::assign(const ObSimpleCCLRuleSchema &other) {
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObSimpleCCLRuleSchema::is_valid() const {
  bool ret = true;
  if (!ObSchema::is_valid() || !is_valid_tenant_id(tenant_id_) ||
      ccl_rule_name_.empty()) {
    ret = false;
  }
  return ret;
}

void ObSimpleCCLRuleSchema::reset() {
  tenant_id_ = OB_INVALID_ID;
  ccl_rule_id_ = OB_INVALID_ID;
  ccl_rule_name_.reset();
  schema_version_ = OB_INVALID_VERSION;
  affect_for_all_databases_ = false;
  affect_for_all_tables_ = false;
  name_case_mode_ = OB_NAME_CASE_INVALID;
  ObSchema::reset();
}

int64_t ObSimpleCCLRuleSchema::get_convert_size() const {
  int64_t convert_size = sizeof(*this);
  convert_size += ccl_rule_name_.length() + 1;
  return convert_size;
}

OB_SERIALIZE_MEMBER(ObSimpleCCLRuleSchema, tenant_id_, ccl_rule_id_, ccl_rule_name_,
                    schema_version_, affect_for_all_databases_,
                    affect_for_all_tables_, affect_dml_, name_case_mode_);

ObCCLRuleSchema::ObCCLRuleSchema() : ObSimpleCCLRuleSchema() { reset(); }

ObCCLRuleSchema::ObCCLRuleSchema(common::ObIAllocator *allocator)
    : ObSimpleCCLRuleSchema(allocator) {
  reset();
}

ObCCLRuleSchema::~ObCCLRuleSchema() {}

ObCCLRuleSchema::ObCCLRuleSchema(const ObCCLRuleSchema &other) : ObSimpleCCLRuleSchema() {
  *this = other;
}

ObCCLRuleSchema &ObCCLRuleSchema::operator=(const ObCCLRuleSchema &other) {
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    ObSimpleCCLRuleSchema::operator=(other);
    if (OB_SUCC(error_ret_)) {
      affect_scope_ = other.affect_scope_;
      max_concurrency_ = other.max_concurrency_;
      if (OB_FAIL(set_affect_database(other.affect_database_))) {
        LOG_WARN("Fail to deep copy ccl_rule affect database");
      } else if (OB_FAIL(set_affect_table(other.affect_table_))) {
        LOG_WARN("Fail to deep copy ccl_rule affect table");
      } else if (OB_FAIL(set_affect_user_name(other.affect_user_name_))) {
        LOG_WARN("Fail to deep copy ccl_rule affect user name");
      } else if (OB_FAIL(set_affect_host(other.affect_host_))) {
        LOG_WARN("Fail to deep copy ccl_rule affect host name");
      } else if (OB_FAIL(set_ccl_keywords(other.ccl_keywords_))) {
        LOG_WARN("Fail to deep copy ccl_rule ccl_keywords");
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < other.ccl_keywords_array_.count(); i++) {
          ObString tmp_keyword;
          if (OB_FAIL(ob_write_string(*allocator_, other.ccl_keywords_array_.at(i), tmp_keyword))) {
            LOG_WARN("ob write string failed", K(ret));
          } else if (OB_FAIL(ccl_keywords_array_.push_back(tmp_keyword))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
    }
    
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObCCLRuleSchema::assign(const ObCCLRuleSchema &other) {
  int ret = OB_SUCCESS;
  *this = other;
  ret = get_err_ret();
  return ret;
}

bool ObCCLRuleSchema::is_valid() const {
  return ObSimpleCCLRuleSchema::is_valid();
}

void ObCCLRuleSchema::reset() {
  affect_database_.reset();
  affect_table_.reset();
  affect_user_name_.reset();
  affect_host_.reset();
  ccl_keywords_.reset();
  ccl_keywords_array_.reset();
  ObSimpleCCLRuleSchema::reset();
  ObSchema::reset();
}

int64_t ObCCLRuleSchema::get_convert_size() const {
  int64_t convert_size = 0;
  convert_size += ObSimpleCCLRuleSchema::get_convert_size();
  convert_size += sizeof(ObCCLRuleSchema) - sizeof(ObSimpleCCLRuleSchema);
  convert_size += affect_database_.length() + 1;
  convert_size += affect_table_.length() + 1;
  convert_size += ccl_keywords_.length() + 1;
  convert_size += affect_user_name_.length() + 1;
  convert_size += affect_host_.length() + 1;
  convert_size += ccl_keywords_array_.get_data_size();
  for (int64_t i = 0; i < ccl_keywords_array_.count(); ++i) {
    convert_size += ccl_keywords_array_.at(i).length() + 1;
  }
  return convert_size;
}

OB_SERIALIZE_MEMBER((ObCCLRuleSchema, ObSimpleCCLRuleSchema), affect_user_name_,
                    affect_host_, affect_database_, affect_table_,
                    affect_scope_, ccl_keywords_, max_concurrency_, ccl_keywords_array_);

int ObCCLRuleSchema::split_strings_with_escape(char separator,
                                               char escape_char) {
  int ret = OB_SUCCESS;
  ObSqlString current_token;
  ObString keyword;
  for (size_t i = 0; OB_SUCC(ret) && i < ccl_keywords_.length(); ++i) {
    char current_char = ccl_keywords_[i];
    if (current_char == escape_char) {
      // when meet '\' and there is no other char remain
      if (i + 1 >= ccl_keywords_.length()) {
        //do nothing
        if (OB_FAIL(current_token.append(&escape_char, 1))){
          // normal character, add into current_token
          LOG_WARN("fail to append escape_char to current_token", K(ret), K(current_token), K(escape_char));
        }
      } else {
        char next_char = ccl_keywords_[++i]; // move to next character
        if (next_char == separator) {
          // '\;' restore to ';'
          if (OB_FAIL(current_token.append(&separator, 1))) {
            LOG_WARN("fail to append separator to current_token", K(ret), K(current_token), K(separator));
          }
        } else if (next_char == escape_char) {
          // '\\' restore to '\'
          if (OB_FAIL(current_token.append(&escape_char, 1))) {
            LOG_WARN("fail to append separator to current_token", K(ret), K(current_token), K(escape_char));
          }
        } else {
          // meet '\X'，X is not '\' nor ';'
          if (OB_FAIL(current_token.append(&escape_char, 1))){
            LOG_WARN("fail to append escape_char to current_token", K(ret), K(current_token), K(escape_char));
          } else if (OB_FAIL(current_token.append(&next_char, 1))){
            LOG_WARN("fail to append next_char to current_token", K(ret), K(current_token), K(next_char));
          }
        }
      }
    } else if (current_char == separator) {
      if (OB_FAIL(ob_write_string(*allocator_, current_token.string(), keyword))) {
        LOG_WARN("fail to deep copy str", K(ret));
      } else if (OB_FAIL(ccl_keywords_array_.push_back(keyword))) {
        LOG_WARN("fail to push_back keyword string", K(ret), K(keyword));
      } else {
        current_token.reset();
      }
    } else if (OB_FAIL(current_token.append(&current_char, 1))){
      // normal character, add into current_token
      LOG_WARN("fail to append current_char to current_token", K(ret), K(current_token), K(current_char));
    }
  }

  // last token must be added into ccl_keywords_array_
  if (OB_FAIL(ob_write_string(*allocator_, current_token.string(), keyword))) {
    LOG_WARN("fail to deep copy str", K(ret));
  } else if (OB_FAIL(ccl_keywords_array_.push_back(keyword))) {
    LOG_WARN("fail to push_back keyword string", K(ret), K(keyword));
  }

  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase