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

#ifndef _OB_SQL_RESOLVER_DDL_SEQUENCE_RESOLVER_H_
#define _OB_SQL_RESOLVER_DDL_SEQUENCE_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/resolver/ob_stmt.h"
#include "share/sequence/ob_sequence_option.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
namespace sql {

template <class T>
class ObSequenceResolver {
public:
  ObSequenceResolver()
  {}
  ~ObSequenceResolver() = default;

public:
  int resolve_sequence_options(T* stmt, ParseNode* node);

private:
  int resolve_sequence_option(T* stmt, ParseNode* node);
  int get_normalized_number(ParseNode& node, common::ObIAllocator& allocator, common::number::ObNumber& num);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSequenceResolver);
};

template <class T>
int ObSequenceResolver<T>::resolve_sequence_options(T* stmt, ParseNode* node)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(node)) {
    if (OB_UNLIKELY(T_SEQUENCE_OPTION_LIST != node->type_ || 0 > node->num_child_ || OB_ISNULL(stmt))) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "invalid node", KP(stmt), K(ret));
    } else {
      ParseNode* option_node = NULL;
      int32_t num = node->num_child_;
      for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
        option_node = node->children_[i];
        if (OB_FAIL(resolve_sequence_option(stmt, option_node))) {
          SQL_LOG(WARN, "resolve sequence option failed", K(ret));
        }
      }

      // conflict check
      if (OB_SUCC(ret)) {
        const ObBitSet<>& option_bitset = stmt->get_arg().get_option_bitset();
        if (option_bitset.has_member(share::ObSequenceArg::MAXVALUE) &&
            option_bitset.has_member(share::ObSequenceArg::NOMAXVALUE)) {
          // conflicting MAXVALUE/NOMAXVALUE specifications
          ret = common::OB_ERR_CONFL_MAXVALUE_SPEC;

        } else if (option_bitset.has_member(share::ObSequenceArg::MINVALUE) &&
                   option_bitset.has_member(share::ObSequenceArg::NOMINVALUE)) {
          // conflicting MINVALUE/NOMINVALUE specifications
          ret = common::OB_ERR_CONFL_MINVALUE_SPEC;

        } else if (option_bitset.has_member(share::ObSequenceArg::CACHE) &&
                   option_bitset.has_member(share::ObSequenceArg::NOCACHE)) {
          // conflicting CACHE/NOCACHE specifications
          ret = common::OB_ERR_CONFL_CACHE_SPEC;

        } else if (option_bitset.has_member(share::ObSequenceArg::ORDER) &&
                   option_bitset.has_member(share::ObSequenceArg::NOORDER)) {
          // conflicting ORDER/NOORDER specifications
          ret = common::OB_ERR_CONFL_ORDER_SPEC;

        } else if (option_bitset.has_member(share::ObSequenceArg::CYCLE) &&
                   option_bitset.has_member(share::ObSequenceArg::NOCYCLE)) {
          // conflicting CYCLE/NOCYCLE specifications
          ret = common::OB_ERR_CONFL_CYCLE_SPEC;
        }
      }
    }
  }
  return ret;
}

template <class T>
int ObSequenceResolver<T>::resolve_sequence_option(T* stmt, ParseNode* node)
{
  using namespace common;
  int ret = common::OB_SUCCESS;
  ParseNode* option_node = node;
  if (OB_ISNULL(stmt) || OB_ISNULL(node)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_LOG(ERROR, "null ptr", KP(stmt), KP(node), K(ret));
  } else if (option_node) {
    ObBitSet<>& option_bitset = stmt->get_arg().get_option_bitset();
    common::number::ObNumber num;
    share::ObSequenceValueAllocator allocator;
    switch (option_node->type_) {
      case T_INCREMENT_BY: {
        if (option_bitset.has_member(share::ObSequenceArg::INCREMENT_BY)) {
          ret = common::OB_ERR_DUP_INCREMENT_BY_SPEC;

        } else if (1 != option_node->num_child_) {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "expect only 1 param", K(option_node->num_child_), K(ret));
        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::INCREMENT_BY))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else if (T_NUMBER == option_node->children_[0]->type_) {
          if (OB_FAIL(get_normalized_number(*option_node->children_[0], allocator, num))) {
            SQL_LOG(WARN, "fail normalize number", K(ret));
          } else {
            ret = stmt->option().set_increment_by(num);
          }
        } else {
          int64_t value = option_node->children_[0]->value_;
          stmt->option().set_increment_by(value);
        }
        break;
      }
      case T_START_WITH: {
        if (stmt::T_ALTER_SEQUENCE == stmt->get_stmt_type()) {
          // duplicate INCREMENT BY specifications
          ret = common::OB_ERR_ALTER_START_SEQ_NUMBER_NOT_ALLOWED;

        } else if (option_bitset.has_member(share::ObSequenceArg::START_WITH)) {
          // duplicate START WITH specifications
          ret = common::OB_ERR_DUP_START_WITH_SPEC;

        } else if (1 != option_node->num_child_) {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "expect only 1 param", K(option_node->num_child_), K(ret));
        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::START_WITH))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else if (T_NUMBER == option_node->children_[0]->type_) {
          if (OB_FAIL(get_normalized_number(*option_node->children_[0], allocator, num))) {
            SQL_LOG(WARN, "fail normalize number", K(ret));
          } else {
            ret = stmt->option().set_start_with(num);
          }
        } else {
          int64_t value = option_node->children_[0]->value_;
          stmt->option().set_start_with(value);
        }
        break;
      }
      case T_MAXVALUE: {
        if (option_bitset.has_member(share::ObSequenceArg::MAXVALUE)) {
          // duplicate MAXVALUE/NOMAXVALUE specifications
          ret = common::OB_ERR_DUP_MAXVALUE_SPEC;

        } else if (1 != option_node->num_child_) {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "expect only 1 param", K(option_node->num_child_), K(ret));
        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::MAXVALUE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else if (T_NUMBER == option_node->children_[0]->type_) {
          if (OB_FAIL(get_normalized_number(*option_node->children_[0], allocator, num))) {
            SQL_LOG(WARN, "fail normalize number", K(ret));
          } else {
            ret = stmt->option().set_max_value(num);
          }
        } else {
          int64_t value = option_node->children_[0]->value_;
          stmt->option().set_max_value(value);
        }
        break;
      }
      case T_MINVALUE: {
        if (option_bitset.has_member(share::ObSequenceArg::MINVALUE)) {
          // duplicate MINVALUE/NOMINVALUE specifications
          ret = common::OB_ERR_DUP_MINVALUE_SPEC;

        } else if (1 != option_node->num_child_) {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "expect only 1 param", K(option_node->num_child_), K(ret));
        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::MINVALUE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else if (T_NUMBER == option_node->children_[0]->type_) {
          if (OB_FAIL(get_normalized_number(*option_node->children_[0], allocator, num))) {
            SQL_LOG(WARN, "fail normalize number", K(ret));
          } else {
            ret = stmt->option().set_min_value(num);
          }
        } else {
          int64_t value = option_node->children_[0]->value_;
          stmt->option().set_min_value(value);
        }
        break;
      }
      case T_NOMAXVALUE: {
        if (option_bitset.has_member(share::ObSequenceArg::NOMAXVALUE)) {
          // duplicate MAXVALUE/NOMAXVALUE specifications
          ret = common::OB_ERR_DUP_MAXVALUE_SPEC;

        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::NOMAXVALUE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_nomaxvalue();
        }
        break;
      }
      case T_NOMINVALUE: {
        if (option_bitset.has_member(share::ObSequenceArg::NOMINVALUE)) {
          // duplicate MINVALUE/NOMINVALUE specifications
          ret = common::OB_ERR_DUP_MINVALUE_SPEC;
        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::NOMINVALUE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_nominvalue();
        }
        break;
      }
      case T_CACHE: {
        if (option_bitset.has_member(share::ObSequenceArg::CACHE)) {
          // duplicate CACHE/NOCACHE specifications
          ret = common::OB_ERR_DUP_CACHE_SPEC;

        } else if (1 != option_node->num_child_) {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_LOG(WARN, "expect only 1 param", K(option_node->num_child_), K(ret));
        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::CACHE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else if (T_NUMBER == option_node->children_[0]->type_) {
          if (OB_FAIL(get_normalized_number(*option_node->children_[0], allocator, num))) {
            SQL_LOG(WARN, "fail normalize number", K(ret));
          } else {
            ret = stmt->option().set_cache_size(num);
          }
        } else {
          int64_t value = option_node->children_[0]->value_;
          stmt->option().set_cache_size(value);
        }
        break;
      }
      case T_NOCACHE: {
        if (option_bitset.has_member(share::ObSequenceArg::NOCACHE)) {
          // duplicate CACHE/NOCACHE specifications
          ret = common::OB_ERR_DUP_CACHE_SPEC;

        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::NOCACHE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_cache_size(share::ObSequenceOption::NO_CACHE);
        }
        break;
      }
      case T_CYCLE: {
        if (option_bitset.has_member(share::ObSequenceArg::CYCLE)) {
          // duplicate CYCLE/NOCYCLE specifications
          ret = common::OB_ERR_DUP_CYCLE_SPEC;

        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::CYCLE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_cycle_flag(true);
        }
        break;
      }
      case T_NOCYCLE: {
        if (option_bitset.has_member(share::ObSequenceArg::NOCYCLE)) {
          // duplicate CYCLE/NOCYCLE specifications
          ret = common::OB_ERR_DUP_CYCLE_SPEC;

        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::NOCYCLE))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_cycle_flag(false);
        }
        break;
      }
      case T_ORDER: {
        if (option_bitset.has_member(share::ObSequenceArg::ORDER)) {
          // duplicate ORDER/NOORDER specifications
          ret = common::OB_ERR_DUP_ORDER_SPEC;

        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::ORDER))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_order_flag(true);
        }
        break;
      }
      case T_NOORDER: {
        if (option_bitset.has_member(share::ObSequenceArg::NOORDER)) {
          // duplicate ORDER/NOORDER specifications
          ret = common::OB_ERR_DUP_ORDER_SPEC;

        } else if (OB_FAIL(option_bitset.add_member(share::ObSequenceArg::NOORDER))) {
          SQL_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          stmt->option().set_order_flag(false);
        }
        break;
      }
      default: {
        /* won't be here */
        ret = common::OB_ERR_UNEXPECTED;
        SQL_LOG(ERROR, "code should not reach here", K(ret));
        break;
      }
    }
  }
  return ret;
}

template <class T>
int ObSequenceResolver<T>::get_normalized_number(
    ParseNode& node, common::ObIAllocator& allocator, common::number::ObNumber& num)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(num.from(node.str_value_, static_cast<int32_t>(node.str_len_), allocator))) {
    SQL_LOG(WARN, "fail convert number", K(ret));
  } else if (num > share::ObSequenceMaxMinInitializer::MAX_VALUE.val()) {
    num.shadow_copy(share::ObSequenceMaxMinInitializer::MAX_VALUE.val());
  } else if (num < share::ObSequenceMaxMinInitializer::MIN_VALUE.val()) {
    num.shadow_copy(share::ObSequenceMaxMinInitializer::MIN_VALUE.val());
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif
