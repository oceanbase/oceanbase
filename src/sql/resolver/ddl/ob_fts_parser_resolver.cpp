/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/fts/ob_fts_parser_property.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "sql/resolver/ddl/ob_fts_parser_resolver.h"

namespace oceanbase
{
namespace sql
{

int ObFTParserResolverHelper::resolve_parser_properties(
    const ParseNode &parse_tree,
    common::ObIAllocator &allocator,
    common::ObString &parser_property)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(parse_tree.num_child_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, parser properties is empty", K(ret), K(parse_tree.num_child_));
  } else {
    storage::ObFTParserJsonProps property;
    if (OB_FAIL(property.init())) {
      LOG_WARN("fail to init parser properties", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
      if (OB_ISNULL(parse_tree.children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("option_node child is nullptr", K(ret));
      } else if (OB_FAIL(resolve_fts_index_parser_properties(parse_tree.children_[i], property))) {
        LOG_WARN("fail to resolve fts index parser properties", K(ret));
      }
    }
    bool has_conflict = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(property.check_conflict_config_for_resolve(has_conflict))) {
      LOG_WARN("invalid argument", K(ret), K(property));
    } else if (has_conflict) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the parser properties has conflict config");
    } else if (OB_FAIL(property.to_format_json(allocator, parser_property))) {
      LOG_WARN("fail to serialize parser properties", K(ret), K(property));
    }
  }
  return ret;
}

int ObFTParserResolverHelper::resolve_fts_index_parser_properties(
    const ParseNode *node,
    storage::ObFTParserJsonProps &property)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || node->num_child_ != 1 || OB_ISNULL(node->children_[0])){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse node", K(ret), KP(node));
  } else {
    switch (node->type_) {
      case T_PARSER_MIN_TOKEN_SIZE: {
        if (OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is null", K(node->children_[0]), K(ret));
        } else if (OB_UNLIKELY(!property.is_valid_min_token_size(node->children_[0]->value_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid min token size.",
                   K(ObString(ObFTSLiteral::MIN_TOKEN_SIZE_SCOPE_STR)),
                   K(ret),
                   K(node->children_[0]->value_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_TOKEN_SIZE_SCOPE_STR);
        } else if (OB_FAIL(property.config_set_min_token_size(node->children_[0]->value_))) {
          LOG_WARN("fail to set min token size", K(ret));
        }
        break;
      }
      case T_PARSER_MAX_TOKEN_SIZE: {
        if (OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is null", K(node->children_[0]), K(ret));
        } else if (OB_UNLIKELY(!property.is_valid_max_token_size(node->children_[0]->value_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid  max_token_size",
                   K(ObString(ObFTSLiteral::MAX_TOKEN_SIZE_SCOPE_STR)),
                   K(ret),
                   K(node->children_[0]->value_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MAX_TOKEN_SIZE_SCOPE_STR);
        } else if (OB_FAIL(property.config_set_max_token_size(node->children_[0]->value_))) {
          LOG_WARN("fail to set max token size", K(ret));
        }
        break;
      }
      case T_PARSER_NGRAM_TOKEN_SIZE: {
        if (OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is null", K(node->children_[0]), K(ret));
        } else if (OB_UNLIKELY(!property.is_valid_ngram_token_size(node->children_[0]->value_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid ngram token size",
                   K(ObString(ObFTSLiteral::NGRAM_TOKEN_SIZE_SCOPE_STR)),
                   K(ret),
                   K(node->children_[0]->value_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::NGRAM_TOKEN_SIZE_SCOPE_STR);
        } else if OB_FAIL (property.config_set_ngram_token_size(node->children_[0]->value_)) {
          LOG_WARN("fail to set ngram token size", K(ret));
        }
        break;
      }
      case T_PARSER_STOPWORD_TABLE: {
        if (OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is nullptr", K(ret));
        } else if (OB_UNLIKELY(node->children_[0]->str_len_ <= 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(node->children_[0]->str_len_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the stopword table is empty");
        } else {
          int32_t str_len = static_cast<int32_t>(node->children_[0]->str_len_);
          if (OB_FAIL(property.config_set_stopword_table(
                  common::ObString(str_len, node->children_[0]->str_value_)))) {
            LOG_WARN("fail to set stopword table", K(ret));
          }
        }
        break;
      }
      case T_PARSER_DICT_TABLE: {
        if (OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is nullptr", K(ret));
        } else if (OB_UNLIKELY(node->children_[0]->str_len_ <= 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(node->children_[0]->str_len_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the dict table is empty");
        } else {
          int32_t str_len = static_cast<int32_t>(node->children_[0]->str_len_);
          if (OB_FAIL(property.config_set_dict_table(
                  common::ObString(str_len, node->children_[0]->str_value_)))) {
            LOG_WARN("fail to set dict table", K(ret));
          }
        }
        break;
      }
      case T_PARSER_QUANTIFIER_TABLE: {
        if (OB_ISNULL(node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is nullptr", K(ret));
        } else if (OB_UNLIKELY(node->children_[0]->str_len_ <= 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(node->children_[0]->str_len_));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "the quanitfier table is empty");
        } else {
          int32_t str_len = static_cast<int32_t>(node->children_[0]->str_len_);
          if (OB_FAIL(property.config_set_quantifier_table(
                  common::ObString(str_len, node->children_[0]->str_value_)))) {
            LOG_WARN("fail to set quantifier table", K(ret));
          }
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid fts index parser properties option", K(ret), K(node->type_));
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
