/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/fts/ob_fts_parser_property.h"

#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/fts/ob_beng_ft_parser.h"
#include "storage/fts/ob_fts_literal.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_ik_ft_parser.h"
#include "storage/fts/ob_ngram_ft_parser.h"
#include "storage/fts/ob_whitespace_ft_parser.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{


int ObFTParserJsonProps::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(root_ = OB_NEWx(ObJsonObject, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObJsonObject", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFTParserJsonProps::config_set_min_token_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObJsonInt *min_token_size = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(min_token_size = OB_NEWx(ObJsonInt, &allocator_, size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonInt", K(ret));
  } else if (!is_valid_min_token_size(size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KCSTRING(ObFTSLiteral::MIN_TOKEN_SIZE_SCOPE_STR), K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_MIN_TOKEN_SIZE),
                                       min_token_size))) {
    LOG_WARN("Fail to add min_token_size", K(ret));
  } else {
  }
  return ret;
}

int ObFTParserJsonProps::config_set_max_token_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObJsonInt *max_token_size = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(max_token_size = OB_NEWx(ObJsonInt, &allocator_, size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonInt", K(ret));
  } else if (!is_valid_max_token_size(size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KCSTRING(ObFTSLiteral::MAX_TOKEN_SIZE_SCOPE_STR), K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_MAX_TOKEN_SIZE),
                                       max_token_size))) {
    LOG_WARN("Fail to add max_token_size", K(ret));
  } else {
  }
  return ret;
}

int ObFTParserJsonProps::config_set_ngram_token_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObJsonInt *ngram_token_size = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(ngram_token_size = OB_NEWx(ObJsonInt, &allocator_, size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonInt", K(ret));
  } else if (!is_valid_ngram_token_size(size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KCSTRING(ObFTSLiteral::NGRAM_TOKEN_SIZE_SCOPE_STR), K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_NGRAM_TOKEN_SIZE),
                                       ngram_token_size))) {
    LOG_WARN("Fail to add ngram_token_size", K(ret));
  } else {
  }
  return ret;
}

// should string deep copy?
int ObFTParserJsonProps::config_set_dict_table(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonString *dict_table = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(dict_table = OB_NEWx(ObJsonString, &allocator_, str))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonString", K(ret));
  } else if (OB_FAIL(
                 root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_DICT_TABLE), dict_table))) {
    LOG_WARN("Fail to add dict_table", K(ret));
  } else {
  }
  return ret;
}

int ObFTParserJsonProps::config_set_stopword_table(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonString *stopword_table = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(stopword_table = OB_NEWx(ObJsonString, &allocator_, str))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonString", K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE),
                                       stopword_table))) {
    LOG_WARN("Fail to add stopword_table", K(ret));
  } else {
  }
  return ret;
}

int ObFTParserJsonProps::config_set_quantifier_table(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonString *quantifier_table = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(quantifier_table = OB_NEWx(ObJsonString, &allocator_, str))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonString", K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE),
                                       quantifier_table))) {
    LOG_WARN("Fail to add quantifier_table", K(ret));
  } else {
  }
  return ret;
}

int ObFTParserJsonProps::config_set_ik_mode(const ObString &ik_mode)
{
  int ret = OB_SUCCESS;
  ObJsonString *ik_mode_node = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(ik_mode_node = OB_NEWx(ObJsonString, &allocator_, ik_mode))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new json string", K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_IK_MODE),
                                       ik_mode_node))) {
    LOG_WARN("Fail to add ik_mode", K(ret));
  }
  return ret;
}

int ObFTParserJsonProps::parse_from_valid_str(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root = nullptr;

  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (str.empty()) {
    // do nothing and use default {}
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator_, str, root))) {
    LOG_WARN("Fail to parse json", K(ret));
  } else {
    root_ = root;
  }
  return ret;
}

bool ObFTParserJsonProps::is_empty() const
{
  return (root_ == nullptr) || (root_->element_count() == 0);
}

int ObFTParserJsonProps::to_format_json(ObIAllocator &alloc, ObString &str)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    if (is_empty()) {
      str = ObString(); // just make it empty;
    } else {
      ObJsonBuffer j_buf(&alloc);
      if (OB_FAIL(root_->print(j_buf, false))) {
        LOG_WARN("Fail to print json", K(ret));
      } else {
        str = j_buf.string();
      }
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_min_token_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_MIN_TOKEN_SIZE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get min_token_size", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_INT) {
      LOG_WARN("value is not int", K(ret));
    } else {
      size = value->get_int();
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_max_token_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_MAX_TOKEN_SIZE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get max_token_size", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_INT) {
      LOG_WARN("value is not int", K(ret));
    } else {
      size = value->get_int();
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_ngram_token_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_NGRAM_TOKEN_SIZE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get dict_table", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_INT) {
      LOG_WARN("value is not int", K(ret));
    } else {
      size = value->get_int();
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_dict_table(ObString &str) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_DICT_TABLE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get dict_table", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_STRING) {
      LOG_WARN("value is not string", K(ret));
    } else {
      ObJsonString *json_str = static_cast<ObJsonString *>(value);
      str = json_str->get_str();
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_stopword_table(ObString &str) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get stopword_table", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_STRING) {
      LOG_WARN("value is not string", K(ret));
    } else {
      ObJsonString *json_str = static_cast<ObJsonString *>(value);
      str = json_str->get_str();
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_quantifier_table(ObString &str) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get quantifier_table", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_STRING) {
      LOG_WARN("value is not string", K(ret));
    } else {
      ObJsonString *json_str = static_cast<ObJsonString *>(value);
      str = json_str->get_str();
    }
  }
  return ret;
}

int ObFTParserJsonProps::config_get_ik_mode(ObString &ik_mode) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_IK_MODE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get ik mode", K(ret));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value is not string", K(ret));
    } else {
      ObJsonString *json_str = static_cast<ObJsonString *>(value);
      ik_mode = json_str->get_str();
    }
  }
  return ret;
}

int ObFTParserJsonProps::check_unsupported_config(const char **config_array,
                                                  int32_t config_count,
                                                  bool &has_unsupported) const
{
  int ret = OB_SUCCESS;
  has_unsupported = false;
  for (int32_t i = 0; OB_SUCC(ret) && i < root_->element_count() && !has_unsupported; ++i) {
    ObString key;
    if (OB_FAIL(root_->get_key(i, key))) {
      LOG_WARN("Fail to get config", K(ret));
    } else {
      bool is_supported = false;
      for (int32_t j = 0; OB_SUCC(ret) && j < config_count && !is_supported; ++j) {
        if (ObString(config_array[j]) == key) {
          is_supported = true;
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (!is_supported) {
        has_unsupported = true;
        LOG_WARN("unsupported config", K(key));
      }
    }
  }

  return ret;
}

int ObFTParserJsonProps::rebuild_props_for_ddl(const ObString &parser_name,
                                               const common::ObCollationType &type,
                                               const bool log_to_user)
{
  int ret = OB_SUCCESS;

  ObFTParser parser;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else if (parser_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parser_name is empty", K(ret));
  } else if (OB_FAIL(parser.parse_from_str(parser_name.ptr(), parser_name.length()))) {
    LOG_WARN("fail to parse name from cstring", K(ret), K(parser_name));
  } else {
    if (parser.is_ik()) {
      if (OB_FAIL(ik_rebuild_props_for_ddl(log_to_user))) {
        LOG_WARN("fail to rebuild props for ddl", K(ret));
      }
    } else if (parser.is_space()) {
      if (OB_FAIL(space_rebuild_props_for_ddl(log_to_user))) {
        LOG_WARN("fail to rebuild props for ddl", K(ret));
      }
    } else if (parser.is_ngram()) {
      if (OB_FAIL(ngram_rebuild_props_for_ddl(log_to_user))) {
        LOG_WARN("fail to rebuild props for ddl", K(ret));
      }
    } else if (parser.is_beng()) {
      if (OB_FAIL(beng_rebuild_props_for_ddl(log_to_user))) {
        LOG_WARN("fail to rebuild props for ddl", K(ret));
      }
    } else if (OB_FAIL(plugin_rebuild_props_for_ddl(log_to_user))) {
      LOG_WARN("fail to rebuild props for ddl", K(ret));
    }
  }

  return ret;
}

int ObFTParserJsonProps::ik_rebuild_props_for_ddl(const bool log_to_user)
{
  int ret = OB_SUCCESS;

  static const char *supported[] = {
      ObFTSLiteral::CONFIG_NAME_DICT_TABLE,
      ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE,
      ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE,
      ObFTSLiteral::CONFIG_NAME_IK_MODE,
  };

  bool has_unsupported = false;
  if (OB_FAIL(check_unsupported_config(supported, ARRAYSIZEOF(supported), has_unsupported))) {
    LOG_WARN("Failed to check unsupported config", K(ret));
  } else if (has_unsupported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported config", K(ret));
    if (log_to_user) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ik config");
    }
  };

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObString table_name = "";
    if (OB_FAIL(config_get_dict_table(table_name))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_dict_table(ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else {
      // check dict table valid
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(config_get_quantifier_table(table_name))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(
                config_set_quantifier_table(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE))) {
          LOG_WARN("Failed to set default quantifier table", K(ret));
        }
      }
    } else {
      // check quantifier table valid
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(config_get_quantifier_table(table_name))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(
                config_set_quantifier_table(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE))) {
          LOG_WARN("Failed to set default quantifier table", K(ret));
        }
      }
    } else {
      // check quantifier table valid
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      ObString ik_mode;
      if (OB_FAIL(config_get_ik_mode(ik_mode))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          if (OB_FAIL(config_set_ik_mode(ObFTSLiteral::FT_IK_MODE_SMART))) {
            LOG_WARN("Failed to set ik mode", K(ret));
          }
        } else {
          LOG_WARN("Fail to get ik mode", K(ret));
          if (log_to_user) {
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::IK_MODE_SCOPE_STR);
          }
        }
      } else {
        if (0 == ObString(ObFTSLiteral::FT_IK_MODE_SMART).case_compare(ik_mode)) {
          // okay
        } else if (0 == ObString(ObFTSLiteral::FT_IK_MODE_MAX_WORD).case_compare(ik_mode)) {
          // okay
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid ik mode", K(ret), K(ik_mode));
          if (log_to_user) {
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::IK_MODE_SCOPE_STR);
          }
        }
      }
    }
  }
  return ret;
}

int ObFTParserJsonProps::ngram_rebuild_props_for_ddl(const bool log_to_user)
{
  int ret = OB_SUCCESS;

  static const char *supported[] = {
      ObFTSLiteral::CONFIG_NAME_NGRAM_TOKEN_SIZE,
  };

  bool has_unsupported = false;
  if (OB_FAIL(check_unsupported_config(supported, ARRAYSIZEOF(supported), has_unsupported))) {
    LOG_WARN("Failed to check unsupported config", K(ret));
  } else if (has_unsupported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported config", K(ret));
    if (log_to_user) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ngram config");
    }
  };

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    int64_t ngram_token_size = 0;

    if (OB_FAIL(config_get_ngram_token_size(ngram_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_ngram_token_size(ObFTSLiteral::FT_DEFAULT_NGRAM_TOKEN_SIZE))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else if (!is_valid_ngram_token_size(ngram_token_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid ngram token size", K(ret), K(ngram_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::NGRAM_TOKEN_SIZE_SCOPE_STR);
      }
    }
  }

  return ret;
}

int ObFTParserJsonProps::space_rebuild_props_for_ddl(const bool log_to_user)
{
  int ret = OB_SUCCESS;

  static const char *supported[] = {
      ObFTSLiteral::CONFIG_NAME_MIN_TOKEN_SIZE,
      ObFTSLiteral::CONFIG_NAME_MAX_TOKEN_SIZE,
  };

  bool has_unsupported = false;
  if (OB_FAIL(check_unsupported_config(supported, ARRAYSIZEOF(supported), has_unsupported))) {
    LOG_WARN("Failed to check unsupported config", K(ret));
  } else if (has_unsupported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported config", K(ret));
    if (log_to_user) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "space config");
    }
  };

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    int64_t min_token_size = 0;
    int64_t max_token_size = 0;

    if (OB_FAIL(config_get_min_token_size(min_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_min_token_size(ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else if (!is_valid_min_token_size(min_token_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid min token size", K(ret), K(min_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_TOKEN_SIZE_SCOPE_STR);
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(config_get_max_token_size(max_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_max_token_size(ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else if (!is_valid_max_token_size(max_token_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid max token size", K(ret), K(max_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MAX_TOKEN_SIZE_SCOPE_STR);
      }
    } else if (min_token_size > max_token_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid token size", K(ret), K(min_token_size), K(max_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_MAX_TOKEN_SIZE_SCOPE_STR);
      }
    }
  }
  return ret;
}

int ObFTParserJsonProps::beng_rebuild_props_for_ddl(const bool log_to_user)
{
  int ret = OB_SUCCESS;

  static const char *supported[] = {
      ObFTSLiteral::CONFIG_NAME_MIN_TOKEN_SIZE,
      ObFTSLiteral::CONFIG_NAME_MAX_TOKEN_SIZE,
  };

  bool has_unsupported = false;
  if (OB_FAIL(check_unsupported_config(supported, ARRAYSIZEOF(supported), has_unsupported))) {
    LOG_WARN("Failed to check unsupported config", K(ret));
  } else if (has_unsupported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported config", K(ret));
    if (log_to_user) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "beng config");
    }
  };

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    int64_t min_token_size = 0;
    int64_t max_token_size = 0;

    if (OB_FAIL(config_get_min_token_size(min_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_min_token_size(ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else if (!is_valid_min_token_size(min_token_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid min token size", K(ret), K(min_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_TOKEN_SIZE_SCOPE_STR);
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(config_get_max_token_size(max_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_max_token_size(ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else if (!is_valid_max_token_size(max_token_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid max token size", K(ret), K(max_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MAX_TOKEN_SIZE_SCOPE_STR);
      }
    } else if (min_token_size > max_token_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid token size", K(ret), K(min_token_size), K(max_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_MAX_TOKEN_SIZE_SCOPE_STR);
      }
    }
  }
  return ret;
}

int ObFTParserJsonProps::plugin_rebuild_props_for_ddl(const bool log_to_user)
{
  int ret = OB_SUCCESS;
  if (!is_empty()) {
    ret = OB_NOT_SUPPORTED;
    if (log_to_user) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Non-builtin parser property");
    }
  };
  return ret;
}

int ObFTParserJsonProps::check_conflict_config_for_resolve(bool &has_conflict) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool has_min = false;
    bool has_max = false;
    int64_t min_token_size = 0;
    int64_t max_token_size = 0;
    if (OB_FAIL(config_get_min_token_size(min_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get min_token_size", K(ret));
      }
    } else {
      has_min = true;
    }

    if (FAILEDx(config_get_max_token_size(max_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get max_token_size", K(ret));
      }
    } else {
      has_max = true;
    }
    min_token_size = has_min ? min_token_size : ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE;
    max_token_size = has_max ? max_token_size : ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE;
    if (has_min && has_max && min_token_size > max_token_size) {
      has_conflict = true;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_MAX_TOKEN_SIZE_SCOPE_STR);
    }
  }
  return ret;
}

ObFTParserJsonProps::ObFTParserJsonProps()
    : allocator_("FTJsonProps"), root_(NULL), is_inited_(false)
{
}

ObFTParserJsonProps::~ObFTParserJsonProps()
{
  if (!OB_ISNULL(root_)) {
    root_->reset();
    root_->~ObIJsonBase();
    root_ = nullptr;
  }
  allocator_.reset();
}

#define __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma)                                                \
  do {                                                                                             \
    if (need_comma) {                                                                              \
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, ","))) {                                      \
        LOG_WARN("fail to printf comma", K(ret), K(buf_len), K(pos));                              \
      }                                                                                            \
    }                                                                                              \
    need_comma = true;                                                                             \
  } while (0)

int ObFTParserJsonProps::show_parser_properties(const ObFTParserJsonProps &properties,
                                                char *buf,
                                                const int64_t buf_len,
                                                int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!properties.is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (properties.is_empty()) {
    // not output properties
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PARSER_PROPERTIES=("))) {
      LOG_WARN("fail to printf parser properties", K(ret), K(buf_len), K(pos), K(properties));
    }

    bool need_comma = false;

    int64_t min_token_size = 0;
    if (FAILEDx(properties.config_get_min_token_size(min_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get min_token_size", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (FAILEDx(databuff_printf(buf, buf_len, pos, "min_token_size=%ld", min_token_size))) {
        LOG_WARN("fail to printf min_token_size", K(ret), K(buf_len), K(pos), K(min_token_size));
      }
    }

    int64_t max_token_size = 0;
    if (FAILEDx(properties.config_get_max_token_size(max_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get max_token_size", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (FAILEDx(databuff_printf(buf, buf_len, pos, "max_token_size=%ld", max_token_size))) {
        LOG_WARN("fail to printf max_token_size", K(ret), K(buf_len), K(pos), K(max_token_size));
      }
    }

    ObString ik_mode;
    if (FAILEDx(properties.config_get_ik_mode(ik_mode))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get ik mode", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (FAILEDx(databuff_printf(buf, buf_len, pos, "ik_mode=\"%s\"", ik_mode.ptr()))) {
        LOG_WARN("fail to printf ik mode", K(ret), K(buf_len), K(pos), K(ik_mode));
      }
    }

    int64_t ngram_token_size = 0;
    if (FAILEDx(properties.config_get_ngram_token_size(ngram_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get ngram_token_size", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (FAILEDx(databuff_printf(buf, buf_len, pos, "ngram_token_size=%ld", ngram_token_size))) {
        LOG_WARN("fail to printf ngram_token_size",
                 K(ret),
                 K(buf_len),
                 K(pos),
                 K(ngram_token_size));
      }
    }

    if (FAILEDx(databuff_printf(buf, buf_len, pos, ") "))) {
      LOG_WARN("fail to printf parser properties", K(ret), K(buf_len), K(pos), K(properties));
    }
  }

  return ret;
}

#undef __FT_PARSER_PROPERTY_SHOW_COMMA

int ObFTParserProperty::parse_for_parser_helper(const ObFTParser &parser, const ObString &json_str)
{
  int ret = OB_SUCCESS;
  ObFTParserJsonProps props;
  if (OB_FAIL(props.init())) {
    LOG_WARN("fail to init props", K(ret));
  } else if (OB_FAIL(props.parse_from_valid_str(json_str))) {
    LOG_WARN("fail to parse from json str", K(ret), K(json_str));
  } else {
    if (parser.is_ik()) {
      // set dict tables and copy dict name
      dict_table_ = ObString(ObFTSLiteral::CONFIG_NAME_DICT_TABLE);
      stopword_table_ = ObString(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE);
      quantifier_table_ = ObString(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE);
      ObString ik_smart;
      if (OB_FAIL(props.config_get_ik_mode(ik_smart))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          // from old version, ik_mode is not set, so use default value
          ik_mode_smart_ = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get ik mode", K(ret));
        }
      } else {
        if (0 == ik_smart.case_compare(ObString(ObFTSLiteral::FT_IK_MODE_SMART))) {
          ik_mode_smart_ = true;
        } else if (0 == ik_smart.case_compare(ObString(ObFTSLiteral::FT_IK_MODE_MAX_WORD))) {
          ik_mode_smart_ = false;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid ik_smart", K(ret), K(ik_smart));
        }
      }
    } else if (parser.is_space()) {
      if (json_str.empty()) {
        min_token_size_ = ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE;
        max_token_size_ = ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE;
      } else if (OB_FAIL(props.config_get_min_token_size(min_token_size_))) {
        LOG_WARN("fail to get min_token_size", K(ret));
      } else if (OB_FAIL(props.config_get_max_token_size(max_token_size_))) {
        LOG_WARN("fail to get max_token_size", K(ret));
      }
    } else if (parser.is_ngram()) {
      if (json_str.empty()) {
        ngram_token_size_ = ObFTSLiteral::FT_DEFAULT_NGRAM_TOKEN_SIZE;
      } else if (OB_FAIL(props.config_get_ngram_token_size(ngram_token_size_))) {
        LOG_WARN("fail to get ngram_token_size", K(ret));
      }
    } else if (parser.is_beng()) {
      if (json_str.empty()) {
        min_token_size_ = ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE;
        max_token_size_ = ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE;
      } else if (OB_FAIL(props.config_get_min_token_size(min_token_size_))) {
        LOG_WARN("fail to get min_token_size", K(ret));
      } else if (OB_FAIL(props.config_get_max_token_size(max_token_size_))) {
        LOG_WARN("fail to get max_token_size", K(ret));
      }
    }
  }
  return ret;
}

ObFTParserProperty::ObFTParserProperty()
    : min_token_size_(ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE),
      max_token_size_(ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE),
      ngram_token_size_(ObFTSLiteral::FT_DEFAULT_NGRAM_TOKEN_SIZE), ik_mode_smart_(true),
      stopword_table_(), dict_table_(), quantifier_table_()
{
}

int ObFTParserJsonProps::tokenize_array_to_props_json(ObIAllocator &allocator,
                                                      ObIJsonBase *array,
                                                      ObString &json_str)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator alloc;
  ObJsonObject properties_root(&alloc);

  if (OB_ISNULL(array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < array->element_count(); i++) {
      ObIJsonBase *array_value_item;
      ObString config_name;
      ObIJsonBase *config_value;
      if (OB_FAIL(array->get_array_element(i, array_value_item))) {
        LOG_WARN("Fail to get array element", K(ret));
      } else if (array_value_item->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
      } else if (1 != array_value_item->element_count()) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(array_value_item->get_object_value(0, config_name, config_value))) {
        LOG_WARN("Fail to get object value", K(ret));
      } else if (OB_FAIL(properties_root.object_add(config_name, config_value))) {
        LOG_WARN("Fail to add object value", K(ret));
      }
    }
    ObJsonBuffer j_buf(&allocator);
    if (FAILEDx(properties_root.print(j_buf, false))) {
      LOG_WARN("Fail to print json", K(ret));
    } else {
      json_str = j_buf.string();
    }
  }

  return ret;
}
} // end namespace storage
} // end namespace oceanbase
