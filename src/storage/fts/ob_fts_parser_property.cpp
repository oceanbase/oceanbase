/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "sql/resolver/ddl/ob_fts_parser_resolver.h"
#include "storage/fts/ob_fts_literal.h"
#include "storage/fts/ob_fts_plugin_helper.h"

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

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIJsonBase, &allocator_, root_);
    root_ = nullptr;
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

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObJsonInt, &allocator_, min_token_size);
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

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObJsonInt, &allocator_, max_token_size);
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

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObJsonInt, &allocator_, ngram_token_size);
  }

  return ret;
}

int ObFTParserJsonProps::config_set_stopword_table_id(const uint64_t table_id)
{
  return config_set_table_id_impl(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE_ID, table_id);
}

int ObFTParserJsonProps::config_set_dict_table_id(const uint64_t table_id)
{
  return config_set_table_id_impl(ObFTSLiteral::CONFIG_NAME_DICT_TABLE_ID, table_id);
}

int ObFTParserJsonProps::config_set_quantifier_table_id(const uint64_t table_id)
{
  return config_set_table_id_impl(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE_ID, table_id);
}

int ObFTParserJsonProps::config_set_stopword_table_name(const ObString &table_name)
{
  return config_set_table_name_impl(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE, table_name);
}

int ObFTParserJsonProps::config_set_dict_table_name(const ObString &table_name)
{
  return config_set_table_name_impl(ObFTSLiteral::CONFIG_NAME_DICT_TABLE, table_name);
}

int ObFTParserJsonProps::config_set_quantifier_table_name(const ObString &table_name)
{
  return config_set_table_name_impl(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE, table_name);
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

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObJsonString, &allocator_, ik_mode_node);
  }

  return ret;
}

int ObFTParserJsonProps::config_set_min_ngram_token_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObJsonInt *ngram_token_size = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(ngram_token_size = OB_NEWx(ObJsonInt, &allocator_, size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonInt", K(ret));
  } else if (!is_valid_min_ngram_token_size(size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KCSTRING(ObFTSLiteral::MIN_NGRAM_SIZE_SCOPE_STR), K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_MIN_NGRAM_SIZE),
                                       ngram_token_size))) {
    LOG_WARN("Fail to add min_ngram_size", K(ret));
  } else {
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObJsonInt, &allocator_, ngram_token_size);
  }

  return ret;
}

int ObFTParserJsonProps::config_set_max_ngram_token_size(const int64_t size)
{
  int ret = OB_SUCCESS;
  ObJsonInt *ngram_token_size = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Props not init", K(ret));
  } else if (OB_ISNULL(ngram_token_size = OB_NEWx(ObJsonInt, &allocator_, size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to new ObJsonInt", K(ret));
  } else if (!is_valid_max_ngram_token_size(size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KCSTRING(ObFTSLiteral::MAX_NGRAM_SIZE_SCOPE_STR), K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(ObFTSLiteral::CONFIG_NAME_MAX_NGRAM_SIZE),
                                       ngram_token_size))) {
    LOG_WARN("Fail to add max_ngram_size", K(ret));
  } else {
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObJsonInt, &allocator_, ngram_token_size);
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
    is_empty_str_ = true;
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

int ObFTParserJsonProps::config_get_stopword_table_id(uint64_t &table_id) const
{
  return config_get_table_id_impl(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE_ID, table_id);
}

int ObFTParserJsonProps::config_get_dict_table_id(uint64_t &table_id) const
{
  return config_get_table_id_impl(ObFTSLiteral::CONFIG_NAME_DICT_TABLE_ID, table_id);
}

int ObFTParserJsonProps::config_get_quantifier_table_id(uint64_t &table_id) const
{
  return config_get_table_id_impl(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE_ID, table_id);
}

int ObFTParserJsonProps::config_get_stopword_table_name(ObString &table_name) const
{
  return config_get_table_name_impl(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE, table_name);
}

int ObFTParserJsonProps::config_get_dict_table_name(ObString &table_name) const
{
  return config_get_table_name_impl(ObFTSLiteral::CONFIG_NAME_DICT_TABLE, table_name);
}

int ObFTParserJsonProps::config_get_quantifier_table_name(ObString &table_name) const
{
  return config_get_table_name_impl(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE, table_name);
}

// Helper function for getting table_id
int ObFTParserJsonProps::config_get_table_id_impl(const char *config_name, uint64_t &table_id) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_empty()) {
    ret = OB_SEARCH_NOT_FOUND;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(root_->get_object_value(ObString(config_name), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get table_id", K(ret), KCSTRING(config_name));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() == ObJsonNodeType::J_UINT) {
      ObJsonUint *json_uint = static_cast<ObJsonUint *>(value);
      table_id = json_uint->get_uint();
    } else if (value->json_type() == ObJsonNodeType::J_INT) {
      // JSON parser may parse numbers as J_INT instead of J_UINT after serialization/deserialization
      ObJsonInt *json_int = static_cast<ObJsonInt *>(value);
      table_id = static_cast<uint64_t>(json_int->get_int());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value is not uint or int", K(ret), "json_type", value->json_type());
    }
  }
  return ret;
}

// Helper function for getting table_name
int ObFTParserJsonProps::config_get_table_name_impl(const char *config_name, ObString &table_name) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else if (is_empty()) {
    ret = OB_SEARCH_NOT_FOUND;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(root_->get_object_value(ObString(config_name), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get table_name", K(ret), KCSTRING(config_name));
      }
    } else if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is null", K(ret));
    } else if (value->json_type() != ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("value is not string", K(ret));
    } else {
      ObJsonString *json_str = static_cast<ObJsonString *>(value);
      table_name = json_str->get_str();
    }
  }
  return ret;
}

// Helper function for setting table_id
int ObFTParserJsonProps::config_set_table_id_impl(const char *config_name, uint64_t table_id)
{
  return config_set_table_value_impl<uint64_t, ObJsonUint>(config_name, table_id);
}

// Helper function for setting table_name
int ObFTParserJsonProps::config_set_table_name_impl(const char *config_name, const ObString &table_name)
{
  return config_set_table_value_impl<ObString, ObJsonString>(config_name, table_name);
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

int ObFTParserJsonProps::config_get_min_ngram_token_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_MIN_NGRAM_SIZE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get min_ngram_token_size", K(ret));
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

int ObFTParserJsonProps::config_get_max_ngram_token_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
  } else {
    ObIJsonBase *value = nullptr;
    if (OB_FAIL(
            root_->get_object_value(ObString(ObFTSLiteral::CONFIG_NAME_MAX_NGRAM_SIZE), value))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
      } else {
        LOG_WARN("Fail to get min_ngram_token_size", K(ret));
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
        if (0 == (ObString(config_array[j]).case_compare(key))) {
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
                                               const bool log_to_user,
                                               const uint64_t tenant_id)
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
    } else if (parser.is_ngram2()) {
      if (OB_FAIL(ngram2_rebuild_props_for_ddl(log_to_user))) {
        LOG_WARN("fail to rebuild props for ddl", K(ret));
      }
    } else if (OB_FAIL(plugin_rebuild_props_for_ddl(log_to_user))) {
      LOG_WARN("fail to rebuild props for ddl", K(ret));
    }
  }

  return ret;
}

int ObFTParserJsonProps::set_default_table_info(
                                     const uint64_t default_table_id,
                                     const ObString &default_table_name)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObString table_name;
  const char *table_id_config_name = ObFTSLiteral::get_config_name_by_table_id(default_table_id);
  const char *table_name_config_name = ObFTSLiteral::get_table_name_config_name(table_id_config_name);

  if (OB_UNLIKELY(OB_ISNULL(table_id_config_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get config name for table_id", K(ret), K(default_table_id));
  } else if (OB_FAIL(config_get_table_id_impl(table_id_config_name, table_id))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      if (OB_FAIL(config_set_table_id_impl(table_id_config_name, default_table_id))) {
        LOG_WARN("failed to set table_id", K(ret), K(default_table_id));
      }
    } else {
      LOG_WARN("failed to get table_id", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    if (OB_UNLIKELY(OB_ISNULL(table_name_config_name))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get config name for table_name", K(ret), K(default_table_name));
    } else if (OB_FAIL(config_get_table_name_impl(table_name_config_name, table_name))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        if (OB_FAIL(config_set_table_name_impl(table_name_config_name, default_table_name))) {
          LOG_WARN("failed to set table_name", K(ret), K(default_table_name));
        }
      } else {
        LOG_WARN("failed to get table_name", K(ret));
      }
    }
  }

  return ret;
}

int ObFTParserJsonProps::ik_rebuild_props_for_ddl(bool log_to_user)
{
  int ret = OB_SUCCESS;

  static const char *supported[] = {
      ObFTSLiteral::CONFIG_NAME_DICT_TABLE,
      ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE,
      ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE,
      ObFTSLiteral::CONFIG_NAME_DICT_TABLE_ID,
      ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE_ID,
      ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE_ID,
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
  }


  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    // Process dict_table_id and table_name
    // For system default tables, table_name is empty (will be resolved from table_id if needed)
    ObString empty_table_name;
    if (OB_FAIL(set_default_table_info(share::OB_FT_DICT_IK_UTF8_TID,
                                       ObString(ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE)))) {
      LOG_WARN("Failed to process main dict table_id and table_name", K(ret));
    } else if (OB_FAIL(set_default_table_info(share::OB_FT_QUANTIFIER_IK_UTF8_TID,
                                              ObString(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE)))) {
      LOG_WARN("Failed to process quantifier table_id and table_name", K(ret));
    } else if (OB_FAIL(set_default_table_info(share::OB_FT_STOPWORD_IK_UTF8_TID,
                                              ObString(ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE)))) {
      LOG_WARN("Failed to process stopword table_id and table_name", K(ret));
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
      ret = OB_INVALID_CONFIG;
      LOG_WARN("Invalid token size", K(ret), K(min_token_size), K(max_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_CONFIG, ObFTSLiteral::MIN_MAX_TOKEN_SIZE_SCOPE_STR);
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
        min_token_size = ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE;
        if (OB_FAIL(config_set_min_token_size(min_token_size))) {
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
        max_token_size = ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE;
        if (OB_FAIL(config_set_max_token_size(max_token_size))) {
          LOG_WARN("Failed to set default dict table", K(ret));
        }
      }
    } else if (!is_valid_max_token_size(max_token_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid max token size", K(ret), K(max_token_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MAX_TOKEN_SIZE_SCOPE_STR);
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
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

int ObFTParserJsonProps::ngram2_rebuild_props_for_ddl(const bool log_to_user)
{
  int ret = OB_SUCCESS;

  static const char *supported[] = {
      ObFTSLiteral::CONFIG_NAME_MIN_NGRAM_SIZE,
      ObFTSLiteral::CONFIG_NAME_MAX_NGRAM_SIZE,
  };

  bool has_unsupported = false;
  if (OB_FAIL(check_unsupported_config(supported, ARRAYSIZEOF(supported), has_unsupported))) {
    LOG_WARN("Failed to check unsupported config", K(ret));
  } else if (has_unsupported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported config", K(ret));
    if (log_to_user) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ngram2 config");
    }
  };

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    int64_t min_ngram_size = 0;
    int64_t max_ngram_size = 0;

    if (OB_FAIL(config_get_min_ngram_token_size(min_ngram_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        min_ngram_size = ObFTSLiteral::FT_DEFAULT_MIN_NGRAM_SIZE;
        if (OB_FAIL(config_set_min_ngram_token_size(min_ngram_size))) {
          LOG_WARN("Failed to set default min ngram token size", K(ret));
        }
      }
    } else if (!is_valid_min_ngram_token_size(min_ngram_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid min ngram token size", K(ret), K(min_ngram_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::MIN_NGRAM_SIZE_SCOPE_STR);
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(config_get_max_ngram_token_size(max_ngram_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        max_ngram_size = ObFTSLiteral::FT_DEFAULT_MAX_NGRAM_SIZE;
        if (OB_FAIL(config_set_max_ngram_token_size(max_ngram_size))) {
          LOG_WARN("Failed to set default max ngram token size", K(ret));
        }
      }
    } else if (!is_valid_max_ngram_token_size(max_ngram_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid max ngram token size", K(ret), K(max_ngram_size));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (min_ngram_size > max_ngram_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid token size", K(ret), K(min_ngram_size), K(max_ngram_size));
      if (log_to_user) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, ObFTSLiteral::NGRAM_MIN_MAX_TOKEN_SIZE_SCOPE_STR);
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

ObFTParserJsonProps::ObFTParserJsonProps()
    : allocator_("FTJsonProps"), root_(NULL), is_inited_(false), is_empty_str_(false)
{
}

ObFTParserJsonProps::~ObFTParserJsonProps()
{
  reset();
}

void ObFTParserJsonProps::reset()
{
  if (!OB_ISNULL(root_)) {
    root_->reset();
    root_->~ObIJsonBase();
    root_ = nullptr;
  }
  allocator_.reset();
  is_inited_ = false;
  is_empty_str_ = false;
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
      if (FAILEDx(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%s=%ld",
                                  ObFTSLiteral::CONFIG_NAME_MIN_TOKEN_SIZE,
                                  min_token_size))) {
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
      if (FAILEDx(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%s=%ld",
                                  ObFTSLiteral::CONFIG_NAME_MAX_TOKEN_SIZE,
                                  max_token_size))) {
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
      if (FAILEDx(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%s=\"%s\"",
                                  ObFTSLiteral::CONFIG_NAME_IK_MODE,
                                  ik_mode.ptr()))) {
        LOG_WARN("fail to printf ik mode", K(ret), K(buf_len), K(pos), K(ik_mode));
      }
    }

    // Process dict_table
    ObString dict_table_name;
    if (OB_FAIL(properties.config_get_dict_table_name(dict_table_name))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;  // Optional property, ignore if not found
      } else {
        LOG_WARN("fail to get dict_table_name", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=\"%.*s\"",
                                  ObFTSLiteral::CONFIG_NAME_DICT_TABLE,
                                  dict_table_name.length(), dict_table_name.ptr()))) {
        LOG_WARN("fail to printf dict_table property", K(ret), K(buf_len), K(pos), K(dict_table_name));
      }
    }

    // Process stopword_table
    if (OB_SUCC(ret)) {
      ObString stopword_table_name;
      if (OB_FAIL(properties.config_get_stopword_table_name(stopword_table_name))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;  // Optional property, ignore if not found
        } else {
          LOG_WARN("fail to get stopword_table_name", K(ret));
        }
      } else {
        __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=\"%.*s\"",
                                    ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE,
                                    stopword_table_name.length(), stopword_table_name.ptr()))) {
          LOG_WARN("fail to printf stopword_table property", K(ret), K(buf_len), K(pos), K(stopword_table_name));
        }
      }
    }

    // Process quantifier_table
    if (OB_SUCC(ret)) {
      ObString quantifier_table_name;
      if (OB_FAIL(properties.config_get_quantifier_table_name(quantifier_table_name))) {
        if (OB_SEARCH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;  // Optional property, ignore if not found
        } else {
          LOG_WARN("fail to get quantifier_table_name", K(ret));
        }
      } else {
        __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=\"%.*s\"",
                                    ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE,
                                    quantifier_table_name.length(), quantifier_table_name.ptr()))) {
          LOG_WARN("fail to printf quantifier_table property", K(ret), K(buf_len), K(pos), K(quantifier_table_name));
        }
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
      if (FAILEDx(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%s=%ld",
                                  ObFTSLiteral::CONFIG_NAME_NGRAM_TOKEN_SIZE,
                                  ngram_token_size))) {
        LOG_WARN("fail to printf ngram_token_size",
                 K(ret),
                 K(buf_len),
                 K(pos),
                 K(ngram_token_size));
      }
    }

    int64_t min_ngram_token_size = 0;
    if (FAILEDx(properties.config_get_min_ngram_token_size(min_ngram_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get min_ngram_token_size", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (FAILEDx(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%s=%ld",
                                  ObFTSLiteral::CONFIG_NAME_MIN_NGRAM_SIZE,
                                  min_ngram_token_size))) {
      }
    }

    int64_t max_ngram_token_size = 0;
    if (FAILEDx(properties.config_get_max_ngram_token_size(max_ngram_token_size))) {
      if (OB_SEARCH_NOT_FOUND == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("Fail to get max_ngram_token_size", K(ret));
      }
    } else {
      __FT_PARSER_PROPERTY_SHOW_COMMA(need_comma);
      if (FAILEDx(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%s=%ld",
                                  ObFTSLiteral::CONFIG_NAME_MAX_NGRAM_SIZE,
                                  max_ngram_token_size))) {
      }
    }

    if (FAILEDx(databuff_printf(buf, buf_len, pos, ") "))) {
      LOG_WARN("fail to printf parser properties", K(ret), K(buf_len), K(pos), K(properties));
    }
  }

  return ret;
}

#undef __FT_PARSER_PROPERTY_SHOW_COMMA

// Helper function to get table_id and table_name from props, or use default values
int ObFTParserProperty::get_table_info_from_props(
    const ObFTParserJsonProps &props,
    const uint64_t default_table_id,
    const ObString &default_table_name,
    uint64_t &table_id,
    ObString &table_name)
{
  int ret = OB_SUCCESS;
  const char *table_id_config_name = ObFTSLiteral::get_config_name_by_table_id(default_table_id);
  const char *table_name_config_name = ObFTSLiteral::get_table_name_config_name(ObString(table_id_config_name));
  if (OB_ISNULL(table_id_config_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get config name for table_id", K(ret), K(default_table_id));
  } else if (OB_ISNULL(table_name_config_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get config name for table_name", K(ret), KCSTRING(table_id_config_name));
  } else if (OB_FAIL(props.config_get_table_id_impl(table_id_config_name, table_id))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      // Use default system table_id and table_name
      table_id = default_table_id;
      table_name = default_table_name;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get table_id", K(ret));
    }
  } else if (OB_FAIL(props.config_get_table_name_impl(table_name_config_name, table_name))) {
    LOG_WARN("fail to get table_name", K(ret));
  }
  return ret;
}

int ObFTParserProperty::parse_for_parser_helper(const ObFTParser &parser, const ObFTParserJsonProps &props)
{
  int ret = OB_SUCCESS;
  if (parser.is_ik()) {
    if (OB_FAIL(get_table_info_from_props(props,
                                          share::OB_FT_DICT_IK_UTF8_TID,
                                          ObString(ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE),
                                          dict_table_id_,
                                          dict_table_name_))) {
      LOG_WARN("fail to get dict table info", K(ret));
    } else if (OB_FAIL(get_table_info_from_props(props,
                                                share::OB_FT_STOPWORD_IK_UTF8_TID,
                                                ObString(ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE),
                                                stopword_table_id_,
                                                stopword_table_name_))) {
      LOG_WARN("fail to get stopword table info", K(ret));
    } else if (OB_FAIL(get_table_info_from_props(props,
                                                share::OB_FT_QUANTIFIER_IK_UTF8_TID,
                                                ObString(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE),
                                                quantifier_table_id_,
                                                quantifier_table_name_))) {
      LOG_WARN("fail to get quantifier table info", K(ret));
    }

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
    if (props.is_empty_json_string()) {
      min_token_size_ = ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE;
      max_token_size_ = ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE;
    } else if (OB_FAIL(props.config_get_min_token_size(min_token_size_))) {
      LOG_WARN("fail to get min_token_size", K(ret));
    } else if (OB_FAIL(props.config_get_max_token_size(max_token_size_))) {
      LOG_WARN("fail to get max_token_size", K(ret));
    }
  } else if (parser.is_ngram()) {
    if (props.is_empty_json_string()) {
      ngram_token_size_ = ObFTSLiteral::FT_DEFAULT_NGRAM_TOKEN_SIZE;
    } else if (OB_FAIL(props.config_get_ngram_token_size(ngram_token_size_))) {
      LOG_WARN("fail to get ngram_token_size", K(ret));
    }
  } else if (parser.is_beng()) {
    if (props.is_empty_json_string()) {
      min_token_size_ = ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE;
      max_token_size_ = ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE;
    } else if (OB_FAIL(props.config_get_min_token_size(min_token_size_))) {
      LOG_WARN("fail to get min_token_size", K(ret));
    } else if (OB_FAIL(props.config_get_max_token_size(max_token_size_))) {
      LOG_WARN("fail to get max_token_size", K(ret));
    }
  } else if (parser.is_ngram2()) {
    if (props.is_empty_json_string()) {
      min_ngram_token_size_ = ObFTSLiteral::FT_DEFAULT_MIN_NGRAM_SIZE;
      max_ngram_token_size_ = ObFTSLiteral::FT_DEFAULT_MAX_NGRAM_SIZE;
    } else if (OB_FAIL(props.config_get_min_ngram_token_size(min_ngram_token_size_))) {
      LOG_WARN("fail to get min_ngram_token_size", K(ret));
    } else if (OB_FAIL(props.config_get_max_ngram_token_size(max_ngram_token_size_))) {
      LOG_WARN("fail to get max_ngram_token_size", K(ret));
    }
  }
  return ret;
}

ObFTParserProperty::ObFTParserProperty()
    : min_token_size_(ObFTSLiteral::FT_DEFAULT_MIN_TOKEN_SIZE),
      max_token_size_(ObFTSLiteral::FT_DEFAULT_MAX_TOKEN_SIZE),
      ngram_token_size_(ObFTSLiteral::FT_DEFAULT_NGRAM_TOKEN_SIZE),
      stopword_table_id_(OB_INVALID_ID),
      dict_table_id_(OB_INVALID_ID),
      quantifier_table_id_(OB_INVALID_ID),
      min_ngram_token_size_(ObFTSLiteral::FT_DEFAULT_MIN_NGRAM_SIZE),
      max_ngram_token_size_(ObFTSLiteral::FT_DEFAULT_MAX_NGRAM_SIZE),
      ik_mode_smart_(true),
      dict_table_name_(),
      stopword_table_name_(),
      quantifier_table_name_()
{
}

static bool need_schema_for_config(const ObString &config_name, ObIJsonBase *config_value)
{
  return nullptr != config_value
         && ObJsonNodeType::J_STRING == config_value->json_type()
         && (0 == config_name.case_compare(ObFTSLiteral::CONFIG_NAME_DICT_TABLE) ||
             0 == config_name.case_compare(ObFTSLiteral::CONFIG_NAME_STOPWORD_TABLE) ||
             0 == config_name.case_compare(ObFTSLiteral::CONFIG_NAME_QUANTIFIER_TABLE));
}

int ObFTParserJsonProps::process_table_name_and_id(ObIAllocator &allocator,
                                                  const ObString &config_name,
                                                  const ObIJsonBase *config_value,
                                                  const ObString &database_name,
                                                  const uint64_t tenant_id,
                                                  share::schema::ObSchemaGetterGuard *schema_guard,
                                                  common::ObIJsonBase &properties_root,
                                                  ObIJsonBase *&result_value)
{
  int ret = OB_SUCCESS;
  result_value = nullptr;

  if (OB_UNLIKELY(OB_ISNULL(schema_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(config_value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config_value is null", K(ret));
  } else {
    ObString table_name_str(config_value->get_data_length(), config_value->get_data());
    uint64_t table_id = OB_INVALID_ID;
    ObString full_table_name;

    if (OB_FAIL(sql::ObFTParserResolverHelper::resolve_dict_table_name_and_id(
            database_name, table_name_str, tenant_id, *schema_guard, allocator,
            false /* no check_database_name */, table_id, full_table_name))) {
      LOG_WARN("Failed to resolve dictionary table name and id", K(ret), K(table_name_str), K(database_name));
    } else {
      ObJsonString *new_config_value = OB_NEWx(ObJsonString, &allocator, full_table_name);
      if (OB_ISNULL(new_config_value)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to create new json string", K(ret));
      } else {
        result_value = new_config_value;
        const char *table_id_config_name_ptr = ObFTSLiteral::get_table_id_config_name(config_name);
        if (OB_ISNULL(table_id_config_name_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get config name for table_id", K(ret), KCSTRING(table_id_config_name_ptr), K(table_id));
        } else {
          ObString table_id_config_name(table_id_config_name_ptr);
          ObJsonUint *table_id_value = OB_NEWx(ObJsonUint, &allocator, table_id);
          if (OB_ISNULL(table_id_value)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("Failed to create json uint for table_id", K(ret));
          } else if (OB_FAIL(properties_root.object_add(table_id_config_name, table_id_value))) {
            OB_DELETEx(ObJsonUint, &allocator, table_id_value);
            LOG_WARN("Fail to add table_id to json", K(ret), K(table_id_config_name), K(table_id));
          }
        }
        if (OB_FAIL(ret)) {
          OB_DELETEx(ObJsonString, &allocator, new_config_value);
          result_value = nullptr;
        }
      }
    }
  }

  return ret;
}

int ObFTParserJsonProps::tokenize_array_to_props_json(ObIAllocator &allocator,
                                                      ObIJsonBase *array,
                                                      const ObString &database_name,
                                                      const uint64_t tenant_id,
                                                      ObString &json_str)
{
  int ret = OB_SUCCESS;

  ObJsonObject properties_root(&allocator);
  share::schema::ObSchemaGetterGuard schema_guard;
  share::schema::ObSchemaGetterGuard *schema_guard_ptr = nullptr;

  if (OB_ISNULL(array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < array->element_count(); i++) {
      ObIJsonBase *array_value_item;
      ObString config_name;
      ObIJsonBase *config_value;
      ObIJsonBase *processed_value = nullptr;

      if (OB_FAIL(array->get_array_element(i, array_value_item))) {
        LOG_WARN("Fail to get array element", K(ret));
      } else if (array_value_item->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
      } else if (1 != array_value_item->element_count()) {
        ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(array_value_item->get_object_value(0, config_name, config_value))) {
        LOG_WARN("Fail to get object value", K(ret));
      } else if (OB_UNLIKELY(OB_ISNULL(config_value))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("config_value is null", K(ret));
      } else if (need_schema_for_config(config_name, config_value)) {
        if (OB_ISNULL(schema_guard_ptr)) {
          if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                  tenant_id, schema_guard))) {
            LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
          } else {
            schema_guard_ptr = &schema_guard;
          }
        }
        if (OB_FAIL(ret)) {
          // already logged
        } else if (OB_FAIL(process_table_name_and_id(allocator, config_name, config_value, database_name,
                                                    tenant_id, schema_guard_ptr, properties_root,
                                                    processed_value))) {
          LOG_WARN("fail to process config", K(ret));
        }
      } else {
        processed_value = config_value;
      }
      if (OB_FAIL(ret)) {
        // already logged
      } else if (OB_FAIL(properties_root.object_add(config_name, processed_value))) {
        LOG_WARN("fail to add object value", K(ret));
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
