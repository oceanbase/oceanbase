/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_FTS_PARSER_PROPERTY_H_
#define OB_FTS_PARSER_PROPERTY_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/json/ob_json.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/schema/ob_schema_struct_fts.h"
#include "storage/fts/ob_fts_literal.h"

#include <cstdint>

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace storage
{
class ObFTParser;

struct ObFTParserProperty;
class ObFTParserJsonProps final
{
public:
  ObFTParserJsonProps();
  ~ObFTParserJsonProps();
  int init();
  void reset();

  int parse_from_valid_str(const ObString &str);

  bool is_empty() const;

  int config_set_min_token_size(const int64_t size);
  int config_set_max_token_size(const int64_t size);
  int config_set_ngram_token_size(const int64_t size);

  // Helper functions for setting table_id and table_name
  int config_set_table_id_impl(const char *config_name, uint64_t table_id);
  int config_set_table_name_impl(const char *config_name, const ObString &table_name);

  int config_set_stopword_table_id(const uint64_t table_id);
  int config_set_dict_table_id(const uint64_t table_id);
  int config_set_quantifier_table_id(const uint64_t table_id);
  int config_set_stopword_table_name(const ObString &table_name);
  int config_set_dict_table_name(const ObString &table_name);
  int config_set_quantifier_table_name(const ObString &table_name);
  int config_set_ik_mode(const ObString &ik_mode);
  int config_set_min_ngram_token_size(const int64_t size);
  int config_set_max_ngram_token_size(const int64_t size);

  int config_get_min_token_size(int64_t &size) const;
  int config_get_max_token_size(int64_t &size) const;
  int config_get_ngram_token_size(int64_t &size) const;

  // Helper functions for getting table_id and table_name
  int config_get_table_id_impl(const char *config_name, uint64_t &table_id) const;
  int config_get_table_name_impl(const char *config_name, ObString &table_name) const;

  int config_get_stopword_table_id(uint64_t &table_id) const;
  int config_get_dict_table_id(uint64_t &table_id) const;
  int config_get_quantifier_table_id(uint64_t &table_id) const;
  int config_get_stopword_table_name(ObString &table_name) const;
  int config_get_dict_table_name(ObString &table_name) const;
  int config_get_quantifier_table_name(ObString &table_name) const;
  int config_get_ik_mode(ObString &ik_mode) const;
  int config_get_min_ngram_token_size(int64_t &size) const;
  int config_get_max_ngram_token_size(int64_t &size) const;

  int rebuild_props_for_ddl(const ObString &parser_name,
                            const common::ObCollationType &type,
                            const bool log_to_user,
                            const uint64_t tenant_id);

  int check_unsupported_config(const char **config_array,
                               int32_t config_count,
                               bool &has_unsupported) const;

  int to_format_json(ObIAllocator &alloc, ObString &str);

  bool is_empty_json_string() const { return is_empty_str_; }

  static bool is_valid_min_token_size(const int64_t size)
  {
    return size >= ObFTSLiteral::FT_MIN_TOKEN_SIZE_LOWER_BOUND
           && size <= ObFTSLiteral::FT_MIN_TOKEN_SIZE_UPPER_BOUND;
  }

  static bool is_valid_max_token_size(const int64_t size)
  {
    return size >= ObFTSLiteral::FT_MAX_TOKEN_SIZE_LOWER_BOUND
           && size <= ObFTSLiteral::FT_MAX_TOKEN_SIZE_UPPER_BOUND;
  }

  static bool is_valid_ngram_token_size(const int64_t size)
  {
    return size >= ObFTSLiteral::FT_NGRAM_TOKEN_SIZE_LOWER_BOUND
           && size <= ObFTSLiteral::FT_NGRAM_TOKEN_SIZE_UPPER_BOUND;
  }

  static bool is_valid_min_ngram_token_size(const int64_t size)
  {
    return size >= ObFTSLiteral::FT_NGRAM_MIN_TOKEN_SIZE_LOWER_BOUND
           && size <= ObFTSLiteral::FT_NGRAM_MIN_TOKEN_SIZE_UPPER_BOUND;
  }

  static bool is_valid_max_ngram_token_size(const int64_t size)
  {
    return size >= ObFTSLiteral::FT_NGRAM_MAX_TOKEN_SIZE_LOWER_BOUND
           && size <= ObFTSLiteral::FT_NGRAM_MAX_TOKEN_SIZE_UPPER_BOUND;
  }

  static int tokenize_array_to_props_json(ObIAllocator &allocator,
                                          ObIJsonBase *array,
                                          const ObString &database_name,
                                          const uint64_t tenant_id,
                                          ObString &json_str);

  static int show_parser_properties(const ObFTParserJsonProps &properties,
                                    char *buf,
                                    const int64_t buf_len,
                                    int64_t &pos);

  TO_STRING_KV(K_(is_inited), K_(is_empty_str));

private:
  static int process_table_name_and_id(ObIAllocator &allocator,
                                      const ObString &config_name,
                                      const ObIJsonBase *config_value,
                                      const ObString &database_name,
                                      const uint64_t tenant_id,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      common::ObIJsonBase &properties_root,
                                      ObIJsonBase *&result_value);

private:
  int set_default_table_info(const uint64_t default_table_id,
                             const ObString &default_table_name);

  int ik_rebuild_props_for_ddl(bool log_to_user);
  int ngram_rebuild_props_for_ddl(bool log_to_user);
  int space_rebuild_props_for_ddl(bool log_to_user);
  int beng_rebuild_props_for_ddl(bool log_to_user);
  int ngram2_rebuild_props_for_ddl(bool log_to_user);
  int plugin_rebuild_props_for_ddl(bool log_to_user);

  // Template helper function for setting table_id or table_name
  template<typename ValueType, typename JsonType>
  int config_set_table_value_impl(const char *config_name, const ValueType &value);

private:
  common::ObArenaAllocator allocator_;
  ObIJsonBase *root_;
  bool is_inited_;
  bool is_empty_str_;

  DISALLOW_COPY_AND_ASSIGN(ObFTParserJsonProps);
};

struct ObFTParserProperty final
{
public:
  ObFTParserProperty();
  ~ObFTParserProperty() = default;
  int parse_for_parser_helper(const ObFTParser &parser, const ObFTParserJsonProps &props);

  bool is_equal(const ObFTParserProperty &other) const
  {
    return min_token_size_ == other.min_token_size_ && max_token_size_ == other.max_token_size_
           && ngram_token_size_ == other.ngram_token_size_ && ik_mode_smart_ == other.ik_mode_smart_
           && stopword_table_id_ == other.stopword_table_id_ && dict_table_id_ == other.dict_table_id_
           && quantifier_table_id_ == other.quantifier_table_id_
           && min_ngram_token_size_ == other.min_ngram_token_size_
           && max_ngram_token_size_ == other.max_ngram_token_size_
           && dict_table_name_ == other.dict_table_name_
           && stopword_table_name_ == other.stopword_table_name_
           && quantifier_table_name_ == other.quantifier_table_name_;
  }

  TO_STRING_KV(K_(min_token_size),
               K_(max_token_size),
               K_(ngram_token_size),
               K_(stopword_table_id),
               K_(dict_table_id),
               K_(quantifier_table_id),
               K_(min_ngram_token_size),
               K_(max_ngram_token_size),
               K_(ik_mode_smart),
               K_(dict_table_name),
               K_(stopword_table_name),
               K_(quantifier_table_name));

private:
  // Helper function to get table_id and table_name from props, or use default values
  int get_table_info_from_props(const ObFTParserJsonProps &props,
                                 const uint64_t default_table_id,
                                 const ObString &default_table_name,
                                 uint64_t &table_id,
                                 ObString &table_name);

public:
  int64_t min_token_size_;
  int64_t max_token_size_;
  int64_t ngram_token_size_;
  uint64_t stopword_table_id_;
  uint64_t dict_table_id_;
  uint64_t quantifier_table_id_;
  int64_t min_ngram_token_size_;
  int64_t max_ngram_token_size_;
  bool ik_mode_smart_;
  ObString dict_table_name_;
  ObString stopword_table_name_;
  ObString quantifier_table_name_;
};

#ifndef USING_LOG_PREFIX
#define MARK_MACRO_DEFINED_BY_FTS_PARSER_PROPERTY_H
#define USING_LOG_PREFIX STORAGE_FTS
#endif

// Template helper function for setting table_id or table_name
template<typename ValueType, typename JsonType>
inline int ObFTParserJsonProps::config_set_table_value_impl(const char *config_name, const ValueType &value)
{
  int ret = OB_SUCCESS;
  JsonType *json_value = nullptr;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    OB_MOD_LOG(STORAGE, WARN, "Props not init", K(ret));
  } else if (OB_ISNULL(json_value = OB_NEWx(JsonType, &allocator_, value))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_MOD_LOG(STORAGE, WARN, "Fail to new JsonType", K(ret));
  } else if (OB_FAIL(root_->object_add(ObString(config_name), json_value))) {
    OB_MOD_LOG(STORAGE, WARN, "Fail to add table value", K(ret), KCSTRING(config_name));
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(JsonType, &allocator_, json_value);
  }

  return ret;
}

#ifdef MARK_MACRO_DEFINED_BY_FTS_PARSER_PROPERTY_H
#undef USING_LOG_PREFIX
#undef MARK_MACRO_DEFINED_BY_FTS_PARSER_PROPERTY_H
#endif

} // end namespace storage
} // end namespace oceanbase
#endif // OB_FTS_PARSER_PROPERTY_H_
