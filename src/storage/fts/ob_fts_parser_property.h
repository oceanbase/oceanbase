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

#ifndef OB_FTS_PARSER_PROPERTY_H_
#define OB_FTS_PARSER_PROPERTY_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/json/ob_json.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ob_fts_literal.h"

namespace oceanbase
{
namespace storage
{
class ObFTParser;

class ObFTParserJsonProps final
{
public:
  ObFTParserJsonProps();
  ~ObFTParserJsonProps();
  int init();

  int parse_from_valid_str(const ObString &str);

  bool is_empty() const;

  int config_set_min_token_size(const int64_t size);
  int config_set_max_token_size(const int64_t size);
  int config_set_ngram_token_size(const int64_t size);
  int config_set_stopword_table(const ObString &str);
  int config_set_dict_table(const ObString &str);
  int config_set_quantifier_table(const ObString &str);
  int config_set_ik_mode(const ObString &ik_mode);

  int config_get_min_token_size(int64_t &size) const;
  int config_get_max_token_size(int64_t &size) const;
  int config_get_ngram_token_size(int64_t &size) const;
  int config_get_stopword_table(ObString &str) const;
  int config_get_dict_table(ObString &str) const;
  int config_get_quantifier_table(ObString &str) const;
  int config_get_ik_mode(ObString &ik_mode) const;

  int rebuild_props_for_ddl(const ObString &parser_name,
                            const common::ObCollationType &type,
                            const bool log_to_user);

  int check_conflict_config_for_resolve(bool &has_conflict_config) const;

  int check_unsupported_config(const char **config_array,
                               int32_t config_count,
                               bool &has_unsupported) const;

  int to_format_json(ObIAllocator &alloc, ObString &str);

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

  static int tokenize_array_to_props_json(ObIAllocator &allocator,
                                          ObIJsonBase *array,
                                          ObString &json_str);

  static int show_parser_properties(const ObFTParserJsonProps &properties,
                                    char *buf,
                                    const int64_t buf_len,
                                    int64_t &pos);

  TO_STRING_KV(K_(is_inited));

private:
  int ik_rebuild_props_for_ddl(bool log_to_user);
  int ngram_rebuild_props_for_ddl(bool log_to_user);
  int space_rebuild_props_for_ddl(bool log_to_user);
  int beng_rebuild_props_for_ddl(bool log_to_user);
  int plugin_rebuild_props_for_ddl(bool log_to_user);

private:
  common::ObArenaAllocator allocator_;
  ObIJsonBase *root_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObFTParserJsonProps);
};

struct ObFTParserProperty final
{
public:
  ObFTParserProperty();
  ~ObFTParserProperty() = default;
  int parse_for_parser_helper(const ObFTParser &parser, const ObString &json_str);

  bool is_equal(const ObFTParserProperty &other) const
  {
    return min_token_size_ == other.min_token_size_ && max_token_size_ == other.max_token_size_
           && ngram_token_size_ == other.ngram_token_size_ && ik_mode_smart_ == other.ik_mode_smart_
           && stopword_table_ == other.stopword_table_ && dict_table_ == other.dict_table_
           && quantifier_table_ == other.quantifier_table_;
  }

  TO_STRING_KV(K_(min_token_size),
               K_(max_token_size),
               K_(ngram_token_size),
               K_(stopword_table),
               K_(dict_table),
               K_(quantifier_table));

public:
  int64_t min_token_size_;
  int64_t max_token_size_;
  int64_t ngram_token_size_;
  bool ik_mode_smart_;
  common::ObString stopword_table_;
  common::ObString dict_table_;
  common::ObString quantifier_table_;
};

} // end namespace storage
} // end namespace oceanbase
#endif // OB_FTS_PARSER_PROPERTY_H_
