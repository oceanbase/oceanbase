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

#ifndef _OCEANBASE_STORAGE_FTS_OB_FTS_LITERAL_H_
#define _OCEANBASE_STORAGE_FTS_OB_FTS_LITERAL_H_

#include <cstdint>
namespace oceanbase
{
namespace storage
{
class ObFTSLiteral final
{
public:
  static constexpr const char *PARSER_NAME_IK = "ik";
  static constexpr const char *PARSER_NAME_BENG = "beng";
  static constexpr const char *PARSER_NAME_SPACE = "space";
  static constexpr const char *PARSER_NAME_NGRAM = "ngram";

  static constexpr const char *CONFIG_NAME_MIN_TOKEN_SIZE = "min_token_size";
  static constexpr const char *CONFIG_NAME_MAX_TOKEN_SIZE = "max_token_size";
  static constexpr const char *CONFIG_NAME_NGRAM_TOKEN_SIZE = "ngram_token_size";
  static constexpr const char *CONFIG_NAME_STOPWORD_TABLE = "stopword_table";
  static constexpr const char *CONFIG_NAME_DICT_TABLE = "dict_table";
  static constexpr const char *CONFIG_NAME_QUANTIFIER_TABLE = "quanitfier_table";

  // config bound
  static constexpr int64_t FT_MIN_TOKEN_SIZE_LOWER_BOUND = 1;
  static constexpr int64_t FT_MIN_TOKEN_SIZE_UPPER_BOUND = 16;

  static constexpr int64_t FT_MAX_TOKEN_SIZE_LOWER_BOUND = 10;
  static constexpr int64_t FT_MAX_TOKEN_SIZE_UPPER_BOUND = 84;

  static constexpr int64_t FT_NGRAM_TOKEN_SIZE_LOWER_BOUND = 1;
  static constexpr int64_t FT_NGRAM_TOKEN_SIZE_UPPER_BOUND = 10;

  // default config
  static constexpr int64_t FT_DEFAULT_MIN_TOKEN_SIZE = 3;
  static constexpr int64_t FT_DEFAULT_MAX_TOKEN_SIZE = 84;
  static constexpr int64_t FT_DEFAULT_NGRAM_TOKEN_SIZE = 2;

  static constexpr const char *FT_DEFAULT_IK_STOPWORD_UTF8_TABLE
      = "oceanbase.__ft_stopword_ik_utf8";
  static constexpr const char *FT_DEFAULT_IK_DICT_UTF8_TABLE = "oceanbase.__ft_dict_ik_utf8";
  static constexpr const char *FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE
      = "oceanbase.__ft_quantifier_ik_utf8";

  static constexpr const char *FT_NONE = "none";
  static constexpr const char *FT_DEFAULT = "default";

  static constexpr const char *ADDITIONAL_ARGS_STR = "additional_args";
  static constexpr const char *ADDITION_NGRAM_TOKEN_SIZE_STR = "token_size";

  // err msg
  static constexpr const char *MIN_TOKEN_SIZE_SCOPE_STR = "the min_token_size must be in [1, 16]";
  static constexpr const char *MAX_TOKEN_SIZE_SCOPE_STR = "the max_token_size must be in [10, 84]";
  static constexpr const char *NGRAM_TOKEN_SIZE_SCOPE_STR
      = "the ngram_token_size must be in [1, 10]";
  static constexpr const char *MIN_MAX_TOKEN_SIZE_SCOPE_STR
      = "the max_token_size must be greater than min_token_size";
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_FTS_LITERAL_H_
