/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_OB_FTS_LITERAL_H_
#define _OCEANBASE_STORAGE_FTS_OB_FTS_LITERAL_H_

#include "lib/utility/ob_macro_utils.h"
#include <cstdint>
#include <array>
#include "lib/string/ob_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"

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
  static constexpr const char *PARSER_NAME_NGRAM2 = "ngram2";

  static constexpr const char *CONFIG_NAME_MIN_TOKEN_SIZE = "min_token_size";
  static constexpr const char *CONFIG_NAME_MAX_TOKEN_SIZE = "max_token_size";
  static constexpr const char *CONFIG_NAME_NGRAM_TOKEN_SIZE = "ngram_token_size";

  static constexpr const char CONFIG_NAME_STOPWORD_TABLE[] = "stopword_table";
  static constexpr const char CONFIG_NAME_DICT_TABLE[] = "dict_table";
  static constexpr const char CONFIG_NAME_QUANTIFIER_TABLE[] = "quantifier_table";
  static constexpr const char CONFIG_NAME_STOPWORD_TABLE_ID[] = "stopword_table_id";
  static constexpr const char CONFIG_NAME_DICT_TABLE_ID[] = "dict_table_id";
  static constexpr const char CONFIG_NAME_QUANTIFIER_TABLE_ID[] = "quantifier_table_id";

  // Static string mapping data: table_name -> table_id config name
  struct TableNameToIdPair {
    const char *name;
    const char *id;
  };

  static constexpr TableNameToIdPair TABLE_NAME_TO_ID_DATA[] = {
    {CONFIG_NAME_DICT_TABLE, CONFIG_NAME_DICT_TABLE_ID},
    {CONFIG_NAME_STOPWORD_TABLE, CONFIG_NAME_STOPWORD_TABLE_ID},
    {CONFIG_NAME_QUANTIFIER_TABLE, CONFIG_NAME_QUANTIFIER_TABLE_ID}
  };

  // Static string mapping data: table_id -> table_name config name (reverse mapping)
  struct TableIdToNamePair {
    const char *id;
    const char *name;
  };

  static constexpr TableIdToNamePair TABLE_ID_TO_NAME_DATA[] = {
    {CONFIG_NAME_DICT_TABLE_ID, CONFIG_NAME_DICT_TABLE},
    {CONFIG_NAME_STOPWORD_TABLE_ID, CONFIG_NAME_STOPWORD_TABLE},
    {CONFIG_NAME_QUANTIFIER_TABLE_ID, CONFIG_NAME_QUANTIFIER_TABLE}
  };

  // Simple hash: combine length + first char
  static constexpr uint32_t hash_len_first(const char *str, const int32_t len)
  {
    return (len <= 0 || nullptr == str) ? 0 : ((len << 8) | (uint8_t)(str[0]));
  }

  // Maps table name to corresponding table ID config name
  static const char *get_table_id_config_name(const common::ObString &table_name_config)
  {
    static constexpr uint32_t KEY_DICT_NAME = hash_len_first(CONFIG_NAME_DICT_TABLE, sizeof(CONFIG_NAME_DICT_TABLE) - 1);
    static constexpr uint32_t KEY_STOPWORD_NAME = hash_len_first(CONFIG_NAME_STOPWORD_TABLE, sizeof(CONFIG_NAME_STOPWORD_TABLE) - 1);
    static constexpr uint32_t KEY_QUANTIFIER_NAME = hash_len_first(CONFIG_NAME_QUANTIFIER_TABLE, sizeof(CONFIG_NAME_QUANTIFIER_TABLE) - 1);

    const uint32_t key = hash_len_first(table_name_config.ptr(), table_name_config.length());
    return (key == KEY_DICT_NAME) ? TABLE_NAME_TO_ID_DATA[0].id
            : (key == KEY_STOPWORD_NAME) ? TABLE_NAME_TO_ID_DATA[1].id
            : (key == KEY_QUANTIFIER_NAME) ? TABLE_NAME_TO_ID_DATA[2].id
            : nullptr;
  }

  // Maps table ID config name to corresponding table name
  static const char *get_table_name_config_name(const common::ObString &table_id_config)
  {
    static constexpr uint32_t KEY_DICT_ID = hash_len_first(CONFIG_NAME_DICT_TABLE_ID, sizeof(CONFIG_NAME_DICT_TABLE_ID) - 1);
    static constexpr uint32_t KEY_STOPWORD_ID = hash_len_first(CONFIG_NAME_STOPWORD_TABLE_ID, sizeof(CONFIG_NAME_STOPWORD_TABLE_ID) - 1);
    static constexpr uint32_t KEY_QUANTIFIER_ID = hash_len_first(CONFIG_NAME_QUANTIFIER_TABLE_ID, sizeof(CONFIG_NAME_QUANTIFIER_TABLE_ID) - 1);

    const uint32_t key = hash_len_first(table_id_config.ptr(), table_id_config.length());
    return (key == KEY_DICT_ID) ? TABLE_ID_TO_NAME_DATA[0].name
            : (key == KEY_STOPWORD_ID) ? TABLE_ID_TO_NAME_DATA[1].name
            : (key == KEY_QUANTIFIER_ID) ? TABLE_ID_TO_NAME_DATA[2].name
            : nullptr;
  }

  static constexpr const char *CONFIG_NAME_IK_MODE = "ik_mode";
  static constexpr const char *CONFIG_NAME_MIN_NGRAM_SIZE = "min_ngram_size";
  static constexpr const char *CONFIG_NAME_MAX_NGRAM_SIZE = "max_ngram_size";

  // config bound
  static constexpr int64_t FT_MIN_TOKEN_SIZE_LOWER_BOUND = 1;
  static constexpr int64_t FT_MIN_TOKEN_SIZE_UPPER_BOUND = 16;

  static constexpr int64_t FT_MAX_TOKEN_SIZE_LOWER_BOUND = 10;
  static constexpr int64_t FT_MAX_TOKEN_SIZE_UPPER_BOUND = 84;

  static constexpr int64_t FT_NGRAM_TOKEN_SIZE_LOWER_BOUND = 1;
  static constexpr int64_t FT_NGRAM_TOKEN_SIZE_UPPER_BOUND = 10;

  static constexpr int64_t FT_NGRAM_MIN_TOKEN_SIZE_LOWER_BOUND = 1;
  static constexpr int64_t FT_NGRAM_MIN_TOKEN_SIZE_UPPER_BOUND = 16;

  static constexpr int64_t FT_NGRAM_MAX_TOKEN_SIZE_LOWER_BOUND = 1;
  static constexpr int64_t FT_NGRAM_MAX_TOKEN_SIZE_UPPER_BOUND = 16;

  static constexpr const char *FT_IK_MODE_SMART = "smart";
  static constexpr const char *FT_IK_MODE_MAX_WORD = "max_word";

  // default config
  static constexpr int64_t FT_DEFAULT_MIN_TOKEN_SIZE = 3;
  static constexpr int64_t FT_DEFAULT_MAX_TOKEN_SIZE = 84;
  static constexpr int64_t FT_DEFAULT_NGRAM_TOKEN_SIZE = 2;

  static constexpr int64_t FT_DEFAULT_MIN_NGRAM_SIZE = 2;
  static constexpr int64_t FT_DEFAULT_MAX_NGRAM_SIZE = 3;

  static constexpr const char *FT_DEFAULT_IK_STOPWORD_UTF8_TABLE
      = "oceanbase.__ft_stopword_ik_utf8";
  static constexpr const char *FT_DEFAULT_IK_DICT_UTF8_TABLE = "oceanbase.__ft_dict_ik_utf8";
  static constexpr const char *FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE
      = "oceanbase.__ft_quantifier_ik_utf8";

  // Mapping from table_id to config_name
  struct TableIdToConfigNamePair {
    uint64_t table_id;
    const char *config_name;
  };

  static constexpr TableIdToConfigNamePair TABLE_ID_TO_CONFIG_NAME_DATA[] = {
    {share::OB_FT_DICT_IK_UTF8_TID, CONFIG_NAME_DICT_TABLE_ID},
    {share::OB_FT_STOPWORD_IK_UTF8_TID, CONFIG_NAME_STOPWORD_TABLE_ID},
    {share::OB_FT_QUANTIFIER_IK_UTF8_TID, CONFIG_NAME_QUANTIFIER_TABLE_ID}
  };

  static const char *get_config_name_by_table_id(const uint64_t table_id)
  {
    return (table_id == share::OB_FT_DICT_IK_UTF8_TID) ? TABLE_ID_TO_CONFIG_NAME_DATA[0].config_name
            : (table_id == share::OB_FT_STOPWORD_IK_UTF8_TID) ? TABLE_ID_TO_CONFIG_NAME_DATA[1].config_name
            : (table_id == share::OB_FT_QUANTIFIER_IK_UTF8_TID) ? TABLE_ID_TO_CONFIG_NAME_DATA[2].config_name
            : nullptr;
  }

  static constexpr const char *FT_NONE = "none";
  static constexpr const char *FT_DEFAULT = "default";

  static constexpr const char *ADDITIONAL_ARGS_STR = "additional_args";
  static constexpr const char *ADDITION_NGRAM_TOKEN_SIZE_STR = "token_size";

  // err msg
  static constexpr const char *MIN_TOKEN_SIZE_SCOPE_STR = "the min_token_size must be in [1, 16]";
  static constexpr const char *MAX_TOKEN_SIZE_SCOPE_STR = "the max_token_size must be in [10, 84]";

  static constexpr const char *MIN_NGRAM_SIZE_SCOPE_STR = "the min_ngram_size must be in [1, 16]";
  static constexpr const char *MAX_NGRAM_SIZE_SCOPE_STR = "the max_ngram_size must be in [1, 16]";

  static constexpr const char *NGRAM_TOKEN_SIZE_SCOPE_STR
      = "the ngram_token_size must be in [1, 10]";
  static constexpr const char *MIN_MAX_TOKEN_SIZE_SCOPE_STR
      = "the max_token_size must be equal to or greater than min_token_size";
  static constexpr const char *NGRAM_MIN_MAX_TOKEN_SIZE_SCOPE_STR
      = "the max_ngram_size must be equal to or greater than min_ngram_size";

  static constexpr const char *IK_MODE_SCOPE_STR = "the ik_mode should be max_word or smart";

};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_FTS_LITERAL_H_
