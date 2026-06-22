/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PARSER

#include "sql/parser/ob_parser_fullwidth_converter_c.h"
#include "sql/parser/ob_parser_fullwidth_converter.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/parser/parse_define.h"
#include "sql/parser/parse_malloc.h"

extern "C" {

int sql_parser_preprocess_fullwidth_symbols(ParseResult *parse_result,
                                            const char *input_sql,
                                            int32_t input_len,
                                            const char **processed_sql,
                                            int32_t *processed_len,
                                            void **full_width_sym_converter)
{
  using oceanbase::sql::ObFullWidthSymbolConverter;
  using oceanbase::common::ObCharset;
  using oceanbase::common::ObCharsetType;
  using oceanbase::common::ObCollationType;
  using oceanbase::common::ObString;

  int ret = OB_PARSER_SUCCESS;
  if (OB_ISNULL(parse_result)
      || OB_ISNULL(input_sql)
      || input_len <= 0
      || OB_ISNULL(processed_sql)
      || OB_ISNULL(processed_len)
      || OB_ISNULL(full_width_sym_converter)
      || OB_ISNULL(parse_result->malloc_pool_)) {
    ret = OB_PARSER_ERR_UNEXPECTED;
  } else {
    *processed_sql = input_sql;
    *processed_len = input_len;
    *full_width_sym_converter = NULL;

    const ObCharsetType charset_type = ObCharset::
    charset_type_by_coll(static_cast<ObCollationType>(parse_result->connection_collation_));
    const ObString input_sql_str(input_len, input_sql);

    if ((0 != (parse_result->sql_mode_ & SMO_ORACLE))
        && ObFullWidthSymbolConverter::need_convert(charset_type, input_sql_str)) {
      void *buf = parse_malloc(sizeof(ObFullWidthSymbolConverter), parse_result->malloc_pool_);
      if (OB_ISNULL(buf)) {
        ret = OB_PARSER_ERR_NO_MEMORY;
        LOG_WARN("failed to alloc full width converter", K(ret), K(input_len));
      } else {
        ObFullWidthSymbolConverter *converter = new(buf) ObFullWidthSymbolConverter(
            static_cast<ObIAllocator *>(parse_result->malloc_pool_), charset_type);
        *full_width_sym_converter = static_cast<void *>(converter);
        ObString converted_sql;
        if (OB_FAIL(converter->convert(input_sql_str, converted_sql))) {
          ret = OB_PARSER_ERR_PARSE_SQL;
          LOG_WARN("failed to preprocess fullwidth symbols in parser base", K(ret), K(input_len));
        } else if (converter->has_conversions()) {
          *processed_sql = converted_sql.ptr();
          *processed_len = converted_sql.length();
          LOG_DEBUG("fullwidth symbols preprocessed in parser", K(input_sql_str), K(converted_sql));
        }
      }
    }
  }
  return ret;
}

void sql_parser_postprocess_fullwidth_symbols(ParseResult *parse_result,
                                              void *full_width_sym_converter,
                                              int32_t processed_len)
{
  using oceanbase::sql::ObFullWidthSymbolConverter;
  if (OB_NOT_NULL(parse_result) && OB_NOT_NULL(full_width_sym_converter) && processed_len > 0) {
    ObFullWidthSymbolConverter *converter = static_cast<ObFullWidthSymbolConverter *>(full_width_sym_converter);
    if (converter->has_conversions()) {
      converter->adjust_parse_result(*parse_result, processed_len);
    }
  }
}

int get_fullwidth_parenlevel_delta_for_pl(const char *input_sql,
                                          int32_t input_len,
                                          int32_t first_column,
                                          int32_t last_column,
                                          int connection_collation)
{
  using oceanbase::sql::ObFullWidthSymbolConverter;
  using oceanbase::common::ObCharset;
  using oceanbase::common::ObCharsetType;
  using oceanbase::common::ObCollationType;
  using oceanbase::common::ObString;

  int delta = 0;
  if (OB_NOT_NULL(input_sql) && input_len > 0
      && first_column >= 0 && last_column >= first_column
      && first_column < input_len) {
    const int32_t token_len = std::min(input_len - first_column,        // until the end of the input sql
                                       last_column - first_column + 1); // until the end of the token
    const ObCharsetType charset_type = ObCharset::
          charset_type_by_coll(static_cast<ObCollationType>(connection_collation));
    const char *token = input_sql + first_column;
    const ObString token_str(token_len, token);

    if (OB_ISNULL(token) || token_len <= 0
        || !ObFullWidthSymbolConverter::need_convert(charset_type, token_str)) {
    } else {
      for (int32_t i = 0; i < token_len; ++i) {
        const int32_t remaining = token_len - i;
        int32_t char_len = 0;
        char ascii = ObFullWidthSymbolConverter::map_fullwidth_symbol_to_ascii(token + i,
                                                                               remaining,
                                                                               charset_type,
                                                                               char_len);
        if (ascii == '(' || ascii == '[') {
          ++delta;
          i += char_len - 1;
        } else if (ascii == ')' || ascii == ']') {
          --delta;
          i += char_len - 1;
        }
      }
    }
  }
  return delta;
}

} // extern "C"
