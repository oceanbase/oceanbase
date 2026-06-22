/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_PARSER_OB_PARSER_FULLWIDTH_CONVERTER_C_H_
#define OCEANBASE_SQL_PARSER_OB_PARSER_FULLWIDTH_CONVERTER_C_H_

#include <stdint.h>

#include "parse_node.h"

/* ======================================================================================================
 * This file defines C interfaces for ObFullWidthSymbolConverter
 * Functions below are used by ORACLE mode's normal parser and PL parser to handle fullwidth symbols
 * ====================================================================================================== */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Preprocess SQL text for fullwidth mapping before lexer/parser runs.
 * The returned processed_sql/processed_len are used as parser input.
 * `full_width_sym_converter` would be allocated from parse_result->malloc_pool_
 */
int sql_parser_preprocess_fullwidth_symbols(ParseResult *parse_result,
                                            const char *input_sql,
                                            int32_t input_len,
                                            const char **processed_sql,
                                            int32_t *processed_len,
                                            void **full_width_sym_converter);

/*
 * This should be called after parser execution regardless of success/failure.
 * Map parse result locations from converted SQL back to original SQL.
 * Do nothing if no conversion happened.
 */
void sql_parser_postprocess_fullwidth_symbols(ParseResult *parse_result,
                                              void *full_width_sym_converter,
                                              int32_t processed_len);

/*
 * Used by Oracle PL parser to calculate full-width parenthesis nesting level delta for SQL stmt.
 * return + N for N-th non-paired fullwidth '(', '['
 * return - N for N-th non-paired fullwidth ')', ']'.
 */
int get_fullwidth_parenlevel_delta_for_pl(const char *input_sql,
                                          int32_t input_len,
                                          int32_t first_column,
                                          int32_t last_column,
                                          int connection_collation);

#ifdef __cplusplus
}
#endif

#endif /* OCEANBASE_SQL_PARSER_OB_PARSER_FULLWIDTH_CONVERTER_C_H_ */
