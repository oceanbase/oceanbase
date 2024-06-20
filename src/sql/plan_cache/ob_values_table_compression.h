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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_VALUES_TABLE_COMPRESSION_
#define OCEANBASE_SQL_PLAN_CACHE_OB_VALUES_TABLE_COMPRESSION_

#include "sql/parser/ob_parser.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"
#include "sql/parser/ob_char_type.h"
#include "sql/parser/ob_fast_parser.h"

namespace oceanbase
{
namespace sql
{
class ObPlanCacheCtx;
struct ObRawSql;
class ObFastParserResult;

// values clause folding params utils class
class ObValuesTableCompression {
public:
  static int try_batch_exec_params(common::ObIAllocator &allocator,
                                   ObPlanCacheCtx &pc_ctx,
                                   ObSQLSessionInfo &session_info,
                                   ObFastParserResult &fp_result);
  // for resolve params in fast parser to match plan set
  static int resolve_params_for_values_clause(ObPlanCacheCtx &pc_ctx,
                                          const stmt::StmtType stmt_type,
                                          const common::ObIArray<NotParamInfo> &not_param_info,
                                          const common::ObIArray<ObCharsetType> &param_charset_type,
                                          const ObBitSet<> &neg_param_index,
                                          const ObBitSet<> &not_param_index,
                                          const ObBitSet<> &must_be_positive_idx,
                                          ParamStore *&ab_params);
  // for resolve array params after handler_parser
  static int resolve_params_for_values_clause(ObPlanCacheCtx &pc_ctx);
  static int parser_values_row_str(common::ObIAllocator &allocator,
                                   const common::ObString &no_param_sql,
                                   const int64_t values_token_pos,
                                   common::ObString &new_no_param_sql,
                                   int64_t &old_end_pos,
                                   int64_t &new_end_pos,
                                   int64_t &row_count,
                                   int64_t &param_count,
                                   int64_t &delta_length,
                                   bool &can_batch_opt);
private:
  static void match_one_state(const char *&p,
                              const char *p_end,
                              const ObParser::State next_state,
                              ObParser::State &state);
  static bool is_support_compress_values_table(const common::ObString &stmt);
  static int add_raw_array_params(common::ObIAllocator &allocator,
                                  ObPlanCacheCtx &pc_ctx,
                                  const ObFastParserResult &fp_result,
                                  const int64_t begin_param,
                                  const int64_t row_count,
                                  const int64_t param_count);
  static int rebuild_new_raw_sql(ObPlanCacheCtx &pc_ctx,
                                 const common::ObIArray<ObPCParam*> &raw_params,
                                 const int64_t begin_idx,
                                 const int64_t param_cnt,
                                 const int64_t delta_length,
                                 const common::ObString &no_param_sql,
                                 common::ObIArray<int64_t> &no_param_pos,
                                 common::ObIArray<int64_t> &raw_sql_offset,
                                 common::ObString &new_raw_sql,
                                 int64_t &no_param_sql_pos,
                                 int64_t &new_raw_pos);
  static bool is_mysql_space(char ch) {
    return ch != INVALID_CHAR && SPACE_FLAGS[static_cast<uint8_t>(ch)];
  }
  static void skip_space(ObRawSql &raw_sql);
  static bool skip_row_constructor(ObRawSql &raw_sql);
  static void get_one_row_str(ObRawSql &no_param_sql,
                             int64_t &param_count,
                             int64_t &end_pos,
                             bool &is_valid);
private:
  static const char *lower_[ObParser::S_MAX];
  static const char *upper_[ObParser::S_MAX];
};

}
}
#endif /* _OB_VALUES_TABLE_COMPRESSION_H */
