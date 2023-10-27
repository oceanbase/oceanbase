/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OB_SQL_UDR_OB_UDR_ANALYZER_H_
#define OB_SQL_UDR_OB_UDR_ANALYZER_H_

#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace sql
{

class ObUDRAnalyzer
{
public:
  ObUDRAnalyzer(common::ObIAllocator &allocator,
                ObSQLMode mode,
                ObCharsets4Parser charsets4parser)
    : allocator_(allocator),
      sql_mode_(mode),
      charsets4parser_(charsets4parser)
  {}
  static bool check_is_allow_stmt_type(stmt::StmtType stmt_type);
  int parse_and_check(const common::ObString &pattern,
                      const common::ObString &replacement);
  int parse_sql_to_gen_match_param_infos(const common::ObString &pattern,
                                         common::ObString &normalized_pattern,
                                         common::ObIArray<ObPCParam*> &raw_params);
  int parse_pattern_to_gen_param_infos(const common::ObString &pattern,
                                       common::ObString &normalized_pattern,
                                       common::ObIArray<ObPCParam*> &raw_params,
                                       ObQuestionMarkCtx &question_mark_ctx);
  int parse_pattern_to_gen_param_infos_str(const common::ObString &pattern,
                                           common::ObString &normalized_pattern,
                                           common::ObString &fixed_param_infos_str,
                                           common::ObString &dynamic_param_infos_str,
                                           common::ObString &def_name_ctx_str);

private:
  template<typename T>
  int serialize_to_hex(const T &infos, common::ObString &infos_str);
  int multiple_query_check(const ObString &sql);
  int traverse_and_check(ParseNode *tree);
  int check_transform_minus_op(ParseNode *tree);
  int find_leftest_const_node(ParseNode &cur_node, ParseNode *&const_node);
  int parse_and_resolve_stmt_type(const common::ObString &sql,
                                  ParseResult &parse_result,
                                  stmt::StmtType &stmt_type);
  int cons_raw_param_infos(const common::ObIArray<ObPCParam*> &raw_params,
                           FixedParamValueArray &fixed_param_infos,
                           DynamicParamInfoArray &dynamic_param_infos);
  int add_fixed_param_value(const int64_t raw_param_idx,
                            const ParseNode *raw_param,
                            FixedParamValueArray &fixed_param_infos);
  int add_dynamic_param_info(const int64_t raw_param_idx,
                             const int64_t question_mark_idx,
                             DynamicParamInfoArray &dynamic_param_infos);

private:
  common::ObIAllocator &allocator_;
  ObSQLMode sql_mode_;
  ObCharsets4Parser charsets4parser_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDRAnalyzer);
};

} // namespace sql end
} // namespace oceanbase end

#endif