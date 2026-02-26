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

#define USING_LOG_PREFIX SQL_OPTIMIZER

#include <gtest/gtest.h>
#include <fstream>
#include "lib/oblog/ob_log.h"
#define private public
#define protected public
#include "sql/resolver/expr/ob_raw_expr.h"
#undef protected
#undef private

using namespace oceanbase::sql;

namespace test
{

#define PRINT_SIZE(out, type) out << "| " << #type << " | " << sizeof(type) << " |" << std::endl;

void verify_results(const char* result_file, const char* tmp_file) {
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
}

TEST(TestRawExprSize, expr_size)
{
  static const char* tmp_file = "./expr/test_raw_expr_size.tmp";
  static const char* result_file = "./expr/test_raw_expr_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  of_result << "| class | sizeof(class) |" << std::endl;
  of_result << "| --- | --- |" << std::endl;
  PRINT_SIZE(of_result, ObRawExpr)
  PRINT_SIZE(of_result, ObTerminalRawExpr)
  PRINT_SIZE(of_result, ObConstRawExpr)
  PRINT_SIZE(of_result, ObVarRawExpr)
  PRINT_SIZE(of_result, ObUserVarIdentRawExpr)
  PRINT_SIZE(of_result, ObExecParamRawExpr)
  PRINT_SIZE(of_result, ObQueryRefRawExpr)
  PRINT_SIZE(of_result, ObColumnRefRawExpr)
  PRINT_SIZE(of_result, ObSetOpRawExpr)
  PRINT_SIZE(of_result, ObAliasRefRawExpr)
  PRINT_SIZE(of_result, ObNonTerminalRawExpr)
  PRINT_SIZE(of_result, ObOpRawExpr)
  PRINT_SIZE(of_result, ObCaseOpRawExpr)
  PRINT_SIZE(of_result, ObAggFunRawExpr)
  PRINT_SIZE(of_result, ObSysFunRawExpr)
  PRINT_SIZE(of_result, ObSequenceRawExpr)
  PRINT_SIZE(of_result, ObNormalDllUdfRawExpr)
  PRINT_SIZE(of_result, ObCollectionConstructRawExpr)
  PRINT_SIZE(of_result, ObObjectConstructRawExpr)
  PRINT_SIZE(of_result, ObUDFRawExpr)
  PRINT_SIZE(of_result, ObPLIntegerCheckerRawExpr)
  PRINT_SIZE(of_result, ObPLGetCursorAttrRawExpr)
  PRINT_SIZE(of_result, ObPLSQLCodeSQLErrmRawExpr)
  PRINT_SIZE(of_result, ObPLSQLVariableRawExpr)
  PRINT_SIZE(of_result, ObCallParamRawExpr)
  PRINT_SIZE(of_result, ObPLAssocIndexRawExpr)
  PRINT_SIZE(of_result, ObObjAccessRawExpr)
  PRINT_SIZE(of_result, ObMultiSetRawExpr)
  PRINT_SIZE(of_result, ObCollPredRawExpr)
  PRINT_SIZE(of_result, ObWinFunRawExpr)
  PRINT_SIZE(of_result, ObPseudoColumnRawExpr)
  PRINT_SIZE(of_result, ObOpPseudoColumnRawExpr)
  PRINT_SIZE(of_result, ObMatchFunRawExpr)
  PRINT_SIZE(of_result, ObUnpivotRawExpr)
  PRINT_SIZE(of_result, ObPlQueryRefRawExpr)
  PRINT_SIZE(of_result, ObUDTConstructorRawExpr)
  PRINT_SIZE(of_result, ObUDTAttributeAccessRawExpr)
  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST(TestRawExprSize, expr_member_size)
{
  static const char* tmp_file = "./expr/test_raw_expr_member_size.tmp";
  static const char* result_file = "./expr/test_raw_expr_member_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());

  {
    ObRawExprResType res_type;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObRawExprResType)
    PRINT_SIZE(of_result, ObObjMeta)
    PRINT_SIZE(of_result, res_type.accuracy_)
    PRINT_SIZE(of_result, res_type.res_flags_)
    of_result << std::endl;
  }
  {
    ObSysFunRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObRawExpr)
    PRINT_SIZE(of_result, expr.type_)
    PRINT_SIZE(of_result, expr.expr_class_)
    PRINT_SIZE(of_result, expr.result_type_)
    PRINT_SIZE(of_result, expr.magic_num_)
    PRINT_SIZE(of_result, expr.reference_type_)
    PRINT_SIZE(of_result, expr.info_)
    PRINT_SIZE(of_result, expr.rel_ids_)
    PRINT_SIZE(of_result, expr.inner_alloc_)
    PRINT_SIZE(of_result, expr.expr_factory_)
    PRINT_SIZE(of_result, expr.alias_column_name_)
    PRINT_SIZE(of_result, expr.expr_name_)
    PRINT_SIZE(of_result, expr.rt_expr_)
    PRINT_SIZE(of_result, expr.extra_)
    PRINT_SIZE(of_result, expr.is_shared_reference_)
    PRINT_SIZE(of_result, expr.is_called_in_sql_)
    PRINT_SIZE(of_result, expr.is_calculated_)
    PRINT_SIZE(of_result, expr.is_deterministic_)
    PRINT_SIZE(of_result, expr.local_session_var_id_)
    PRINT_SIZE(of_result, expr.expr_hash_)
    of_result << std::endl;
  }
  {
    ObConstRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObConstRawExpr)
    PRINT_SIZE(of_result, ObTerminalRawExpr)
    PRINT_SIZE(of_result, expr.value_)
    PRINT_SIZE(of_result, expr.literal_prefix_)
    PRINT_SIZE(of_result, expr.obj_meta_)
    PRINT_SIZE(of_result, expr.is_date_unit_)
    PRINT_SIZE(of_result, expr.is_literal_bool_)
    PRINT_SIZE(of_result, expr.is_batch_stmt_parameter_)
    PRINT_SIZE(of_result, expr.is_dynamic_eval_questionmark_)
    PRINT_SIZE(of_result, expr.orig_questionmark_type_)
    of_result << std::endl;
  }
  {
    ObColumnRefRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObColumnRefRawExpr)
    PRINT_SIZE(of_result, ObTerminalRawExpr)
    PRINT_SIZE(of_result, expr.table_id_)
    PRINT_SIZE(of_result, expr.column_id_)
    PRINT_SIZE(of_result, expr.database_name_)
    PRINT_SIZE(of_result, expr.table_name_)
    PRINT_SIZE(of_result, expr.synonym_name_)
    PRINT_SIZE(of_result, expr.synonym_db_name_)
    PRINT_SIZE(of_result, expr.column_name_)
    PRINT_SIZE(of_result, expr.column_flags_)
    PRINT_SIZE(of_result, expr.dependant_expr_)
    PRINT_SIZE(of_result, expr.is_lob_column_)
    PRINT_SIZE(of_result, expr.is_joined_dup_column_)
    PRINT_SIZE(of_result, expr.is_hidden_)
    PRINT_SIZE(of_result, expr.from_alias_table_)
    PRINT_SIZE(of_result, expr.is_rowkey_column_)
    PRINT_SIZE(of_result, expr.is_unique_key_column_)
    PRINT_SIZE(of_result, expr.is_mul_key_column_)
    PRINT_SIZE(of_result, expr.is_pseudo_column_ref_)
    PRINT_SIZE(of_result, expr.is_strict_json_column_)
    PRINT_SIZE(of_result, expr.srs_id_)
    PRINT_SIZE(of_result, expr.udt_set_id_)
    of_result << std::endl;
  }
  {
    ObOpRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObNonTerminalRawExpr)
    PRINT_SIZE(of_result, ObRawExpr)
    PRINT_SIZE(of_result, expr.op_)
    of_result << std::endl;
  }
  {
    ObOpRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObOpRawExpr)
    PRINT_SIZE(of_result, ObNonTerminalRawExpr)
    PRINT_SIZE(of_result, expr.exprs_)
    of_result << std::endl;
  }
  {
    ObAggFunRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObAggFunRawExpr)
    PRINT_SIZE(of_result, ObRawExpr)
    PRINT_SIZE(of_result, expr.real_param_exprs_)
    PRINT_SIZE(of_result, expr.distinct_)
    PRINT_SIZE(of_result, expr.order_items_)
    PRINT_SIZE(of_result, expr.separator_param_expr_)
    PRINT_SIZE(of_result, expr.udf_meta_)
    PRINT_SIZE(of_result, expr.expr_in_inner_stmt_)
    PRINT_SIZE(of_result, expr.is_need_deserialize_row_)
    PRINT_SIZE(of_result, expr.pl_agg_udf_expr_)
    of_result << std::endl;
  }
  {
    ObSysFunRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObSysFunRawExpr)
    PRINT_SIZE(of_result, ObOpRawExpr)
    PRINT_SIZE(of_result, expr.func_name_)
    of_result << std::endl;
  }
  {
    ObQueryRefRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObQueryRefRawExpr)
    PRINT_SIZE(of_result, ObRawExpr)
    PRINT_SIZE(of_result, expr.ref_id_)
    PRINT_SIZE(of_result, expr.ref_stmt_)
    PRINT_SIZE(of_result, expr.output_column_)
    PRINT_SIZE(of_result, expr.is_set_)
    PRINT_SIZE(of_result, expr.is_cursor_)
    PRINT_SIZE(of_result, expr.has_nl_param_)
    PRINT_SIZE(of_result, expr.is_multiset_)
    PRINT_SIZE(of_result, expr.column_types_)
    PRINT_SIZE(of_result, expr.exec_params_)
    of_result << std::endl;
  }
  {
    ObObjAccessRawExpr expr;
    of_result << "| class | sizeof(class) |" << std::endl;
    of_result << "| --- | --- |" << std::endl;
    PRINT_SIZE(of_result, ObObjAccessRawExpr)
    PRINT_SIZE(of_result, ObOpRawExpr)
    PRINT_SIZE(of_result, expr.get_attr_func_)
    PRINT_SIZE(of_result, expr.func_name_)
    PRINT_SIZE(of_result, expr.access_indexs_)
    PRINT_SIZE(of_result, expr.var_indexs_)
    PRINT_SIZE(of_result, expr.for_write_)
    PRINT_SIZE(of_result, expr.property_type_)
    PRINT_SIZE(of_result, expr.orig_access_indexs_)
    of_result << std::endl;
  }
  of_result.close();
  verify_results(result_file, tmp_file);
}

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
