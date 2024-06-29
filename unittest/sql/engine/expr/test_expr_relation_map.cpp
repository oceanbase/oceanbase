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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#define protected public
#define private public
#include "lib/utility/ob_test_util.h"
#include "common/object/ob_obj_type.h"
#undef protected
#undef private
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

template <typename T, std::size_t row, std::size_t col>
constexpr bool is_array_fully_initialized(const T (&arr)[row][col], int row_depth)
{
  return true;
}

#include "sql/engine/expr/ob_expr_relational_cmp_type.map"
#include "sql/engine/expr/ob_expr_arithmetic_result_type.map"
#include "sql/engine/expr/ob_expr_merge_result_type_oracle.map"
#include "sql/engine/expr/ob_expr_relational_result_type.map"

// Do these maps need to be symmetric?
// ob_expr_div_result_type.map
// ob_expr_int_div_result_type.map
// ob_expr_mod_result_type.map

void check_type_map_symmetry(const ObObjType map[ObMaxType][ObMaxType], const char *map_name)
{
  for (int64_t i = 0; i < ObMaxType; ++i) {
    for (int64_t j = i; j < ObMaxType; ++j) {
      if (map[i][j] != map[j][i]) {
        EXPECT_EQ(map[i][j], map[j][i]);
        fprintf(stdout, "%s not match %ld %ld\n", map_name, i, j);
      }
    }
  }
}

TEST(ObExprRelationMap, expr_map_symmetry)
{
  check_type_map_symmetry(RELATIONAL_CMP_TYPE, "RELATIONAL_CMP_TYPE");
  check_type_map_symmetry(ORACLE_RELATIONAL_CMP_TYPE, "ORACLE_RELATIONAL_CMP_TYPE");
  // todo
  // check_type_map_symmetry(ARITH_RESULT_TYPE, "ARITH_RESULT_TYPE");
  // check_type_map_symmetry(MERGE_RESULT_TYPE, "MERGE_RESULT_TYPE");
  // check_type_map_symmetry(MERGE_RESULT_TYPE_ORACLE, "MERGE_RESULT_TYPE_ORACLE");
  // check_type_map_symmetry(RELATIONAL_RESULT_TYPE, "RELATIONAL_RESULT_TYPE");
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
