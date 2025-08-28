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
#include "sql/test_sql_utils.h"
#include "common/object/ob_obj_type.h"
#include "lib/utility/ob_test_util.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/string/ob_sql_string.h"
#undef protected
#undef private
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;

const int64_t BUF_LEN = 102400;
class ObRawExprEqualTest: public test::TestSqlUtils, public ::testing::Test
{
public:

  ObRawExprEqualTest() {};
  virtual ~ObRawExprEqualTest() {};
  virtual void SetUp();
  virtual void TearDown();
  int set_equal_param_exprs(ObOpRawExpr *eq_expr, ObRawExpr *param1, ObRawExpr *param2);
  int free_cast_expr(ObOpRawExpr *eq_expr);
};

void ObRawExprEqualTest::SetUp()
{
  init();
}

void ObRawExprEqualTest::TearDown()
{
}

int ObRawExprEqualTest::set_equal_param_exprs(ObOpRawExpr *eq_expr, ObRawExpr *param1, ObRawExpr *param2)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eq_expr) || OB_ISNULL(param1) || OB_ISNULL(param2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(eq_expr), K(param1), K(param2));
  } else if (FALSE_IT(eq_expr->get_param_exprs().reset())) {
    // do nothing
  } else if (OB_FAIL(eq_expr->set_param_exprs(param1, param2))) {
    LOG_WARN("failed to set param exprs", K(ret));
  } else if (OB_FAIL(eq_expr->formalize(&session_info_))) {
    LOG_WARN("failed to formalize expr", K(ret));
  }
  return ret;
}

class ObExprResTypeIterator
{
public:
  ObExprResTypeIterator();
  ~ObExprResTypeIterator() {}

  int next(ObRawExprResType &res_type);
private:
  int set_res_type(ObRawExprResType &res_type);
  int update_type_idx();
private:
  const static int64_t COLLATION_TYPE_COUNT = 8;
  const static int64_t DOUBLE_SCALE_COUNT = 4;
  const static ObCollationType COLLATION_TYPES[COLLATION_TYPE_COUNT];
  const static int64_t DOUBLE_SCALES[DOUBLE_SCALE_COUNT];
private:
  ObObjType type_;
  int64_t sub_type_idx_;
};

ObExprResTypeIterator::ObExprResTypeIterator()
  : type_(ObTinyIntType),
    sub_type_idx_(0)
{
}

const ObCollationType ObExprResTypeIterator::COLLATION_TYPES[COLLATION_TYPE_COUNT] = {
  CS_TYPE_UTF8MB4_GENERAL_CI,
  CS_TYPE_UTF8MB4_BIN,
  CS_TYPE_LATIN1_BIN,
  CS_TYPE_GBK_CHINESE_CI,
  CS_TYPE_GBK_BIN,
  CS_TYPE_UTF16_GENERAL_CI,
  CS_TYPE_UTF16_BIN,
  CS_TYPE_BINARY
};

const int64_t ObExprResTypeIterator::DOUBLE_SCALES[DOUBLE_SCALE_COUNT] = {
  SCALE_UNKNOWN_YET,
  0,
  10,
  OB_MAX_DOUBLE_FLOAT_SCALE
};

int ObExprResTypeIterator::next(ObRawExprResType &res_type)
{
  int ret = OB_SUCCESS;
  res_type.reset();
  if (ObMaxType == type_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(set_res_type(res_type))) {
    LOG_WARN("failed to set res type", K(ret));
  } else if (OB_FAIL(update_type_idx())) {
    LOG_WARN("failed to update type idx", K(ret));
  }
  return ret;
}

int ObExprResTypeIterator::set_res_type(ObRawExprResType &res_type)
{
  int ret = OB_SUCCESS;
  res_type.set_type(type_);
  res_type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[type_].precision_);
  res_type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[type_].scale_);
  res_type.set_collation_type(CS_TYPE_BINARY);
  res_type.set_collation_level(ObRawExprUtils::get_column_collation_level(type_));
  if (ob_is_nstring(type_)) {
    res_type.set_collation_type(CS_TYPE_UTF16_BIN);
    res_type.set_length(100);
  } else if (ob_is_string_or_lob_type(type_)) {
    res_type.set_collation_type(COLLATION_TYPES[sub_type_idx_]);
    res_type.set_length(100);
  } else if (ob_is_double_type(type_)) {
    res_type.set_scale(DOUBLE_SCALES[sub_type_idx_]);
  }
  return ret;
}

int ObExprResTypeIterator::update_type_idx()
{
  int ret = OB_SUCCESS;
  ++sub_type_idx_;
  if ((ob_is_string_or_lob_type(type_) && !ob_is_nstring(type_) 
       && sub_type_idx_ < 1) ||
      (ob_is_double_type(type_) && sub_type_idx_ < 1)) {
    // do nothing
  } else {
    type_ = static_cast<ObObjType>(type_ + 1);
    sub_type_idx_ = 0;
  }
  return ret;
}

TEST_F(ObRawExprEqualTest, equal_compare_func)
{
  int ret = OB_SUCCESS;
  ObExprResType type1;
  ObExprResType type2;
  ObExprResTypeIterator iter1;
  while (OB_SUCC(ret)) {
    ObExprResTypeIterator iter2;
    if (OB_FAIL(iter1.next(type1))) {
      LOG_WARN("failed to iter type A", K(ret));
      break;
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter2.next(type2))) {
        LOG_WARN("failed to iter type B", K(ret));
        break;
      } else {
        bool has_lob_header = type1.has_lob_header() || type2.has_lob_header();
        auto func_ptr = ObExprCmpFuncsHelper::get_eval_expr_cmp_func(type1.get_type(),
                                                                    type2.get_type(),
                                                                    type1.get_precision(),
                                                                    type2.get_precision(),
                                                                    type1.get_scale(),
                                                                    type2.get_scale(),
                                                                    CO_EQ,
                                                                    lib::is_oracle_mode(),
                                                                    CS_TYPE_BINARY,
                                                                    has_lob_header);
        bool has_cmp_func = NULL != func_ptr;
        ObObjType equal_type = ObMaxType;
        ASSERT_EQ(OB_SUCCESS, ObExprResultTypeUtil::get_relational_equal_type(
                                                  equal_type, type1.get_type(), type2.get_type()));
        bool can_deduce_equal = ObMaxType != equal_type;
        if (can_deduce_equal && !has_cmp_func) {
          EXPECT_EQ(can_deduce_equal, has_cmp_func);
          fprintf(stdout, "unexpected cmp func %d %d %d %d %d\n",
                         type1.get_type(), type1.get_collation_type(),
                         type2.get_type(), type2.get_collation_type(),
                         equal_type);
          LOG_WARN("unexpected cmp func", K(type1.get_type()), K(type2.get_type()), KP(func_ptr));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST_F(ObRawExprEqualTest, type_comparable)
{
  int ret = OB_SUCCESS;
  ObExprResTypeIterator iter1;
  ObColumnRefRawExpr *expr1 = NULL;
  ObColumnRefRawExpr *expr2 = NULL;
  ObOpRawExpr *eq_expr = NULL;
  ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_REF_COLUMN, expr1));
  ASSERT_EQ(OB_SUCCESS, expr_factory_.create_raw_expr(T_REF_COLUMN, expr2));
  bool can_compare = true;
  ObObjType equal_type = ObMaxType;
  const ObRawExprResType &type1 = expr1->get_result_type();
  const ObRawExprResType &type2 = expr2->get_result_type();
  LinkExecCtxGuard link_guard(session_info_, exec_ctx_);
  while (OB_SUCC(ret)) {
    ObExprResTypeIterator iter2;
    ObArenaAllocator allocator;
    ObRawExprFactory expr_factory(allocator);
    ASSERT_EQ(OB_SUCCESS, expr_factory.create_raw_expr(T_OP_EQ, eq_expr));
    if (OB_FAIL(iter1.next(expr1->result_type_))) {
      LOG_WARN("failed to iter type A", K(ret));
      break;
    }
    while (OB_SUCC(ret)) 
    {
      can_compare = true;
      if (OB_FAIL(iter2.next(expr2->result_type_))) {
        LOG_WARN("failed to iter type B", K(ret));
        break;
      } else if (OB_FAIL(set_equal_param_exprs(eq_expr, expr1, expr2))) {
        LOG_WARN("failed to set equal param exprs", K(ret));
        // unable to compare, just continue
        ret = OB_SUCCESS;
        can_compare = false;
      } else if (eq_expr->get_param_expr(0) != expr1 || eq_expr->get_param_expr(1) != expr2) {
        can_compare = false;
      }
      ASSERT_EQ(OB_SUCCESS, ObExprResultTypeUtil::get_relational_equal_type(equal_type,
                                                         type1.get_type(), type2.get_type()));
      if (!can_compare && ObMaxType != equal_type) {
        EXPECT_EQ(ObMaxType, equal_type);
        fprintf(stdout, "unexpected equal type %d %d %d %d %d\n",
                        type1.get_type(), type1.get_collation_type(),
                        type2.get_type(), type2.get_collation_type(),
                        equal_type);
        LOG_INFO("unexpected equal type", K(can_compare), K(equal_type),
                                          K(type1), K(type2), KPC(eq_expr));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  EXPECT_EQ(OB_ITER_END, ret);
}

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -rf test_raw_expr_equal.log*");
  OB_LOGGER.set_file_name("test_raw_expr_equal.log", true);
  if(argc >= 2) {
    OB_LOGGER.set_log_level(argv[1]);
  } else {
    OB_LOGGER.set_log_level("INFO");
  }
  init_sql_factories();
  ContextParam param;
  param.set_mem_attr(1001, "Transformer", ObCtxIds::WORK_AREA)
       .set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);

  ::testing::InitGoogleTest(&argc,argv);
  CREATE_WITH_TEMP_CONTEXT(param) {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
