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
#include "lib/oblog/ob_log.h"
#include "lib/container/ob_fixed_array.h"
#define private public
#define protected public
#include "sql/engine/expr/ob_expr_res_type.h"
#undef protected
#undef private

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace test
{

class ObExprResTypeDeprecated : public ObExprResType
{
  OB_UNIS_VERSION(1);
public:
  ObExprResTypeDeprecated() : ObExprResType(),
    inner_alloc_("ExprResType"),
    row_calc_cmp_types_(&inner_alloc_, 0)
  {
  }
public:
  ModulePageAllocator inner_alloc_;
  ObFixedArray<ObExprCalcType, ObIAllocator> row_calc_cmp_types_;
};

OB_SERIALIZE_MEMBER_INHERIT(ObExprResTypeDeprecated,
                            ObObjMeta,
                            accuracy_,
                            calc_accuracy_,
                            calc_type_,
                            res_flags_,
                            row_calc_cmp_types_);

TEST(TestResTypeEncode, res_type_serialize)
{
  ObExprResType res_type;
  ObExprResTypeDeprecated res_type_dep;
  ObExprResType res_type_new;
  ObObjMeta obj_meta;
  ObAccuracy accuracy;
  ObObjMeta calc_meta;
  ObAccuracy calc_accuracy;

  obj_meta.set_number();
  accuracy.set_accuracy(0x0123456789abcde);
  calc_meta.set_varchar();
  calc_meta.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  calc_meta.set_collation_level(CS_LEVEL_IMPLICIT);
  calc_accuracy.set_accuracy(0xedcba9876543210);

  res_type.set_meta(obj_meta);
  res_type.set_accuracy(accuracy);
  res_type.set_calc_meta(calc_meta);
  res_type.set_calc_accuracy(calc_accuracy);

  ASSERT_EQ(obj_meta, res_type.get_obj_meta());
  ASSERT_EQ(accuracy, res_type.get_accuracy());
  ASSERT_EQ(calc_meta, res_type.get_calc_meta());
  ASSERT_EQ(calc_accuracy, res_type.get_calc_accuracy());

  const int64_t buf_len = 2048;
  char buf[buf_len] = {};
  int64_t pos = 0;
  int64_t deserialize_pos = 0;
  const int64_t TYPES_NUM = 10;

  ASSERT_EQ(OB_SUCCESS, res_type.serialize(buf, buf_len, pos));
  ASSERT_EQ(OB_SUCCESS, res_type_dep.deserialize(buf, pos, deserialize_pos));
  EXPECT_EQ(pos, deserialize_pos);

  ASSERT_EQ(obj_meta, res_type_dep.get_obj_meta());
  ASSERT_EQ(accuracy, res_type_dep.get_accuracy());
  ASSERT_EQ(calc_meta, res_type_dep.get_calc_meta());
  ASSERT_EQ(calc_accuracy, res_type_dep.get_calc_accuracy());

  MEMSET(buf, 0, buf_len);
  pos = 0;
  deserialize_pos = 0;

  ASSERT_EQ(OB_SUCCESS, res_type_dep.row_calc_cmp_types_.reserve(TYPES_NUM));
  for (int64_t i = 0; i < TYPES_NUM; ++i) {
    ASSERT_EQ(OB_SUCCESS, res_type_dep.row_calc_cmp_types_.push_back(calc_meta));
  }

  ASSERT_EQ(OB_SUCCESS, res_type_dep.serialize(buf, buf_len, pos));
  ASSERT_EQ(OB_SUCCESS, res_type_new.deserialize(buf, pos, deserialize_pos));
  EXPECT_EQ(pos, deserialize_pos);

  ASSERT_EQ(obj_meta, res_type_new.get_obj_meta());
  ASSERT_EQ(accuracy, res_type_new.get_accuracy());
  ASSERT_EQ(calc_meta, res_type_new.get_calc_meta());
  ASSERT_EQ(calc_accuracy, res_type_new.get_calc_accuracy());
}

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
