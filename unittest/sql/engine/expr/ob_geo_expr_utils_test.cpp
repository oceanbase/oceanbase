/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains testcase for ob_geo_expr_utils.
 */

#include <gtest/gtest.h>
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprGeoUtilsTest : public ::testing::Test
{
public:
  ObExprGeoUtilsTest();
  virtual ~ObExprGeoUtilsTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprGeoUtilsTest);
};

ObExprGeoUtilsTest::ObExprGeoUtilsTest()
{
}

ObExprGeoUtilsTest::~ObExprGeoUtilsTest()
{
}

void ObExprGeoUtilsTest::SetUp()
{
}

void ObExprGeoUtilsTest::TearDown()
{
}

TEST_F(ObExprGeoUtilsTest, parse_axis_order_test)
{
  ObGeoAxisOrder axis_order;
  ASSERT_EQ(OB_SUCCESS, ObGeoExprUtils::parse_axis_order(ObString("axis-order=lat-long"), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::LAT_LONG, axis_order);
  ASSERT_EQ(OB_SUCCESS, ObGeoExprUtils::parse_axis_order(ObString("   axis-order= long-lat "), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::LONG_LAT, axis_order);
  ASSERT_EQ(OB_SUCCESS, ObGeoExprUtils::parse_axis_order(ObString("   axis-order= srid-defined "), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::SRID_DEFINED, axis_order);

  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObGeoExprUtils::parse_axis_order(ObString("   axis-order: = long-lat ,"), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::INVALID, axis_order);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObGeoExprUtils::parse_axis_order(ObString("   axis-order= long-lat ,"), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::INVALID, axis_order);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObGeoExprUtils::parse_axis_order(ObString("   axis-order= srid-defined, axis-order = srid-defined"), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::INVALID, axis_order);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObGeoExprUtils::parse_axis_order(ObString(" . = ."), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::INVALID, axis_order);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, ObGeoExprUtils::parse_axis_order(ObString(""), "unit_test", axis_order));
  ASSERT_EQ(ObGeoAxisOrder::INVALID, axis_order);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
