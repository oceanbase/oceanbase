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

#include "share/schema/ob_schema_utils.h"
#include "lib/time/ob_time_utility.h"
#include "share/parameter/ob_parameter_macro.h"
#include <gtest/gtest.h>
namespace oceanbase
{
using namespace share;
using namespace share::schema;
namespace common
{
ObConfigContainer l_container;
static ObConfigContainer *local_container()
{
  return &l_container;
}
class TestObParallelDDLControl : public ::testing::Test
{
public:
#undef OB_TENANT_PARAMETER
#define OB_TENANT_PARAMETER(args...) args
DEF_MODE_WITH_PARSER(_parallel_ddl_control, OB_TENANT_PARAMETER, "",
        common::ObParallelDDLControlParser,
        "switch for parallel capability of parallel DDL",
        ObParameterAttr(Section::ROOT_SERVICE, Source::DEFAULT, EditLevel::DYNAMIC_EFFECTIVE));
#undef OB_TENANT_PARAMETER
};

TEST_F(TestObParallelDDLControl, test_parse)
{
  ObParallelDDLControlParser parser;
  uint8_t arr[32];

  ASSERT_EQ(true, parser.parse("", arr, 32));
  ASSERT_EQ(arr[0], 0b00000000);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("truncate_table:off", arr, 32));
  ASSERT_EQ(arr[0], 0b00000001);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("set_comment:off", arr, 32));
  ASSERT_EQ(arr[0], 0b00000100);

  MEMSET(arr, 0, 32);
  ASSERT_EQ(true, parser.parse("create_index:off", arr, 32));
  ASSERT_EQ(arr[0], 0b00010000);

  MEMSET(arr, 0,32);
  ASSERT_EQ(true, parser.parse("set_comment:on, set_comment:off, create_index:off", arr, 32));
  ASSERT_EQ(arr[0], 0b00010100);

  MEMSET(arr, 0,32);
  ASSERT_EQ(true, parser.parse("set_comment:off, set_comment:on, create_index: off", arr, 32));
  ASSERT_EQ(arr[0], 0b00011000);

  MEMSET(arr, 0,32);
  ASSERT_EQ(false, parser.parse("set_comment=on", arr,32));

  MEMSET(arr, 0,32);
  ASSERT_EQ(false, parser.parse("set_commentt:on", arr,32));

  MEMSET(arr, 0,32);
  ASSERT_EQ(false, parser.parse("set_comment:oon", arr,32));

  MEMSET(arr, 0,32);
  ASSERT_EQ(false, parser.parse("set_comment:on, create_index:oof", arr,32));
}

TEST_F(TestObParallelDDLControl, testObParallelDDLControlMode)
{
  ObParallelDDLControlMode ddl_mode;
  _parallel_ddl_control.init_mode(ddl_mode);
  bool is_parallel = false;
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::TRUNCATE_TABLE, is_parallel));
  ASSERT_EQ(true, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::SET_COMMENT, is_parallel));
  ASSERT_EQ(false, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::CREATE_INDEX, is_parallel));
  ASSERT_EQ(false, is_parallel);

  ASSERT_EQ(OB_INVALID_ARGUMENT, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::MAX_TYPE, is_parallel));

  ASSERT_EQ(true, _parallel_ddl_control.set_value("truncate_table:on"));
  _parallel_ddl_control.init_mode(ddl_mode);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::TRUNCATE_TABLE, is_parallel));
  ASSERT_EQ(true, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::SET_COMMENT, is_parallel));
  ASSERT_EQ(false, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::CREATE_INDEX, is_parallel));
  ASSERT_EQ(false, is_parallel);

  ASSERT_EQ(true, _parallel_ddl_control.set_value("set_comment:on"));
  _parallel_ddl_control.init_mode(ddl_mode);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::TRUNCATE_TABLE, is_parallel));
  ASSERT_EQ(true, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::SET_COMMENT, is_parallel));
  ASSERT_EQ(true, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::CREATE_INDEX, is_parallel));
  ASSERT_EQ(false, is_parallel);

  ASSERT_EQ(true, _parallel_ddl_control.set_value("create_index:on, set_comment:on"));
  _parallel_ddl_control.init_mode(ddl_mode);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::TRUNCATE_TABLE, is_parallel));
  ASSERT_EQ(true, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::SET_COMMENT, is_parallel));
  ASSERT_EQ(true, is_parallel);
  ASSERT_EQ(OB_SUCCESS, ddl_mode.is_parallel_ddl(ObParallelDDLControlMode::CREATE_INDEX, is_parallel));
  ASSERT_EQ(true, is_parallel);
}

} // common
} // oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}