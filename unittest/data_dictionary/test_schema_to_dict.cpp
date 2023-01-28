/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * This file defines test_ob_cdc_part_trans_resolver.cpp
 */

#include "gtest/gtest.h"

#define private public
#include "logservice/data_dictionary/ob_data_dict_struct.h"
#undef private
#include "data_dict_test_utils.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace share::schema;

namespace oceanbase
{
namespace datadict
{

TEST(ObDictTenantMeta, schema_to_meta)
{
  ObMockSchemaBuilder schema_builder;
  ObArenaAllocator allocator;
  ObTenantSchema tenant_schema;
  EXPECT_EQ(OB_SUCCESS, schema_builder.build_tenant_schema(tenant_schema));
  ObDictTenantMeta tenant_meta(&allocator);
  ObLSArray ls_arr;
  EXPECT_EQ(OB_SUCCESS, ls_arr.push_back(ObLSID(1)));
  EXPECT_EQ(OB_SUCCESS, ls_arr.push_back(ObLSID(1001)));
  EXPECT_EQ(OB_SUCCESS, tenant_meta.init_with_ls_info(tenant_schema, ls_arr));
  EXPECT_TRUE(tenant_schema.get_tenant_name_str() == tenant_meta.tenant_name_);
}

} // namespace datadict
} // namespace oceanbase

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_schema_to_dict.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_ob_schema_to_dict.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG, META_DICT.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
