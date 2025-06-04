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

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "share/catalog/odps/ob_odps_catalog.h"
#include "share/schema/ob_table_schema.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#define private public
#define protected public
using namespace oceanbase::share;
using namespace oceanbase::common;
class TestOdpsCatalog : public ::testing::Test
{
public:
  TestOdpsCatalog();
  ~TestOdpsCatalog();
  virtual void SetUp();
  virtual void TearDown();
  ObArenaAllocator allocator_;
  ObOdpsCatalog catalog_;

private:
  ObNameCaseMode case_mode_ = ObNameCaseMode::OB_ORIGIN_AND_INSENSITIVE;
};

TestOdpsCatalog::TestOdpsCatalog() : catalog_(allocator_)
{
}

TestOdpsCatalog::~TestOdpsCatalog()
{
}

void TestOdpsCatalog::SetUp()
{
  ASSERT_EQ(0, catalog_.init(""));
}

void TestOdpsCatalog::TearDown()
{
}

TEST_F(TestOdpsCatalog, list_namespace_names)
{
  ObArray<ObString> db_names;
  ASSERT_EQ(0, catalog_.list_namespace_names(db_names));
  LOG_INFO("check list_database_names", K(db_names));
}

TEST_F(TestOdpsCatalog, list_table_names) {
  ObArray<ObString> tb_names;
  ASSERT_EQ(0, catalog_.list_table_names("DEFAULT", case_mode_, tb_names));
  LOG_INFO("check list_table_names", K(tb_names));
}

TEST_F(TestOdpsCatalog, fetch_table_schema) {
  schema::ObTableSchema table_schema;
  ASSERT_EQ(0, catalog_.fetch_table_schema("DEFAULT", "t1", case_mode_, table_schema));
  LOG_INFO("check list_table_names", K(table_schema));
}

int main(int argc, char **argv)
{
  // unusable ut
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
