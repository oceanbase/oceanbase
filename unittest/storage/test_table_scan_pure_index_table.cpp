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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>

#include "test_dml_common.h"
#include "share/schema/ob_table_dml_param.h"

namespace oceanbase
{
namespace storage
{
class TestTableScanPureIndexTable : public ::testing::Test
{
public:
  TestTableScanPureIndexTable();
  virtual ~TestTableScanPureIndexTable() = default;
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
public:
  void insert_data_to_tablet(MockObAccessService *access_service);
  void table_scan(
      ObAccessService *access_service,
      const share::schema::ObTableSchema &table_schema,
      ObNewRowIterator *&result);
protected:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

TestTableScanPureIndexTable::TestTableScanPureIndexTable()
  : tenant_id_(OB_SYS_TENANT_ID),
    ls_id_(TestDmlCommon::TEST_LS_ID),
    tablet_id_(TestDmlCommon::TEST_TABLE_ID)
{

}

void TestTableScanPureIndexTable::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestTableScanPureIndexTable::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTableScanPureIndexTable::insert_data_to_tablet(MockObAccessService *access_service)
{

}

TEST_F(TestTableScanPureIndexTable, table_scan_pure_index_table)
{
  int ret = OB_SUCCESS;

  ret = TestDmlCommon::create_data_tablet(tenant_id_, ls_id_, tablet_id_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mock ls tablet service and access service
  ObLSTabletService *tablet_service = nullptr;
  ret = TestDmlCommon::mock_ls_tablet_service(ls_id_, tablet_service);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, tablet_service);

  MockObAccessService *access_service = nullptr;
  ret = TestDmlCommon::mock_access_service(tablet_service, access_service);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, access_service);

  insert_data_to_tablet(access_service);

  // table scan
  ObNewRowIterator *iter = nullptr;
  table_scan(access_service, table_schema, iter);

  // clean env
  TestDmlCommon::delete_mocked_access_service(access_service);
  TestDmlCommon::delete_mocked_ls_tablet_service(tablet_service);
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_table_scan_pure_index_table.log*");
  OB_LOGGER.set_file_name("test_table_scan_pure_index_table.log", true);
  OB_LOGGER.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
