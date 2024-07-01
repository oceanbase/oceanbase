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
#define private public
#define protected public
#include "storage/test_dml_common.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/test_tablet_helper.h"

namespace oceanbase
{
namespace transaction {
  int ObTransService::gen_trans_id_(ObTransID &trans_id) {
    trans_id = ObTransID(1001);
    return OB_SUCCESS;
  }
}

namespace storage
{
class TestTableScanPureDataTable : public ::testing::Test
{
public:
  TestTableScanPureDataTable();
  virtual ~TestTableScanPureDataTable() = default;
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
      ObNewRowIterator *&result);
protected:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObArenaAllocator allocator_;
};

TestTableScanPureDataTable::TestTableScanPureDataTable()
  : tenant_id_(OB_SYS_TENANT_ID),
    ls_id_(TestDmlCommon::TEST_LS_ID),
    tablet_id_(TestDmlCommon::TEST_DATA_TABLE_ID)
{

}

void TestTableScanPureDataTable::SetUpTestCase()
{
  uint64_t version = cal_version(4, 3, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().init(version));
  ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(MTL_ID()));
  ObClusterVersion::get_instance().tenant_config_mgr_ = &omt::ObTenantConfigMgr::get_instance();

  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  // MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
  //   [](transaction::ObTransID &tx_id) { tx_id = transaction::ObTransID(1001); return OB_SUCCESS; };
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
}

void TestTableScanPureDataTable::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTableScanPureDataTable::insert_data_to_tablet(MockObAccessService *access_service)
{
  ASSERT_NE(nullptr, access_service);

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id_, tablet_handle));
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // insert rows
  ObMockNewRowIterator mock_iter;
  ObSEArray<uint64_t, 512> column_ids;
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 0); // pk
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 1); // c1
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 2); // c2
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 3); // c3
  column_ids.push_back(OB_APP_MIN_COLUMN_ID + 4); // c4

  ASSERT_EQ(OB_SUCCESS, mock_iter.from(TestDmlCommon::data_row_str));

  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. create savepoint (can be rollbacked)
  ObTxParam tx_param;
  TestDmlCommon::build_tx_param(tx_param);
  ObTxSEQ savepoint;
  ASSERT_EQ(OB_SUCCESS, tx_service->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
  // 3. acquire snapshot (write also need snapshot)
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

  // 4. storage dml
  ObStoreCtxGuard store_ctx_guard;
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  dml_param.is_total_quantity_log_ = false;
  dml_param.tz_info_ = NULL;
  dml_param.sql_mode_ = SMO_DEFAULT;
  dml_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
  dml_param.snapshot_ = read_snapshot;
  dml_param.store_ctx_guard_ = &store_ctx_guard;

  ObArenaAllocator allocator;
  share::schema::ObTableDMLParam table_dml_param(allocator);

  share::schema::ObTableSchema table_schema;
  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);

  ObSEArray<const ObTableSchema *, 4> index_schema_array;

  ASSERT_EQ(OB_SUCCESS, table_dml_param.convert(&table_schema, 1, column_ids));
  dml_param.table_param_ = &table_dml_param;
  ASSERT_EQ(OB_SUCCESS, access_service->get_write_store_ctx_guard(ls_id_,
                                                                  dml_param.timeout_,
                                                                  *tx_desc,
                                                                  read_snapshot,
                                                                  0,/*branch_id*/
                                                                  store_ctx_guard));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, access_service->insert_rows(ls_id_, tablet_id_,
      *tx_desc, dml_param, column_ids, &mock_iter, affected_rows));
  store_ctx_guard.reset();

  ASSERT_EQ(12, affected_rows);

  // 5. serialize trans result and ship
  // 6. merge result if necessary

  // 7. end data access, if failed, rollback
  //expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  //ASSERT_EQ(OB_SUCCESS, tx_service->rollback_to_implicit_savepoint(*tx_desc, savepoint, expire_ts, NULL));

  // 8. submit transaction, or rollback
  expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

  // 9. release tx desc
  tx_service->release_tx(*tx_desc);
}

void TestTableScanPureDataTable::table_scan(
    ObAccessService *access_service,
    ObNewRowIterator *&result)
{
  // prepare table schema
  share::schema::ObTableSchema table_schema;
  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);

  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. get read snapshot
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

  // 3. storage dml
  // build table param
  ObArenaAllocator allocator;
  share::schema::ObTableParam table_param(allocator);
  ObSArray<uint64_t> colunm_ids;
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 0);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 1);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 2);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 3);
  colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 4);

  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_param(table_schema, colunm_ids, table_param));

  ObTableScanParam scan_param;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_table_scan_param(tenant_id_, read_snapshot, table_param, scan_param));

  const share::ObLSID ls_id(TestDmlCommon::TEST_LS_ID);
  ASSERT_EQ(OB_SUCCESS, access_service->table_scan(scan_param, result));
  ASSERT_TRUE(nullptr != result);

  // print data
  int ret = OB_SUCCESS;
  int cnt = 0;
  ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(result);
  blocksstable::ObDatumRow *row = nullptr;
  while (OB_SUCC(ret)) {
    ret = table_scan_iter->get_next_row(row);
    if (OB_SUCCESS == ret) {
      ++cnt;
    }
    LOG_INFO("table scan row", KPC(row), K(ret));
  }
  ASSERT_EQ(12, cnt);

  // 4. submit transaction, or rollback
  expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

  // 5. release tx desc
  tx_service->release_tx(*tx_desc);
}

TEST_F(TestTableScanPureDataTable, table_scan_pure_data_table)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  TestDmlCommon::create_ls(tenant_id_, ls_id_, ls_handle);

  ObTableSchema table_schema;
  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id_, table_schema, allocator_));

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
  table_scan(access_service, iter);


  // clean env
  TestDmlCommon::delete_mocked_access_service(access_service);
  TestDmlCommon::delete_mocked_ls_tablet_service(tablet_service);

  // for exist
  // the iter has store ctx and store ctx has one ls handle.
  iter->reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_));
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_table_scan_pure_data_table.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_scan_pure_data_table.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
