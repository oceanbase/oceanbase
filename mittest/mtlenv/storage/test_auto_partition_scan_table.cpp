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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public

#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_dml_param.h"
#include "mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"


/*
  0. 用主表模拟局部索引表扫描，两者差别在于主表会进行范围切割，局部索引表会进行 行过滤
  1. 创建分区
	2. 设置分区为split状态，把自己设置为局部索引，并且把origin tablet id设置为自己
	3. 启动table_scan
	4. 预期逻辑
    1）同时获取origin table的tables ；
    2）不会cut range；
    3）根据当前tablet id 过滤数据，当然当前代码里分裂后的schema没有变更，计算分区方式也没有改变，因此数据全部符合
*/

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace unittest
{
class FakeObScanTable : public ::testing::Test
{
public:
  FakeObScanTable() :
    tenant_id_(OB_SYS_TENANT_ID),
    ls_id_(TestDmlCommon::TEST_LS_ID),
    tablet_id_(TestDmlCommon::TEST_DATA_TABLE_ID),
    orig_tablet_id_(51)
  {}

  ~FakeObScanTable(){}

  static void SetUpTestCase();
  static void TearDownTestCase();

  void table_scan(ObAccessService *access_service, ObNewRowIterator *&result);
  void insert_data_to_tablet(MockObAccessService *access_service, ObTabletID &tablet_id, const char *data);
  int set_tablet_split_info(ObTabletID &src_tablet_id, ObTabletSplitType split_type, const int64_t split_cnt);
  int gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, ObDatumRowkey &datum_rowkey);
  ObDatumRowkey &get_start_key() { return start_key_; }
  ObDatumRowkey &get_end_key() { return end_key_; }

protected:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID orig_tablet_id_;

private:
  ObArenaAllocator allocator_;
  ObRowkey start_key_;
  ObRowkey end_key_;

public:
  static constexpr const char *data_row_str_01 =
      "bigint  bigint   bigint  var         var        dml          \n"
      "1       62       20      Houston     Rockets    T_DML_INSERT \n"
      "2       65       17      SanAntonio  Spurs      T_DML_INSERT \n"
      "3       58       24      Dallas      Mavericks  T_DML_INSERT \n"
      "4       51       31      LosAngeles  Lakers     T_DML_INSERT \n"
      "5       57       25      Phoenix     Suns       T_DML_INSERT \n"
      "6       32       50      NewJersey   Nets       T_DML_INSERT \n"
      "7       44       38      Miami       Heats      T_DML_INSERT \n"
      "8       21       61      Chicago     Bulls      T_DML_INSERT \n"
      "9       47       35      Cleveland   Cavaliers  T_DML_INSERT \n"
      "10      59       23      Detroit     Pistons    T_DML_INSERT \n"
      "11      40       42      Utah        Jazz       T_DML_INSERT \n"
      "12      50       32      Boston      Celtics    T_DML_INSERT \n";

  static constexpr const char *data_row_str_02 =
      "bigint   bigint   bigint  var         var        dml          \n"
      "13       62       20      Houston     Rockets    T_DML_INSERT \n"
      "14       65       17      SanAntonio  Spurs      T_DML_INSERT \n"
      "15       58       24      Dallas      Mavericks  T_DML_INSERT \n"
      "16       51       31      LosAngeles  Lakers     T_DML_INSERT \n"
      "17       57       25      Phoenix     Suns       T_DML_INSERT \n"
      "18       32       50      NewJersey   Nets       T_DML_INSERT \n"
      "19       44       38      Miami       Heats      T_DML_INSERT \n"
      "20       21       61      Chicago     Bulls      T_DML_INSERT \n"
      "21       47       35      Cleveland   Cavaliers  T_DML_INSERT \n"
      "22       59       23      Detroit     Pistons    T_DML_INSERT \n"
      "23       40       42      Utah        Jazz       T_DML_INSERT \n"
      "24       50       32      Boston      Celtics    T_DML_INSERT \n";

};

void FakeObScanTable::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
    [](transaction::ObTransID &tx_id) { tx_id = transaction::ObTransID(1001); return OB_SUCCESS; };
 
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
}

void FakeObScanTable::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

int FakeObScanTable::set_tablet_split_info(
    ObTabletID &src_tablet_id,
    ObTabletSplitType split_type,
    const int64_t split_cnt)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletSplitTscInfo split_info;
  ObLSHandle ls_handle;
  ObLSID test_ls_id(ls_id_);

  if (OB_FAIL(MTL(ObLSService *)->get_ls(test_ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "fail to get log stream", K(ret), K(ls_handle));
  } else if (OB_UNLIKELY(nullptr == ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(src_tablet_id, tablet_handle))) {
    STORAGE_LOG(WARN, "fail to get tablet", K(ret), K(src_tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet handle obj is null", K(ret), K(tablet_handle));
  } else {
    split_info.split_cnt_= split_cnt;
    split_info.split_type_ = split_type;
    split_info.start_key_ = start_key_;   // not set is invalid
    split_info.end_key_ = end_key_;       // not set is invalid
    split_info.src_tablet_handle_ = tablet_handle;
    // tablet_handle.get_obj()->set_split_info(split_info);
  }
  return ret;
}

int FakeObScanTable::gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, ObDatumRowkey &datum_rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *key_val_obj = NULL;
  ObRowkey rowkey;
  if (NULL == (key_val_obj = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * key_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "out of memory", K(ret));
  } else {
    for (int64_t i = 0; i < key_cnt; ++i) {
      key_val_obj[i].set_int(key_val);
      // key_val_obj[i].set_min();
    }
    rowkey.assign(key_val_obj, key_cnt);
    if (OB_FAIL(datum_rowkey.from_rowkey(rowkey, allocator_))) {
      STORAGE_LOG(WARN, "fail to from rowkey", K(ret));
    }
  }
  return ret;
}

void FakeObScanTable::insert_data_to_tablet(MockObAccessService *access_service, ObTabletID &tablet_id, const char *data)
{
  ASSERT_NE(nullptr, access_service);
  ASSERT_NE(nullptr, data);

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
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

  ASSERT_EQ(OB_SUCCESS, mock_iter.from(data));

  transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
  // 1. get tx desc
  transaction::ObTxDesc *tx_desc = nullptr;
  ASSERT_EQ(OB_SUCCESS, TestDmlCommon::build_tx_desc(tenant_id_, tx_desc));

  // 2. create savepoint (can be rollbacked)
  ObTxParam tx_param;
  TestDmlCommon::build_tx_param(tx_param);
  int64_t savepoint = 0;
  ASSERT_EQ(OB_SUCCESS, tx_service->create_implicit_savepoint(*tx_desc, tx_param, savepoint, true));
  // 3. acquire snapshot (write also need snapshot)
  ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
  int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ObTxReadSnapshot read_snapshot;
  ASSERT_EQ(OB_SUCCESS, tx_service->get_read_snapshot(*tx_desc, isolation, expire_ts, read_snapshot));

  // 4. storage dml
  ObDMLBaseParam dml_param;
  dml_param.timeout_ = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  dml_param.is_total_quantity_log_ = false;
  dml_param.tz_info_ = NULL;
  dml_param.sql_mode_ = SMO_DEFAULT;
  dml_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
  dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
  dml_param.snapshot_ = read_snapshot;

  ObArenaAllocator allocator;
  share::schema::ObTableDMLParam table_dml_param(allocator);

  share::schema::ObTableSchema table_schema;
  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);

  ASSERT_EQ(OB_SUCCESS, table_dml_param.convert(&table_schema, 1, column_ids));
  dml_param.table_param_ = &table_dml_param;

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, access_service->insert_rows(ls_id_, tablet_id,
      *tx_desc, dml_param, column_ids, &mock_iter, affected_rows));

  ASSERT_EQ(12, affected_rows);

  // 5. submit transaction, or rollback
  expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

  // 6. release tx desc
  tx_service->release_tx(*tx_desc);
}

void FakeObScanTable::table_scan(
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
  ObNewRow *row = nullptr;
  while (OB_SUCC(ret)) {
    ret = result->get_next_row(row);
    if (OB_SUCCESS == ret) {
      ++cnt;
    }
    STORAGE_LOG(WARN, "table scan row", KPC(row));
  }
  ASSERT_EQ(24, cnt);

  // 4. submit transaction, or rollback
  expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
  ASSERT_EQ(OB_SUCCESS, tx_service->commit_tx(*tx_desc, expire_ts));

  // 5. release tx desc
  tx_service->release_tx(*tx_desc);
}

TEST_F(FakeObScanTable, table_scan_pure_data_table)
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(tenant_id_, ls_id_, ls_handle);
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

  ObTableSchema table_schema;
  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id_, table_schema, allocator_));

  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, orig_tablet_id_, table_schema, allocator_));

  insert_data_to_tablet(access_service, tablet_id_, data_row_str_01);
  insert_data_to_tablet(access_service, orig_tablet_id_, data_row_str_02);

  // set split info
  ObTabletID src_tablet_id(TestDmlCommon::TEST_DATA_TABLE_ID);
  ObTabletID ori_tablet_id(51);
  int64_t split_cnt = 1.0;
  int64_t start_val = 0;   // not include eage
  int64_t end_val = 26;    // not include eage
  int64_t key_cnt = 1;
  ret = gen_datum_rowkey(start_val, key_cnt, get_start_key());
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = gen_datum_rowkey(end_val, key_cnt, get_end_key());
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = set_tablet_split_info(src_tablet_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  // table scan
  ObNewRowIterator *iter = nullptr;
  table_scan(access_service, iter);

  // clean env
  TestDmlCommon::delete_mocked_access_service(access_service);
  TestDmlCommon::delete_mocked_ls_tablet_service(tablet_service);

  // for exist
  // the iter has store ctx and store ctx has one ls handle.
  iter->reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_, false));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_auto_partition_scan_table.log");
  OB_LOGGER.set_file_name("test_auto_partition_scan_table.log", true);
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_auto_partition_scan_table");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
