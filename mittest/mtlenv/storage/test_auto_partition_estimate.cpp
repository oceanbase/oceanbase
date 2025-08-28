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

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace unittest
{

typedef ObSEArray<share::schema::ObColDesc, 8> ColDescArray;
static int64_t TEST_COLUMN_CNT=5;
static int64_t TEST_ROWKEY_COLUMN_CNT=1;

class FakeObTabletEstimate : public ::testing::Test
{
public:
  FakeObTabletEstimate() :
    tenant_id_(OB_SYS_TENANT_ID),
    ls_id_(TestDmlCommon::TEST_LS_ID),
    tablet_id_(TestDmlCommon::TEST_DATA_TABLE_ID),
    access_service_(nullptr),
    ls_service_(nullptr),
    tx_desc_(nullptr),
    table_param_(allocator_)
  {}

  ~FakeObTabletEstimate(){}

  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

  int prepare_scan_range();
  int reset_scan_range();
  int prepare_scan_param();
  int insert_data();
  int set_tablet_split_info(ObTabletID &src_tablet_id, ObTabletID &origin_tablet_id, ObLSID &ls_id, ObTabletSplitType split_type, const int64_t split_cnt);
  int estimate_row_count(int64_t &logical_row_count, int64_t &physical_row_count);
  int estimate_block_count_and_row_count(ObTabletID &tablet_id, int64_t &macro_block_count, int64_t &micro_block_count, int64_t &sstable_row_count, int64_t &memtable_row_count);
  ObLSTabletService *get_ls_service() { return ls_service_; }
  int gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, ObDatumRowkey &rowkey);
  ObDatumRowkey &get_start_key() { return start_key_; }
  ObDatumRowkey &get_end_key() { return end_key_; }

private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  MockObAccessService *access_service_;
  ObLSTabletService *ls_service_;
  transaction::ObTxDesc *tx_desc_;
  ColDescArray col_descs_;
  ObTableScanRange scan_range_;
  ObTableScanParam scan_param_;
  ObTableParam table_param_;
  ObTableSchema table_schema_;
  ObTabletHandle tablet_handle_;
  ObArenaAllocator allocator_;
  ObDatumRowkey start_key_;
  ObDatumRowkey end_key_;
};

void FakeObTabletEstimate::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
    [](transaction::ObTransID &tx_id) { tx_id = transaction::ObTransID(1001); return OB_SUCCESS; };
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
}

void FakeObTabletEstimate::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void FakeObTabletEstimate::SetUp()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(tenant_id_, ls_id_, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  // mock ls tablet service and access service
  ret = TestDmlCommon::mock_ls_tablet_service(ls_id_, ls_service_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, ls_service_);

  ret = TestDmlCommon::mock_access_service(ls_service_, access_service_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, access_service_);

  ObTableSchema table_schema;
  TestDmlCommon::build_data_table_schema(tenant_id_, table_schema);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id_, table_schema, allocator_));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id_, tablet_handle_));

  ASSERT_EQ(OB_SUCCESS, prepare_scan_param()); // prepare scan range (defaut whole range)
  ret = col_descs_.assign(tablet_handle_.get_obj()->get_rowkey_read_info().get_columns_desc());
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, insert_data());
}

void FakeObTabletEstimate::TearDown()
{
  access_service_ = nullptr;
  tx_desc_ = nullptr;
  tablet_handle_.reset();
  col_descs_.reset();
  scan_range_.reset();
  table_param_.reset();
  start_key_.reset();
  end_key_.reset();

  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_, false));

  TestDmlCommon::delete_mocked_access_service(access_service_);
  TestDmlCommon::delete_mocked_ls_tablet_service(ls_service_);
}

int FakeObTabletEstimate::set_tablet_split_info(
    ObTabletID &src_tablet_id, 
    ObTabletID &origin_tablet_id, 
    ObLSID &ls_id,
    ObTabletSplitType split_type,
    const int64_t split_count)
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
    split_info.split_cnt_= split_count;
    split_info.split_type_ = split_type;
    split_info.start_key_ = start_key_;   // not set is invalid
    split_info.end_key_ = end_key_;       // not set is invalid
    split_info.src_tablet_handle_ = tablet_handle;
    tablet_handle.get_obj()->set_split_info(split_info);
  }

  return ret;
}

int FakeObTabletEstimate::prepare_scan_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_range_.init(scan_param_))) {
    STORAGE_LOG(WARN, "fail to init scan range.", K(ret), K(scan_range_));
  }
  return ret;
}

int FakeObTabletEstimate::reset_scan_range()
{
  scan_range_.reset();
  return OB_SUCCESS;
}

int FakeObTabletEstimate::estimate_row_count(int64_t &logical_row_count, int64_t &physical_row_count)
{
  int ret = OB_SUCCESS;
  logical_row_count = 0;
  physical_row_count = 0;
  common::ObSEArray<common::ObEstRowCountRecord, 2, common::ModulePageAllocator, true> est_records;
  ls_service_->enable_to_read();
  if (OB_ISNULL(ls_service_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected.", K(ret), K(ls_service_));
  } else if (OB_FAIL(ls_service_->estimate_row_count(
      scan_param_, scan_range_, est_records, logical_row_count, physical_row_count))) {
    STORAGE_LOG(WARN, "fail to do estimate row count", K(ret), K(scan_param_), K(scan_range_));
  }
  return ret;
}

int FakeObTabletEstimate::insert_data()
{
  int ret = OB_SUCCESS;
  if (nullptr == access_service_ || OB_ISNULL(tablet_handle_.get_obj()) || OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "null pointer, unexpected.", K(ret), K(access_service_), K(tablet_handle_), K(tx_desc_));
  } else {
    // insert rows
    ObMockNewRowIterator mock_iter;
    ObSEArray<uint64_t, 512> column_ids;
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + 0); // pk
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + 1); // c1
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + 2); // c2
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + 3); // c3
    column_ids.push_back(OB_APP_MIN_COLUMN_ID + 4); // c4

    if (OB_FAIL(mock_iter.from(TestDmlCommon::data_row_str))) {
      STORAGE_LOG(WARN, "mock iter from fail.", K(ret), K(mock_iter));
    } else {
      ObTxParam tx_param;
      int64_t savepoint = 0;
      transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
      if (FALSE_IT(TestDmlCommon::build_tx_param(tx_param))) {
      } else if (OB_FAIL(tx_service->create_implicit_savepoint(*tx_desc_, tx_param, savepoint, true))) {
        STORAGE_LOG(WARN, "fail to create implicit savepoint.", K(ret), K(tx_desc_), K(tx_param));
      } else {
        ObTxReadSnapshot read_snapshot;
        ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
        int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
        if (OB_FAIL(tx_service->get_read_snapshot(*tx_desc_, isolation, expire_ts, read_snapshot))) {
          STORAGE_LOG(WARN, "fail to get read snapshot.", K(ret), K(expire_ts));
        } else {
          ObDMLBaseParam dml_param;
          dml_param.timeout_ = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
          dml_param.is_total_quantity_log_ = false;
          dml_param.tz_info_ = NULL;
          dml_param.sql_mode_ = SMO_DEFAULT;
          dml_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
          dml_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
          dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
          dml_param.snapshot_ = read_snapshot;

          share::schema::ObTableDMLParam table_dml_param(allocator_);
          if (OB_FAIL(table_dml_param.convert(&table_schema_, 1, column_ids))) {
            STORAGE_LOG(WARN, "fail to covert dml param.", K(ret), K(table_schema_));
          } else {
            dml_param.table_param_ = &table_dml_param;
            int64_t affected_rows = 0;
            if (OB_FAIL(access_service_->insert_rows(
              ls_id_, tablet_id_, *tx_desc_, dml_param, column_ids, &mock_iter, affected_rows))) {
              STORAGE_LOG(WARN, "fail to insert rows.", K(ret));
            } else if (affected_rows != 12) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "affected rows not equal to insert rows(12).", K(affected_rows));
            } else {
              expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
              if (OB_FAIL(tx_service->commit_tx(*tx_desc_, expire_ts))) {
                STORAGE_LOG(WARN, "fail to commit tx.", K(ret), K(expire_ts));
              } else {
                tx_service->release_tx(*tx_desc_);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int FakeObTabletEstimate::estimate_block_count_and_row_count(
    ObTabletID &tablet_id, 
    int64_t &macro_block_count, 
    int64_t &micro_block_count,
    int64_t &sstable_row_count,
    int64_t &memtable_row_count)
{
  int ret = OB_SUCCESS;
  macro_block_count = 0;
  micro_block_count = 0;

  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid tablet id.", K(ret), K(tablet_id));
  } else if (OB_ISNULL(ls_service_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected.", K(ret), K(ls_service_));
  } else if (OB_FAIL(ls_service_->estimate_block_count_and_row_count(tablet_id, macro_block_count, micro_block_count, sstable_row_count, memtable_row_count))) {
    STORAGE_LOG(WARN, "fail to do estimate block count.", K(ret), K(tablet_id));
  }

  return ret;
}

int FakeObTabletEstimate::prepare_scan_param()
{
  int ret = OB_SUCCESS;
  // prepare table schema
  if (OB_FALSE_IT(TestDmlCommon::build_data_table_schema(tenant_id_, table_schema_))) {
  } else if (OB_FAIL(TestDmlCommon::build_tx_desc(tenant_id_, tx_desc_))) {  // 1. get tx desc
    STORAGE_LOG(WARN, "fail to build tx desc.", K(ret), K(tenant_id_));
  } else {
    ObTxIsolationLevel isolation = ObTxIsolationLevel::RC;
    int64_t expire_ts = ObTimeUtility::current_time() + TestDmlCommon::TX_EXPIRE_TIME_US;
    ObTxReadSnapshot read_snapshot;
    transaction::ObTransService *tx_service = MTL(transaction::ObTransService*);
    if (OB_FAIL(tx_service->get_read_snapshot(*tx_desc_, isolation, expire_ts, read_snapshot))) {  // 2. get read snapshot
      STORAGE_LOG(WARN, "fail to get_read_snapshot.", K(ret));
    } else {
      ObSArray<uint64_t> colunm_ids;
      colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 0);
      colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 1);
      colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 2);
      colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 3);
      colunm_ids.push_back(OB_APP_MIN_COLUMN_ID + 4);
      if (OB_FAIL(TestDmlCommon::build_table_param(table_schema_, colunm_ids, table_param_))) {  // build table param
        STORAGE_LOG(WARN, "fail to build table param.", K(ret), K(colunm_ids), K(table_schema_));
      } else if (OB_FAIL(TestDmlCommon::build_table_scan_param(tenant_id_, read_snapshot, table_param_, scan_param_))) { // 4. build scan param
        STORAGE_LOG(WARN, "fail to build table scan param.", K(ret), K(tenant_id_), K(read_snapshot));
      }
    }
  }
  return ret;
}

int FakeObTabletEstimate::gen_datum_rowkey(const int64_t key_val, const int64_t key_cnt, ObDatumRowkey &datum_rowkey)
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

TEST_F(FakeObTabletEstimate, test_split_tablet_estimate)
{
// =================================== not set split info ===============================================
  int ret = OB_SUCCESS;
  
  int64_t macro_block_cnt = 0;
  int64_t micro_block_cnt = 0;
  int64_t sstable_row_cnt = 0;
  int64_t memtable_row_cnt = 0;
  int64_t logical_row_count = 0;
  int64_t physical_row_count = 0;
  int64_t  split_cnt = 1;
  
  // 1. do not set split info
  ObTabletID src_tablet_id(TestDmlCommon::TEST_DATA_TABLE_ID);
  ObTabletID ori_tablet_id(TestDmlCommon::TEST_DATA_TABLE_ID);
  
  ObLSID ls_id(TestDmlCommon::TEST_LS_ID);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count); // TestIndexBlockDataPrepare::12
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());
  // TODO: local index table

  // ============================test_estimate_row_set_wrong_split_info===================================
  // 2. wrong ls_id;
  ObLSID wrong_ls_id(111);
  /* main table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, wrong_ls_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());
  
  /* local index table */ 
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, wrong_ls_id, ObTabletSplitType::NONE_RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  // 3. wrong ori tablet_id;
  ObTabletID wrong_ori_tablet_id(222);
  /* main table */ 
  ret = set_tablet_split_info(src_tablet_id, wrong_ori_tablet_id, ls_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  /* local index table */
  ret = set_tablet_split_info(src_tablet_id, wrong_ori_tablet_id, ls_id, ObTabletSplitType::NONE_RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  // 4. wrong split_cnt
  split_cnt = 0;
  /* main table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  /* local index table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::NONE_RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret); 
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  // // ==================================== test_estimate_row_right_set_split_info ======================================
  int64_t start_val = 0;
  int64_t end_val = 0;

  // 1. do not split range
  split_cnt = 1.0;
  /* main table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count); // not set split key, invalid spilt info.
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  /* local index table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::NONE_RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(12, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());


  // 2. do split range
  split_cnt = 1.0;
  start_val = 1;  // include eage
  end_val = 6;    // not include eage
  int64_t key_cnt = 1;
  ret = gen_datum_rowkey(start_val, key_cnt, get_start_key());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = gen_datum_rowkey(end_val, key_cnt, get_end_key());
  ASSERT_EQ(OB_SUCCESS, ret);
  
  /* main table */ 
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());  // cut scan range using split info
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(10, logical_row_count);  // two tablets
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());

  /* local index table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::NONE_RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());  // cut scan range using split info
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(24, logical_row_count); //  two tablets total data
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, reset_scan_range());
  // ### because estimate block is calculate block metas count, has no relation with range

  // 3. set split ratio 0.5;
  split_cnt = 2;
  start_val = 1;    // include edge
  end_val = 6;      // not include edge
  int64_t key_cnt = 1;
  ret = gen_datum_rowkey(start_val, key_cnt, get_start_key());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = gen_datum_rowkey(end_val, key_cnt, get_end_key());
  ASSERT_EQ(OB_SUCCESS, ret);

  /* main table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());  // cut scan range using split info
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(10, logical_row_count);  // counts of two tablets in split range
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, reset_scan_range());
  
  /* local index table */
  ret = set_tablet_split_info(src_tablet_id, ori_tablet_id, ls_id, ObTabletSplitType::NONE_RANGE, split_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_range());  // cut scan range using split info
  ret = estimate_row_count(logical_row_count, physical_row_count);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(24, logical_row_count);
  ret = estimate_block_count_and_row_count(src_tablet_id, macro_block_cnt, micro_block_cnt, sstable_row_cnt, memtable_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, reset_scan_range()); 
  // ### because estimate block is calculate block metas count, has no relation with range
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_auto_partition_estimate.log");
  OB_LOGGER.set_file_name("test_auto_partition_estimate.log", true);
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_auto_partition_estimate");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
