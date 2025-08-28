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
 */

#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/access/ob_sstable_multi_version_row_iterator.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/mock_access_service.h"
#include "storage/mock_ls_tablet_service.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;

namespace storage 
{

ObBitmap *MOCK_FILTERED_BITMAP = nullptr;

int ObBlockRowStore::init(const ObTableAccessParam &param, common::hash::ObHashSet<int32_t> *agg_col_mask)
{
  UNUSED(agg_col_mask);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBlockRowStore init twice", K(ret));
  } else if (OB_ISNULL(context_.stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to init store pushdown filter", K(ret));
  } else {
    is_inited_ = true;
    iter_param_ = &param.iter_param_;
  }
  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

ObSEArray<ObTxData, 8> TX_DATA_ARR;
static int64_t MAX_TRANS_VERSION = 10000;

int ObTxTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  ret = TX_DATA_ARR.push_back(*tx_data);
  return ret;
}

int ObTxTable::check_with_tx_data(ObReadTxDataArg &read_tx_data_arg, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < TX_DATA_ARR.count(); i++)
  {
    if (read_tx_data_arg.tx_id_ == TX_DATA_ARR.at(i).tx_id_) {
      if (TX_DATA_ARR.at(i).state_ == ObTxData::RUNNING) {
        SCN tmp_scn;
        tmp_scn.convert_from_ts(30);
        ObTxCCCtx tmp_ctx(ObTxState::PREPARE, tmp_scn);
        ret = fn(TX_DATA_ARR[i], &tmp_ctx);
      } else {
        ret = fn(TX_DATA_ARR[i]);
      }
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "check with tx data failed", KR(ret), K(read_tx_data_arg), K(TX_DATA_ARR.at(i)));
      }
      break;
    }
  }
  return ret;
}

void clear_tx_data()
{
  TX_DATA_ARR.reset();
};

struct MultiVersionDIResult
{
  uint8_t row_flag_; // insert: 3, delete: 4, not_exists: 0 insert_delete: 131
  int64_t insert_version_;
  bool is_insert_filtered_;
  int64_t delete_version_;
  bool is_delete_filtered_;
  int64_t col4_value_;
  MultiVersionDIResult(uint8_t row_flag, int64_t insert_version, bool is_insert_filtered, int64_t delete_version, bool is_delete_filtered, int64_t col4_value) :
    row_flag_(row_flag), insert_version_(insert_version), is_insert_filtered_(is_insert_filtered), delete_version_(delete_version), is_delete_filtered_(is_delete_filtered), col4_value_(col4_value) {}
  bool operator== (const MultiVersionDIResult &other) const
  {
    return (row_flag_ == other.row_flag_)
           && (insert_version_ == other.insert_version_)
           && (is_insert_filtered_ == other.is_insert_filtered_)
           && (delete_version_ == other.delete_version_)
           && (is_delete_filtered_ == other.is_delete_filtered_)
           && (col4_value_ == other.col4_value_);
  }
  TO_STRING_KV(K_(row_flag), K_(insert_version), K_(is_insert_filtered), K_(delete_version), K_(is_delete_filtered), K_(col4_value));
};

const static int SCHEMA_ROWKEY_CNT = 2;

class TestMultiVersionDeleteInsertBlockscan : public ObMultiVersionSSTableTest
{
public:
  TestMultiVersionDeleteInsertBlockscan();
  virtual ~TestMultiVersionDeleteInsertBlockscan() {}

  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_query_param(const ObVersionRange &version_range);
  void prepare_block_row_store();
  void destroy_block_row_store();
  void prepare_tx_data(const int64_t txn_cnt, const int64_t commit_cnt);
  bool is_bitmap_equal(ObCGBitmap *bitmap, const std::vector<bool> &bitmap_arr);
  void prepare_filtered_result(ObIMicroBlockRowScanner *micro_scanner, const std::vector<bool> &bitmap_arr);
  int get_next_micro_scanner(ObIMicroBlockRowScanner *&micro_scanner);
  int get_next_preprocessed_bitmap(ObCGBitmap *&bitmap);
  int generate_range(const int64_t start, const int64_t end, ObDatumRange &range);
  void test_preprocess_bitmap_basic();
  void test_preprocess_bitmap_version_range();
  void test_preprocess_bitmap_uncommitted();
  void test_get_next_compacted_row_basic();
  void test_get_next_compacted_row_with_cached_row1();
  void test_get_next_compacted_row_with_cached_row2();
  void test_get_next_compacted_row_with_filtered_row();
  void test_meet_empty_range_block();
  void test_shadow_row_output();
  void test_with_uncommitted_lock_row();

public:
  ObStoreCtx store_ctx_;
  ObTableAccessParam access_param_;
  ObBlockRowStore *block_row_store_;
  ObSSTableMultiVersionRowScanner scanner_;
};

TestMultiVersionDeleteInsertBlockscan::TestMultiVersionDeleteInsertBlockscan()
  : ObMultiVersionSSTableTest("test_multi_version_delete_insert_blockscan")
{
}

void TestMultiVersionDeleteInsertBlockscan::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestMultiVersionDeleteInsertBlockscan::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
}

void TestMultiVersionDeleteInsertBlockscan::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));
}

void TestMultiVersionDeleteInsertBlockscan::TearDownTestCase()
{
  clear_tx_data();
  ObMultiVersionSSTableTest::TearDownTestCase();
}

void TestMultiVersionDeleteInsertBlockscan::prepare_query_param(
    const ObVersionRange &version_range)
{
  context_.reset();
  store_ctx_.reset();
  ObLSID ls_id(ls_id_);
  access_param_.iter_param_.table_id_ = table_id_;
  access_param_.iter_param_.tablet_id_ = tablet_id_;
  access_param_.iter_param_.read_info_ = &full_read_info_;
  access_param_.iter_param_.out_cols_project_ = nullptr;
  access_param_.iter_param_.is_same_schema_column_ = true;
  access_param_.iter_param_.has_virtual_columns_ = false;
  access_param_.iter_param_.vectorized_enabled_ = false;
  access_param_.iter_param_.is_delete_insert_ = true;
  SCN snapshot_version;
  snapshot_version.convert_for_tx(version_range.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS,
            store_ctx_.init_for_read(ls_id,
                                     access_param_.iter_param_.tablet_id_,
                                     INT64_MAX, // query_expire_ts
                                     -1, // lock_timeout_us
                                     snapshot_version));
  ObQueryFlag query_flag(ObQueryFlag::NoOrder,
                         false, /*is daily merge scan*/
                         false, /*is read multiple macro block*/
                         false, /*sys task scan, read one macro block in single io*/
                         false /*full row scan flag, obsoleted*/,
                         false,/*index back*/
                         false); /*query_stat*/
  query_flag.set_not_use_row_cache();
  query_flag.set_not_use_block_cache();
  ASSERT_EQ(OB_SUCCESS,
            context_.init(query_flag,
                          store_ctx_,
                          allocator_,
                          allocator_,
                          version_range));
  context_.limit_param_ = nullptr;
}

void TestMultiVersionDeleteInsertBlockscan::prepare_block_row_store()
{
  void *buf = allocator_.alloc(sizeof(ObBlockRowStore));
  ASSERT_NE(nullptr, buf);
  block_row_store_ = new (buf) ObBlockRowStore(context_);
  ASSERT_EQ(OB_SUCCESS, block_row_store_->init(access_param_));
  context_.block_row_store_ = block_row_store_;
}

void TestMultiVersionDeleteInsertBlockscan::destroy_block_row_store()
{
  allocator_.free(block_row_store_);
  block_row_store_ = nullptr;
}

void TestMultiVersionDeleteInsertBlockscan::prepare_tx_data(const int64_t txn_cnt, const int64_t commit_cnt)
{
  ASSERT_LE(commit_cnt, txn_cnt);
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTxTable *tx_table = nullptr;
  ObTxTableGuard tx_table_guard;
  ls_handle.get_ls()->get_tx_table_guard(tx_table_guard);
  ASSERT_NE(nullptr, tx_table = tx_table_guard.get_tx_table());
  for (int64_t i = 1; i <= txn_cnt; i++) {
    ObTxData *tx_data = new ObTxData();
    transaction::ObTransID tx_id = i;
    tx_data->tx_id_ = tx_id;
    if (i >= commit_cnt) {
      tx_data->state_ = ObTxData::RUNNING;
    } else {
      tx_data->commit_version_.convert_for_tx(i * 10);
      tx_data->state_ = ObTxData::COMMIT;
    }
    OK(tx_table->insert(tx_data));
    delete tx_data;
  }
}

bool TestMultiVersionDeleteInsertBlockscan::is_bitmap_equal(ObCGBitmap *bitmap, const std::vector<bool> &bitmap_arr) 
{
  bool ret = true;
  const ObBitmap *inner_bitmap = bitmap->get_inner_bitmap();
  if (inner_bitmap->size() != bitmap_arr.size()) {
    ret = false;
  } else {
    for (int64_t idx = 0; idx < bitmap_arr.size(); ++idx) {
      if (inner_bitmap->test(idx) != bitmap_arr[idx]) {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

void TestMultiVersionDeleteInsertBlockscan::prepare_filtered_result(ObIMicroBlockRowScanner *micro_scanner, const std::vector<bool> &bitmap_arr)
{
  if (bitmap_arr.size() > 0) {
    void *buf = allocator_.alloc(sizeof(ObBitmap));
    ASSERT_NE(nullptr, buf);
    MOCK_FILTERED_BITMAP = new (buf) ObBitmap(allocator_);
    OK(MOCK_FILTERED_BITMAP->init(bitmap_arr.size(), false));
    for (int64_t idx = 0; idx < bitmap_arr.size(); ++idx) {
      if (bitmap_arr[idx]) {
        MOCK_FILTERED_BITMAP->set(idx, true);
      }
    }
    OK(micro_scanner->init_bitmap(micro_scanner->filter_bitmap_, true/*is_all_true*/));
    micro_scanner->use_private_bitmap_ = true;
    ObCGBitmap *bitmap = micro_scanner->filter_bitmap_;
    ASSERT_NE(nullptr, bitmap);
    OK(bitmap->bit_and(*MOCK_FILTERED_BITMAP));
    allocator_.free(MOCK_FILTERED_BITMAP);
    MOCK_FILTERED_BITMAP = nullptr;
    int ret = OB_SUCCESS;
  }
}

int TestMultiVersionDeleteInsertBlockscan::get_next_micro_scanner(ObIMicroBlockRowScanner *&micro_scanner)
{
  int ret = OB_SUCCESS;
  ObIndexTreeMultiPassPrefetcher<32, 3> &prefetcher = scanner_.prefetcher_;
  ObMicroBlockData block_data;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(prefetcher.prefetch())) {
      STORAGE_LOG(WARN, "fail to prefetch micro block", K(ret), K(prefetcher));
    } else if (prefetcher.cur_range_fetch_idx_ >= prefetcher.cur_range_prefetch_idx_)  {
      if (OB_LIKELY(prefetcher.is_prefetch_end_)) {
        ret = OB_ITER_END;
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    } else if (prefetcher.read_wait()) {
      continue;
    } else if (-1 == prefetcher.current_read_handle().micro_begin_idx_) {
      ret = OB_ITER_END;
      STORAGE_LOG(WARN, "scan empty read handle", K(ret), K(prefetcher.current_read_handle()));
    } else if (-1 == prefetcher.cur_micro_data_fetch_idx_
               && FALSE_IT(prefetcher.cur_micro_data_fetch_idx_ = prefetcher.current_read_handle().micro_begin_idx_)) {
    } else if (nullptr == scanner_.micro_scanner_) {
      if (OB_FAIL(scanner_.init_micro_scanner())) {
        STORAGE_LOG(WARN, "fail to init micro scanner", K(ret));
      } else {
        scanner_.micro_scanner_->reuse();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(scanner_.micro_scanner_->set_range(*prefetcher.current_read_handle().range_))) {
        STORAGE_LOG(WARN, "fail to set range for micro scanner", K(ret));
      } else if (OB_FAIL(prefetcher.current_micro_handle().get_micro_block_data(&scanner_.macro_block_reader_, block_data))) {
        STORAGE_LOG(WARN, "fail to get block data", K(ret), K(prefetcher.current_micro_handle()));
      } else if (OB_FAIL(scanner_.micro_scanner_->open(prefetcher.current_micro_handle().macro_block_id_,
                                                      block_data,
                                                      prefetcher.current_micro_info().is_left_border(),
                                                      prefetcher.current_micro_info().is_right_border()))) {
        STORAGE_LOG(WARN, "fail to open micro scanner and preprocess delete insert bitmap", K(ret), K(prefetcher));
      }
    }
    if (OB_SUCC(ret)) {
      micro_scanner = scanner_.micro_scanner_;
      ++prefetcher.cur_micro_data_fetch_idx_;
      break;
    } else if (OB_ITER_END == ret) {
      ++prefetcher.cur_range_fetch_idx_;
    }
  }
  return ret;
}

int TestMultiVersionDeleteInsertBlockscan::get_next_preprocessed_bitmap(ObCGBitmap *&bitmap)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;
  if (OB_FAIL(get_next_micro_scanner(micro_scanner))) {
    STORAGE_LOG(WARN, "fail to get next micro_scanner", K(ret));
  } else {
    bitmap = scanner_.micro_scanner_->di_bitmap_;
  }
  return ret;
}

int TestMultiVersionDeleteInsertBlockscan::generate_range(const int64_t start, const int64_t end, ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (start < 0) {
    range.start_key_.set_min_rowkey();
  } else {
    std::string start_str = "var" + std::to_string(start);
    range.start_key_.datum_cnt_ = SCHEMA_ROWKEY_CNT;
    void *buf = allocator_.alloc(sizeof(ObStorageDatum) * SCHEMA_ROWKEY_CNT);
    ObStorageDatum *datums = new (buf) ObStorageDatum[SCHEMA_ROWKEY_CNT];
    datums[0].set_int(start);
    datums[1].set_string(start_str.c_str(), start_str.length());
    range.start_key_.datums_ = datums;
    range.border_flag_.set_inclusive_start();
  }

  if (end < 0) {
    range.end_key_.set_max_rowkey();
  } else {
    std::string end_str = "var" + std::to_string(end);
    range.end_key_.datum_cnt_ = SCHEMA_ROWKEY_CNT;
    void *buf = allocator_.alloc(sizeof(ObStorageDatum) * SCHEMA_ROWKEY_CNT);
    ObStorageDatum *datums = new (buf) ObStorageDatum[SCHEMA_ROWKEY_CNT];
    datums[0].set_int(end);
    datums[1].set_string(end_str.c_str(), end_str.length());
    range.end_key_.datums_ = datums;
    range.border_flag_.set_inclusive_end();
  }
  return ret;
}

void TestMultiVersionDeleteInsertBlockscan::test_preprocess_bitmap_basic()
{
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  const int64_t snapshot_version = 20;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(20);
  ObSSTable *sstable = nullptr;
  ObCGBitmap *bitmap = nullptr;
  prepare_tx_data(5, 5);

  // case 1: insert1, delete2, insert2, delete3, insert3 -> insert3
  const char *micro_data[1];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 9       9     INSERT    NORMAL        CF\n"
      "1        var1  -20      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "1        var1  -10      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       0          1       1     INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr1 = {true, false, false, false, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr1));
  handle.reset();
  scanner_.reset();

  // case 2: insert1, delete2, insert2, delete3 -> delete2
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "1        var1  MIN      -100       9       1     DELETE    NORMAL        UCF                  trans_id_1\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL        UC                   trans_id_1\n"
      "1        var1  MIN      -79        1       1     DELETE    NORMAL        UC                   trans_id_1\n"
      "1        var1  MIN      -70        1       1     INSERT    NORMAL        UCL                  trans_id_1\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr2 = {false, false, true, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr2));
  handle.reset();
  scanner_.reset();

  // case 3: delete1, insert1, delete2, insert2, delete3, insert3 -> delete1, insert3
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 19      19    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "1        var1  -10      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -8       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "1        var1  -8       0          1       1     DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr3 = {true, false, false, false, false, true};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr3));
  handle.reset();
  scanner_.reset();

  // case 4: delete1, insert1, delete2, insert2, delete3 -> delete1
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "1        var1  MIN      -100       9       9     DELETE    NORMAL        UCF                  trans_id_1\n"
      "1        var1  MIN      -90        9       9     INSERT    NORMAL        UC                   trans_id_1\n"
      "1        var1  MIN      -89        9       1     DELETE    NORMAL        UC                   trans_id_1\n"
      "1        var1  MIN      -80        9       1     INSERT    NORMAL        UC                   trans_id_1\n"
      "1        var1  MIN      -79        1       1     DELETE    NORMAL        UCL                  trans_id_1\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr4 = {false, false, false, false, true};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr4));
  handle.reset();
  scanner_.reset();

  // case 5: multi key
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 9       9     INSERT    NORMAL        CF\n"
      "1        var1  -20      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "1        var1  -10      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       0          1       1     INSERT    NORMAL        CL\n"
      "2        var2  -20      DI_VERSION 19      19    INSERT    NORMAL        CF\n"
      "2        var2  -20      0          9       1     DELETE    NORMAL        C\n"
      "2        var2  -10      DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "2        var2  -10      0          9       1     DELETE    NORMAL        C\n"
      "2        var2  -8       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "2        var2  -8       0          1       1     DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr5 = {true, false, false, false, false, true, false, false, false, false, true};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr5));
  handle.reset();
  scanner_.reset();

  // case 6: shadow row
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      MIN        9       9     DELETE    INSERT_DELETE SCF\n"
      "1        var1  -10      0          9       9     DELETE    NORMAL        C\n"
      "1        var1  -5       DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "1        var1  -5       0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -4       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "1        var1  -4       0          1       1     DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr6 = {false, false, false, false, false, true};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr6));
  handle.reset();
  scanner_.reset();
  clear_tx_data();
}

void TestMultiVersionDeleteInsertBlockscan::test_preprocess_bitmap_version_range()
{
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 30;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(38);
  ObSSTable *sstable = nullptr;
  ObCGBitmap *bitmap = nullptr;

  const char *micro_data[1];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -38      DI_VERSION 29      29    INSERT    NORMAL        CF\n"
      "1        var1  -38      0          19      19    DELETE    NORMAL        C\n"
      "1        var1  -30      0          19      19    INSERT    NORMAL        C\n"
      "1        var1  -28      0          9       9     DELETE    NORMAL        C\n"
      "1        var1  -20      DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "1        var1  -20      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "1        var1  -10      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       0          1       1     INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  // case 1: push base version
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 10;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr1 = {true, false, false, false, false, true, false, false, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr1));
  scanner_.reset();
  // case 2: downgrade snapshot version
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = 28;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr2 = {false, false, false, false, false, false, false, true, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  OK(dynamic_cast<ObMultiVersionDIMicroBlockRowScanner*>(scanner_.micro_scanner_)->try_cache_unfinished_row(-1, -1));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr2));
  ASSERT_EQ(true, dynamic_cast<ObMultiVersionDIMicroBlockRowScanner*>(scanner_.micro_scanner_)->prev_micro_row_.row_flag_.is_not_exist());
  scanner_.reset();
  // case 3: both case1 and case 2
  trans_version_range.base_version_ = 20;
  trans_version_range.snapshot_version_ = 30;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr3 = {false, false, true, true, false, false, false, false, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  OK(dynamic_cast<ObMultiVersionDIMicroBlockRowScanner*>(scanner_.micro_scanner_)->try_cache_unfinished_row(-1, -1));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr3));
  ASSERT_EQ(true, dynamic_cast<ObMultiVersionDIMicroBlockRowScanner*>(scanner_.micro_scanner_)->prev_micro_row_.row_flag_.is_not_exist());
  scanner_.reset();
  handle.reset();
}

void TestMultiVersionDeleteInsertBlockscan::test_preprocess_bitmap_uncommitted()
{
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 120;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(100);
  ObSSTable *sstable = nullptr;
  ObCGBitmap *bitmap = nullptr;

  const char *micro_data[1];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "1        var1  MIN      -100        29      29    INSERT    NORMAL        UCF                 trans_id_1\n"
      "1        var1  MIN      -99         19      19    DELETE    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -80         19      19    INSERT    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -79         9       9     DELETE    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -70         9       9     INSERT    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -69         9       1     DELETE    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -60         9       1     INSERT    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -59         1       1     DELETE    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -50         1       1     INSERT    NORMAL        UCL                 trans_id_1\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));

  // case 1: all committed state, commit version out-of-range
  prepare_tx_data(5, 5);
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 10;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr1 = {false, false, false, false, false, false, false, false, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr1));
  scanner_.reset();
  clear_tx_data();

  // case 2: all visible committed state
  prepare_tx_data(5, 5);
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr2 = {true, false, false, false, false, false, false, false, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr2));
  scanner_.reset();
  clear_tx_data();
  handle.reset();

  // case 3: with running state
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "1        var1  MIN      -100        29      29    INSERT    NORMAL        UCF                 trans_id_3\n"
      "1        var1  MIN      -99         19      19    DELETE    NORMAL        UC                  trans_id_3\n"
      "1        var1  MIN      -80         19      19    INSERT    NORMAL        UC                  trans_id_3\n"
      "1        var1  MIN      -79         9       9     DELETE    NORMAL        UC                  trans_id_3\n"
      "1        var1  MIN      -70         9       9     INSERT    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -69         9       1     DELETE    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -60         9       1     INSERT    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -59         1       1     DELETE    NORMAL        UC                  trans_id_1\n"
      "1        var1  MIN      -50         1       1     INSERT    NORMAL        UCL                 trans_id_1\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  prepare_tx_data(5, 2);
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 20;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<bool> bitmap_arr3 = {false, false, false, false, true, false, false, false, false};
  OK(get_next_preprocessed_bitmap(bitmap));
  ASSERT_EQ(true, is_bitmap_equal(bitmap, bitmap_arr3));
  scanner_.reset();
  clear_tx_data();
  handle.reset();
}

void TestMultiVersionDeleteInsertBlockscan::test_get_next_compacted_row_basic()
{
  prepare_tx_data(5, 5);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 35;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(30);
  ObSSTable *sstable = nullptr;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  prepare_block_row_store();
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;

  const char *micro_data[10];
  // case 1: with insert row and delete version
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION 48      47    INSERT    NORMAL        C\n"
      "1        var1  -10      0          46      45    DELETE    NORMAL        CL\n";
  // case 2: with insert row and without delete version
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        var2  -20      DI_VERSION 44      43    INSERT    NORMAL        CF\n"
      "2        var2  -20      0          42      41    DELETE    NORMAL        C\n"
      "2        var2  -10      DI_VERSION 42      41    INSERT    NORMAL        CL\n"
      "3        var3  -20      DI_VERSION 40      39    INSERT    NORMAL        CLF\n";
  // case 3: with delete row 1
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "4        var4  MIN      -100       38      37    DELETE    NORMAL        UCF                 trans_id_2\n"
      "4        var4  -10      DI_VERSION 38      37    INSERT    NORMAL        CL                  trans_id_0\n";
  // case 4: with delete row 2
  micro_data[3] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "5        var5  -20      MIN        36      35    DELETE    INSERT_DELETE SCF                 trans_id_0\n"
      "5        var5  -20      0          36      35    DELETE    NORMAL        CL                  trans_id_0\n"
      "6        var6  MIN      -100       34      33    DELETE    NORMAL        UCFL                trans_id_2\n";
  // case 5: insert row and delete row from another rowkey
  micro_data[4] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "7        var7  -20      DI_VERSION 32      31    INSERT    NORMAL        CF\n"
      "7        var7  -20      0          30      29    DELETE    NORMAL        CL\n"
      "8        var8  -18      MIN        28      27    DELETE    INSERT_DELETE SCF\n"
      "8        var8  -18      0          28      27    DELETE    NORMAL        CL\n";
  // case 6: insert row and insert row from another rowkey
  micro_data[5] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "9        var9  -20      DI_VERSION 26      25    INSERT    NORMAL        CF\n"
      "9        var9  -20      0          24      23    DELETE    NORMAL        CL\n"
      "10       var10 -10      DI_VERSION 22      21    INSERT    NORMAL        CF\n"
      "10       var10 -10      0          20      19    DELETE    NORMAL        CL\n";
  // case 7: delete row and delete row from another rowkey
  micro_data[6] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "11       var11 MIN      -99        18      17    DELETE    NORMAL        UCFL                trans_id_2\n"
      "12       var12 -10      MIN        16      15    DELETE    INSERT_DELETE SCF                 trans_id_0\n"
      "12       var12 -10      0          16      15    DELETE    NORMAL        CL                  trans_id_0\n";
  // case 8: delete row and insert row from another rowkey
  micro_data[7] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "13       var13 -18      MIN        14      13    DELETE    INSERT_DELETE SCF\n"
      "13       var13 -18      0          14      13    DELETE    NORMAL        CL\n"
      "14       var14 -10      DI_VERSION 12      11    INSERT    NORMAL        CF\n"
      "14       var14 -10      0          10      9     DELETE    NORMAL        CL\n";
  // case 9: normal delete and insert rows
  micro_data[8] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "15       var15 -25      0           8     8      DELETE    NORMAL        CLF\n"
      "16       var17 -25      0           7     7      DELETE    NORMAL        CLF\n"
      "17       var16 -25      DI_VERSION  6     6      INSERT    NORMAL        CLF\n"
      "18       var17 -25      0           5     5      DELETE    NORMAL        CLF\n"
      "19       var18 -25      DI_VERSION  4     4      INSERT    NORMAL        CLF\n"
      "20       var18 -25      DI_VERSION  3     3      INSERT    NORMAL        CLF\n";
  // case 10: with filtered rowkey and version not fit rowkey
  micro_data[9] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "21       var19 -10      DI_VERSION  99    98     INSERT    NORMAL        CF\n"
      "21       var19 -10      0           97    96     DELETE    NORMAL        CL\n"
      "22       var20 -15      DI_VERSION  95    94     INSERT    NORMAL        CF\n"
      "22       var20 -15      0           93    92     DELETE    NORMAL        CL\n"
      "23       var20 -40      DI_VERSION  91    90     INSERT    NORMAL        CF\n"
      "23       var20 -40      0           89    88     DELETE    NORMAL        CL\n"
      "24       var21 -20      DI_VERSION  87    86     INSERT    NORMAL        CF\n"
      "24       var21 -20      0           85    84     DELETE    NORMAL        CL\n";
  
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 10);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results = {
    MultiVersionDIResult(3, 20, false, 10, false, 50), MultiVersionDIResult(3, 20, false, 0, false, 44), MultiVersionDIResult(3, 20, false, 0, false, 40), MultiVersionDIResult(4, 0, false, 20, false, 38),
    MultiVersionDIResult(4, 0, false, 20, false, 36), MultiVersionDIResult(4, 0, false, 20, false, 34), MultiVersionDIResult(3, 20, false, 20, false, 32),
    MultiVersionDIResult(4, 0, false, 18, false, 28), MultiVersionDIResult(3, 20, false, 20, false, 26), MultiVersionDIResult(3, 10, false, 10, false, 22),
    MultiVersionDIResult(4, 0, false, 20, false, 18), MultiVersionDIResult(4, 0, false, 10, false, 16), MultiVersionDIResult(4, 0, false, 18, false, 14),
    MultiVersionDIResult(3, 10, false, 10, false, 12), MultiVersionDIResult(4, 0, false, 25, false, 8), MultiVersionDIResult(4, 0, false, 25, false, 7),
    MultiVersionDIResult(3, 25, false, 0, false, 6), MultiVersionDIResult(4, 0, false, 25, false, 5), MultiVersionDIResult(3, 25, false, 0, false, 4),
    MultiVersionDIResult(3, 25, false, 0, false, 3), MultiVersionDIResult(3, 10, false, 10, false, 99), 
    MultiVersionDIResult(3, 15, true, 15, true, 95), MultiVersionDIResult(3, 40, true, 40, true, 91), MultiVersionDIResult(3, 20, false, 20, false, 87)
  };
  std::vector<std::vector<bool>> bitmap_arrays = {
    {true, true, true, true},
    {true, true, true, true},
    {true, true},
    {true, true, true},
    {true, true, true, true},
    {true, true, true, true},
    {true, true, true},
    {true, true, true, true},
    {true, true, true, true, true, true},
    {true, true, false, false, false, false, true, true}
  };
  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  clear_tx_data();
}

void TestMultiVersionDeleteInsertBlockscan::test_get_next_compacted_row_with_cached_row1()
{
  prepare_tx_data(5, 5);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 35;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(30);
  ObSSTable *sstable = nullptr;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 4;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  prepare_block_row_store();
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;

  // case 1: with cached insert row and invalid delete version
  const char *micro_data[3];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results = {
    MultiVersionDIResult(3, 20, false, 0, false, 50)
  };
  std::vector<std::vector<bool>> bitmap_arrays;
  bitmap_arrays = {
    {true, true}, {true}
  };

  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 1", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  
  // case 2: with cached insert row and an delete row with different rowkey
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -8       0          46      45    DELETE    NORMAL        CL\n"
      "2        var2  -10      MIN        44      43    DELETE    INSERT_DELETE SCF\n"
      "2        var2  -10      0          44      43    DELETE    NORMAL        L\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, false, 8, false, 50), MultiVersionDIResult(4, 0, false, 10, false, 44)
  };
  bitmap_arrays = {
    {true, true, true}, {true, true, true}
  };
  
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 2", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 3: with cached insert row and an insert row with different rowkey
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        CL\n"
      "2        var2  -20      0          46      45    INSERT    NORMAL        CLF\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, false, 0, false, 50), MultiVersionDIResult(3, 20, false, 0, false, 46)
  };
  bitmap_arrays = {
    {true, true}, {true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 3", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 4: with cached insert row and meet iter end
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, false, 0, false, 50)
  };
  bitmap_arrays = {
    {true, true}, {true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 4", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 5: with cached insert row and multi micro blocks
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      DI_VERSION 48      47    INSERT    NORMAL        C\n"
      "1        var1  -10      0          46      45    DELETE    NORMAL        C\n";
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -8       DI_VERSION 46      45    INSERT    NORMAL        C\n"
      "1        var1  -8       0          44      43    DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 3);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, false, 8, false, 50)
  };
  bitmap_arrays = {
    {true, true}, {true, true}, {true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 5", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 6: with cached delete row and output
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN      -100       48      47    DELETE    NORMAL        UCF                 trans_id_2\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  -2       DI_VERSION 48      47    INSERT    NORMAL        C                   trans_id_0\n"
      "1        var1  -2       0          46      45    DELETE    NORMAL        CL                  trans_id_0\n"
      "2        var2  MIN      -99        44      43    DELETE    NORMAL        UCF                 trans_id_2\n";
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  -2       DI_VERSION 44      43    INSERT    NORMAL        C                   trans_id_0\n"
      "2        var2  -2       0          42      41    DELETE    NORMAL        CL                  trans_id_0\n"
      "3        var3  MIN      -98        40      39    INSERT    NORMAL        UCFL                trans_id_2\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 3);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(4, 0, false, 20, false, 48), MultiVersionDIResult(4, 0, false, 20, false, 44), MultiVersionDIResult(3, 20, false, 0, false, 40)
  };
  bitmap_arrays = {
    {true}, {true, true, true}, {true, true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 6", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 7: with cached delete row and overwritten
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN      -100       48      47    DELETE    NORMAL        UCF                 trans_id_2\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  -12      DI_VERSION 48      47    INSERT    NORMAL        C                   trans_id_0\n"
      "1        var1  -12      0          46      45    DELETE    NORMAL        CL                  trans_id_0\n"
      "2        var2  MIN      -99        44      43    DELETE    NORMAL        UCF                 trans_id_2\n";
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "2        var2  -12      DI_VERSION 44      43    INSERT    NORMAL        C                   trans_id_0\n"
      "2        var2  -12      0          42      41    DELETE    NORMAL        CL                  trans_id_0\n"
      "3        var3  MIN      -98        40      39    INSERT    NORMAL        UCFL                trans_id_2\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 3);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(4, 0, false, 12, false, 46), MultiVersionDIResult(4, 0, false, 12, false, 42), MultiVersionDIResult(3, 20, false, 0, false, 40)
  };
  bitmap_arrays = {
    {true}, {true, true, true}, {true, true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 7", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 8: with cached delete row and meet iter end
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN      -100       48      47    DELETE    NORMAL        UCF                 trans_id_2\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -2       DI_VERSION 48      47    INSERT    NORMAL        C\n"
      "1        var1  -2       0          46      45    DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(4, 0, false, 20, false, 48)
  };
  bitmap_arrays = {
    {true}, {true, false}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 8", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 9: with cached delete row, multi micro blocks and outpur earliest delete row
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN      -100       48      47    DELETE    NORMAL        UCF                 trans_id_2\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -18      DI_VERSION 48      47    INSERT    NORMAL        C\n"
      "1        var1  -18      0          46      45    DELETE    NORMAL        C\n";
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -12      DI_VERSION 46      45    INSERT    NORMAL        C\n"
      "1        var1  -12      0          44      43    DELETE    NORMAL        C\n"
      "1        var1  -10      0          44      43    INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 3);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = { 
    MultiVersionDIResult(4, 0, false, 12, false, 44)
  };
  bitmap_arrays = {
    {true}, {true, true}, {true, true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 9", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 10: with cached insert row and valid delete version
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -2       0          48      47    INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = { 
    MultiVersionDIResult(3, 20, false, 20, false, 50)
  };
  bitmap_arrays = {
    {true, true}, {true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 10", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  clear_tx_data();
}

void TestMultiVersionDeleteInsertBlockscan::test_get_next_compacted_row_with_cached_row2()
{
  prepare_tx_data(5, 5);
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 35;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(30);
  ObSSTable *sstable = nullptr;
  const char *micro_data[13];

  // case 1: with insert row to cache, with delete version
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 9       9     INSERT    NORMAL        CF\n"
      "1        var1  -20      0          9       1     DELETE    NORMAL        C\n"
      "1        var1  -10      DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "1        var1  -10      0          1       1     DELETE    NORMAL        C\n"
      "1        var1  -8       0          1       1     INSERT    NORMAL        CL\n"
      "2        var2  -20      DI_VERSION 19      19    INSERT    NORMAL        CF\n"
      "2        var2  -20      0          9       1     DELETE    NORMAL        C\n";
  // case 2: with delete row to cache and shadow row
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "2        var2  -10      DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "2        var2  -10      0          9       1     DELETE    NORMAL        C\n"
      "2        var2  -8       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "2        var2  -8       0          1       1     DELETE    NORMAL        CL\n"
      "3        var3  -10      MIN        9       9     DELETE    INSERT_DELETE SCF\n"
      "3        var3  -10      0          9       9     DELETE    NORMAL        C\n";
  // case 3: finish scanning current rowkey with out-of-range base version
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "3        var3  -8       DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "3        var3  -8       0          9       1     DELETE    NORMAL        C\n"
      "3        var3  -6       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "3        var3  -6       0          1       1     DELETE    NORMAL        CL\n"
      "4        var4  -20      DI_VERSION 19      19    INSERT    NORMAL        CF\n"
      "4        var4  -20      0          9       9     DELETE    NORMAL        C\n"
      "4        var4  -4       DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "4        var4  -4       0          9       1     DELETE    NORMAL        C\n";
  // case 4: with delete row to cache
  micro_data[3] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "4        var4  -2       DI_VERSION 9       9     INSERT    NORMAL        C                    trans_id_0\n"
      "4        var4  -2       0          9       1     DELETE    NORMAL        CL                   trans_id_0\n"
      "5        var5  MIN      -100       19      19    DELETE    NORMAL        UCF                  trans_id_2\n"
      "5        var5  -18      DI_VERSION 19      19    INSERT    NORMAL        C                    trans_id_0\n"
      "5        var5  -18      0          19      9     DELETE    NORMAL        C                    trans_id_0\n";
  // case 5: with insert row to cache, without delete version
  micro_data[4] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "5        var5  -8       DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "5        var5  -8       0          9       1     DELETE    NORMAL        C\n"
      "5        var5  -6       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "5        var5  -6       0          1       1     DELETE    NORMAL        CL\n"
      "6        var6  -20      DI_VERSION 29      29    INSERT    NORMAL        CF\n"
      "6        var6  -20      0          19      19    DELETE    NORMAL        C\n"
      "6        var6  -18      0          19      19    INSERT    NORMAL        C\n";
  // case 6: with insert row to cache, with uncommitted delete version
  micro_data[5] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag   trans_id\n"
      "6        var6  -15      0          9       9     DELETE    NORMAL        C                    trans_id_0\n"
      "6        var6  -8       DI_VERSION 9       9     INSERT    NORMAL        C                    trans_id_0\n"
      "6        var6  -8       0          9       1     DELETE    NORMAL        C                    trans_id_0\n"
      "6        var6  -6       0          9       1     INSERT    NORMAL        CL                   trans_id_0\n"
      "7        var7  MIN      -99        19      19    INSERT    NORMAL        UCF                  trans_id_2\n"
      "7        var7  MIN      -98        19      19    DELETE    NORMAL        UC                   trans_id_2\n";
  // case 7: with out-of-range snapshot version, without row to cache
  micro_data[6] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "7        var7  -14      0          9       9     INSERT    NORMAL        C\n"
      "7        var7  -12      0          6       6     DELETE    NORMAL        CL\n"
      "8        var8  -30      DI_VERSION 29      29    INSERT    NORMAL        CF\n"
      "8        var8  -30      0          19      19    DELETE    NORMAL        C\n"
      "8        var8  -28      DI_VERSION 19      19    INSERT    NORMAL        C\n"
      "8        var8  -28      0          19      9     DELETE    NORMAL        C\n";
  // case 8: with cached insert row across multiple micro blocks, with dynamic updated delete version
  micro_data[7] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "8        var8  -20       DI_VERSION 29      29    INSERT    NORMAL        C\n"
      "8        var8  -20       0          19      19    DELETE    NORMAL        CL\n"
      "9        var9  -18       DI_VERSION 19      19    INSERT    NORMAL        CF\n"
      "9        var9  -18       0          9       9     DELETE    NORMAL        C\n";
  micro_data[8] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "9        var9  -16       DI_VERSION 9       9     INSERT    NORMAL        C\n"
      "9        var9  -16       0          9       1     DELETE    NORMAL        C\n"
      "9        var9  -14       0          9       1     INSERT    NORMAL        C\n"
      "9        var9  -12       0          4       1     DELETE    NORMAL        C\n";
  micro_data[9] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "9        var9  -10       DI_VERSION 4       1     INSERT    NORMAL        C\n"
      "9        var9  -10       0          1       1     DELETE    NORMAL        CL\n";
  // case 9: with cached delete row across multiple micro blocks
  micro_data[10] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag    trans_id\n"
      "10       var10 MIN       -100       19      19    DELETE    NORMAL        UCF                  trans_id_2\n";
  micro_data[11] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       var10 -16       DI_VERSION 19      19    INSERT    NORMAL        C\n"
      "10       var10 -16       0          9       1     DELETE    NORMAL        C\n";
  micro_data[12] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "10       var10 -10       DI_VERSION 9       1     INSERT    NORMAL        C\n"
      "10       var10 -10       0          1       1     DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 13);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 5;
  trans_version_range.snapshot_version_ = 20;
  prepare_query_param(trans_version_range);
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results;
  std::vector<std::vector<bool>> bitmap_arrays;

  results = { 
    MultiVersionDIResult(3, 20, false, 0, false, 9), MultiVersionDIResult(3, 20, false, 8, false, 19), MultiVersionDIResult(4, 0, false, 6, false, 1), 
    MultiVersionDIResult(3, 20, false, 20, false, 19), MultiVersionDIResult(4, 0, false, 6, false, 1), MultiVersionDIResult(3, 20, false, 0, false, 29), 
    MultiVersionDIResult(3, 20, false, 12, false, 19), MultiVersionDIResult(3, 20, false, 20, false, 29), MultiVersionDIResult(3, 18, false, 10, false, 19), 
    MultiVersionDIResult(4, 0, false, 10, false, 1)
  };
  bitmap_arrays = {
    {true, true, true, true, true, true, true},
    {true, true, true, true, true, true},
    {true, true, true, true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true, true, true},
    {true, true, true, true, true, true},
    {true, true, true, true, true, true},
    {true, true, true, true},
    {true, true, true, true},
    {true, true},
    {true},
    {true, true},
    {true, true}
  };
  int ret = OB_SUCCESS;
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;
  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for cached row 2", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  clear_tx_data();
}

void TestMultiVersionDeleteInsertBlockscan::test_get_next_compacted_row_with_filtered_row()
{
  prepare_tx_data(5, 5);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 35;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(30);
  ObSSTable *sstable = nullptr;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  prepare_block_row_store();
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;

  // case 1: with cached insert row filtered and delete version
  const char *micro_data[3];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      DI_VERSION 48      47    INSERT    NORMAL        C\n"
      "1        var1  -10      0          46      45    DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results = {
    MultiVersionDIResult(3, 20, true, 10, false, 50)
  };
  std::vector<std::vector<bool>> bitmap_arrays = {
    {false, false}, {true, true}
  };
  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 1", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  
  // case 2: with cached insert row filtered and a delete row with different rowkey
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        CL\n"
      "2        var2  -10      MIN        46      45    DELETE    INSERT_DELETE SCF\n"
      "2        var2  -10      0          46      45    DELETE    NORMAL        L\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, true, 0, false, 50), MultiVersionDIResult(4, 0, false, 10, false, 46)
  };
  bitmap_arrays = {
    {false, false}, {true, true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 2", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 3: with cached insert row filtered and an insert row with different rowkey
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        CL\n"
      "2        var2  -20      0          46      45    INSERT    NORMAL        CLF\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, true, 0, false, 50), MultiVersionDIResult(3, 20, false, 0, false, 46)
  };
  bitmap_arrays = {
    {false, false}, {true, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 3", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 4: with cached insert row filtered and meet iter end
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      0          48      47    INSERT    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, true, 0, false, 50)
  };
  bitmap_arrays = {
    {false, false}, {true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 4", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();

  // case 5: with cached insert row filtered and multi micro blocks
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      DI_VERSION 50      49    INSERT    NORMAL        CF\n"
      "1        var1  -20      0          48      47    DELETE    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -10      DI_VERSION 48      47    INSERT    NORMAL        C\n"
      "1        var1  -10      0          46      45    DELETE    NORMAL        C\n";
  micro_data[2] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -8       DI_VERSION 44      43    INSERT    NORMAL        C\n"
      "1        var1  -8       0          42      41    DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 3);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  results = {
    MultiVersionDIResult(3, 20, true, 8, false, 50)
  };
  bitmap_arrays = {
    {false, false}, {false, false}, {false, true}
  };
  res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for case 5", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  clear_tx_data();
}

void TestMultiVersionDeleteInsertBlockscan::test_meet_empty_range_block()
{
  prepare_tx_data(5, 5);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObDatumRange range;
  OK(generate_range(1, 5, range));
  const int64_t snapshot_version = 35;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(30);
  ObSSTable *sstable = nullptr;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 4;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  prepare_block_row_store();
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;
  
  const char *micro_data[2];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -20      0          50      49    INSERT    NORMAL        CLF\n"
      "2        var2  -20      0          48      47    INSERT    NORMAL        CLF\n"
      "3        var3  -20      0          46      45    INSERT    NORMAL        CLF\n"
      "4        var4  -20      0          44      43    INSERT    NORMAL        CLF\n"
      "5        var5  -20      DI_VERSION 42      41    INSERT    NORMAL        CF\n"
      "5        var5  -20      0          2       1     DELETE    NORMAL        CL\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "6        var6  -20      0          40      39    INSERT    NORMAL        CLF\n"
      "7        var7  -20      DI_VERSION 38      37    INSERT    NORMAL        CF\n"
      "7        var7  -20      0          4       3     DELETE    NORMAL        CL\n";
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results = {
    MultiVersionDIResult(3, 20, false, 0, false, 50), MultiVersionDIResult(3, 20, false, 0, false, 48), MultiVersionDIResult(3, 20, false, 0, false, 46),
    MultiVersionDIResult(3, 20, false, 0, false, 44), MultiVersionDIResult(3, 20, false, 20, false, 42)
  };
  std::vector<std::vector<bool>> bitmap_arrays = {
    {true, true, true, true, true, true}, {}
  };
  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for empty range block", K(res_iter), K(results[res_iter]), K(result));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;                 
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  if (nullptr != range.start_key_.datums_) {
    allocator_.free(range.start_key_.datums_);
  }
  if (nullptr != range.end_key_.datums_) {
    allocator_.free(range.end_key_.datums_);
  }
  clear_tx_data();
}

void TestMultiVersionDeleteInsertBlockscan::test_shadow_row_output()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 50;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(30);
  scn_range.end_scn_.convert_for_gts(50);
  ObSSTable *sstable = nullptr;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  prepare_block_row_store();
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;
  const char *micro_data[2];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -50      MIN        50      50    INSERT    INSERT_DELETE SCF\n"
      "1        var1  -50      DI_VERSION 50      50    INSERT    NORMAL        C\n"
      "1        var1  -50      0          49      49    DELETE    NORMAL        C\n"
      "1        var1  -49      DI_VERSION 49      49    INSERT    NORMAL        C\n"
      "1        var1  -49      0          48      48    DELETE    NORMAL        C\n"
      "1        var1  -48      DI_VERSION 48      48    INSERT    NORMAL        C\n"
      "1        var1  -48      0          47      47    DELETE    NORMAL        C\n"
      "1        var1  -47      DI_VERSION 47      47    INSERT    NORMAL        C\n"
      "1        var1  -47      0          46      46    DELETE    NORMAL        C\n"
      "1        var1  -46      DI_VERSION 46      46    INSERT    NORMAL        C\n"
      "1        var1  -46      0          45      45    DELETE    NORMAL        C\n"
      "1        var1  -45      DI_VERSION 45      45    INSERT    NORMAL        C\n"
      "1        var1  -45      0          44      44    DELETE    NORMAL        C\n"
      "1        var1  -44      DI_VERSION 44      44    INSERT    NORMAL        C\n";
  micro_data[1] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag\n"
      "1        var1  -44      0          43      43    DELETE    NORMAL        C\n"
      "1        var1  -43      DI_VERSION 43      43    INSERT    NORMAL        C\n"
      "1        var1  -43      0          42      42    DELETE    NORMAL        C\n"
      "1        var1  -42      DI_VERSION 42      42    INSERT    NORMAL        C\n"
      "1        var1  -42      0          41      41    DELETE    NORMAL        C\n"
      "1        var1  -41      DI_VERSION 41      41    INSERT    NORMAL        C\n"
      "1        var1  -41      0          40      40    DELETE    NORMAL        C\n"
      "1        var1  -40      DI_VERSION 40      40    INSERT    NORMAL        C\n"
      "1        var1  -40      0          39      39    DELETE    NORMAL        C\n"
      "1        var1  -39      DI_VERSION 39      39    INSERT    NORMAL        C\n"
      "1        var1  -39      0          38      38    DELETE    NORMAL        C\n"
      "1        var1  -38      DI_VERSION 37      37    INSERT    NORMAL        C\n"
      "1        var1  -38      0          37      37    DELETE    NORMAL        C\n"
      "1        var1  -37      DI_VERSION 36      36    INSERT    NORMAL        C\n"
      "1        var1  -37      0          36      36    DELETE    NORMAL        C\n"
      "1        var1  -36      DI_VERSION 35      35    INSERT    NORMAL        C\n"
      "1        var1  -36      0          35      35    DELETE    NORMAL        C\n"
      "1        var1  -35      DI_VERSION 34      34    INSERT    NORMAL        C\n"
      "1        var1  -35      0          34      34    DELETE    NORMAL        C\n"
      "1        var1  -34      DI_VERSION 33      33    INSERT    NORMAL        C\n"
      "1        var1  -34      0          33      33    DELETE    NORMAL        C\n"
      "1        var1  -33      DI_VERSION 32      32    INSERT    NORMAL        C\n"
      "1        var1  -33      0          32      32    DELETE    NORMAL        C\n"
      "1        var1  -32      DI_VERSION 31      31    INSERT    NORMAL        C\n"
      "1        var1  -32      0          31      31    DELETE    NORMAL        C\n"
      "1        var1  -31      DI_VERSION 31      31    INSERT    NORMAL        CL\n";

  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 2);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results = { MultiVersionDIResult(131, 50, false, 0, false, 50) };
  std::vector<std::vector<bool>> bitmap_arrays = {
    {true, true, true, true, true, true, true, true, true, true, true, true, true, true}, 
    {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true}
  };
  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for empty range block", K(res_iter), K(results[res_iter]), K(result), KPC(row));
        ASSERT_EQ(results[res_iter], result);
        res_iter += 1;                 
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
}

void TestMultiVersionDeleteInsertBlockscan::test_with_uncommitted_lock_row()
{
  prepare_tx_data(5, 5);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObDatumRange range;
  range.set_whole_range();
  const int64_t snapshot_version = 35;
  ObScnRange scn_range;
  scn_range.start_scn_.convert_for_gts(1);
  scn_range.end_scn_.convert_for_gts(30);
  ObSSTable *sstable = nullptr;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = INT64_MAX;
  prepare_query_param(trans_version_range);
  prepare_block_row_store();
  const ObDatumRow *row = nullptr;
  ObIMicroBlockRowScanner *micro_scanner = nullptr;
  
  const char *micro_data[1];
  micro_data[0] = 
      "bigint   var   bigint  bigint     bigint bigint  flag     flag_type  multi_version_row_flag  trans_id\n"
      "1        var1  MIN      -20        50      49    INSERT    NORMAL        FU                trans_id_1\n"
      "1        var1  MIN      -19        48      47    DELETE    NORMAL        U                 trans_id_1\n"
      "1        var1  MIN      -18        NOP     NOP   LOCK      NORMAL        LU                trans_id_1\n"
      "2        var2  -20      DI_VERSION 44      43    INSERT    NORMAL        CF                trans_id_0\n"
      "2        var2  -20      0          42      41    DELETE    NORMAL        CL                trans_id_0\n";
  
  prepare_table_schema(micro_data, SCHEMA_ROWKEY_CNT, scn_range, snapshot_version, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle);
  OK(handle.get_sstable(sstable));
  OK(scanner_.init(access_param_.iter_param_, context_, sstable, &range));
  std::vector<MultiVersionDIResult> results = {
    MultiVersionDIResult(3, 10, false, 10, false, 50), MultiVersionDIResult(3, 20, false, 20, false, 44)
  };
  std::vector<std::vector<bool>> bitmap_arrays = {
    {true, true, true, true, true}
  };
  int64_t res_iter = 0;
  for (int64_t index = 0; index <= bitmap_arrays.size() - 1; index += 1) {
    OK(get_next_micro_scanner(micro_scanner));
    prepare_filtered_result(micro_scanner, bitmap_arrays[index]);
    while (OB_SUCC(ret)) {
      if (OB_FAIL(micro_scanner->inner_get_next_row(row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        MultiVersionDIResult result(row->row_flag_.get_serialize_flag(),
                                    row->insert_version_,
                                    row->is_insert_filtered_,
                                    row->delete_version_,
                                    row->is_delete_filtered_,
                                    row->storage_datums_[4].get_int());
        STORAGE_LOG(INFO, "check multi version di result for uncommitted lock row", K(res_iter), K(results[res_iter]), K(result));
        EXPECT_EQ(results[res_iter], result);
        res_iter += 1;                 
      }
    }
    ret = OB_SUCCESS;
  }
  ASSERT_EQ(results.size(), res_iter);
  scanner_.reset();
  handle.reset();
  clear_tx_data();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_preprocess_bitmap_basic)
{
  test_preprocess_bitmap_basic();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_preprocess_bitmap_version_range)
{
  test_preprocess_bitmap_version_range();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_preprocess_bitmap_uncommitted)
{
  test_preprocess_bitmap_uncommitted();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_get_next_compacted_row_basic)
{
  test_get_next_compacted_row_basic();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_get_next_compacted_row_with_cached_row1)
{
  test_get_next_compacted_row_with_cached_row1();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_get_next_compacted_row_with_cached_row2)
{
  test_get_next_compacted_row_with_cached_row2();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_get_next_compacted_row_with_filtered_row)
{
  test_get_next_compacted_row_with_filtered_row();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_meet_empty_range_block)
{
  test_meet_empty_range_block();
}

TEST_F(TestMultiVersionDeleteInsertBlockscan, test_shadow_row_output)
{
  test_shadow_row_output();
}

// always time out
TEST_F(TestMultiVersionDeleteInsertBlockscan, DISABLED_test_with_uncommitted_lock_row)
{
  test_with_uncommitted_lock_row();
}
}
}

int main(int argc, char **argv)
{
  system("rm -rf test_multi_version_delete_insert_blockscan.log*");
  OB_LOGGER.set_file_name("test_multi_version_delete_insert_blockscan.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
