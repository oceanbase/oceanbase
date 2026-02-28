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

#define private public
#define protected public

#include "storage/compaction/ob_partition_merger.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/test_tablet_helper.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/access/ob_table_estimator.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace rpc::frame;
using namespace memtable;
namespace storage
{

static ObArenaAllocator global_allocator_;

class TestIndexSSTableEstimator: public ObMultiVersionSSTableTest
{
public:
  static const int64_t MAX_MACRO_CNT = 20;

  TestIndexSSTableEstimator() : ObMultiVersionSSTableTest("test_index_sstable_estimator") {}

  virtual ~TestIndexSSTableEstimator() = default;

  void SetUp() { ObMultiVersionSSTableTest::SetUp(); }

  void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    TRANS_LOG(INFO, "teardown success");
  }

  static void SetUpTestCase();
  static void TearDownTestCase();

  void prepare_sstable_handle(ObTableHandleV2 &handle,
                              const int64_t macro_idx_start,
                              const int64_t macro_cnt,
                              const int64_t snapshot_version,
                              const ObITable::TableType table_type = ObITable::MINOR_SSTABLE,
                              const int64_t micro_cnt = 1);

  void prepare_comprehensive_test_data(ObTableHandleV2 &handle,
                                       const int64_t snapshot_version,
                                       const int64_t base_version);

  void build_datum_range(ObDatumRange &datum_range, const char *rowkey_data);

  void get_tablet_handle(ObTabletHandle &tablet_handle);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObMockIterator range_iter_;
};

void TestIndexSSTableEstimator::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // Mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // Create tablet
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ls_id, tablet_id, create_tablet_arg, 1, &table_schema));

  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, ObMultiVersionSSTableTest::allocator_));
}

void TestIndexSSTableEstimator::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // Reset sequence no
  ObClockGenerator::destroy();
}

void TestIndexSSTableEstimator::get_tablet_handle(ObTabletHandle &tablet_handle)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  int ret = OB_SUCCESS;
  ret = ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestIndexSSTableEstimator::build_datum_range(ObDatumRange &datum_range, const char *rowkey_data)
{
  const ObStoreRow *row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  ASSERT_TRUE(nullptr != rowkey_data);
  range_iter_.reset();
  OK(range_iter_.from(rowkey_data));
  ASSERT_TRUE(range_iter_.count() >= 2);

  ObStoreRange store_range;
  OK(range_iter_.get_row(0, row));
  ASSERT_TRUE(nullptr != row);
  store_range.get_start_key().assign(row->row_val_.cells_, rowkey_col_cnt);
  OK(range_iter_.get_row(1, row));
  ASSERT_TRUE(nullptr != row);
  store_range.get_end_key().assign(row->row_val_.cells_, rowkey_col_cnt);
  store_range.set_left_closed();
  store_range.set_right_closed();

  ASSERT_EQ(OB_SUCCESS, datum_range.from_range(store_range, allocator_));
}

void TestIndexSSTableEstimator::prepare_sstable_handle(ObTableHandleV2 &handle,
                                                       const int64_t macro_idx_start,
                                                       const int64_t macro_cnt,
                                                       const int64_t snapshot_version,
                                                       const ObITable::TableType table_type,
                                                       const int64_t micro_cnt)
{
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(true, macro_idx_start + macro_cnt <= MAX_MACRO_CNT);

  // Create data with different trans versions
  // Each macro block contains different ranges of trans versions
  // Filter logic: only blocks that can be completely filtered by base_version will be filtered (max_snapshot <= base_version)
  // Other cases are calculated at 100%, no partial filtering
  // macro 0-2: trans version 5-10 (max=10 > base_version=3, will not be filtered, calculated at 100%)
  // macro 3-5: trans version 1-3 (max=3 <= base_version=3, will be completely filtered)
  // macro 6-8: trans version 2-5 (max=5 > base_version=3, will not be filtered, calculated at 100%)
  const char *macro_data[20];

  // macro 0-2: trans version 5-10 (all retained)
  macro_data[0] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1    -5    0     EXIST   CLF\n"
      "1        var1    -6    0     EXIST   CLF\n";
  macro_data[1] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "2        var1    -7    0     EXIST   CLF\n"
      "3        var1    -8    0     EXIST   CLF\n";
  macro_data[2] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "4        var1    -9    0     EXIST   CLF\n"
      "5        var1    -10    0     EXIST   CLF\n";

  // macro 3-5: trans version 1-3 (all filtered)
  macro_data[3] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "6        var1    -1    0     EXIST   CLF\n"
      "7        var1    -2    0     EXIST   CLF\n";
  macro_data[4] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "8        var1    -2    0     EXIST   CLF\n"
      "9        var1    -3    0     EXIST   CLF\n";
  macro_data[5] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "10       var1    -3    0     EXIST   CLF\n"
      "11       var1    -3    0     EXIST   CLF\n";

  // macro 6-8: trans version 2-5 (will not be filtered, calculated at 100%)
  // macro 6: trans version 2-4, min=2, max=4, base_version=3
  //   max (4) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  macro_data[6] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "12       var1    -2    0     EXIST   CLF\n"
      "13       var1    -4    0     EXIST   CLF\n";
  // macro 7: trans version 4-5, min=4, max=5, base_version=3
  //   max (5) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  macro_data[7] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "14       var1    -4    0     EXIST   CLF\n"
      "15       var1    -5    0     EXIST   CLF\n";
  // macro 8: trans version 1-4, min=1, max=4, base_version=3
  //   max (4) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  macro_data[8] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "16       var1    -1    0     EXIST   CLF\n"
      "17       var1    -4    0     EXIST   CLF\n";

  macro_data[9] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "18       var1    -5    0     EXIST   CLF\n"
      "19       var1    -6    0     EXIST   CLF\n";

  // macro 10: Multi-version row data for testing logical row estimation
  // Note: Version comparison uses absolute value: abs(-7)=7 > abs(-6)=6 > abs(-5)=5
  // Storage order: from old to new (trans_version -7, -6, -5)
  // SCF marks the oldest version (largest absolute value), L marks the newest version (smallest absolute value)
  // rowkey=0: delete (trans_version=-7, SCF) + update (trans_version=-6, C) + insert (trans_version=-5, L)
  // rowkey=1: update (trans_version=-6, SCF) + insert (trans_version=-5, L)
  // rowkey=2: insert (trans_version=-5, CLF) - single version row
  macro_data[10] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1    -7    MIN   EXIST   SCF\n"      // oldest version (abs=7, trans_version=-7)
      "0        var1    -6    0     EXIST   C\n"       // middle version (abs=6, trans_version=-6)
      "0        var1    -5    0     DELETE  L\n"      // newest version (abs=5, trans_version=-5)
      "1        var1    -6    MIN   EXIST   SCF\n"      // oldest version (abs=6, trans_version=-6)
      "1        var1    -5    0     EXIST   L\n"      // newest version (abs=5, trans_version=-5)
      "2        var1    -5    0     EXIST   CLF\n";   // single version (trans_version=-5)

  // macro 11: For testing leaf_node_traversal, need range to hit the middle of micro block
  macro_data[11] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "20       var1    -5    0     EXIST   CLF\n"
      "21       var1    -6    0     EXIST   CLF\n";

  int schema_rowkey_cnt = 2;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  if (!table_schema_.is_valid()) {
    prepare_table_schema(macro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  }
  reset_writer(snapshot_version);
  data_iter_cursor_ = 0;
  for (int64_t i = 0; i < MAX_MICRO_BLOCK_CNT; i++) {
    data_iter_[i].reset();
  }
  OK(data_iter_[0].from(macro_data[macro_idx_start]));
  for (int64_t i = macro_idx_start; i < macro_idx_start + macro_cnt * micro_cnt; i += micro_cnt) {
    prepare_one_macro(&macro_data[i], micro_cnt);
  }
  prepare_data_end(handle, table_type);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  sstable->meta_->basic_meta_.occupy_size_ = (2<<20) * macro_cnt;
  sstable->key_.table_type_ = table_type;

  // For inc_major sstable, set upper_trans_version
  if (table_type == ObITable::INC_MAJOR_SSTABLE) {
    sstable->meta_->basic_meta_.set_upper_trans_version(10);
    sstable->meta_cache_.set_upper_trans_version(10);
  }
}

// Prepare test data for comprehensive test: 5 macros, each with 3 micros
// Each micro contains 4-6 physical rows, 2-3 logical rows
// One micro is completely filtered by base_version, others are partially filtered
void TestIndexSSTableEstimator::prepare_comprehensive_test_data(ObTableHandleV2 &handle,
                                                                 const int64_t snapshot_version,
                                                                 const int64_t base_version)
{
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();

  // 5 macros, each with 3 micros, total 15 micros
  // base_version = 5, so trans_version <= 5 will be completely filtered
  // trans_version > 5 will not be filtered (calculated at 100%)
  const char *macro_data[15];

  // Macro 0: 3 micros, all partially filtered (trans_version > 5)
  // Micro 0: rowkey 0-2, 3 logical rows (rowkey 0 has 2 versions, rowkey 1-2 have 1 version each)
  macro_data[0] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1    -7    MIN   EXIST   SCF\n"     // rowkey 0, oldest version (abs=7, trans_version=-7)
      "0        var1    -6    0     EXIST   L\n"      // rowkey 0, newest version (abs=6, trans_version=-6)
      "1        var1    -6    0     EXIST   CLF\n"    // rowkey 1, single version (trans_version=-6)
      "2        var1    -8    0     EXIST   CLF\n";    // rowkey 2, single version (trans_version=-8)

  // Micro 1: rowkey 3-5, 3 logical rows
  macro_data[1] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "3        var1    -9    MIN   EXIST   SCF\n"      // rowkey 3, oldest version (abs=9, trans_version=-9)
      "3        var1    -7    0     EXIST   L\n"      // rowkey 3, newest version (abs=7, trans_version=-7)
      "4        var1    -6    0     EXIST   CLF\n"    // rowkey 4, single version (trans_version=-6)
      "5        var1    -8    MIN   EXIST   SCF\n"      // rowkey 5, oldest version (abs=8, trans_version=-8)
      "5        var1    -7    0     EXIST   L\n";    // rowkey 5, newest version (abs=7, trans_version=-7)

  // Micro 2: rowkey 6-9, 4 logical rows (4 physical rows)
  macro_data[2] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "6        var1    -6    0     EXIST   CLF\n"    // rowkey 6, single version (trans_version=6)
      "7        var1    -7    0     EXIST   CLF\n"    // rowkey 7, single version (trans_version=7)
      "8        var1    -8    0     EXIST   CLF\n"     // rowkey 8, single version (trans_version=8)
      "9        var1    -6    0     EXIST   CLF\n";   // rowkey 9, single version (trans_version=6)

  // Macro 1: 3 micros, all partially filtered (trans_version > 5)
  // Micro 0: rowkey 10-12, 3 logical rows (5 physical rows)
  macro_data[3] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "10       var1    -7    MIN   EXIST   SCF\n"      // rowkey 10, oldest version (abs=7, trans_version=-7)
      "10       var1    -6    0     EXIST   L\n"      // rowkey 10, newest version (abs=6, trans_version=-6)
      "11       var1    -6    0     EXIST   CLF\n"    // rowkey 11, single version (trans_version=-6)
      "12       var1    -9    MIN   EXIST   SCF\n"      // rowkey 12, oldest version (abs=9, trans_version=-9)
      "12       var1    -7    0     EXIST   L\n";    // rowkey 12, newest version (abs=7, trans_version=-7)

  // Micro 1: rowkey 13-16, 4 logical rows (4 physical rows)
  macro_data[4] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "13       var1    -6    0     EXIST   CLF\n"    // rowkey 13, single version (trans_version=6)
      "14       var1    -7    0     EXIST   CLF\n"    // rowkey 14, single version (trans_version=7)
      "15       var1    -8    0     EXIST   CLF\n"    // rowkey 15, single version (trans_version=8)
      "16       var1    -6    0     EXIST   CLF\n";   // rowkey 16, single version (trans_version=6)

  // Micro 2: rowkey 17-19, 3 logical rows (5 physical rows)
  macro_data[5] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "17       var1    -7    MIN   EXIST   SCF\n"      // rowkey 17, oldest version (abs=7, trans_version=-7)
      "17       var1    -6    0     EXIST   L\n"      // rowkey 17, newest version (abs=6, trans_version=-6)
      "18       var1    -6    0     EXIST   CLF\n"    // rowkey 18, single version (trans_version=-6)
      "19       var1    -8    MIN   EXIST   SCF\n"      // rowkey 19, oldest version (abs=8, trans_version=-8)
      "19       var1    -7    0     EXIST   L\n";    // rowkey 19, newest version (abs=7, trans_version=-7)

  // Macro 2: 3 micros, one completely filtered (trans_version <= 5), others partially filtered
  // Micro 0: rowkey 20-23, completely filtered (trans_version <= 5), 4 logical rows (6 physical rows)
  macro_data[6] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "20       var1    -4    MIN   EXIST   SCF\n"      // rowkey 20, oldest version (abs=4, trans_version=-4)
      "20       var1    -3    0     EXIST   L\n"      // rowkey 20, newest version (abs=3, trans_version=-3)
      "21       var1    -3    0     EXIST   CLF\n"    // rowkey 21, single version (trans_version=-3)
      "22       var1    -5    MIN   EXIST   SCF\n"      // rowkey 22, oldest version (abs=5, trans_version=-5)
      "22       var1    -4    0     EXIST   L\n"      // rowkey 22, newest version (abs=4, trans_version=-4)
      "23       var1    -3    0     EXIST   CLF\n";   // rowkey 23, single version (trans_version=-3)

  // Micro 1: rowkey 24-27, partially filtered (trans_version > 5), 4 logical rows (4 physical rows)
  macro_data[7] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "24       var1    -6    0     EXIST   CLF\n"    // rowkey 24, single version (trans_version=6)
      "25       var1    -7    0     EXIST   CLF\n"    // rowkey 25, single version (trans_version=7)
      "26       var1    -8    0     EXIST   CLF\n"     // rowkey 26, single version (trans_version=8)
      "27       var1    -6    0     EXIST   CLF\n";    // rowkey 27, single version (trans_version=6)

  // Micro 2: rowkey 28-30, partially filtered (trans_version > 5), 3 logical rows (5 physical rows)
  macro_data[8] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "28       var1    -7    MIN   EXIST   SCF\n"      // rowkey 28, oldest version (abs=7, trans_version=-7)
      "28       var1    -6    0     EXIST   L\n"      // rowkey 28, newest version (abs=6, trans_version=-6)
      "29       var1    -6    0     EXIST   CLF\n"     // rowkey 29, single version (trans_version=-6)
      "30       var1    -8    MIN   EXIST   SCF\n"      // rowkey 30, oldest version (abs=8, trans_version=-8)
      "30       var1    -7    0     EXIST   L\n";     // rowkey 30, newest version (abs=7, trans_version=-7)

  // Macro 3: 3 micros, all partially filtered (trans_version > 5)
  // Micro 0: rowkey 31-34, 4 logical rows (4 physical rows)
  macro_data[9] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "31       var1    -6    0     EXIST   CLF\n"    // rowkey 31, single version (trans_version=6)
      "32       var1    -7    0     EXIST   CLF\n"    // rowkey 32, single version (trans_version=7)
      "33       var1    -8    0     EXIST   CLF\n"     // rowkey 33, single version (trans_version=8)
      "34       var1    -6    0     EXIST   CLF\n";    // rowkey 34, single version (trans_version=6)

  // Micro 1: rowkey 35-37, 3 logical rows (5 physical rows)
  macro_data[10] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "35       var1    -7    MIN   EXIST   SCF\n"      // rowkey 35, oldest version (abs=7, trans_version=-7)
      "35       var1    -6    0     EXIST   L\n"      // rowkey 35, newest version (abs=6, trans_version=-6)
      "36       var1    -6    0     EXIST   CLF\n"    // rowkey 36, single version (trans_version=-6)
      "37       var1    -9    MIN   EXIST   SCF\n"      // rowkey 37, oldest version (abs=9, trans_version=-9)
      "37       var1    -7    0     EXIST   L\n";    // rowkey 37, newest version (abs=7, trans_version=-7)

  // Micro 2: rowkey 38-41, 4 logical rows (4 physical rows)
  macro_data[11] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "38       var1    -6    0     EXIST   CLF\n"    // rowkey 38, single version (trans_version=6)
      "39       var1    -7    0     EXIST   CLF\n"     // rowkey 39, single version (trans_version=7)
      "40       var1    -8    0     EXIST   CLF\n"     // rowkey 40, single version (trans_version=8)
      "41       var1    -6    0     EXIST   CLF\n";    // rowkey 41, single version (trans_version=6)

  // Macro 4: 3 micros, all partially filtered (trans_version > 5)
  // Micro 0: rowkey 42-44, 3 logical rows (5 physical rows)
  macro_data[12] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "42       var1    -7    MIN   EXIST   SCF\n"      // rowkey 42, oldest version (abs=7, trans_version=-7)
      "42       var1    -6    0     EXIST   L\n"      // rowkey 42, newest version (abs=6, trans_version=-6)
      "43       var1    -6    0     EXIST   CLF\n"     // rowkey 43, single version (trans_version=-6)
      "44       var1    -8    MIN   EXIST   SCF\n"       // rowkey 44, oldest version (abs=8, trans_version=-8)
      "44       var1    -7    0     EXIST   L\n";     // rowkey 44, newest version (abs=7, trans_version=-7)

  // Micro 1: rowkey 45-48, 4 logical rows (4 physical rows)
  macro_data[13] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "45       var1    -6    0     EXIST   CLF\n"    // rowkey 45, single version (trans_version=6)
      "46       var1    -7    0     EXIST   CLF\n"     // rowkey 46, single version (trans_version=7)
      "47       var1    -8    0     EXIST   CLF\n"     // rowkey 47, single version (trans_version=8)
      "48       var1    -6    0     EXIST   CLF\n";    // rowkey 48, single version (trans_version=6)

  // Micro 2: rowkey 49-51, 3 logical rows (5 physical rows)
  macro_data[14] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "49       var1    -7    MIN   EXIST   SCF\n"      // rowkey 49, oldest version (abs=7, trans_version=-7)
      "49       var1    -6    0     EXIST   L\n"      // rowkey 49, newest version (abs=6, trans_version=-6)
      "50       var1    -6    0     EXIST   CLF\n"     // rowkey 50, single version (trans_version=-6)
      "51       var1    -9    MIN   EXIST   SCF\n"       // rowkey 51, oldest version (abs=9, trans_version=-9)
      "51       var1    -7    0     EXIST   L\n";     // rowkey 51, newest version (abs=7, trans_version=-7)

  int schema_rowkey_cnt = 2;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(snapshot_version);
  if (!table_schema_.is_valid()) {
    prepare_table_schema(macro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  }
  reset_writer(snapshot_version);
  data_iter_cursor_ = 0;
  for (int64_t i = 0; i < MAX_MICRO_BLOCK_CNT; i++) {
    data_iter_[i].reset();
  }
  OK(data_iter_[0].from(macro_data[0]));
  // Prepare 5 macros, each with 3 micros
  for (int64_t i = 0; i < 5; i++) {
    prepare_one_macro(&macro_data[i * 3], 3);
  }
  prepare_data_end(handle, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  sstable->meta_->basic_meta_.occupy_size_ = (2<<20) * 5;
  sstable->key_.table_type_ = ObITable::MINOR_SSTABLE;
}

// Test major sstable: should not be filtered by base_version
TEST_F(TestIndexSSTableEstimator, test_major_sstable_no_filter)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 9, 10, ObITable::MAJOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3 /* base_version */);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  // Major sstable should not be filtered by base_version, should return all rows
  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Verify result: major sstable will not be filtered, should return all rows
  // 9 macros, each macro has 2 rows, total 18 rows
  ASSERT_EQ(part_est.physical_row_count_, 18);
  ASSERT_EQ(part_est.logical_row_count_, part_est.physical_row_count_);
}

// Test minor sstable: completely not filtered case (base_version < all trans versions)
TEST_F(TestIndexSSTableEstimator, test_minor_sstable_no_filter)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  // Use macro 0-2, trans version 5-10, base_version=3, should be completely not filtered
  prepare_sstable_handle(handle, 0, 3, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3 /* base_version, less than all trans versions */);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Verify result: base_version=3 < all trans versions (5-10), should be completely not filtered
  // 3 macros, each macro has 2 rows, total 6 rows
  // In the case of completely not filtered, the result should be 100% accurate
  ASSERT_EQ(6, part_est.physical_row_count_);
  ASSERT_EQ(6, part_est.logical_row_count_);
}

// Test minor sstable: completely filtered case (base_version >= all trans versions)
TEST_F(TestIndexSSTableEstimator, test_minor_sstable_full_filter)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  // Use macro 3-5, trans version 1-3, base_version=3, should be completely filtered
  prepare_sstable_handle(handle, 3, 3, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
  ASSERT_EQ(part_est.physical_row_count_, 0);
  ASSERT_EQ(part_est.logical_row_count_, 0);
}

// Test minor sstable: base_version within trans version range case
// Note: Filter logic is that only blocks that can be completely filtered by base_version will be filtered (max_snapshot <= base_version)
// Other cases are calculated at 100%, no partial filtering
TEST_F(TestIndexSSTableEstimator, test_minor_sstable_partial_filter)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  // Use macro 6-8, trans version 2-5, base_version=3
  prepare_sstable_handle(handle, 6, 3, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3 /* base_version, within trans version range */);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Use macro 6-8, trans version 2-5, base_version=3
  // macro 6: trans version 2-4, min=2, max=4, base_version=3
  //   max (4) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  // macro 7: trans version 4-5, min=4, max=5, base_version=3
  //   max (5) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  // macro 8: trans version 1-4, min=1, max=4, base_version=3
  //   max (4) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  // Expected result: 2 + 2 + 2 = 6 rows
  ASSERT_EQ(6, part_est.physical_row_count_);
  ASSERT_EQ(6, part_est.logical_row_count_);
}

// Test inc_major sstable: completely filtered case
TEST_F(TestIndexSSTableEstimator, test_inc_major_sstable_full_filter)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 3, 10, ObITable::INC_MAJOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  // upper_trans_version = 10, base_version = 11, should be completely filtered
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 11 /* base_version */);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Verify result: base_version=11 > upper_trans_version=10, should be completely filtered
  ASSERT_EQ(0, part_est.physical_row_count_);
  ASSERT_EQ(0, part_est.logical_row_count_);
}

// Test inc_major sstable: completely not filtered case
TEST_F(TestIndexSSTableEstimator, test_inc_major_sstable_no_filter)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 3, 10, ObITable::INC_MAJOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  // upper_trans_version = 10, base_version = 5, should be completely not filtered
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 5 /* base_version */);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Verify result: base_version=5 < upper_trans_version=10, should be completely not filtered
  ASSERT_EQ(part_est.physical_row_count_, 6);
  ASSERT_EQ(part_est.logical_row_count_, 6);
}

// Test different ranges: whole range
TEST_F(TestIndexSSTableEstimator, test_whole_range)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 3, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
  ASSERT_EQ(part_est.physical_row_count_, 6);
  ASSERT_EQ(part_est.logical_row_count_, 6);
}

// Test different ranges: partial range
TEST_F(TestIndexSSTableEstimator, test_partial_range)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 6, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                 "2        var1   -9    MIN      EXIST   CLF\n"
                                 "8        var1   -9    MIN      EXIST   CLF\n");
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Query range [2, 8], use macro 0-5, base_version=3
  // macro 0: rowkey 0-1, not in query range, not counted
  // macro 1: rowkey 2-3, trans version 7-8, min=7, max=8, base_version=3
  //   max (8) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  // macro 2: rowkey 4-5, trans version 9-10, min=9, max=10, base_version=3
  //   max (10) > base_version (3), will not be filtered, calculated at 100% = 2 rows
  // macro 3: rowkey 6-7, trans version 1-2, min=1, max=2, base_version=3
  //   max (2) <= base_version (3), completely filtered, not counted = 0 rows
  // macro 4: rowkey 8-9, trans version 2-3, min=2, max=3, base_version=3
  //   max (3) <= base_version (3), completely filtered, not counted = 0 rows
  //   Although rowkey 8 is in query range, due to macro-level filtering, the entire macro is filtered
  // macro 5: rowkey 10-11, not in query range, not counted
  // Expected result: 2 + 2 + 0 + 0 = 4 rows
  ASSERT_EQ(4, part_est.physical_row_count_);
  ASSERT_EQ(4, part_est.logical_row_count_);
}

// Test mixed scenario: contains both completely filtered and completely not filtered macros
// Note: Filter logic is that only blocks that can be completely filtered by base_version will be filtered (max_snapshot <= base_version)
// Other cases are calculated at 100%, no partial filtering
TEST_F(TestIndexSSTableEstimator, test_mixed_scenario)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  // Use macro 0-8, contains two cases:
  // macro 0-2: trans version 5-10 (max=10 > base_version=3, will not be filtered, calculated at 100%)
  // macro 3-5: trans version 1-3 (max=3 <= base_version=3, will be completely filtered)
  // macro 6-8: trans version 2-5 (max=5 > base_version=3, will not be filtered, calculated at 100%)
  prepare_sstable_handle(handle, 0, 9, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3 /* base_version */);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Verify result, base_version=3:
  // macro 0-2: trans version 5-10, min=5, max=10, base_version=3
  //   max (10) > base_version (3), will not be filtered, calculated at 100% = 6 rows
  // macro 3-5: trans version 1-3, min=1, max=3, base_version=3
  //   max (3) <= base_version (3), completely filtered, not counted = 0 rows
  // macro 6-8: trans version 2-5, will not be filtered, calculated at 100%
  //   macro 6: trans version 2-4, max=4 > base_version=3, will not be filtered = 2 rows
  //   macro 7: trans version 4-5, max=5 > base_version=3, will not be filtered = 2 rows
  //   macro 8: trans version 1-4, max=4 > base_version=3, will not be filtered = 2 rows
  //   Subtotal: 2 + 2 + 2 = 6 rows
  // Expected result: 6 + 0 + 6 = 12 rows
  ASSERT_EQ(12, part_est.physical_row_count_);
  ASSERT_EQ(12, part_est.logical_row_count_);
}

// Test accuracy of completely filtered and completely not filtered cases: should be 100% accurate
TEST_F(TestIndexSSTableEstimator, test_accuracy_full_filter_and_no_filter)
{
  allocator_.reuse();

  // Test completely not filtered case
  {
    ObTableHandleV2 handle;
    prepare_sstable_handle(handle, 0, 3, 10, ObITable::MINOR_SSTABLE);
    ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

    ObTabletHandle tablet_handle;
    get_tablet_handle(tablet_handle);
    ObQueryFlag query_flag;
    ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 1 /* base_version, much less than all trans versions */);
    ObIndexBlockScanEstimator estimator(context);

    ObDatumRange datum_range;
    datum_range.set_whole_range();
    ObPartitionEst part_est;

    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

    // In the case of completely not filtered, the result should be 100% accurate
    // 3 macros, each macro has 2 rows, total 6 rows
    ASSERT_EQ(6, part_est.physical_row_count_);
    ASSERT_EQ(6, part_est.logical_row_count_);
  }

  // Test completely filtered case
  {
    ObTableHandleV2 handle;
    prepare_sstable_handle(handle, 3, 3, 10, ObITable::MINOR_SSTABLE);
    ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

    ObTabletHandle tablet_handle;
    get_tablet_handle(tablet_handle);
    ObQueryFlag query_flag;
    ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 10 /* base_version, much greater than all trans versions */);
    ObIndexBlockScanEstimator estimator(context);

    ObDatumRange datum_range;
    datum_range.set_whole_range();
    ObPartitionEst part_est;

    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

    // In the case of completely filtered, the result should be 100% accurate, should be 0
    ASSERT_EQ(0, part_est.physical_row_count_);
    ASSERT_EQ(0, part_est.logical_row_count_);
  }
}

// Test macro_block and micro_block counting
TEST_F(TestIndexSSTableEstimator, test_block_count)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 3, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 1);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  int64_t macro_block_cnt = 0;
  int64_t micro_block_cnt = 0;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_block_count(*sstable, datum_range, macro_block_cnt, micro_block_cnt));

  // 3 macros, each macro has 1 micro block
  // Since estimate_block_count returns MAX(result.macro_block_cnt_, 1), it should be at least 1
  ASSERT_EQ(macro_block_cnt, 3);
  ASSERT_EQ(micro_block_cnt, 3);

  // Verify row count estimation is also correct
  ObPartitionEst part_est;
  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
  ASSERT_EQ(6, part_est.physical_row_count_);
}

// Test case where one macro contains multiple micros
TEST_F(TestIndexSSTableEstimator, test_one_macro_multiple_micros)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 3, 10, ObITable::MINOR_SSTABLE, 3 /* micro_cnt */);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 3);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  ASSERT_EQ(12, part_est.physical_row_count_);
  ASSERT_EQ(12, part_est.logical_row_count_);

  // Verify block count
  int64_t macro_block_cnt = 0;
  int64_t micro_block_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, estimator.estimate_block_count(*sstable, datum_range, macro_block_cnt, micro_block_cnt));
  ASSERT_GE(macro_block_cnt, 2);
  ASSERT_GE(micro_block_cnt, 6);
}

// Test logical row estimation for multi-version rows: ensure logical row count is correct
// Multi-version rows: same rowkey has multiple version rows (insert + update + delete)
// SCF = Shadow + Compact + First (oldest version, largest absolute value), L = Last (newest version, smallest absolute value)
// C = Compacted (middle versions), CLF = Compacted + Last + First (single version)
// Version comparison uses absolute value: abs(-7)=7 > abs(-6)=6 > abs(-5)=5
// Storage order: from old to new (trans_version -7, -6, -5)
// Oldest version (largest abs): SCF, Newest version (smallest abs): L, Middle versions: C
TEST_F(TestIndexSSTableEstimator, test_multi_version_row_logical_count)
{
  allocator_.reuse();

  // Use macro_data[10], contains multi-version row data
  // Storage order: from old to new (trans_version -7, -6, -5)
  // rowkey=0: delete (trans_version=-7, SCF, oldest) + update (trans_version=-6, C) + insert (trans_version=-5, L, newest)
  // rowkey=1: update (trans_version=-6, SCF, oldest) + insert (trans_version=-5, L, newest)
  // rowkey=2: insert (trans_version=-5, CLF) - single version row
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 10, 1, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 1);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  datum_range.set_whole_range();
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  // Physical row count: 6 rows (all version rows)
  // Logical row count: 3 rows (3 different rowkeys, newest version row of each rowkey)
  // Version comparison uses absolute value: abs(-7)=7 > abs(-6)=6 > abs(-5)=5
  // rowkey=0: 3 version rows, storage order is -7(SCF, oldest), -6(C), -5(L, newest), only -5 row is newest version (L), delta=1
  // rowkey=1: 2 version rows, storage order is -6(SCF, oldest), -5(L, newest), only -5 row is newest version (L), delta=1
  // rowkey=2: 1 version row, is oldest and newest version (CLF), delta=1
  // Logical row count = 1 + 1 + 1 = 3
  ASSERT_EQ(6, part_est.physical_row_count_);
  ASSERT_EQ(3, part_est.logical_row_count_);
}

TEST_F(TestIndexSSTableEstimator, test_leaf_node_traversal)
{
  allocator_.reuse();

  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 10, 2, 10, ObITable::MINOR_SSTABLE);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, 1);
  ObIndexBlockScanEstimator estimator(context);

  ObDatumRange datum_range;
  build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                 "20       var1   -9    MIN      EXIST   CLF\n"
                                 "20       var1   -9    MIN      EXIST   CLF\n");
  ObPartitionEst part_est;

  ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));

  ASSERT_EQ(part_est.physical_row_count_, 1);
  ASSERT_EQ(part_est.logical_row_count_, 1);
}

// Comprehensive test: 5 macros, each with 3 micros
// Each micro contains 4-6 physical rows, 2-3 logical rows
// One micro is completely filtered by base_version, others are partially filtered
// Enumerate all possible ranges and verify estimation results are accurate
TEST_F(TestIndexSSTableEstimator, test_comprehensive_multi_version_filtering)
{
  allocator_.reuse();

  const int64_t snapshot_version = 20;
  const int64_t base_version = 5;

  ObTableHandleV2 handle;
  prepare_comprehensive_test_data(handle, snapshot_version, base_version);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  ObTabletHandle tablet_handle;
  get_tablet_handle(tablet_handle);
  ObQueryFlag query_flag;
  ObIndexSSTableEstimateContext context(tablet_handle, query_flag, base_version);
  ObIndexBlockScanEstimator estimator(context);

  // Expected results for base_version=5:
  // Note: If entire macro is in query range, macro-level filtering applies (macro not filtered if any micro not filtered)
  // If only part of macro is in query range, micro-level filtering applies
  // Macro 0: 3 micros, all partially filtered (trans_version > 5)
  //   Micro 0: 4 physical rows, 3 logical rows (rowkey 0-2)
  //   Micro 1: 5 physical rows, 3 logical rows (rowkey 3-5)
  //   Micro 2: 4 physical rows, 4 logical rows (rowkey 6-9)
  //   Total: 13 physical rows, 10 logical rows
  // Macro 1: 3 micros, all partially filtered (trans_version > 5)
  //   Micro 0: 5 physical rows, 3 logical rows (rowkey 10-12)
  //   Micro 1: 4 physical rows, 4 logical rows (rowkey 13-16)
  //   Micro 2: 5 physical rows, 3 logical rows (rowkey 17-19)
  //   Total: 14 physical rows, 10 logical rows
  // Macro 2: 3 micros, one completely filtered, others partially filtered
  //   Micro 0: 6 physical rows, 4 logical rows (rowkey 20-23) - COMPLETELY FILTERED (trans_version <= 5)
  //   Micro 1: 4 physical rows, 4 logical rows (rowkey 24-27) - partially filtered (trans_version > 5)
  //   Micro 2: 5 physical rows, 3 logical rows (rowkey 28-30) - partially filtered (trans_version > 5)
  //   If entire macro in range: 6 + 4 + 5 = 15 physical rows, 4 + 4 + 3 = 11 logical rows (macro not filtered)
  //   If partial macro in range: 0 + 4 + 5 = 9 physical rows, 0 + 4 + 3 = 7 logical rows (micro-level filtering)
  // Macro 3: 3 micros, all partially filtered (trans_version > 5)
  //   Micro 0: 4 physical rows, 4 logical rows (rowkey 31-34)
  //   Micro 1: 5 physical rows, 3 logical rows (rowkey 35-37)
  //   Micro 2: 4 physical rows, 4 logical rows (rowkey 38-41)
  //   Total: 13 physical rows, 11 logical rows
  // Macro 4: 3 micros, all partially filtered (trans_version > 5)
  //   Micro 0: 5 physical rows, 3 logical rows (rowkey 42-44)
  //   Micro 1: 4 physical rows, 4 logical rows (rowkey 45-48)
  //   Micro 2: 5 physical rows, 3 logical rows (rowkey 49-51)
  //   Total: 14 physical rows, 10 logical rows
  // Grand total (whole range, all macros included): 13 + 14 + 15 + 13 + 14 = 69 physical rows, 10 + 10 + 11 + 11 + 10 = 52 logical rows

  // Test case 1: Whole range
  {
    ObDatumRange datum_range;
    datum_range.set_whole_range();
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(69, part_est.physical_row_count_);
    ASSERT_EQ(52, part_est.logical_row_count_);
  }

  // Test case 2: Range covering first macro (rowkey 0-9)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "0        var1   -9    MIN      EXIST   CLF\n"
                                   "9        var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(13, part_est.physical_row_count_);
    ASSERT_EQ(10, part_est.logical_row_count_);
  }

  // Test case 3: Range covering second macro (rowkey 10-19)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "10       var1   -9    MIN      EXIST   CLF\n"
                                   "19       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(14, part_est.physical_row_count_);
    ASSERT_EQ(10, part_est.logical_row_count_);
  }

  // Test case 4: Range covering third macro (includes filtered micro, rowkey 20-30)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "20       var1   -9    MIN      EXIST   CLF\n"
                                   "30       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(9, part_est.physical_row_count_);
    ASSERT_EQ(7, part_est.logical_row_count_);
  }

  // Test case 5: Range covering fourth macro (rowkey 31-41)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "31       var1   -9    MIN      EXIST   CLF\n"
                                   "41       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(13, part_est.physical_row_count_);
    ASSERT_EQ(11, part_est.logical_row_count_);
  }

  // Test case 6: Range covering fifth macro (rowkey 42-51)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "42       var1   -9    MIN      EXIST   CLF\n"
                                   "51       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(14, part_est.physical_row_count_);
    ASSERT_EQ(10, part_est.logical_row_count_);
  }

  // Test case 7: Range spanning multiple macros (first two, rowkey 0-19)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "0        var1   -9    MIN      EXIST   CLF\n"
                                   "19       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    ASSERT_EQ(27, part_est.physical_row_count_);
    ASSERT_EQ(20, part_est.logical_row_count_);
  }

  // Test case 8: Range spanning multiple macros (middle three, rowkey 10-41)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "10       var1   -9    MIN      EXIST   CLF\n"
                                   "41       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Macro 1: 14 physical, 10 logical
    // Macro 2: Entire macro in range, so macro-level filtering applies - 15 physical, 11 logical (all micros count)
    // Macro 3: 13 physical, 11 logical
    ASSERT_EQ(42, part_est.physical_row_count_);
    ASSERT_EQ(32, part_est.logical_row_count_);
  }

  // Test case 9: Range covering partial macro (first micro of macro 0, rowkey 0-2)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "0        var1   -9    MIN      EXIST   CLF\n"
                                   "2        var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Only covers first micro of macro 0: 4 physical rows, 3 logical rows
    ASSERT_EQ(4, part_est.physical_row_count_);
    ASSERT_EQ(3, part_est.logical_row_count_);
  }

  // Test case 10: Range covering filtered micro only (should return 0, rowkey 20-23)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "20       var1   -9    MIN      EXIST   CLF\n"
                                   "23       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // This micro is completely filtered (trans_version <= 5)
    ASSERT_EQ(0, part_est.physical_row_count_);
    ASSERT_EQ(0, part_est.logical_row_count_);
  }

  // Test case 11: Range covering last two macros (rowkey 31-51)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "31       var1   -9    MIN      EXIST   CLF\n"
                                   "51       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Macro 3: 13 physical, 11 logical
    // Macro 4: 14 physical, 10 logical
    ASSERT_EQ(27, part_est.physical_row_count_);
    ASSERT_EQ(21, part_est.logical_row_count_);
  }

  // Test case 12: Range covering all macros (same as whole range)
  {
    ObDatumRange datum_range;
    datum_range.set_whole_range();
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Same as test case 1: all macros included, Macro 2 not filtered at macro level
    ASSERT_EQ(69, part_est.physical_row_count_);
    ASSERT_EQ(52, part_est.logical_row_count_);
  }

  // Test case 13: Range spanning multiple micros within Macro 0, boundaries not aligned (rowkey 1-8)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "1        var1   -9    MIN      EXIST   CLF\n"
                                   "8        var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Partial Macro 0: micro-level filtering applies
    // Micro 0: rowkey 1-2 = 2 physical rows (rowkey 1: 1 version, rowkey 2: 1 version), 2 logical rows
    // Micro 1: rowkey 3-5 = 5 physical rows, 3 logical rows
    // Micro 2: rowkey 6-8 = 3 physical rows (rowkey 6, 7, 8: 1 version each), 3 logical rows
    ASSERT_EQ(10, part_est.physical_row_count_);
    ASSERT_EQ(8, part_est.logical_row_count_);
  }

  // Test case 14: Range spanning Macro 0 and Macro 1, boundaries not aligned (rowkey 4-15)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "4        var1   -9    MIN      EXIST   CLF\n"
                                   "15       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Partial Macro 0 and Macro 1: micro-level filtering applies
    // Macro 0 Micro 1: rowkey 4-5 = 3 physical rows (rowkey 4: 1 version, rowkey 5: 2 versions), 2 logical rows
    // Macro 0 Micro 2: rowkey 6-9 = 4 physical rows, 4 logical rows
    // Macro 1 Micro 0: rowkey 10-12 = 5 physical rows, 3 logical rows
    // Macro 1 Micro 1: rowkey 13-15 = 3 physical rows (rowkey 13, 14, 15: 1 version each), 3 logical rows
    ASSERT_EQ(15, part_est.physical_row_count_);
    ASSERT_EQ(12, part_est.logical_row_count_);
  }

  // Test case 15: Range spanning Macro 2 micros, boundaries not aligned, includes filtered micro (rowkey 22-26)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "22       var1   -9    MIN      EXIST   CLF\n"
                                   "26       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Partial Macro 2: micro-level filtering applies
    // Macro 2 Micro 0: rowkey 22-23 = completely filtered (trans_version <= 5), 0 rows
    // Macro 2 Micro 1: rowkey 24-26 = 3 physical rows (rowkey 24, 25, 26: 1 version each), 3 logical rows
    ASSERT_EQ(3, part_est.physical_row_count_);
    ASSERT_EQ(3, part_est.logical_row_count_);
  }

  // Test case 16: Range spanning Macro 1 and Macro 2, boundaries not aligned (rowkey 14-29)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "14       var1   -9    MIN      EXIST   CLF\n"
                                   "29       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Partial Macro 1 and Macro 2: micro-level filtering applies
    // Macro 1 Micro 1: rowkey 14-16 = 3 physical rows, 3 logical rows
    // Macro 1 Micro 2: rowkey 17-19 = 5 physical rows, 3 logical rows
    // Macro 2 Micro 0: rowkey 20-23 = completely filtered, 0 rows
    // Macro 2 Micro 1: rowkey 24-27 = 4 physical rows, 4 logical rows
    // Macro 2 Micro 2: rowkey 28-29 = 3 physical rows (rowkey 28: 2 versions, rowkey 29: 1 version), 2 logical rows
    ASSERT_EQ(15, part_est.physical_row_count_);
    ASSERT_EQ(12, part_est.logical_row_count_);
  }

  // Test case 17: Range spanning Macro 3 and Macro 4, boundaries not aligned (rowkey 35-47)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "35       var1   -9    MIN      EXIST   CLF\n"
                                   "47       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Partial Macro 3 and Macro 4: micro-level filtering applies
    // Macro 3 Micro 1: rowkey 35-37 = 5 physical rows, 3 logical rows
    // Macro 3 Micro 2: rowkey 38-41 = 4 physical rows, 4 logical rows
    // Macro 4 Micro 0: rowkey 42-44 = 5 physical rows, 3 logical rows
    // Macro 4 Micro 1: rowkey 45-47 = 3 physical rows (rowkey 45, 46, 47: 1 version each), 3 logical rows
    ASSERT_EQ(17, part_est.physical_row_count_);
    ASSERT_EQ(13, part_est.logical_row_count_);
  }

  // Test case 18: Range covering partial micros across multiple macros (rowkey 1-11)
  {
    ObDatumRange datum_range;
    build_datum_range(datum_range, "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                                   "1        var1   -9    MIN      EXIST   CLF\n"
                                   "11       var1   -9    MIN      EXIST   CLF\n");
    ObPartitionEst part_est;
    ASSERT_EQ(OB_SUCCESS, estimator.estimate_row_count(*sstable, datum_range, part_est));
    // Partial Macro 0 and Macro 1: micro-level filtering applies
    // Macro 0 Micro 0: rowkey 1-2 = 2 physical rows, 2 logical rows
    // Macro 0 Micro 1: rowkey 3-5 = 5 physical rows, 3 logical rows
    // Macro 0 Micro 2: rowkey 6-9 = 4 physical rows, 4 logical rows
    // Macro 1 Micro 0: rowkey 10-11 = 3 physical rows (rowkey 10: 2 versions, rowkey 11: 1 version), 2 logical rows
    ASSERT_EQ(14, part_est.physical_row_count_);
    ASSERT_EQ(11, part_est.logical_row_count_);
  }
}

}
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_partition_row_estimator.log*");
  OB_LOGGER.set_file_name("test_partition_row_estimator.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
