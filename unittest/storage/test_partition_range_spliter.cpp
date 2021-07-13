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
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_array.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/ob_partition_component_factory.h"
#include "blocksstable/ob_data_file_prepare.h"
#include "blocksstable/ob_row_generate.h"
#include "storage/ob_ms_row_iterator.h"
#include "observer/ob_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"

#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_i_store.h"
#include "share/ob_srv_rpc_proxy.h"

#include "storage/ob_i_table.h"
#include "storage/compaction/ob_partition_merge_builder.h"
#include "ob_multi_version_sstable_test.h"
#include "mock_ob_partition_report.h"
#include "storage/ob_sstable_merge_info_mgr.h"

#include "memtable/utils_rowkey_builder.h"
#include "memtable/utils_mock_row.h"

#include "storage/compaction/ob_partition_merge.h"
#include "ob_uncommitted_trans_test.h"
#include "mockcontainer/mock_ob_trans_service.h"
#include "storage/ob_partition_range_spliter.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace rpc::frame;
using namespace memtable;
namespace storage {

class TestRangeSpliter : public ObMultiVersionSSTableTest {
public:
  static const int64_t MAX_PARALLEL_DEGREE = 10;
  static const int64_t MAX_MACRO_CNT = 20;
  static const int64_t RANGE_MAX = 0;
  static const int64_t RANGE_MIN = 1;
  static const int64_t RANGE_NORMAL = 2;
  TestRangeSpliter() : ObMultiVersionSSTableTest("test_range_spliter")
  {}
  virtual ~TestRangeSpliter()
  {}

  virtual void SetUp()
  {
    init_tenant_mgr();
    ObTableMgr::get_instance().init();
    ObMultiVersionSSTableTest::SetUp();
    ObPartitionKey pkey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().init());
    ASSERT_EQ(OB_SUCCESS, ObPartitionService::get_instance().get_pg_index().add_partition(pkey, pkey));
    ObPartitionKey& part = const_cast<ObPartitionKey&>(test_trans_part_ctx_.get_partition());
    part = pkey;
    trans_service_.part_trans_ctx_mgr_.mgr_cache_.set(pkey.hash(), &test_trans_part_ctx_);
    ObPartitionService::get_instance().txs_ = &trans_service_;
  }
  virtual void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    ObTableMgr::get_instance().destroy();
    ObPartitionService::get_instance().get_pg_index().destroy();
  }

  void prepare_schema();
  int init_tenant_mgr();
  void build_store_range(ObIArray<ObStoreRange>& ranges, const char* rowkey_data, const bool compaction = true);
  void build_query_range(ObIArray<ObStoreRange>& ranges, const char* rowkey_data);
  bool equal_ranges(ObStoreRange& range1, ObStoreRange& range2);
  bool loop_equal_ranges(ObIArray<ObStoreRange>& ranges1, ObIArray<ObStoreRange>& ranges2);
  void prepare_sstable(
      ObSSTable& sstable, const int64_t macro_idx_start, const int64_t macro_cnt, const int64_t snapshot_version);
  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  MockObIPartitionReport report_;
  share::schema::ObTenantSchema tenant_schema_;
  ObMockIterator range_iter_;
  transaction::MockObTransService trans_service_;
  TestUncommittedMinorMergeScan test_trans_part_ctx_;
  TestUncommittedMinorMergeScan scan_trans_part_ctx_;
};

void TestRangeSpliter::prepare_schema()
{
  int ret = OB_SUCCESS;
  int64_t micro_block_size = 16 * 1024;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t table_id = combine_id(tenant_id, TABLE_ID);
  ObColumnSchemaV2 column;

  // generate data table schema
  table_schema_.reset();
  ret = table_schema_.set_table_name("test_merge_multi_version");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema_.set_tenant_id(tenant_id);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(SPARSE_ROW_STORE);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObColDesc col_desc;
  const int64_t column_ids[] = {16, 17, 20, 21};
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    COMMON_LOG(INFO, "add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
    col_desc.col_id_ = column.get_column_id();
    col_desc.col_type_ = column.get_meta_type();
    ASSERT_EQ(OB_SUCCESS, columns_.push_back(col_desc));
  }
  COMMON_LOG(INFO, "dump stable schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema_));
}

int TestRangeSpliter::init_tenant_mgr()
{
  ObTenantManager& tm = ObTenantManager::get_instance();
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy common_rpc_proxy;
  share::ObRsMgr rs_mgr;

  int ret = tm.init(self, rpc_proxy, common_rpc_proxy, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  return OB_SUCCESS;
}

bool TestRangeSpliter::equal_ranges(ObStoreRange& range1, ObStoreRange& range2)
{
  bool bret = false;
  if (!range1.get_start_key().simple_equal(range2.get_start_key())) {
  } else if (!range1.get_end_key().simple_equal(range2.get_end_key())) {
  } else {
    bret = range1.get_border_flag().get_data() == range2.get_border_flag().get_data();
  }
  if (!bret) {
    STORAGE_LOG(WARN, "ranges are not equal", K(range1), K(range2));
  }

  return bret;
}

bool TestRangeSpliter::loop_equal_ranges(ObIArray<ObStoreRange>& ranges1, ObIArray<ObStoreRange>& ranges2)
{
  bool bret = false;
  if (ranges1.count() != ranges2.count()) {
    STORAGE_LOG(WARN, "ranges count are not equal", K(ranges1), K(ranges2));
  } else {
    bret = true;
    for (int64_t i = 0; bret && i < ranges1.count(); i++) {
      bret = equal_ranges(ranges1.at(i), ranges2.at(i));
    }
  }

  return bret;
}

void TestRangeSpliter::build_store_range(ObIArray<ObStoreRange>& ranges, const char* rowkey_data, const bool compaction)
{
  const ObStoreRow* row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  const int64_t mv_col_cnt = compaction ? ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() : 0;
  ObStoreRange range, dst_range;
  ASSERT_TRUE(nullptr != rowkey_data);
  ranges.reset();
  range_iter_.reset();
  OK(range_iter_.from(rowkey_data));
  ASSERT_TRUE(range_iter_.count() > 0);

  range.get_start_key().set_min();
  range.set_left_open();
  for (int64_t i = 0; i < range_iter_.count(); i++) {
    OK(range_iter_.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    for (int64_t i = 0; i < mv_col_cnt; i++) {
      row->row_val_.cells_[rowkey_col_cnt + i].set_max_value();
    }
    range.get_end_key().assign(row->row_val_.cells_, rowkey_col_cnt + mv_col_cnt);
    range.set_right_closed();
    ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
    range.reset();
    dst_range.reset();
    range.get_start_key().assign(row->row_val_.cells_, rowkey_col_cnt + mv_col_cnt);
    range.set_left_open();
  }
  range.get_end_key().set_max();
  range.set_right_open();
  ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
}

void TestRangeSpliter::build_query_range(ObIArray<ObStoreRange>& ranges, const char* rowkey_data)
{
  const ObStoreRow* row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  ObStoreRange range, dst_range;
  ASSERT_TRUE(nullptr != rowkey_data);
  ranges.reset();
  range_iter_.reset();
  OK(range_iter_.from(rowkey_data));
  ASSERT_TRUE(range_iter_.count() > 0 && range_iter_.count() % 2 == 0);

  for (int64_t i = 0; i < range_iter_.count(); i += 2) {
    OK(range_iter_.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    range.get_start_key().assign(row->row_val_.cells_, rowkey_col_cnt);
    OK(range_iter_.get_row(i + 1, row));
    ASSERT_TRUE(nullptr != row);
    range.get_end_key().assign(row->row_val_.cells_, rowkey_col_cnt);
    if (range.get_start_key().compare(range.get_end_key()) == 0) {
      range.set_left_closed();
    } else {
      range.set_left_open();
    }
    range.set_right_closed();
    ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
    range.reset();
    dst_range.reset();
  }
}

void TestRangeSpliter::prepare_sstable(
    ObSSTable& sstable, const int64_t macro_idx_start, const int64_t macro_cnt, const int64_t snapshot_version)
{
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(true, macro_idx_start + macro_cnt <= MAX_MACRO_CNT);

  const char* macro_data[20];
  macro_data[0] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "0        var1   MIN    -9     EXIST   CLF\n"
                  "1        var1   MIN    -9     EXIST   CF\n";

  macro_data[1] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "1        var1   MIN    -8     EXIST   L\n"
                  "2        var1   MIN    -9     EXIST   CLF\n";

  macro_data[2] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "3        var1   MIN    -8     EXIST   CLF\n"
                  "4        var1   MIN    -9     EXIST   CF\n";

  macro_data[3] = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                  "4        var1   MIN    -8     EXIST   L\n"
                  "5        var1   MIN    -9     EXIST   CLF\n";

  macro_data[4] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "6        var1   MIN    -9     EXIST   CLF\n"
                  "7        var1   MIN    -9     EXIST   CF\n";

  macro_data[5] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "7        var1   MIN    -8     EXIST   L\n"
                  "8        var1   MIN    -9     EXIST   CLF\n";

  macro_data[6] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "9        var1   MIN    -8     EXIST   CLF\n"
                  "10       var1   MIN    -9     EXIST   CF\n";

  macro_data[7] = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                  "10       var1   MIN    -8     EXIST   L\n"
                  "11       var1   MIN    -9     EXIST   CLF\n";

  macro_data[8] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "12       var1   MIN    -9     EXIST   CLF\n"
                  "13       var1   MIN    -9     EXIST   CF\n";

  macro_data[9] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                  "13       var1   MIN    -8     EXIST   L\n"
                  "14       var1   MIN    -9     EXIST   CLF\n";

  macro_data[10] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "15       var1   MIN    -8     EXIST   CLF\n"
                   "16       var1   MIN    -9     EXIST   CF\n";

  macro_data[11] = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                   "16       var1   MIN    -8     EXIST   L\n"
                   "17       var1   MIN    -9     EXIST   CLF\n";

  macro_data[12] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "18       var1   MIN    -9     EXIST   CLF\n"
                   "19       var1   MIN    -9     EXIST   CF\n";

  macro_data[13] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "19       var1   MIN    -8     EXIST   L\n"
                   "20       var1   MIN    -9     EXIST   CLF\n";

  macro_data[14] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "21       var1   MIN    -8     EXIST   CLF\n"
                   "22       var1   MIN    -9     EXIST   CF\n";

  macro_data[15] = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                   "22       var1   MIN    -8     EXIST   L\n"
                   "23       var1   MIN    -9     EXIST   CLF\n";

  macro_data[16] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "24       var1   MIN    -9     EXIST   CLF\n"
                   "25       var1   MIN    -9     EXIST   CF\n";

  macro_data[17] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "26       var1   MIN    -8     EXIST   L\n"
                   "27       var1   MIN    -9     EXIST   CLF\n";

  macro_data[18] = "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
                   "28       var1   MIN    -8     EXIST   CLF\n"
                   "29       var1   MIN    -9     EXIST   CF\n";

  macro_data[19] = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                   "29       var1   MIN    -8     EXIST   L\n"
                   "30       var1   MIN    -9     EXIST   CLF\n";

  prepare_data_start(sstable, &macro_data[macro_idx_start], rowkey_cnt, snapshot_version, "none", FLAT_ROW_STORE, 0);
  for (int64_t i = macro_idx_start; i < macro_idx_start + macro_cnt; i++) {
    prepare_one_macro(&macro_data[i], 1, INT64_MAX, nullptr, nullptr, false);
  }
  prepare_data_end(sstable);
  sstable.meta_.occupy_size_ = (2 << 20) * macro_cnt;
  {
    ObFullMacroBlockMeta full_meta;
    blocksstable::ObMacroBlockCtx macro_block_ctx;
    for (int64_t i = 0; i < sstable.get_macro_block_count(); i++) {
      ASSERT_EQ(OB_SUCCESS, sstable.get_macro_block_ctx(i, macro_block_ctx));
      ASSERT_EQ(OB_SUCCESS, sstable.get_meta(macro_block_ctx.sstable_block_id_.macro_block_id_, full_meta));
      (const_cast<ObMacroBlockMetaV2*>(full_meta.meta_))->occupy_size_ = 2 << 20;
      STORAGE_LOG(INFO, "dump macro meta", K(full_meta));
    }
  }
}

TEST_F(TestRangeSpliter, test_single_basic)
{
  allocator_.reuse();
  ObSSTable sstable;
  ObArray<ObSSTable*> sstables;
  ObStoreRange split_range;
  ObPartitionRangeSpliter range_spliter;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges;

  split_range.set_whole_range();
  prepare_sstable(sstable, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(&sstable));
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(2);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(2, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));

  const char* rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                            "11       var1   MIN    -9     EXIST   CLF\n";
  ObArray<ObStoreRange> ranges;
  build_store_range(ranges, rowkey_data);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(3);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(3, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char* rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "11       var1   MIN    -9     EXIST   CLF\n"
                             "20       var1   MIN    -9     EXIST   CLF\n";
  build_store_range(ranges, rowkey_data1);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(4);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(4, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char* rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "8        var1   MIN    -9     EXIST   CLF\n"
                             "17       var1   MIN    -9     EXIST   CLF\n"
                             "23       var1   MIN    -9     EXIST   CLF\n";
  build_store_range(ranges, rowkey_data2);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  range_spliter.reset();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(9);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(9, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char* rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "5        var1   MIN    -9     EXIST   CLF\n"
                             "8        var1   MIN    -9     EXIST   CLF\n"
                             "11        var1   MIN    -9     EXIST   CLF\n"
                             "14       var1   MIN    -9     EXIST   CLF\n"
                             "17        var1   MIN    -9     EXIST   CLF\n"
                             "20       var1   MIN    -9     EXIST   CLF\n"
                             "23        var1   MIN    -9     EXIST   CLF\n"
                             "27       var1   MIN    -9     EXIST   CLF\n";
  build_store_range(ranges, rowkey_data3);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));
}

TEST_F(TestRangeSpliter, test_single_multi_sstable)
{
  allocator_.reuse();
  ObArray<ObSSTable*> sstables;
  ObStoreRange split_range;
  ObPartitionRangeSpliter range_spliter;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges;
  ObArray<ObStoreRange> ranges;
  ObSSTable sstable, sstable1;

  split_range.set_whole_range();
  prepare_sstable(sstable, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(&sstable));
  prepare_sstable(sstable1, 5, 15, 20);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(&sstable1));

  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(2);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(2, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char* rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                            "19       var1   MIN    -9     EXIST   CLF\n";
  build_store_range(ranges, rowkey_data);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(5);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(5, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char* rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "10       var1   MIN    -9     EXIST   CLF\n"
                             "16       var1   MIN    -9     EXIST   CLF\n"
                             "20       var1   MIN    -9     EXIST   CLF\n"
                             "25       var1   MIN    -9     EXIST   CLF\n";
  build_store_range(ranges, rowkey_data1);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));

  ObSSTable sstable2;
  prepare_sstable(sstable2, 0, 5, 30);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(&sstable2));

  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(sstables, split_range, range_split_info));
  range_split_info.set_parallel_target(3);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_split_info, allocator, true, split_ranges));
  ASSERT_EQ(3, split_ranges.count());
  STORAGE_LOG(INFO, "finish split ranges", K(split_ranges));
  const char* rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "10       var1   MIN    -9     EXIST   CLF\n"
                             "20       var1   MIN    -9     EXIST   CLF\n";
  build_store_range(ranges, rowkey_data2);
  ASSERT_EQ(true, loop_equal_ranges(ranges, split_ranges));
}

TEST_F(TestRangeSpliter, test_mulit_basic)
{
  allocator_.reuse();
  ObTablesHandle tables_handle;
  ObStoreRange split_range;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges, ranges;
  ObArray<ObStoreRange> query_ranges;
  ObSSTable sstable;
  ObPartitionMultiRangeSpliter spliter;
  ObArrayArray<ObStoreRange> ranges_array;
  int64_t total_size = 0;
  int64_t parallel_cnt = 0;

  prepare_sstable(sstable, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable));

  // normal case
  const char* query_rowkey = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "1        var1   MIN    -9     EXIST   CLF\n"
                             "6        var1   MIN    -9     EXIST   CLF\n"
                             "10       var1   MIN    -9     EXIST   CLF\n"
                             "20       var1   MIN    -9     EXIST   CLF\n";
  build_query_range(query_ranges, query_rowkey);
  ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
  parallel_cnt = 3;
  ASSERT_EQ(
      OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
  STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
  ASSERT_EQ(3, ranges_array.count());
  const char* rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "1        var1   MIN    -9     EXIST   CLF\n"
                             "6        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data1);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
  const char* rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "10       var1   MIN    -9     EXIST   CLF\n"
                             "17       var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data2);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
  const char* rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "17        var1   MIN    -9     EXIST   CLF\n"
                             "20        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data3);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));

  // single task case
  ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
  parallel_cnt = 1;
  ASSERT_EQ(
      OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
  STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
  ASSERT_EQ(1, ranges_array.count());
  ASSERT_EQ(true, loop_equal_ranges(query_ranges, ranges_array.at(0)));

  // more task case
  ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
  parallel_cnt = 5;
  ASSERT_EQ(
      OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
  STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
  ASSERT_EQ(5, ranges_array.count());
  const char* rowkey_data21 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   MIN    -9     EXIST   CLF\n"
                              "5        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data21);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
  const char* rowkey_data22 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "5        var1   MIN    -9     EXIST   CLF\n"
                              "6        var1   MIN    -9     EXIST   CLF\n"
                              "10        var1   MIN    -9     EXIST   CLF\n"
                              "11        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data22);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
  const char* rowkey_data23 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "11        var1   MIN    -9     EXIST   CLF\n"
                              "16        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data23);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));
  const char* rowkey_data24 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "16        var1   MIN    -9     EXIST   CLF\n"
                              "19        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data24);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(3)));
  const char* rowkey_data25 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "19       var1   MIN    -9     EXIST   CLF\n"
                              "20       var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data25);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(4)));

  // seperate range case, 5 ranges
  const char* query_rowkey2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   MIN    -9     EXIST   CLF\n"
                              "8        var1   MIN    -9     EXIST   CLF\n"
                              "10       var1   MIN    -9     EXIST   CLF\n"
                              "12       var1   MIN    -9     EXIST   CLF\n"
                              "13       var1   MIN    -9     EXIST   CLF\n"
                              "15       var1   MIN    -9     EXIST   CLF\n"
                              "16       var1   MIN    -9     EXIST   CLF\n"
                              "18       var1   MIN    -9     EXIST   CLF\n"
                              "22       var1   MIN    -9     EXIST   CLF\n"
                              "28       var1   MIN    -9     EXIST   CLF\n";
  build_query_range(query_ranges, query_rowkey2);
  ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
  parallel_cnt = 3;
  ASSERT_EQ(
      OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
  STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
  ASSERT_EQ(3, ranges_array.count());
  const char* rowkey_data31 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   MIN    -9     EXIST   CLF\n"
                              "8        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data31);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
  const char* rowkey_data32 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "10       var1   MIN    -9     EXIST   CLF\n"
                              "12       var1   MIN    -9     EXIST   CLF\n"
                              "13        var1   MIN    -9     EXIST   CLF\n"
                              "15        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data32);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
  const char* rowkey_data33 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "16        var1   MIN    -9     EXIST   CLF\n"
                              "18        var1   MIN    -9     EXIST   CLF\n"
                              "22        var1   MIN    -9     EXIST   CLF\n"
                              "28        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data33);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));

  const char* query_rowkey3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "8        var1   MIN    -9     EXIST   CLF\n"
                              "8        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(query_ranges, query_rowkey3);
  ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
  parallel_cnt = 3;
  ASSERT_EQ(
      OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
  STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
  ASSERT_EQ(1, ranges_array.count());
}

TEST_F(TestRangeSpliter, test_mulit_multi)
{
  allocator_.reuse();
  ObTablesHandle tables_handle;
  ObStoreRange split_range;
  ObRangeSplitInfo range_split_info;
  ObArenaAllocator allocator;
  ObArray<ObStoreRange> split_ranges, ranges;
  ObArray<ObStoreRange> query_ranges;
  ObSSTable sstable, sstable1, sstable2;
  ObPartitionMultiRangeSpliter spliter;
  ObArrayArray<ObStoreRange> ranges_array;
  int64_t total_size = 0;
  int64_t parallel_cnt = 0;

  prepare_sstable(sstable, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable));
  prepare_sstable(sstable1, 5, 5, 20);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable1));
  prepare_sstable(sstable2, 10, 5, 30);
  ASSERT_EQ(OB_SUCCESS, tables_handle.add_table(&sstable2));

  // normal case
  const char* query_rowkey = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "1        var1   MIN    -9     EXIST   CLF\n"
                             "6        var1   MIN    -9     EXIST   CLF\n"
                             "10       var1   MIN    -9     EXIST   CLF\n"
                             "20       var1   MIN    -9     EXIST   CLF\n";
  build_query_range(query_ranges, query_rowkey);
  ASSERT_EQ(OB_SUCCESS, spliter.get_multi_range_size(tables_handle, query_ranges, total_size));
  parallel_cnt = 3;
  ASSERT_EQ(
      OB_SUCCESS, spliter.get_split_multi_ranges(tables_handle, query_ranges, parallel_cnt, allocator, ranges_array));
  STORAGE_LOG(INFO, "finish split ranges array", K(ranges_array));
  ASSERT_EQ(3, ranges_array.count());
  const char* rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "1        var1   MIN    -9     EXIST   CLF\n"
                             "6        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data1);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(0)));
  const char* rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "10       var1   MIN    -9     EXIST   CLF\n"
                             "14       var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data2);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(1)));
  const char* rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                             "14        var1   MIN    -9     EXIST   CLF\n"
                             "20        var1   MIN    -9     EXIST   CLF\n";
  build_query_range(ranges, rowkey_data3);
  ASSERT_EQ(true, loop_equal_ranges(ranges, ranges_array.at(2)));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -rf test_range_spliter.log*");
  OB_LOGGER.set_file_name("test_range_spliter.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
