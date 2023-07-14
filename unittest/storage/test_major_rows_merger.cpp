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
#include "storage/compaction/ob_partition_rows_merger.h"
#include "lib/container/ob_se_array.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/test_tablet_helper.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace unittest;
namespace storage
{
namespace mds
{
void *MdsAllocator::alloc(const int64_t size) {
  return ob_malloc(size, "MDS");
}
void MdsAllocator::free(void *ptr) {
  return ob_free(ptr);
}
}
template<typename T>
int prepare_partition_merge_iter(ObMergeParameter &merge_param,
                                 common::ObIAllocator &allocator,
                                 const ObRowStoreType row_store_type,
                                 const int iter_idx,
                                 ObPartitionMergeIter *&merge_iter)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  common::ObArray<share::schema::ObColDesc, common::ObIAllocator &> multi_version_column_ids(common::OB_MAX_COLUMN_NUMBER, allocator);
  multi_version_column_ids.reset();
  if(OB_FAIL(merge_param.merge_schema_->get_multi_version_column_descs(multi_version_column_ids))) {
    STORAGE_LOG(WARN, "get multi version column descs failed", K(ret));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
  } else {
    merge_iter = new (buf) T();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merge_iter->init(merge_param, multi_version_column_ids,
     row_store_type, iter_idx))) {
    STORAGE_LOG(WARN, "failed to init merge iter", K(ret));
  }
  return ret;
}

class ObMajorRowsMergerTest: public ObMultiVersionSSTableTest
{
public:
  ObMajorRowsMergerTest() : ObMultiVersionSSTableTest("test_multi_version_merge") {}
  virtual ~ObMajorRowsMergerTest() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObTabletMergeCtx &merge_context);

  ObStorageSchema table_merge_schema_;
};

void ObMajorRowsMergerTest::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_));
}

void ObMajorRowsMergerTest::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

void ObMajorRowsMergerTest::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void ObMajorRowsMergerTest::TearDown()
{

  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void ObMajorRowsMergerTest::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  ObTabletMergeCtx &merge_context)
{
  bool has_lob = false;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  merge_context.ls_handle_ = ls_handle;

  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  merge_context.tablet_handle_ = tablet_handle;

  table_merge_schema_.reset();
  OK(table_merge_schema_.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  merge_context.schema_ctx_.storage_schema_ = &table_merge_schema_;

  merge_context.is_full_merge_ = is_full_merge;
  merge_context.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  merge_context.param_.merge_type_ = merge_type;
  merge_context.param_.merge_version_ = 0;
  merge_context.param_.ls_id_ = ls_id_;
  merge_context.param_.tablet_id_ = tablet_id_;
  merge_context.sstable_version_range_ = trans_version_range;
  merge_context.param_.report_ = &rs_reporter_;
  merge_context.progressive_merge_num_ = 0;
  const int64_t tables_count = merge_context.tables_handle_.get_count();
  merge_context.scn_range_.start_scn_ = merge_context.tables_handle_.get_table(0)->get_start_scn();
  merge_context.scn_range_.end_scn_ = merge_context.tables_handle_.get_table(tables_count - 1)->get_end_scn();
  merge_context.merge_scn_ = merge_context.scn_range_.end_scn_;

  ASSERT_EQ(OB_SUCCESS, merge_context.init_merge_info());
  ASSERT_EQ(OB_SUCCESS, merge_context.merge_info_.prepare_index_builder(index_desc_));
  index_desc_.major_working_cluster_version_ = DATA_VERSION_4_0_0_0;
}


TEST_F(ObMajorRowsMergerTest, test_compare_func)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "2        var1  -8       MIN      3       2     T_DML_UPDATE EXIST   SCF\n"
      "2        var1  -8       0        3       NOP   T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var1  -6       0        2       2     T_DML_INSERT EXIST   CL\n"
      "3        var1  -8       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "4        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n";
  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObMergeParameter merge_param;
  OK(merge_param.init(merge_context, 0));
  ObPartitionMergeIter *iter_0 = nullptr;
  ObPartitionMergeIter *iter_1 = nullptr;
  prepare_partition_merge_iter<ObPartitionMinorMacroMergeIter>(merge_param, allocator_, FLAT_ROW_STORE, 0, iter_0);
  prepare_partition_merge_iter<ObPartitionMinorMacroMergeIter>(merge_param, allocator_, FLAT_ROW_STORE, 1, iter_1);
  OK(iter_0->next());
  OK(iter_1->next());

  const ObITableReadInfo &read_info = iter_0->get_read_info();
  int64_t cmp_ret;
  ObPartitionMergeLoserTreeItem item_0, item_1;
  item_0.iter_ = iter_0;
  item_0.iter_idx_ = 0;
  item_1.iter_ = iter_1;
  item_0.iter_idx_ = 1;

  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());

  //compare both range
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::ALL_MACRO_NEED_OPEN, cmp_ret);

  OK(item_0.iter_->next());
  OK(item_0.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(1, cmp_ret);
  OK(cmp.compare(item_1, item_0, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);

  OK(item_1.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::LEFT_MACRO_NEED_OPEN, cmp_ret);
  OK(cmp.compare(item_1, item_0, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::RIGHT_MACRO_NEED_OPEN, cmp_ret);

  //compare rowkey and range
  OK(item_0.iter_->open_curr_range(false));
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::RIGHT_MACRO_NEED_OPEN, cmp_ret);
  OK(cmp.compare(item_1, item_0, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::LEFT_MACRO_NEED_OPEN, cmp_ret);

  //compare both rowkey
  OK(item_1.iter_->open_curr_range(false));
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);

  OK(item_0.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(0, cmp_ret);

  OK(item_0.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(1, cmp_ret);
  if (OB_NOT_NULL(iter_0)) {
    iter_0->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_1)) {
    iter_1->~ObPartitionMergeIter();
  }
}

TEST_F(ObMajorRowsMergerTest, single)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  ObMergeParameter merge_param;
  OK(merge_param.init(merge_context, 0));
  ObPartitionMergeIter *iter_0 = nullptr;
  ObPartitionMergeIter *iter_1 = nullptr;
  OK(prepare_partition_merge_iter<ObPartitionMinorMacroMergeIter>(merge_param, allocator_, FLAT_ROW_STORE, 0, iter_0));
  OK(prepare_partition_merge_iter<ObPartitionMinorMacroMergeIter>(merge_param, allocator_, FLAT_ROW_STORE, 1, iter_1));
  iter_0->next();
  iter_1->next();
  ObPartitionMergeLoserTreeItem item_0, item_1;
  item_0.iter_ = iter_0; item_0.iter_idx_ = 0;
  item_1.iter_ = iter_1; item_1.iter_idx_ = 1;
  const ObITableReadInfo &read_info = iter_0->get_read_info();
  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());

  ObPartitionMajorRowsMerger merger(cmp);

  // not init
  ASSERT_EQ(ObPartitionMajorRowsMerger::NOT_INIT, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());

  // 0 player
  ret = merger.init(0, allocator_);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // init twice
  OK(merger.init(1, allocator_));
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.init(1, allocator_);
  ASSERT_EQ(ret, OB_INIT_TWICE);
  ASSERT_EQ(0, merger.count());

  const ObPartitionMergeLoserTreeItem *top;
  //push base_iter
  ASSERT_EQ(OB_SUCCESS, merger.push(item_0));
  ASSERT_EQ(OB_ERR_UNEXPECTED, merger.push(item_0));
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(OB_SUCCESS, merger.top(top));
  ASSERT_EQ(item_0.iter_idx_, top->iter_idx_);
  ASSERT_EQ(OB_SUCCESS, merger.pop());

  //push other iter
  ASSERT_EQ(OB_SUCCESS, merger.push(item_1));
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(OB_SUCCESS, merger.top(top));
  ASSERT_EQ(item_1.iter_idx_, top->iter_idx_);
  ASSERT_EQ(OB_SUCCESS, merger.pop());
  if (OB_NOT_NULL(iter_0)) {
    iter_0->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_1)) {
    iter_1->~ObPartitionMergeIter();
  }
}

TEST_F(ObMajorRowsMergerTest, two_iters)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag\n"
      "1        var1  -8       0        1        1      EXIST\n"
      "8        var1  -8       0        2        2      EXIST\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  DELETE   LF\n"
      "1        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  //prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, merge_context);
  ObMergeParameter merge_param;
  OK(merge_param.init(merge_context, 0));
  ObPartitionMergeIter *iter_0 = nullptr;
  ObPartitionMergeIter *iter_1 = nullptr;
  OK(prepare_partition_merge_iter<ObPartitionMacroMergeIter>(merge_param, allocator_, FLAT_ROW_STORE, 0, iter_0));
  OK(prepare_partition_merge_iter<ObPartitionRowMergeIter>(merge_param, allocator_, FLAT_ROW_STORE, 1, iter_1));
  OK(iter_0->next());
  OK(iter_1->next());
  const ObPartitionMergeLoserTreeItem *top;
  ObPartitionMergeLoserTreeItem item_0, item_1;
  item_0.iter_ = iter_0;
  item_0.iter_idx_ = 0;
  item_1.iter_ = iter_1;
  item_1.iter_idx_ = 1;
  const ObITableReadInfo &read_info = iter_0->get_read_info();
  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());
  ObPartitionMajorRowsMerger merger(cmp);

  //need purge
  OK(merger.init(2, allocator_));
  OK(merger.push(item_0));
  OK(merger.push(item_1));
  ASSERT_EQ(2, merger.count());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_REBUILD, merger.merger_state_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, merger.pop());
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());

  //get purge iter
  OK(merger.top(top));
  ASSERT_EQ(1, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP_REBUILD, merger.merger_state_);
  OK(item_1.iter_->next());
  OK(merger.push_top(item_1));
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_FALSE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(1, top->iter_idx_);
  OK(merger.pop());
  OK(merger.top(top));
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(0, top->iter_idx_);
  OK(merger.pop());
  ASSERT_TRUE(merger.empty());
  if (OB_NOT_NULL(iter_0)) {
    iter_0->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_1)) {
    iter_1->~ObPartitionMergeIter();
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_major_rows_merger.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_major_rows_merger.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
