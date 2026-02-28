/**
 * Copyright (c) 2025 OceanBase
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
#define UNITTEST
#include "storage/compaction/ob_partition_merger.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "unittest/storage/ob_ttl_filter_info_helper.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/test_tablet_helper.h"
#include "mtlenv/storage/access/test_merge_basic.h"
#include "storage/compaction/filter/ob_mds_info_compaction_filter.h"
#include "storage/ob_trans_version_skip_index_util.h"
#include "unittest/storage/test_schema_prepare.h"
#include "storage/column_store/ob_co_merge_dag.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace storage;
using namespace unittest;

namespace storage {

class ObMajorWithTTLFilterTest : public TestMergeBasic {
public:
  ObMajorWithTTLFilterTest()
  : TestMergeBasic("test_major_with_ttl_filter"), schema_rowkey_cnt_(1), row_id_seed_(1) {}
  virtual ~ObMajorWithTTLFilterTest() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp();
  void TearDown();

protected:
  // Build a micro block with specified min/max merged snapshot
  int build_micro_block(
    const int64_t min_snapshot,
    const int64_t max_snapshot,
    const int64_t row_cnt);
  int build_macro_block()
  {
    return macro_writer_.try_switch_macro_block();
  }
  int check_macro_and_micro_reuse(
    const ObTablet &tablet,
    ObSSTable &major,
    const int64_t filter_max_version);

  int prepare_ttl_filter(
    const int64_t filter_max_version,
    ObBasicTabletMergeCtx &merge_context);

  void prepare_merge_context(const ObMergeType &merge_type, const bool is_full_merge, const ObVersionRange &trans_version_range, ObBasicTabletMergeCtx &merge_context);

  int64_t schema_rowkey_cnt_;
  ObTabletMergeExecuteDag merge_dag_;
  int64_t row_id_seed_;
  ObCOMergeDagParam param_;
  ObCOMergeDagNet dag_net_;
  static const char *micro_data_[1];
};

const char *ObMajorWithTTLFilterTest::micro_data_[1] = {
      "bigint bigint bigint bigint\n"
      "1      -10    1      1\n"};

void ObMajorWithTTLFilterTest::SetUpTestCase() {
  ObMultiVersionSSTableTest::SetUpTestCase();
  ObClockGenerator::init();
  TestMergeBasic::create_tablet();
}

void ObMajorWithTTLFilterTest::TearDownTestCase() {
  ObMultiVersionSSTableTest::TearDownTestCase();
  ObClockGenerator::destroy();
}

void ObMajorWithTTLFilterTest::SetUp() {
  ObMultiVersionSSTableTest::SetUp();
}

void ObMajorWithTTLFilterTest::TearDown() {
  ObMultiVersionSSTableTest::TearDown();
}

void ObMajorWithTTLFilterTest::prepare_merge_context(const ObMergeType &merge_type, const bool is_full_merge, const ObVersionRange &trans_version_range, ObBasicTabletMergeCtx &merge_context) {
  TestMergeBasic::prepare_merge_context(merge_type, is_full_merge, trans_version_range, &merge_dag_, merge_context, false/*is_delete_insert_merge*/);
}

int ObMajorWithTTLFilterTest::build_micro_block(
  const int64_t min_snapshot,
  const int64_t max_snapshot,
  const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  std::stringstream ss;
  ss << "bigint bigint bigint bigint\n";
  for (int i = 0; i < row_cnt; ++i) {
    int64_t row_id = row_id_seed_++;
    int64_t trans_version = i == 0 ? max_snapshot : MIN(min_snapshot + (row_cnt - 1 - i), max_snapshot);
    ss << row_id << " " << -trans_version << " "<<0<<" " << row_id << "\n";
  }
  std::string micro_str = ss.str();
  ObMockIterator iter;
  if (OB_FAIL(iter.from(micro_str.c_str()))) {
    COMMON_LOG(WARN, "failed to from micro data", K(ret));
  } else {
    append_micro_block(iter);
    if (OB_FAIL(macro_writer_.build_micro_block())) {
      COMMON_LOG(WARN, "failed to build micro block", K(ret));
    }
  }
  return ret;
}

int ObMajorWithTTLFilterTest::check_macro_and_micro_reuse(
  const ObTablet &tablet,
  ObSSTable &major_sstable,
  const int64_t filter_max_version)
{
  int ret = OB_SUCCESS;
  ObIMacroBlockIterator *macro_block_iter = nullptr;
  ObIndexBlockMicroIterator micro_block_iter;
  const blocksstable::ObMicroBlock *micro_block = nullptr;
  ObDatumRange range;
  range.set_whole_range();
  ObMacroBlockDesc macro_desc;
  ObDataMacroBlockMeta block_meta;
  macro_desc.macro_meta_ = &block_meta;
  bool meet_border_macro = false;
  const int64_t cur_major_snapshot = major_sstable.get_snapshot_version();
  if (OB_FAIL(major_sstable.scan_macro_block(
        range,
        tablet.get_rowkey_read_info(),
        allocator_,
        macro_block_iter,
        false, /* reverse scan */
        true, /* need micro info */
        true /* need secondary meta */))) {
    LOG_WARN("Fail to scan macro block", K(ret));
  }
  ObTransVersionSkipIndexInfo skip_index_info;
  while (OB_SUCC(ret) && OB_SUCC(macro_block_iter->get_next_macro_block(macro_desc))) {
    if (OB_ISNULL(macro_desc.macro_meta_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null macro meta", K(ret), K(macro_desc.macro_meta_->val_));
    } else if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
        macro_desc, schema_rowkey_cnt_, skip_index_info))) {
      LOG_WARN("Failed to read min max snapshot", K(ret), K(macro_desc));
    } else if (skip_index_info.min_snapshot_ < filter_max_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should not exist row under filter_version", K(ret), K(micro_block->header_), K(skip_index_info), K(filter_max_version));
    } else if (skip_index_info.max_snapshot_ <= filter_max_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("macro should be filtered", K(ret), K(macro_desc.macro_meta_->val_), K(cur_major_snapshot), K(filter_max_version));
    } else {
      micro_block_iter.reset();
      if (OB_FAIL(micro_block_iter.init(
                  macro_desc,
                  tablet.get_rowkey_read_info(),
                  macro_block_iter->get_micro_index_infos(),
                  macro_block_iter->get_micro_endkeys(),
                  static_cast<ObRowStoreType>(macro_desc.row_store_type_),
                  &major_sstable))) {
          LOG_WARN("Failed to init micro_block_iter", K(ret), K(macro_desc));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(micro_block_iter.next(micro_block))) {
          LOG_INFO("print micro block", K(ret), K(micro_block->header_));
          if (OB_FAIL(ObTransVersionSkipIndexReader::read_min_max_snapshot(
            *micro_block, schema_rowkey_cnt_, skip_index_info))) {
            LOG_WARN("Failed to read min max snapshot", K(ret), K(micro_block));
          } else if (skip_index_info.min_snapshot_ < filter_max_version) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("should not exist row under filter_version", K(ret), K(micro_block->header_), K(skip_index_info), K(filter_max_version));
          }
        } // while
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
      LOG_INFO("print macro meta", K(ret), K(macro_desc.macro_meta_->val_));
      ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
    }
  } // while
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

int ObMajorWithTTLFilterTest::prepare_ttl_filter(
  const int64_t filter_max_version,
  ObBasicTabletMergeCtx &merge_context) {

  int ret = OB_SUCCESS;
  std::stringstream ss;
  ss << "tx_id    commit_ver  filter_type   filter_value   filter_col\n";
  ss << "1 " << 100 << " "<<static_cast<int>(ObTTLFilterInfo::ObTTLFilterColType::ROWSCN)
    <<" " << filter_max_version << " "<<schema_rowkey_cnt_<<"\n";
  std::string micro_str = ss.str();
  ObMdsInfoDistinctMgr mds_info_mgr;
  ObTTLFilterInfoDistinctMgr &ttl_mgr = mds_info_mgr.ttl_filter_info_distinct_mgr_;
  ObTTLFilterInfoArray &mock_array = ttl_mgr.array_;

  TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(allocator_, micro_str.c_str(), mock_array);

  if (OB_UNLIKELY(mock_array.count() != 1
      || !mock_array.at(0)->is_valid()
      || mock_array.at(0)->ttl_filter_value_ != filter_max_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ttl filter info array count", KR(ret), K(mock_array));
  } else if (OB_FAIL(ttl_mgr.distinct_array_.push_back(mock_array.at(0)))) {
    LOG_WARN("unexpected ttl filter info array count", KR(ret), K(mock_array));
  } else {
    ttl_mgr.is_inited_ = true;
    ObTabletID tablet_id(tablet_id_);
    if (OB_FAIL(ObCompactionFilterFactory::alloc_compaction_filter<ObMdsInfoCompactionFilter>(
      allocator_,
      merge_context.filter_ctx_.compaction_filter_,
      allocator_,
      tablet_id,
      schema_rowkey_cnt_,
      merge_context.static_param_.multi_version_column_descs_,
      mds_info_mgr))) {
      LOG_WARN("failed to build compaction filter", KR(ret), K(merge_context));
    }
  }
  return ret;
}

#define BUILD_MICRO_BLOCK(min_snapshot, max_snapshot, row_cnt) \
  ASSERT_EQ(OB_SUCCESS, build_micro_block(min_snapshot, max_snapshot, row_cnt));
#define BUILD_MACRO_BLOCK() \
  ASSERT_EQ(OB_SUCCESS, build_macro_block());

// Test case: all data filtered by TTLFilter
TEST_F(ObMajorWithTTLFilterTest, all_data_filtered)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 90;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 60/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 70/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(40/*min_snapshot*/, 80/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 200;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 20);
}

TEST_F(ObMajorWithTTLFilterTest, all_data_reused)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 90;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(12/*min_snapshot*/, 30/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 60/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 70/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(40/*min_snapshot*/, 80/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 10;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 0);
}

TEST_F(ObMajorWithTTLFilterTest, partial_macro_reused) {
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 10/*row_cnt*/); // filter
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 190/*max_snapshot*/, 2/*row_cnt*/); // open and filter
  BUILD_MICRO_BLOCK(120/*min_snapshot*/, 140/*max_snapshot*/, 5/*row_cnt*/); // reuse
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 99/*max_snapshot*/, 15/*row_cnt*/); // filter
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(100/*min_snapshot*/, 110/*max_snapshot*/, 5/*row_cnt*/); // open
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 100/*max_snapshot*/, 5/*row_cnt*/); // filter
  BUILD_MICRO_BLOCK(170/*min_snapshot*/, 180/*max_snapshot*/, 5/*row_cnt*/); // reuse
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 300;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  LOG_INFO("merge ctx", K(merge_context));
  prepare_ttl_filter(filter_max_version, merge_context);
  // minor mrege
  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(15, merged_sstable->get_row_count());

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER], 1);
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_NONE], 0);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_REMOVE], 2);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_KEEP], 5);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 30);
}

TEST_F(ObMajorWithTTLFilterTest, co_major_all_data_filtered)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 90;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);
  TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 60/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 70/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(40/*min_snapshot*/, 80/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::COLUMN_ORIENTED_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 200;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  TestMergeBasic::prepare_co_major_merge_context(
    MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, &merge_dag_, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  LOG_INFO("merge ctx", K(merge_context));
  prepare_ttl_filter(filter_max_version, merge_context);

  ObTableHandleV2 table_handle;
  TestMergeBasic::co_major_merge(local_arena_, ObCOMergeTestType::NORMAL, merge_context, 0, 0, 2);
  ASSERT_EQ(1, merge_context.merged_cg_tables_handle_.get_count());
  merge_context.merged_cg_tables_handle_.get_table(0, table_handle);
  ObSSTable *merged_sstable = static_cast<ObSSTable *>(table_handle.get_table());

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 20);
}

TEST_F(ObMajorWithTTLFilterTest, co_major_all_data_reused)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 90;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);
  TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(12/*min_snapshot*/, 30/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 60/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 70/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(40/*min_snapshot*/, 80/*max_snapshot*/, 5/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::COLUMN_ORIENTED_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 10;

  TestMergeBasic::prepare_co_major_merge_context(
    MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, &merge_dag_, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  LOG_INFO("merge ctx", K(merge_context));
  prepare_ttl_filter(filter_max_version, merge_context);

  ObTableHandleV2 table_handle;
  TestMergeBasic::co_major_merge(local_arena_, ObCOMergeTestType::NORMAL, merge_context, 0, 0, 2);
  ASSERT_EQ(2, merge_context.merged_cg_tables_handle_.get_count());
  merge_context.merged_cg_tables_handle_.get_table(0, table_handle);
  ObSSTable *merged_sstable = static_cast<ObSSTable *>(table_handle.get_table());

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 0);
}

TEST_F(ObMajorWithTTLFilterTest, co_major_partial_macro_reused) {
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);
  TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 10/*row_cnt*/); // filter
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 190/*max_snapshot*/, 2/*row_cnt*/); // open
  BUILD_MICRO_BLOCK(120/*min_snapshot*/, 140/*max_snapshot*/, 5/*row_cnt*/); // reuse
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 99/*max_snapshot*/, 15/*row_cnt*/); // filter
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(100/*min_snapshot*/, 110/*max_snapshot*/, 5/*row_cnt*/); // open
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 100/*max_snapshot*/, 5/*row_cnt*/); // filter
  BUILD_MICRO_BLOCK(170/*min_snapshot*/, 180/*max_snapshot*/, 5/*row_cnt*/); // reuse
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::COLUMN_ORIENTED_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 300;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  TestMergeBasic::prepare_co_major_merge_context(
    MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, &merge_dag_, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  LOG_INFO("merge ctx", K(merge_context));
  prepare_ttl_filter(filter_max_version, merge_context);

  ObTableHandleV2 table_handle;
  TestMergeBasic::co_major_merge(local_arena_, ObCOMergeTestType::NORMAL, merge_context, 0, 0, 2);
  ASSERT_EQ(2, merge_context.merged_cg_tables_handle_.get_count());
  merge_context.merged_cg_tables_handle_.get_table(0, table_handle);
  ObSSTable *merged_sstable = static_cast<ObSSTable *>(table_handle.get_table());

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER], 1);
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_NONE], 0);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_REMOVE], 2);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_KEEP], 5);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 30);
}

// Test case: filter_version equals min - should OPEN and filter some rows
TEST_F(ObMajorWithTTLFilterTest, boundary_filter_equals_min)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 100;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 80/*max_snapshot*/, 10/*row_cnt*/); // filter_version = min, should OPEN
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 150;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 50; // equals min

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_GT(merged_sstable->get_row_count(), 0); // some rows filtered, some retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 1);
  ASSERT_GT(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_REMOVE], 0);
}

// Test case: filter_version equals max - should FILTER entire block
TEST_F(ObMajorWithTTLFilterTest, boundary_filter_equals_max)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 100;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 80/*max_snapshot*/, 10/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 150;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 80; // equals max, should FILTER

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(0, merged_sstable->get_row_count()); // all rows filtered

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 1);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 10);
}

// Test case: filter_version < min - should REUSE
TEST_F(ObMajorWithTTLFilterTest, boundary_filter_less_than_min)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 100;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 80/*max_snapshot*/, 10/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 150;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 40; // < min, should REUSE

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(10, merged_sstable->get_row_count()); // all rows reused

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 1);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 0);
}

// Test case: filter_version > max - should FILTER
TEST_F(ObMajorWithTTLFilterTest, boundary_filter_greater_than_max)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 100;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 80/*max_snapshot*/, 10/*row_cnt*/);
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 150;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 90; // > max, should FILTER

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(0, merged_sstable->get_row_count()); // all rows filtered

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 1);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 10);
}

// Test case: macro block with all micro blocks need OPEN
TEST_F(ObMajorWithTTLFilterTest, macro_with_all_open_micros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 150/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(80/*min_snapshot*/, 140/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(95/*min_snapshot*/, 160/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_GT(merged_sstable->get_row_count(), 0); // some rows filtered, some retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 3);
}

// Test case: macro block with FILTER and REUSE micro blocks
TEST_F(ObMajorWithTTLFilterTest, macro_with_filter_and_reuse_micros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 5/*row_cnt*/);  // FILTER
  BUILD_MICRO_BLOCK(150/*min_snapshot*/, 170/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 50/*max_snapshot*/, 5/*row_cnt*/);  // FILTER
  BUILD_MICRO_BLOCK(180/*min_snapshot*/, 190/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(10, merged_sstable->get_row_count()); // 10 rows filtered, 10 rows retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 10);
}

// Test case: macro block with all three types of micro blocks
TEST_F(ObMajorWithTTLFilterTest, macro_with_all_three_types_micros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 8/*row_cnt*/);   // FILTER
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 150/*max_snapshot*/, 6/*row_cnt*/);  // OPEN
  BUILD_MICRO_BLOCK(150/*min_snapshot*/, 170/*max_snapshot*/, 4/*row_cnt*/); // REUSE
  BUILD_MICRO_BLOCK(80/*min_snapshot*/, 140/*max_snapshot*/, 7/*row_cnt*/);  // OPEN
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 50/*max_snapshot*/, 5/*row_cnt*/);   // FILTER
  BUILD_MICRO_BLOCK(180/*min_snapshot*/, 190/*max_snapshot*/, 3/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(9, merged_sstable->get_row_count()); // 24 rows filtered, 9 rows retained (1+4+1+3)

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 13);
}

// Test case: multiple consecutive OPEN macro blocks
TEST_F(ObMajorWithTTLFilterTest, multiple_consecutive_open_macros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 150/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(80/*min_snapshot*/, 140/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(95/*min_snapshot*/, 160/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(85/*min_snapshot*/, 145/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(92/*min_snapshot*/, 155/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_GT(merged_sstable->get_row_count(), 0); // some rows filtered, some retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN], 3);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 5);
}

// Test case: alternating FILTER and REUSE macro blocks
TEST_F(ObMajorWithTTLFilterTest, alternating_filter_and_reuse_macros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 30/*max_snapshot*/, 5/*row_cnt*/); // FILTER
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 40/*max_snapshot*/, 5/*row_cnt*/); // FILTER
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(150/*min_snapshot*/, 170/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MICRO_BLOCK(160/*min_snapshot*/, 180/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(15/*min_snapshot*/, 35/*max_snapshot*/, 5/*row_cnt*/); // FILTER
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(155/*min_snapshot*/, 175/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(15, merged_sstable->get_row_count()); // 15 rows filtered, 15 rows reused

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_NONE], 2);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 15);
}

// Test case: alternating OPEN and REUSE macro blocks
TEST_F(ObMajorWithTTLFilterTest, alternating_open_and_reuse_macros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 150/*max_snapshot*/, 5/*row_cnt*/); // OPEN, filter 4 rows
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(160/*min_snapshot*/, 180/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(85/*min_snapshot*/, 145/*max_snapshot*/, 5/*row_cnt*/); // OPEN, filter 4 rows
  BUILD_MACRO_BLOCK();

  BUILD_MICRO_BLOCK(165/*min_snapshot*/, 185/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(12, merged_sstable->get_row_count()); // 10 rows from OPEN blocks, 10 rows reused

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_NONE], 2);
}

// Test case: micro blocks with min = max (single version)
TEST_F(ObMajorWithTTLFilterTest, single_version_micro_blocks)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 50/*max_snapshot*/, 5/*row_cnt*/);  // FILTER: max <= filter
  BUILD_MICRO_BLOCK(100/*min_snapshot*/, 100/*max_snapshot*/, 5/*row_cnt*/); // FILTER: max <= filter
  BUILD_MICRO_BLOCK(150/*min_snapshot*/, 150/*max_snapshot*/, 5/*row_cnt*/); // REUSE: min > filter
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(5, merged_sstable->get_row_count()); // 10 rows filtered, 5 rows reused

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 1);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 10);
}

// Test case: very large version range in micro blocks
TEST_F(ObMajorWithTTLFilterTest, very_large_version_range)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 10000;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(10/*min_snapshot*/, 5000/*max_snapshot*/, 10/*row_cnt*/); // filter
  BUILD_MICRO_BLOCK(6000/*min_snapshot*/, 9000/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 12000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 5000;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(merged_sstable->get_row_count(), 5); // large range, some rows filtered, some retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 1);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 1);
}

// Test case: single row micro blocks with different operations
TEST_F(ObMajorWithTTLFilterTest, single_row_micro_blocks)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(20/*min_snapshot*/, 20/*max_snapshot*/, 1/*row_cnt*/);  // FILTER
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 110/*max_snapshot*/, 2/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(150/*min_snapshot*/, 150/*max_snapshot*/, 1/*row_cnt*/); // REUSE
  BUILD_MICRO_BLOCK(95/*min_snapshot*/, 115/*max_snapshot*/, 2/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(30/*min_snapshot*/, 30/*max_snapshot*/, 1/*row_cnt*/);  // FILTER
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(3, merged_sstable->get_row_count()); // 2 rows filtered, 3 rows (2 OPEN + 1 REUSE)

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 1);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 2);
}

// Test case: CO major with all OPEN micro blocks in one macro
TEST_F(ObMajorWithTTLFilterTest, co_major_macro_with_all_open_micros)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);
  TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(90/*min_snapshot*/, 150/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(80/*min_snapshot*/, 140/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MICRO_BLOCK(95/*min_snapshot*/, 160/*max_snapshot*/, 5/*row_cnt*/); // OPEN
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::COLUMN_ORIENTED_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  TestMergeBasic::prepare_co_major_merge_context(
    MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, &merge_dag_, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObTableHandleV2 table_handle;
  TestMergeBasic::co_major_merge(local_arena_, ObCOMergeTestType::NORMAL, merge_context, 0, 0, 2);
  ASSERT_EQ(2, merge_context.merged_cg_tables_handle_.get_count());
  merge_context.merged_cg_tables_handle_.get_table(0, table_handle);
  ObSSTable *merged_sstable = static_cast<ObSSTable *>(table_handle.get_table());
  ASSERT_GT(merged_sstable->get_row_count(), 0); // some rows filtered, some retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 3);
}

// Test case: CO major with single version micro blocks
TEST_F(ObMajorWithTTLFilterTest, co_major_single_version_micro_blocks)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObCOTabletMergeCtx merge_context(dag_net_, param_, allocator_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 200;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);
  TestSchemaPrepare::add_rowkey_and_each_column_group(allocator_, table_schema_);

  reset_writer(snapshot_version, MAJOR_MERGE);
  BUILD_MICRO_BLOCK(50/*min_snapshot*/, 50/*max_snapshot*/, 5/*row_cnt*/);  // FILTER
  BUILD_MICRO_BLOCK(100/*min_snapshot*/, 100/*max_snapshot*/, 5/*row_cnt*/); // FILTER
  BUILD_MICRO_BLOCK(150/*min_snapshot*/, 150/*max_snapshot*/, 5/*row_cnt*/); // REUSE
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::COLUMN_ORIENTED_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 250;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  TestMergeBasic::prepare_co_major_merge_context(
    MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, &merge_dag_, merge_context);
  merge_context.static_param_.co_static_param_.co_major_merge_strategy_.set(false/*build_all_cg_only*/, false/*only_use_row_store*/);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObTableHandleV2 table_handle;
  TestMergeBasic::co_major_merge(local_arena_, ObCOMergeTestType::NORMAL, merge_context, 0, 0, 1);
  ASSERT_EQ(1, merge_context.merged_cg_tables_handle_.get_count());
  merge_context.merged_cg_tables_handle_.get_table(0, table_handle);
  ObSSTable *merged_sstable = static_cast<ObSSTable *>(table_handle.get_table());
  ASSERT_EQ(5, merged_sstable->get_row_count()); // 10 rows filtered, 5 rows reused

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 2);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 1);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 10);
}

// Test case: many small micro blocks
TEST_F(ObMajorWithTTLFilterTest, many_small_micro_blocks)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 20000;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  // Create many small micro blocks with different version ranges
  for (int i = 1; i <= 20; ++i) {
    int64_t base_version = i * 10;
    if (base_version < 100) {
      BUILD_MICRO_BLOCK(base_version/*min*/, base_version + 5/*max*/, 2/*row_cnt*/); // FILTER
    } else if (base_version == 100) {
      BUILD_MICRO_BLOCK(base_version/*min*/, base_version + 5/*max*/, 2/*row_cnt*/); // OPEN, filter 1 row
    } else {
      BUILD_MICRO_BLOCK(base_version/*min*/, base_version + 5/*max*/, 2/*row_cnt*/); // REUSE
    }
  }
  BUILD_MACRO_BLOCK();
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 25000;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 100;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_EQ(21, merged_sstable->get_row_count()); // 20 rows filtered, 20 rows reused

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 9);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 1);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 10);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 18);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_KEEP], 1);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_REMOVE], 1);
}

// Test case: large macro blocks with complex version pattern
TEST_F(ObMajorWithTTLFilterTest, large_macro_blocks_with_complex_pattern)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);
  ObPartitionMajorMerger merger(local_arena_, merge_context.static_param_);

  ObTableHandleV2 handle1;
  int64_t snapshot_version = 500;
  prepare_table_schema(micro_data_, schema_rowkey_cnt_, ObScnRange(), snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);

  // Macro 1: Complex mix
  BUILD_MICRO_BLOCK(10/*min*/, 50/*max*/, 15/*row_cnt*/);   // FILTER
  BUILD_MICRO_BLOCK(180/*min*/, 220/*max*/, 10/*row_cnt*/); // OPEN, filter 9 rows
  BUILD_MICRO_BLOCK(300/*min*/, 350/*max*/, 8/*row_cnt*/);  // REUSE
  BUILD_MICRO_BLOCK(190/*min*/, 230/*max*/, 12/*row_cnt*/); // OPEN, filter 11 rows
  BUILD_MICRO_BLOCK(20/*min*/, 60/*max*/, 10/*row_cnt*/);   // FILTER
  BUILD_MACRO_BLOCK();

  // Macro 2: Another complex mix
  BUILD_MICRO_BLOCK(195/*min*/, 235/*max*/, 14/*row_cnt*/); // OPEN, filter 6 rows
  BUILD_MICRO_BLOCK(310/*min*/, 360/*max*/, 9/*row_cnt*/);  // REUSE
  BUILD_MICRO_BLOCK(30/*min*/, 70/*max*/, 11/*row_cnt*/);   // FILTER
  BUILD_MICRO_BLOCK(200/*min*/, 240/*max*/, 13/*row_cnt*/); // OPEN, filter 1 row
  BUILD_MACRO_BLOCK();

  // Macro 3: Edge cases
  BUILD_MICRO_BLOCK(200/*min*/, 200/*max*/, 5/*row_cnt*/);  // FILTER (single version = filter)
  BUILD_MICRO_BLOCK(201/*min*/, 201/*max*/, 5/*row_cnt*/);  // REUSE (single version > filter)
  BUILD_MICRO_BLOCK(190/*min*/, 250/*max*/, 20/*row_cnt*/); // OPEN, filter 11 rows
  BUILD_MACRO_BLOCK();

  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 600;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  const int64_t filter_max_version = 200;

  prepare_merge_context(MAJOR_MERGE, false/*is_full_merge*/, trans_version_range, merge_context);
  prepare_ttl_filter(filter_max_version, merge_context);

  ObSSTable *merged_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, merger.merge_partition(merge_context, 0));
  build_sstable(merge_context, merged_sstable);
  ASSERT_GT(merged_sstable->get_row_count(), 0); // complex pattern, some rows filtered, some retained

  ASSERT_EQ(OB_SUCCESS, check_macro_and_micro_reuse(*merge_context.get_tablet(), *merged_sstable, filter_max_version));
  const ObICompactionFilter::ObFilterStatistics &filter_statistics = merge_context.filter_ctx_.filter_statistics_;
  LOG_INFO("filter statistics", K(filter_statistics));
  ASSERT_EQ(filter_statistics.macro_cnt_[ObBlockOp::OP_OPEN], 3);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_OPEN], 5);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_FILTER], 4);
  ASSERT_EQ(filter_statistics.micro_cnt_[ObBlockOp::OP_NONE], 3);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_KEEP], 31);
  ASSERT_EQ(filter_statistics.row_cnt_[ObICompactionFilter::ObFilterRet::FILTER_RET_REMOVE], 38);
  ASSERT_EQ(filter_statistics.filter_block_row_cnt_, 41);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv) {
  system("rm -rf test_major_with_filter.log*");
  OB_LOGGER.set_file_name("test_major_with_filter.log");
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
