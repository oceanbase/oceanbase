/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public

#include "unittest/storage/ob_ttl_filter_info_helper.h"
#include "unittest/storage/ob_truncate_info_helper.h"
#include "storage/access/ob_mds_filter_mgr.h"
#include "storage/compaction_ttl/ob_base_version_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/truncate_info/ob_truncate_info_array.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::storage;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;

class ObMDSFilterMgrTest : public ::testing::Test
{
public:
  ObMDSFilterMgrTest() : schema_rowkey_cnt_(0), cols_desc_() {}

private:
  void get_index_table_cols_desc();
  void get_index_table_cols_param(const bool nullable);

  // Helper functions for building filters
  void build_ttl_filter_info(const int64_t filter_value, const int64_t col_idx, ObTTLFilterInfo &ttl_filter_info);
  void build_ttl_filter(const ObTTLFilterInfo &ttl_filter_info, ObTTLFilter &filter);
  void build_ttl_filter(const ObTTLFilterInfoArray &ttl_filter_info_array, ObTTLFilter &filter);
  void build_base_version_filter(const int64_t base_version, ObBaseVersionFilter &filter);
  // Helper function to create a simple TTL filter for testing
  void create_simple_ttl_filter(ObTTLFilter &filter);
  // Helper function to create a simple base version filter for testing
  void create_simple_base_version_filter(ObBaseVersionFilter &filter);
  // Helper function to initialize MDSFilterMgr for unittest
  void init_mds_filter_mgr_for_unittest(ObMDSFilterMgr &mds_filter_mgr,
                                       const bool need_ttl_filter,
                                       const bool need_base_version_filter);

  static const int64_t trans_id = 1;
  int64_t schema_rowkey_cnt_;
  ObSEArray<ObColDesc, 8> cols_desc_;
  ObSEArray<ObColumnParam *, 8> cols_param_;
  int64_t col_idxs_[8];
  int64_t sub_col_idxs_[8];
  ObArenaAllocator allocator_;
};

void ObMDSFilterMgrTest::get_index_table_cols_desc()
{
  cols_desc_.reuse();
  schema_rowkey_cnt_ = 2;
  share::schema::ObColDesc tmp_col_desc;
  ObObjMeta meta_type;
  meta_type.set_int();
  tmp_col_desc.col_type_ = meta_type;
  tmp_col_desc.col_order_ = ObOrderType::ASC;
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
  ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
}

void ObMDSFilterMgrTest::get_index_table_cols_param(const bool nullable)
{
  cols_param_.reuse();
  ObColumnParam *column = nullptr;
  for (int64_t i = 0; i < schema_rowkey_cnt_ + 2 + 2; ++i) {
    column = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObTableParam::alloc_column(allocator_, column));
    column->set_nullable_for_write(nullable);
    if (i < schema_rowkey_cnt_) {
      column->set_column_id(i + 16);
    } else if (i < schema_rowkey_cnt_ + 2) {
      column->set_column_id(OB_MIN_SHADOW_COLUMN_ID + i - schema_rowkey_cnt_);
    } else {
      column->set_column_id(OB_HIDDEN_TRANS_VERSION_COLUMN_ID);
    }
    ASSERT_EQ(OB_SUCCESS, cols_param_.push_back(column));
  }
}

void ObMDSFilterMgrTest::build_ttl_filter_info(const int64_t filter_value,
                                               const int64_t col_idx,
                                               ObTTLFilterInfo &ttl_filter_info)
{
  TTLFilterInfoHelper::mock_ttl_filter_info(
      trans_id,
      1,
      static_cast<int64_t>(ObTTLFilterColType::ROWSCN),
      filter_value,
      col_idx,
      ttl_filter_info);
}

void ObMDSFilterMgrTest::build_ttl_filter(const ObTTLFilterInfo &ttl_filter_info, ObTTLFilter &filter)
{
  ObTTLFilterInfoArray ttl_filter_info_array;
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.init_for_first_creation(allocator_));
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.append_with_deep_copy(ttl_filter_info));
  ASSERT_EQ(OB_SUCCESS,
            filter.init_ttl_filter_for_unittest(
                schema_rowkey_cnt_, cols_desc_, ttl_filter_info_array));
}

void ObMDSFilterMgrTest::build_ttl_filter(const ObTTLFilterInfoArray &ttl_filter_info_array,
                                          ObTTLFilter &filter)
{
  ASSERT_EQ(OB_SUCCESS,
            filter.init_ttl_filter_for_unittest(
                schema_rowkey_cnt_, cols_desc_, ttl_filter_info_array));
}

void ObMDSFilterMgrTest::build_base_version_filter(const int64_t base_version, ObBaseVersionFilter &filter)
{
  ASSERT_EQ(OB_SUCCESS, filter.init_base_version_filter_for_unittest(schema_rowkey_cnt_, base_version));
}

// Helper function to create a simple TTL filter for testing
void ObMDSFilterMgrTest::create_simple_ttl_filter(ObTTLFilter &filter)
{
  ObTTLFilterInfo ttl_filter_info;
  build_ttl_filter_info(800, 2, ttl_filter_info);  // Use 800 as TTL filter value
  build_ttl_filter(ttl_filter_info, filter);
}

// Helper function to create a simple base version filter for testing
void ObMDSFilterMgrTest::create_simple_base_version_filter(ObBaseVersionFilter &filter)
{
  build_base_version_filter(1200, filter);  // Use 1200 as base version filter value
}

// Helper function to initialize MDSFilterMgr for unittest
void ObMDSFilterMgrTest::init_mds_filter_mgr_for_unittest(ObMDSFilterMgr &mds_filter_mgr,
                                                          const bool need_ttl_filter,
                                                          const bool need_base_version_filter)
{
  // Create TTL filter if needed
  if (need_ttl_filter) {
    ObTTLFilter *ttl_filter = OB_NEWx(ObTTLFilter, &allocator_, allocator_);
    ASSERT_NE(nullptr, ttl_filter);
    create_simple_ttl_filter(*ttl_filter);
    mds_filter_mgr.ttl_filter_ = ttl_filter;
    mds_filter_mgr.build_filter_flags_.add_type(ObMDSFilterFlags::MDSFilterType::TTL_FILTER);
  }

  // Create base version filter if needed
  if (need_base_version_filter) {
    ObBaseVersionFilter *base_version_filter = OB_NEWx(ObBaseVersionFilter, &allocator_, allocator_);
    ASSERT_NE(nullptr, base_version_filter);
    create_simple_base_version_filter(*base_version_filter);
    mds_filter_mgr.base_version_filter_ = base_version_filter;
    mds_filter_mgr.build_filter_flags_.add_type(ObMDSFilterFlags::MDSFilterType::BASE_VERSION_FILTER);
  }
}

TEST_F(ObMDSFilterMgrTest, base_version_filter)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);

  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  // Test base version filter
  ObBaseVersionFilter base_version_filter(allocator_);
  create_simple_base_version_filter(base_version_filter);

#define CHECK_BASE_VERSION_FILTER_ROW(scn_val, base_version_filter, expected)                      \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, base_version_filter.filter(row, filtered));                                \
  ASSERT_##expected(filtered);

  CHECK_BASE_VERSION_FILTER_ROW(-800, base_version_filter, TRUE);
  CHECK_BASE_VERSION_FILTER_ROW(-999, base_version_filter, TRUE);
  CHECK_BASE_VERSION_FILTER_ROW(-1000, base_version_filter, TRUE);
  CHECK_BASE_VERSION_FILTER_ROW(-1200, base_version_filter, TRUE);
  CHECK_BASE_VERSION_FILTER_ROW(-1201, base_version_filter, FALSE);
  CHECK_BASE_VERSION_FILTER_ROW(-1300, base_version_filter, FALSE);
}

TEST_F(ObMDSFilterMgrTest, ttl_filter)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);

  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  // Test TTL filter
  ObTTLFilter ttl_filter(allocator_);
  create_simple_ttl_filter(ttl_filter);

#define CHECK_TTL_FILTER_ROW(scn_val, ttl_filter, expected)                                        \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, ttl_filter.filter(row, filtered));                                         \
  ASSERT_##expected(filtered);

  CHECK_TTL_FILTER_ROW(-600, ttl_filter, TRUE);
  CHECK_TTL_FILTER_ROW(-799, ttl_filter, TRUE);
  CHECK_TTL_FILTER_ROW(-800, ttl_filter, TRUE);
  CHECK_TTL_FILTER_ROW(-801, ttl_filter, FALSE);
  CHECK_TTL_FILTER_ROW(-900, ttl_filter, FALSE);
}

TEST_F(ObMDSFilterMgrTest, mixed_filters)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);

  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  // Test MDSFilterMgr with both TTL filter and base version filter
  ObMDSFilterMgr mds_filter_mgr(allocator_);
  init_mds_filter_mgr_for_unittest(mds_filter_mgr, true, true);

  // Test filter with check_filter=true, check_version=false (only TTL filter)
#define CHECK_MIXED_FILTER_ROW_CHECK_FILTER(scn_val, expected)                                    \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.filter(row, filtered, true, false));                       \
  ASSERT_##expected(filtered);

  CHECK_MIXED_FILTER_ROW_CHECK_FILTER(-600, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER(-799, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER(-800, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER(-801, FALSE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER(-900, FALSE);

  // Test filter with check_filter=false, check_version=true (only base version filter)
#define CHECK_MIXED_FILTER_ROW_CHECK_VERSION(scn_val, expected)                                   \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.filter(row, filtered, false, true));                       \
  ASSERT_##expected(filtered);

  CHECK_MIXED_FILTER_ROW_CHECK_VERSION(-1000, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION(-1199, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION(-1200, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION(-1201, FALSE);
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION(-1300, FALSE);

  // Test filter with check_filter=true, check_version=true (both filters)
#define CHECK_MIXED_FILTER_ROW_BOTH(scn_val, expected)                                            \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.filter(row, filtered, true, true));                        \
  ASSERT_##expected(filtered);

  CHECK_MIXED_FILTER_ROW_BOTH(-600, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH(-799, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH(-800, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH(-801, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH(-900, TRUE);
}

TEST_F(ObMDSFilterMgrTest, mixed_filters_ttl_only)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);

  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  // Test MDSFilterMgr with only TTL filter (need_base_version_filter = false)
  ObMDSFilterMgr mds_filter_mgr(allocator_);
  init_mds_filter_mgr_for_unittest(mds_filter_mgr, true, false);

  // Test filter with check_filter=true, check_version=false (only TTL filter)
#define CHECK_MIXED_FILTER_ROW_CHECK_FILTER_TTL_ONLY(scn_val, expected)                          \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.filter(row, filtered, true, false));                       \
  ASSERT_##expected(filtered);

  CHECK_MIXED_FILTER_ROW_CHECK_FILTER_TTL_ONLY(-600, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER_TTL_ONLY(-799, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER_TTL_ONLY(-800, TRUE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER_TTL_ONLY(-801, FALSE);
  CHECK_MIXED_FILTER_ROW_CHECK_FILTER_TTL_ONLY(-900, FALSE);

  // Test filter with check_filter=false, check_version=true (no base version filter)
#define CHECK_MIXED_FILTER_ROW_CHECK_VERSION_TTL_ONLY(scn_val, expected)                         \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.filter(row, filtered, false, true));                       \
  ASSERT_##expected(filtered);

  // Since there's no base version filter, all values should not be filtered
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION_TTL_ONLY(-1000, FALSE);
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION_TTL_ONLY(-1200, FALSE);
  CHECK_MIXED_FILTER_ROW_CHECK_VERSION_TTL_ONLY(-1300, FALSE);

  // Test filter with check_filter=true, check_version=true (only TTL filter)
#define CHECK_MIXED_FILTER_ROW_BOTH_TTL_ONLY(scn_val, expected)                                  \
  filtered = false;                                                                                \
  scn_datum.set_int(scn_val);                                                                      \
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.filter(row, filtered, true, true));                        \
  ASSERT_##expected(filtered);

  CHECK_MIXED_FILTER_ROW_BOTH_TTL_ONLY(-600, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH_TTL_ONLY(-799, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH_TTL_ONLY(-800, TRUE);
  CHECK_MIXED_FILTER_ROW_BOTH_TTL_ONLY(-801, FALSE);
  CHECK_MIXED_FILTER_ROW_BOTH_TTL_ONLY(-900, FALSE);
}

TEST_F(ObMDSFilterMgrTest, check_filter_row_complete)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);

  ObDatumRow row;
  bool complete = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));

  // Test MDSFilterMgr with both TTL filter and base version filter
  ObMDSFilterMgr mds_filter_mgr(allocator_);
  init_mds_filter_mgr_for_unittest(mds_filter_mgr, true, true);

  // Test with complete row (all columns have values)
  for (int64_t i = 0; i < row.count_; ++i) {
    row.storage_datums_[i].set_int(100);
  }
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.check_filter_row_complete(row, complete));
  ASSERT_TRUE(complete);

  // Test with incomplete row (some columns are NOP)
  row.storage_datums_[2].set_nop();
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.check_filter_row_complete(row, complete));
  ASSERT_FALSE(complete);

  // Test with incomplete row (some columns are NULL)
  row.storage_datums_[2].set_null();
  ASSERT_EQ(OB_SUCCESS, mds_filter_mgr.check_filter_row_complete(row, complete));
  ASSERT_FALSE(complete);
}

// Test build_distinct_array: mock array_ directly and verify distinct result
TEST_F(ObMDSFilterMgrTest, build_distinct_array_ttl_filter_info)
{
  ObTTLFilterInfoDistinctMgr ttl_mgr;
  common::ObVersionRange read_version_range(0, 1000);

  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.init_for_first_creation(allocator_));

  // ROWSCN: replace when filter_value is larger (commit_version not checked)
  // Mock TTL filter infos: same col_idx=2, (commit_version=100, value=800) and (commit_version=200, value=900)
  ObTTLFilterInfo info1, info2;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id, 100, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 800, 2, info1);
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 1, 200, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 900, 2, info2);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info1));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info2));

  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(1, ttl_mgr.distinct_array_.count());
  ASSERT_EQ(200, ttl_mgr.distinct_array_.at(0)->commit_version_);
  ASSERT_EQ(900, ttl_mgr.distinct_array_.at(0)->ttl_filter_value_);
  ASSERT_EQ(2, ttl_mgr.distinct_array_.at(0)->ttl_filter_col_idx_);

  // Test different col_idx: both should be in distinct array
  ttl_mgr.distinct_array_.reuse();
  ObTTLFilterInfo info3;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 2, 150, static_cast<int64_t>(ObTTLFilterColType::TIMESTAMP), 700, 3, info3);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info3));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(2, ttl_mgr.distinct_array_.count());

  // ROWSCN: commit_version larger but filter_value smaller -> no replace (only filter_value matters)
  ttl_mgr.array_.reset();
  ttl_mgr.distinct_array_.reuse();
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.init_for_first_creation(allocator_));
  ObTTLFilterInfo info_commit_larger;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id, 100, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 800, 2, info_commit_larger);
  ObTTLFilterInfo info_value_smaller;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 1, 200, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 700, 2, info_value_smaller);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_commit_larger));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_value_smaller));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(1, ttl_mgr.distinct_array_.count());
  ASSERT_EQ(100, ttl_mgr.distinct_array_.at(0)->commit_version_);
  ASSERT_EQ(800, ttl_mgr.distinct_array_.at(0)->ttl_filter_value_);

  // ROWSCN: filter_value larger but commit_version smaller -> replace (only filter_value matters for ROWSCN)
  ttl_mgr.array_.reset();
  ttl_mgr.distinct_array_.reuse();
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.init_for_first_creation(allocator_));
  ObTTLFilterInfo info_first;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id, 200, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 800, 2, info_first);
  ObTTLFilterInfo info_value_larger;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 2, 150, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 900, 2, info_value_larger);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_first));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_value_larger));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(1, ttl_mgr.distinct_array_.count());
  ASSERT_EQ(150, ttl_mgr.distinct_array_.at(0)->commit_version_);
  ASSERT_EQ(900, ttl_mgr.distinct_array_.at(0)->ttl_filter_value_);

  // Non-ROWSCN (TIMESTAMP): replace requires BOTH commit_version and filter_value larger
  // filter_value larger but commit_version smaller -> no replace
  ttl_mgr.array_.reset();
  ttl_mgr.distinct_array_.reuse();
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.init_for_first_creation(allocator_));
  ObTTLFilterInfo info_ts_first;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id, 200, static_cast<int64_t>(ObTTLFilterColType::TIMESTAMP), 800, 2, info_ts_first);
  ObTTLFilterInfo info_ts_value_larger;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 2, 150, static_cast<int64_t>(ObTTLFilterColType::TIMESTAMP), 900, 2, info_ts_value_larger);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_ts_first));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_ts_value_larger));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(2, ttl_mgr.distinct_array_.count());
  ASSERT_EQ(150, ttl_mgr.distinct_array_.at(0)->commit_version_);
  ASSERT_EQ(900, ttl_mgr.distinct_array_.at(0)->ttl_filter_value_);
  ASSERT_EQ(200, ttl_mgr.distinct_array_.at(1)->commit_version_);
  ASSERT_EQ(800, ttl_mgr.distinct_array_.at(1)->ttl_filter_value_);

  // Non-ROWSCN: both larger -> replace
  ttl_mgr.array_.reset();
  ttl_mgr.distinct_array_.reuse();
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.init_for_first_creation(allocator_));
  ObTTLFilterInfo info_ts_old;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id, 100, static_cast<int64_t>(ObTTLFilterColType::TIMESTAMP), 800, 2, info_ts_old);
  ObTTLFilterInfo info_ts_new;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 1, 200, static_cast<int64_t>(ObTTLFilterColType::TIMESTAMP), 900, 2, info_ts_new);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_ts_old));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info_ts_new));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(1, ttl_mgr.distinct_array_.count());
  ASSERT_EQ(200, ttl_mgr.distinct_array_.at(0)->commit_version_);
  ASSERT_EQ(900, ttl_mgr.distinct_array_.at(0)->ttl_filter_value_);

  // Test version range filter: commit_version outside (base_version, snapshot_version] should be skipped
  ttl_mgr.array_.reset();
  ttl_mgr.distinct_array_.reuse();
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.init_for_first_creation(allocator_));
  ObTTLFilterInfo info4, info5;
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id, 50, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 600, 2, info4);
  TTLFilterInfoHelper::mock_ttl_filter_info(trans_id + 1, 500, static_cast<int64_t>(ObTTLFilterColType::ROWSCN), 700, 2, info5);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info4));
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.array_.append_with_deep_copy(info5));
  common::ObVersionRange range2(100, 1000);
  ASSERT_EQ(OB_SUCCESS, ttl_mgr.build_distinct_array(range2, false));
  ASSERT_EQ(1, ttl_mgr.distinct_array_.count());
  ASSERT_EQ(500, ttl_mgr.distinct_array_.at(0)->commit_version_);
}

// Test build_distinct_array for truncate info
TEST_F(ObMDSFilterMgrTest, build_distinct_array_truncate_info)
{
  ObTruncateInfoDistinctMgr truncate_mgr;
  common::ObVersionRange read_version_range(0, 1000);

  ASSERT_EQ(OB_SUCCESS, truncate_mgr.array_.init_for_first_creation(allocator_));

  // Mock truncate infos with same partition: (commit_version=100, schema=1) and (commit_version=200, schema=2)
  // When equal, the newer one should replace the older
  ObTruncateInfo info1;
  TruncateInfoHelper::mock_truncate_info(allocator_, trans_id, 1, 100, info1);
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, info1.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, info1.truncate_part_));

  ObTruncateInfo info2;
  TruncateInfoHelper::mock_truncate_info(allocator_, trans_id + 1, 2, 200, info2);
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 100, 200, info2.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, info2.truncate_part_));

  ASSERT_EQ(OB_SUCCESS, truncate_mgr.array_.append_with_deep_copy(info1));
  ASSERT_EQ(OB_SUCCESS, truncate_mgr.array_.append_with_deep_copy(info2));

  ASSERT_EQ(OB_SUCCESS, truncate_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(1, truncate_mgr.distinct_array_.count());
  ASSERT_EQ(200, truncate_mgr.distinct_array_.at(0)->commit_version_);
  ASSERT_EQ(2, truncate_mgr.distinct_array_.at(0)->schema_version_);

  // Test different partitions: both should be in distinct array
  truncate_mgr.distinct_array_.reuse();
  ObTruncateInfo info3;
  TruncateInfoHelper::mock_truncate_info(allocator_, trans_id + 2, 1, 150, info3);
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_truncate_partition(allocator_, 300, 400, info3.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::mock_part_key_idxs(allocator_, 1, info3.truncate_part_));
  ASSERT_EQ(OB_SUCCESS, truncate_mgr.array_.append_with_deep_copy(info3));

  ASSERT_EQ(OB_SUCCESS, truncate_mgr.build_distinct_array(read_version_range, false));
  ASSERT_EQ(2, truncate_mgr.distinct_array_.count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_filter_mgr.log*");
  OB_LOGGER.set_file_name("test_mds_filter_mgr.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}