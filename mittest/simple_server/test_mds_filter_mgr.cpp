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

#include "unittest/storage/ob_ttl_filter_info_helper.h"
#include "storage/access/ob_mds_filter_mgr.h"
#include "storage/compaction_ttl/ob_base_version_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter.h"

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
      static_cast<int64_t>(ObTTLFilterInfo::ObTTLFilterColType::ROWSCN),
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
                schema_rowkey_cnt_, cols_desc_, &cols_param_, ttl_filter_info_array));
}

void ObMDSFilterMgrTest::build_ttl_filter(const ObTTLFilterInfoArray &ttl_filter_info_array,
                                          ObTTLFilter &filter)
{
  ASSERT_EQ(OB_SUCCESS,
            filter.init_ttl_filter_for_unittest(
                schema_rowkey_cnt_, cols_desc_, &cols_param_, ttl_filter_info_array));
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
    ObTTLFilter *ttl_filter = OB_NEWx(ObTTLFilter, &allocator_, mds_filter_mgr);
    ASSERT_NE(nullptr, ttl_filter);
    create_simple_ttl_filter(*ttl_filter);
    mds_filter_mgr.ttl_filter_ = ttl_filter;
    mds_filter_mgr.build_filter_flags_.add_type(ObMDSFilterFlags::MDSFilterType::TTL_FILTER);
  }

  // Create base version filter if needed
  if (need_base_version_filter) {
    ObBaseVersionFilter *base_version_filter = OB_NEWx(ObBaseVersionFilter, &allocator_, mds_filter_mgr);
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

  ObMDSFilterMgr mds_filter_mgr(nullptr);
  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  // Test base version filter
  ObBaseVersionFilter base_version_filter(mds_filter_mgr);
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

  ObMDSFilterMgr mds_filter_mgr(nullptr);
  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  // Test TTL filter
  ObTTLFilter ttl_filter(mds_filter_mgr);
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
  ObMDSFilterMgr mds_filter_mgr(&allocator_);
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
  ObMDSFilterMgr mds_filter_mgr(&allocator_);
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
  ObMDSFilterMgr mds_filter_mgr(&allocator_);
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