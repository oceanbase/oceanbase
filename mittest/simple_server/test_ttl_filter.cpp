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

#include "storage/compaction_ttl/ob_ttl_filter.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "unittest/storage/ob_ttl_filter_info_helper.h"
#include "storage/access/ob_mds_filter_mgr.h"

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

class ObTTLFilterTest : public ::testing::Test
{
public:
  ObTTLFilterTest() : schema_rowkey_cnt_(0), cols_desc_() {}

private:
  void build_ttl_filter_info(const int64_t filter_value, const int64_t col_idx, ObTTLFilterInfo &ttl_filter_info);
  void build_filter(const ObTTLFilterInfo &ttl_filter_info, ObTTLFilter &filter);
  void build_filter(const ObTTLFilterInfoArray &ttl_filter_info_array, ObTTLFilter &filter);

  void get_index_table_cols_desc();
  void get_index_table_cols_param(const bool nullable);

  static const int64_t trans_id = 1;
  int64_t schema_rowkey_cnt_;
  ObSEArray<ObColDesc, 8> cols_desc_;
  ObSEArray<ObColumnParam*, 8> cols_param_;
  int64_t col_idxs_[8];
  int64_t sub_col_idxs_[8];
  ObArenaAllocator allocator_;
};

void ObTTLFilterTest::build_ttl_filter_info(const int64_t filter_value,
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

void ObTTLFilterTest::get_index_table_cols_desc()
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

void ObTTLFilterTest::get_index_table_cols_param(const bool nullable)
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

void ObTTLFilterTest::build_filter(const ObTTLFilterInfo &ttl_filter_info, ObTTLFilter &filter)
{
  ObTTLFilterInfoArray ttl_filter_info_array;
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.init_for_first_creation(allocator_));
  ASSERT_EQ(OB_SUCCESS, ttl_filter_info_array.append_with_deep_copy(ttl_filter_info));
  ASSERT_EQ(OB_SUCCESS,
            filter.init_ttl_filter_for_unittest(
                schema_rowkey_cnt_, cols_desc_, &cols_param_, ttl_filter_info_array));
}

void ObTTLFilterTest::build_filter(const ObTTLFilterInfoArray &ttl_filter_info_array,
                                   ObTTLFilter &filter)
{
  ASSERT_EQ(OB_SUCCESS,
            filter.init_ttl_filter_for_unittest(
                schema_rowkey_cnt_, cols_desc_, &cols_param_, ttl_filter_info_array));
}

TEST_F(ObTTLFilterTest, basic)
{
  get_index_table_cols_desc();
  get_index_table_cols_param(false);

  ObMDSFilterMgr mds_filter_mgr(nullptr);
  ObDatumRow row;
  bool filtered = false;

  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 4));
  ObStorageDatum &scn_datum = row.storage_datums_[2];

  ObTTLFilterInfo ttl_filter_info1;
  build_ttl_filter_info(1000, 2, ttl_filter_info1);
  ObTTLFilter ttl_filter1(mds_filter_mgr);
  build_filter(ttl_filter_info1, ttl_filter1);

#define CHECK_TTL_FILTER_ROW(scn_val, ttl_filter, expected) \
  filtered = false; \
  scn_datum.set_int(scn_val); \
  ASSERT_EQ(OB_SUCCESS, ttl_filter.filter(row, filtered)); \
  ASSERT_##expected(filtered);

  CHECK_TTL_FILTER_ROW(-800, ttl_filter1, TRUE);
  CHECK_TTL_FILTER_ROW(-999, ttl_filter1, TRUE);
  CHECK_TTL_FILTER_ROW(-1000, ttl_filter1, TRUE);
  CHECK_TTL_FILTER_ROW(-1001, ttl_filter1, FALSE);
  CHECK_TTL_FILTER_ROW(-1100, ttl_filter1, FALSE);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_ttl_filter.log*");
  OB_LOGGER.set_file_name("test_ttl_filter.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}