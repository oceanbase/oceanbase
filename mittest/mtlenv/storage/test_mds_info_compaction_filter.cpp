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

#include "mtlenv/storage/medium_info_common.h"
#include "storage/compaction_ttl/ob_ttl_filter_info_array.h"
#include "storage/tablet/ob_tablet.h"
#include "unittest/storage/ob_ttl_filter_info_helper.h"
#include "storage/compaction/filter/ob_mds_info_compaction_filter.h"
#include "storage/compaction/ob_block_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
using namespace compaction;
namespace storage
{

class TestMdsInfoCompactionFilter : public MediumInfoCommon
{
public:
  TestMdsInfoCompactionFilter()
    : schema_rowkey_cnt_(1),
      cols_desc_(),
      cols_param_()
  {
  }
  virtual ~TestMdsInfoCompactionFilter() = default;

  virtual void SetUp() override
  {
    if (cols_desc_.count() == 0) {
      share::schema::ObColDesc tmp_col_desc;
      ObObjMeta meta_type;
      meta_type.set_int();
      tmp_col_desc.col_type_ = meta_type;
      tmp_col_desc.col_order_ = ObOrderType::ASC;
      ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
      ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
      ASSERT_EQ(OB_SUCCESS, cols_desc_.push_back(tmp_col_desc));
    }
    if (cols_param_.count() == 0) {
      ObColumnParam *column = nullptr;
      for (int64_t i = 0; i < 4; ++i) {
        column = nullptr;
        ASSERT_EQ(OB_SUCCESS, ObTableParam::alloc_column(allocator_, column));
        ASSERT_EQ(OB_SUCCESS, cols_param_.push_back(column));
      }
    }
  }
  int64_t schema_rowkey_cnt_;
  ObSEArray<ObColDesc, 8> cols_desc_;
  ObSEArray<ObColumnParam*, 8> cols_param_;
};

TEST_F(TestMdsInfoCompactionFilter, test_get_ttl_filter_op)
{
  int ret = OB_SUCCESS;

  const char *key_data = "tx_id    commit_ver  filter_type   filter_value   filter_col\n"
                         "4        1000         1           1              1          \n"
                         "3        2000         1           1000           1          \n"
                         "2        3000         1           2000           1          \n"
                         "1        6000         1           4000           1          \n";
  // create tablet
  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTTLFilterInfoArray mock_array;
  ObTTLFilterInfoArray ttl_filter_info_array;

  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(allocator_, key_data, mock_array));
  ASSERT_EQ(4, mock_array.count());

  for (int64_t idx = 0; idx < 4; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*mock_array.at(idx)));
  }
  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  ObMdsInfoDistinctMgr mds_info_mgr;
  ASSERT_EQ(OB_SUCCESS, mds_info_mgr.init(allocator_, *tablet_handle.get_obj(), nullptr, ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false));
  ASSERT_FALSE(mds_info_mgr.is_empty());
  ObMdsInfoCompactionFilter filter;
  ObBlockOp op;

  ASSERT_EQ(OB_SUCCESS, filter.init(allocator_, tablet_id, schema_rowkey_cnt_, cols_desc_, mds_info_mgr));
  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(100, 500, op));
  ASSERT_EQ(ObBlockOp::OP_FILTER, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(500, 1000, op));
  ASSERT_EQ(ObBlockOp::OP_FILTER, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(1000, 4000, op));
  ASSERT_EQ(ObBlockOp::OP_FILTER, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(1500, 5000, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(4000, 4500, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(5000, 5500, op));
  ASSERT_EQ(ObBlockOp::OP_NONE, op.block_op_);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_filter_op(5000, 2500, op));

  // compat min merged snapshot
  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(0, 4000, op));
  ASSERT_EQ(ObBlockOp::OP_FILTER, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(0, 4000, op));
  ASSERT_EQ(ObBlockOp::OP_FILTER, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(0, 4500, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);
  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(0, 4500, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);
}

TEST_F(TestMdsInfoCompactionFilter, test_get_truncate_filter_op)
{
  int ret = OB_SUCCESS;

  const char *key_data =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "1        1000         10100         100            200        \n"
    "7        2000         10200         100            200        \n"
    "6        3000         10300         100            200        \n"
    "3        4000         10400         200            300        \n";
  // create tablet
  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));
  ObTruncateInfoArray mock_array;
  ObTruncateInfoArray truncate_info_array;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(allocator_, key_data, mock_array));
  ASSERT_EQ(4, mock_array.count());

  for (int64_t idx = 0; idx < 4; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*mock_array.at(idx)));
  }
  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());
  ObMdsInfoDistinctMgr mds_info_mgr;
  ASSERT_EQ(OB_SUCCESS, mds_info_mgr.init(allocator_, *tablet_handle.get_obj(), nullptr, ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false));
  ASSERT_FALSE(mds_info_mgr.is_empty());
  ObMdsInfoCompactionFilter filter;
  ObBlockOp op;

  ASSERT_EQ(OB_SUCCESS, filter.init(allocator_, tablet_id, schema_rowkey_cnt_, cols_desc_, mds_info_mgr));
  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(100, 500, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(500, 1000, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(1000, 4000, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(1500, 5000, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(4000, 4500, op));
  ASSERT_EQ(ObBlockOp::OP_OPEN, op.block_op_);

  ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(5000, 5500, op));
  ASSERT_EQ(ObBlockOp::OP_NONE, op.block_op_);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_info_compaction_filter.log*");
  system("rm -f test_mds_info_compaction_filter_rs.log*");
  system("rm -f test_mds_info_compaction_filter_election.log*");
  OB_LOGGER.set_file_name("test_mds_info_compaction_filter.log",
                          true,
                          true,
                          "test_mds_info_compaction_filter_rs.log",        // RS 日志文件
                          "test_mds_info_compaction_filter_election.log"); // ELEC 日志文件
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}