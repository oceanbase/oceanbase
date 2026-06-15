/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"
#include "storage/blocksstable/index_block/ob_index_block_util.h"
#include "common/ob_version_def.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;

namespace oceanbase
{
using namespace compaction;
using namespace blocksstable;
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
  // Generic interface: build an ObAggRowCachedReader from an explicit col_meta array and
  // a datum row whose i-th datum corresponds to agg_col_arr[i].
  int make_agg_row_cached_reader(
      const ObIArray<ObSkipIndexColMeta> &agg_col_arr,
      const ObDatumRow &agg_datum_row,
      ObAggRowCachedReader &reader)
  {
    int ret = OB_SUCCESS;
    const int64_t agg_col_cnt = agg_col_arr.count();
    ObSkipIndexAggResult agg_result;

    if (OB_FAIL(agg_result.init(agg_col_cnt, allocator_))) {
      OB_LOG(WARN, "failed to init agg result", K(ret));
    } else {
      ObDatumRow &result_row = agg_result.get_agg_datum_row();
      for (int64_t i = 0; OB_SUCC(ret) && i < agg_col_cnt; ++i) {
        result_row.storage_datums_[i] = agg_datum_row.storage_datums_[i];
      }
      ObAggRowWriter writer;
      if (OB_FAIL(writer.init(agg_col_arr, agg_result, ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION_V2, allocator_))) {
        OB_LOG(WARN, "failed to init agg row writer", K(ret));
      } else {
        const int64_t buf_size = writer.get_serialize_data_size();
        char *buf = static_cast<char *>(allocator_.alloc(buf_size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(WARN, "failed to alloc buf", K(ret), K(buf_size));
        } else {
          int64_t pos = 0;
          if (OB_FAIL(writer.write_agg_data(buf, buf_size, pos))) {
            OB_LOG(WARN, "failed to write agg data", K(ret));
          } else if (OB_FAIL(reader.init(buf, buf_size))) {
            OB_LOG(WARN, "failed to init cached reader", K(ret));
          }
        }
      }
    }
    return ret;
  }

  // Convenience wrapper: build an ObAggRowCachedReader for a single column's MIN/MAX skip index.
  // For trans_version (stored as negative), pass: min_val = -max_snapshot, max_val = -min_snapshot.
  int make_agg_row_cached_reader_for_col(
      const int64_t col_idx,
      const int64_t min_val,
      const int64_t max_val,
      ObAggRowCachedReader &reader)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObSkipIndexColMeta, 3> agg_col_arr;
    ObDatumRow agg_datum_row;

    if (OB_FAIL(agg_col_arr.push_back(ObSkipIndexColMeta(col_idx, SK_IDX_MIN)))) {
      OB_LOG(WARN, "failed to push back min meta", K(ret));
    } else if (OB_FAIL(agg_col_arr.push_back(ObSkipIndexColMeta(col_idx, SK_IDX_MAX)))) {
      OB_LOG(WARN, "failed to push back max meta", K(ret));
    } else if (OB_FAIL(agg_col_arr.push_back(ObSkipIndexColMeta(col_idx, SK_IDX_NULL_COUNT)))) {
      OB_LOG(WARN, "failed to push back max meta", K(ret));
    } else if (OB_FAIL(agg_datum_row.init(allocator_, 3))) {
      OB_LOG(WARN, "failed to init agg datum row", K(ret));
    } else {
      agg_datum_row.storage_datums_[0].set_int(min_val);
      agg_datum_row.storage_datums_[1].set_int(max_val);
      agg_datum_row.storage_datums_[2].set_int(0);
      ret = make_agg_row_cached_reader(agg_col_arr, agg_datum_row, reader);
    }
    return ret;
  }

  // Shortcut for the trans_version column (col_idx=schema_rowkey_cnt_).
  // trans_version is stored as negative: SK_IDX_MIN=-max_snapshot, SK_IDX_MAX=-min_snapshot.
  int make_agg_row_cached_reader(
      const int64_t min_snapshot,
      const int64_t max_snapshot,
      ObAggRowCachedReader &reader)
  {
    const int64_t trans_ver_col_idx = schema_rowkey_cnt_;
    return make_agg_row_cached_reader_for_col(
        trans_ver_col_idx, -max_snapshot, -min_snapshot, reader);
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

  ASSERT_EQ(OB_SUCCESS, filter.init(allocator_, tablet_id, nullptr, schema_rowkey_cnt_, cols_desc_, mds_info_mgr));

// TTL filter data: commit_ver=[1000,2000,3000,6000], filter_value=[1,1000,2000,4000] (ROWSCN type)
// The effective TTL filter for a block is: all rows with rowscn <= max_filter_value=4000 are filtered.
// skip_index_filter reads SK_IDX_MIN/MAX of the trans_version column (col_idx=1).
// trans_version is stored as negative: datum = -snapshot_version.
// Filter condition (ROWSCN): datum > commit_version => -snapshot > commit_ver => snapshot < -commit_ver
// always_false (OP_FILTER): max_stored_val <= commit_ver => -min_snapshot <= commit_ver => min_snapshot >= -commit_ver
// For TTL with commit_ver=4000: max_val(-min_snap) <= 4000 => -min_snap <= 4000 => min_snap >= -4000
// Since snapshots are positive and stored as negatives: max_snapshot < 4000 => always_false (FILTER)
// more precisely: block max trans_version < filter_val => all expired => FILTER

#define MAKE_READER_AND_CHECK(min_snap, max_snap, expected_op)               \
  do {                                                                       \
    ObAggRowCachedReader reader;                                              \
    ASSERT_EQ(OB_SUCCESS, make_agg_row_cached_reader(min_snap, max_snap, reader)); \
    ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(reader, op));                 \
    ASSERT_EQ(expected_op, op.block_op_);                                    \
  } while (0)

  // Block [100, 500]: max_snapshot=500 < filter_val=4000 => all rows expired => OP_FILTER
  MAKE_READER_AND_CHECK(100, 500, ObBlockOp::OP_FILTER);

  // Block [500, 1000]: max_snapshot=1000 <= filter_val=4000 => all rows expired => OP_FILTER
  MAKE_READER_AND_CHECK(500, 1000, ObBlockOp::OP_FILTER);

  // Block [1000, 4000]: max_snapshot=4000 <= filter_val=4000 => all rows expired => OP_FILTER
  MAKE_READER_AND_CHECK(1000, 4000, ObBlockOp::OP_FILTER);

  // Block [1500, 5000]: max_snapshot=5000 > filter_val=4000 but min_snapshot=1500 < 4000 => uncertain => OP_OPEN
  MAKE_READER_AND_CHECK(1500, 5000, ObBlockOp::OP_OPEN);

  // Block [4000, 4500]: max_snapshot=4500 > filter_val=4000 but min_snapshot=4000 <= 4000 => uncertain => OP_OPEN
  MAKE_READER_AND_CHECK(4000, 4500, ObBlockOp::OP_OPEN);

  // Block [5000, 5500]: min_snapshot=5000 > filter_val=4000 => no rows expired => OP_NONE
  MAKE_READER_AND_CHECK(5000, 5500, ObBlockOp::OP_NONE);

  // Test uninitialized reader returns OB_INVALID_ARGUMENT
  {
    ObAggRowCachedReader uninitialized_reader;
    ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_filter_op(uninitialized_reader, op));
  }

  // compat min merged snapshot (min_snapshot=0 means use max_snapshot only)
  MAKE_READER_AND_CHECK(0, 4000, ObBlockOp::OP_OPEN);
  MAKE_READER_AND_CHECK(0, 4000, ObBlockOp::OP_OPEN);
  MAKE_READER_AND_CHECK(0, 4500, ObBlockOp::OP_OPEN);
  MAKE_READER_AND_CHECK(0, 4500, ObBlockOp::OP_OPEN);

#undef MAKE_READER_AND_CHECK
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

  ASSERT_EQ(OB_SUCCESS, filter.init(allocator_, tablet_id, nullptr, schema_rowkey_cnt_, cols_desc_, mds_info_mgr));

// Truncate filter data: commit_ver=[1000,2000,3000,4000], get_filter_max_val()=4000.
// Truncate skip_index logic: if filter_max_val(=4000) >= min_snapshot => OP_OPEN, else OP_NONE.
// min_snapshot is derived from SK_IDX_MAX datum: min_snapshot = -SK_IDX_MAX_datum.

#define MAKE_READER_AND_CHECK_TRUNC(min_snap, max_snap, expected_op)              \
  do {                                                                            \
    ObAggRowCachedReader reader;                                                   \
    ASSERT_EQ(OB_SUCCESS, make_agg_row_cached_reader(min_snap, max_snap, reader)); \
    ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(reader, op));                      \
    ASSERT_EQ(expected_op, op.block_op_);                                         \
  } while (0)

  // Block [100, 500]: min_snapshot=100, filter_max_val=4000 >= 100 => OP_OPEN
  MAKE_READER_AND_CHECK_TRUNC(100, 500, ObBlockOp::OP_OPEN);

  // Block [500, 1000]: min_snapshot=500, filter_max_val=4000 >= 500 => OP_OPEN
  MAKE_READER_AND_CHECK_TRUNC(500, 1000, ObBlockOp::OP_OPEN);

  // Block [1000, 4000]: min_snapshot=1000, filter_max_val=4000 >= 1000 => OP_OPEN
  MAKE_READER_AND_CHECK_TRUNC(1000, 4000, ObBlockOp::OP_OPEN);

  // Block [1500, 5000]: min_snapshot=1500, filter_max_val=4000 >= 1500 => OP_OPEN
  MAKE_READER_AND_CHECK_TRUNC(1500, 5000, ObBlockOp::OP_OPEN);

  // Block [4000, 4500]: min_snapshot=4000, filter_max_val=4000 >= 4000 => OP_OPEN
  MAKE_READER_AND_CHECK_TRUNC(4000, 4500, ObBlockOp::OP_OPEN);

  // Block [5000, 5500]: min_snapshot=5000, filter_max_val=4000 < 5000 => OP_NONE
  MAKE_READER_AND_CHECK_TRUNC(5000, 5500, ObBlockOp::OP_NONE);

#undef MAKE_READER_AND_CHECK_TRUNC
}

TEST_F(TestMdsInfoCompactionFilter, test_get_combined_filter_op)
{
  int ret = OB_SUCCESS;

  // TTL filter: commit_ver=[1000,2000,3000,6000], filter_value=[1,1000,2000,4000] (ROWSCN type)
  // The effective TTL skip index filter: max_filter_value=4000
  //   - Block max_snapshot <= 4000 => all rows expired => op_ttl = OP_FILTER
  //   - Block spans across 4000 (min<4000<=max) => uncertain => op_ttl = OP_OPEN
  //   - Block min_snapshot > 4000 => no rows expired => op_ttl = OP_NONE
  //
  // Truncate filter: commit_ver=[1000,2000,3000,6000], get_filter_max_val()=6000
  //   - filter_max_val(=6000) >= min_snapshot => op_truncate = OP_OPEN
  //   - filter_max_val(=6000) < min_snapshot  => op_truncate = OP_NONE
  //
  // Final op = fuse(op_ttl, op_truncate) = MAX(op_ttl, op_truncate)
  // BlockOp ordering: NONE(0) < OPEN(1) < FILTER(3)

  const char *ttl_key_data =
    "tx_id    commit_ver  filter_type   filter_value   filter_col\n"
    "4        1000         1           1              1          \n"
    "3        2000         1           1000           1          \n"
    "2        3000         1           2000           1          \n"
    "1        6000         1           4000           1          \n";

  // truncate filter_max_val = 6000 (> ttl filter_val=4000), enabling TTL=NONE + Truncate=OPEN case
  const char *truncate_key_data =
    "tx_id    commit_ver    schema_ver    lower_bound    upper_bound\n"
    "1        1000         10100         100            200        \n"
    "7        2000         10200         100            200        \n"
    "6        3000         10300         100            200        \n"
    "3        6000         10400         200            300        \n";

  const ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, create_tablet(tablet_id, tablet_handle));

  // Insert TTL filter infos
  ObTTLFilterInfoArray ttl_mock_array;
  ASSERT_EQ(OB_SUCCESS, TTLFilterInfoHelper::batch_mock_ttl_filter_info_without_sort(
      allocator_, ttl_key_data, ttl_mock_array));
  ASSERT_EQ(4, ttl_mock_array.count());
  for (int64_t idx = 0; idx < 4; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_ttl_filter_info(*ttl_mock_array.at(idx)));
  }

  // Insert truncate infos
  ObTruncateInfoArray truncate_mock_array;
  ASSERT_EQ(OB_SUCCESS, TruncateInfoHelper::batch_mock_truncate_info(
      allocator_, truncate_key_data, truncate_mock_array));
  ASSERT_EQ(4, truncate_mock_array.count());
  for (int64_t idx = 0; idx < 4; ++idx) {
    ASSERT_EQ(OB_SUCCESS, insert_truncate_info(*truncate_mock_array.at(idx)));
  }

  ASSERT_EQ(OB_SUCCESS, MediumInfoCommon::get_tablet(tablet_id, tablet_handle));
  ASSERT_TRUE(tablet_handle.is_valid());

  ObMdsInfoDistinctMgr mds_info_mgr;
  ASSERT_EQ(OB_SUCCESS, mds_info_mgr.init(allocator_, *tablet_handle.get_obj(), nullptr,
      ObVersionRange(0, EXIST_READ_SNAPSHOT_VERSION), false));
  ASSERT_FALSE(mds_info_mgr.is_empty());

  ObMdsInfoCompactionFilter filter;
  ObBlockOp op;
  ASSERT_EQ(OB_SUCCESS, filter.init(allocator_, tablet_id, nullptr, schema_rowkey_cnt_, cols_desc_, mds_info_mgr));
  ASSERT_TRUE(filter.ttl_filter_.is_valid());
  ASSERT_TRUE(filter.truncate_filter_.is_valid());

#define MAKE_READER_AND_CHECK_COMBINED(min_snap, max_snap, expected_op)           \
  do {                                                                            \
    ObAggRowCachedReader reader;                                                   \
    ASSERT_EQ(OB_SUCCESS, make_agg_row_cached_reader(min_snap, max_snap, reader)); \
    ASSERT_EQ(OB_SUCCESS, filter.get_filter_op(reader, op));                      \
    ASSERT_EQ(expected_op, op.block_op_);                                         \
  } while (0)

  // Case 1: TTL=FILTER, Truncate=OPEN => fuse=FILTER
  // Block [100, 500]: max_snapshot=500 < ttl_filter_val=4000 => op_ttl=FILTER
  //                  min_snapshot=100, truncate_max=6000 >= 100 => op_truncate=OPEN
  //                  fuse = MAX(FILTER=3, OPEN=1) = FILTER
  MAKE_READER_AND_CHECK_COMBINED(100, 500, ObBlockOp::OP_FILTER);

  // Case 2: TTL=FILTER, Truncate=OPEN => fuse=FILTER
  // Block [1000, 4000]: max_snapshot=4000 <= ttl_filter_val=4000 => op_ttl=FILTER
  //                    min_snapshot=1000, truncate_max=6000 >= 1000 => op_truncate=OPEN
  //                    fuse = MAX(FILTER=3, OPEN=1) = FILTER
  MAKE_READER_AND_CHECK_COMBINED(1000, 4000, ObBlockOp::OP_FILTER);

  // Case 3: TTL=OPEN, Truncate=OPEN => fuse=OPEN
  // Block [3000, 5000]: max_snapshot=5000 > 4000 but min_snapshot=3000 < 4000 => op_ttl=OPEN
  //                    min_snapshot=3000, truncate_max=6000 >= 3000 => op_truncate=OPEN
  //                    fuse = MAX(OPEN=1, OPEN=1) = OPEN
  MAKE_READER_AND_CHECK_COMBINED(3000, 5000, ObBlockOp::OP_OPEN);

  // Case 4: TTL=OPEN, Truncate=OPEN => fuse=OPEN
  // Block [4000, 4500]: max_snapshot=4500 > 4000 but min_snapshot=4000 <= 4000 => op_ttl=OPEN
  //                    min_snapshot=4000, truncate_max=6000 >= 4000 => op_truncate=OPEN
  //                    fuse = MAX(OPEN=1, OPEN=1) = OPEN
  MAKE_READER_AND_CHECK_COMBINED(4000, 4500, ObBlockOp::OP_OPEN);

  // Case 5: TTL=NONE, Truncate=OPEN => fuse=OPEN
  // Block [4001, 5000]: min_snapshot=4001 > ttl_filter_val=4000 => op_ttl=NONE
  //                    min_snapshot=4001, truncate_max=6000 >= 4001 => op_truncate=OPEN
  //                    fuse = MAX(NONE=0, OPEN=1) = OPEN
  MAKE_READER_AND_CHECK_COMBINED(4001, 5000, ObBlockOp::OP_OPEN);

  // Case 6: TTL=NONE, Truncate=NONE => fuse=NONE
  // Block [7000, 7500]: min_snapshot=7000 > ttl_filter_val=4000 => op_ttl=NONE
  //                    min_snapshot=7000, truncate_max=6000 < 7000 => op_truncate=NONE
  //                    fuse = MAX(NONE=0, NONE=0) = NONE
  MAKE_READER_AND_CHECK_COMBINED(7000, 7500, ObBlockOp::OP_NONE);

  // Case 7: TTL=OPEN, Truncate=OPEN => fuse=OPEN (boundary: min_snap crosses both thresholds)
  // Block [1500, 4500]: min_snapshot=1500, max_snapshot=4500
  //   op_ttl: max_snap=4500 > 4000 and min_snap=1500 < 4000 => uncertain => OPEN
  //   op_truncate: truncate_max=6000 >= min_snap=1500 => OPEN
  //   fuse = MAX(OPEN=1, OPEN=1) = OPEN
  MAKE_READER_AND_CHECK_COMBINED(1500, 4500, ObBlockOp::OP_OPEN);

#undef MAKE_READER_AND_CHECK_COMBINED
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_info_compaction_filter.log*");
  system("rm -f test_mds_info_compaction_filter_rs.log*");
  system("rm -f test_mds_info_compaction_filter_election.log*");
  OB_LOGGER.set_file_name("test_mds_info_compaction_filter.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}