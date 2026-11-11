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

// Unit tests for the compact ObDASScanCtDef serialization:
//  - point A: ObExpr* references use varint indexes only in the compact record;
//  - point B: external members are gated by is_external_table_ /
//    (is_external_table_ || table_type == VIRTUAL_TABLE);

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_scan_ctdef_compact.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;

namespace sql
{

class TestDASCtDefSerialize : public ::testing::Test
{
public:
  TestDASCtDefSerialize()
  {}
  virtual ~TestDASCtDefSerialize()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

class ClusterVersionGuard
{
public:
  explicit ClusterVersionGuard(const uint64_t cluster_version) :
    saved_(ObClusterVersion::get_instance().get_cluster_version())
  {
    ObClusterVersion::get_instance().update_cluster_version(cluster_version);
  }
  ~ClusterVersionGuard()
  {
    ObClusterVersion::get_instance().update_cluster_version(saved_);
  }

private:
  uint64_t saved_;
};

class ExprSerCtxGuard
{
public:
  explicit ExprSerCtxGuard(ObExpr::ObExprIArray *exprs) :
    saved_array_(ObExpr::get_serialize_array()), encoding_guard_(false), decoding_guard_(false)
  {
    ObExpr::get_serialize_array() = exprs;
  }
  ~ExprSerCtxGuard()
  {
    ObExpr::get_serialize_array() = saved_array_;
  }

private:
  ObExpr::ObExprIArray *saved_array_;
  serialization::SerCompactEncodingGuard encoding_guard_;
  serialization::CompactExprPtrGuard decoding_guard_;
};

// Frozen oracle for the compact ScanCtDef bytes before the codec was moved
// out of ObDASScanCtDef. Keep this independent of das_compact::* so the test
// catches added envelopes, presence bytes, reordered fields, and ref-width
// regressions.
static int serialize_previous_compact_scan_ctdef(const ObDASScanCtDef &ctdef, char *buf, const int64_t buf_len,
                                                 int64_t &pos)
{
  int ret = OB_SUCCESS;
  serialization::SerCompactEncodingGuard encoding_guard(true);
  LST_DO_CODE(OB_UNIS_ENCODE, ctdef.ref_table_id_, ctdef.access_column_ids_, ctdef.schema_version_, ctdef.table_param_,
              ctdef.pd_expr_spec_, ctdef.aggregate_column_ids_, ctdef.group_id_expr_, ctdef.result_output_,
              ctdef.is_get_, ctdef.is_external_table_);
  if (OB_SUCC(ret) && ctdef.is_external_table_) {
    LST_DO_CODE(OB_UNIS_ENCODE, ctdef.external_file_location_, ctdef.external_file_access_info_, ctdef.external_files_,
                ctdef.external_file_format_str_);
  }
  LST_DO_CODE(OB_UNIS_ENCODE, ctdef.trans_info_expr_, ctdef.group_by_column_ids_, ctdef.ir_scan_type_,
              ctdef.rowkey_exprs_, ctdef.table_scan_opt_, ctdef.doc_id_idx_, ctdef.vec_vid_idx_, ctdef.multivalue_idx_,
              ctdef.multivalue_type_, ctdef.index_merge_idx_, ctdef.flags_);
  if (OB_SUCC(ret) && ctdef.is_external_table_) { OB_UNIS_ENCODE(ctdef.partition_infos_); }
  LST_DO_CODE(OB_UNIS_ENCODE, ctdef.domain_id_idxs_, ctdef.domain_types_, ctdef.domain_tids_, ctdef.pre_query_range_,
              ctdef.pre_range_graph_);
  if (OB_SUCC(ret) && ctdef.is_external_table_) { OB_UNIS_ENCODE(ctdef.external_file_pattern_); }
  if (OB_SUCC(ret)
      && (ctdef.is_external_table_ || ctdef.table_param_.get_table_type() == share::schema::VIRTUAL_TABLE)) {
    OB_UNIS_ENCODE(ctdef.external_object_ctx_);
  }
  if (OB_SUCC(ret) && ctdef.is_external_table_) { OB_UNIS_ENCODE(ctdef.external_pushdown_filters_); }
  OB_UNIS_ENCODE(ctdef.aggregate_param_props_);
  if (OB_SUCC(ret) && ctdef.is_external_table_) { OB_UNIS_ENCODE(ctdef.lake_table_format_); }
  LST_DO_CODE(OB_UNIS_ENCODE, ctdef.push_down_topn_, ctdef.cache_aware_row_num_);
  if (OB_SUCC(ret) && ctdef.is_external_table_) { OB_UNIS_ENCODE(ctdef.external_file_pattern_type_); }
  return ret;
}

TEST_F(TestDASCtDefSerialize, test_external_gating_off)
{
  ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_3);
  ObArenaAllocator alloc;
  ObDASScanCtDef src(alloc);
  src.ref_table_id_ = 500123;
  src.schema_version_ = 1234;
  src.is_get_ = true;
  src.is_external_table_ = false;
  src.cache_aware_row_num_ = 7;
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.init(2));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(16));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(17));

  const int64_t size = das_compact::scan_ctdef_size(src);
  char *buf = static_cast<char *>(alloc.alloc(size));
  ASSERT_TRUE(nullptr != buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
  ASSERT_EQ(size, pos);

  ObDASScanCtDef dst(alloc);
  int64_t dpos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, nullptr));
  ASSERT_EQ(size, dpos);
  ASSERT_EQ(src.ref_table_id_, dst.ref_table_id_);
  ASSERT_EQ(src.schema_version_, dst.schema_version_);
  ASSERT_TRUE(dst.is_get_);
  ASSERT_FALSE(dst.is_external_table_);
  ASSERT_EQ(2, dst.access_column_ids_.count());
  ASSERT_EQ(16, dst.access_column_ids_.at(0));
  ASSERT_EQ(17, dst.access_column_ids_.at(1));
  ASSERT_EQ(7, dst.cache_aware_row_num_);
  // gated members keep constructor defaults
  ASSERT_EQ(share::ObLakeTableFormat::INVALID, dst.lake_table_format_);
  ASSERT_EQ(static_cast<int64_t>(share::schema::REGEXP_EXTERNAL_FILE_PATTERN), dst.external_file_pattern_type_);
  ASSERT_EQ(0, dst.external_file_location_.str_.length());
  ASSERT_EQ(0, dst.external_file_access_info_.str_.length());
  ASSERT_EQ(0, dst.external_file_format_str_.str_.length());
  ASSERT_EQ(0, dst.external_file_pattern_.str_.length());
  ASSERT_EQ(0, dst.external_files_.count());
  ASSERT_EQ(0, dst.partition_infos_.count());
  ASSERT_EQ(0, dst.external_pushdown_filters_.strs_.count());
}

TEST_F(TestDASCtDefSerialize, test_external_gating_on)
{
  ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_3);
  ObArenaAllocator alloc;
  ObDASScanCtDef src(alloc);
  src.ref_table_id_ = 500124;
  src.is_external_table_ = true;
  src.lake_table_format_ = share::ObLakeTableFormat::HIVE;
  src.external_file_pattern_type_ = static_cast<int64_t>(share::schema::REGEXP_EXTERNAL_FILE_PATTERN);
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(16));
  ASSERT_EQ(OB_SUCCESS, src.external_file_location_.store_str(ObString::make_string("file:///tmp/ext")));
  ASSERT_EQ(OB_SUCCESS, src.external_file_access_info_.store_str(ObString::make_string("access_id=xxx")));
  ASSERT_EQ(OB_SUCCESS, src.external_file_format_str_.store_str(ObString::make_string("format=csv")));
  ASSERT_EQ(OB_SUCCESS, src.external_file_pattern_.store_str(ObString::make_string(".*\\.csv")));

  const int64_t size = das_compact::scan_ctdef_size(src);
  char *buf = static_cast<char *>(alloc.alloc(size));
  ASSERT_TRUE(nullptr != buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, buf, size, pos));
  ASSERT_EQ(size, pos);

  ObDASScanCtDef dst(alloc);
  int64_t dpos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::deserialize_scan_ctdef(dst, buf, size, dpos, nullptr));
  ASSERT_EQ(size, dpos);
  ASSERT_TRUE(dst.is_external_table_);
  ASSERT_EQ(share::ObLakeTableFormat::HIVE, dst.lake_table_format_);
  ASSERT_EQ(0, dst.external_file_location_.str_.compare("file:///tmp/ext"));
  ASSERT_EQ(0, dst.external_file_access_info_.str_.compare("access_id=xxx"));
  ASSERT_EQ(0, dst.external_file_format_str_.str_.compare("format=csv"));
  ASSERT_EQ(0, dst.external_file_pattern_.str_.compare(".*\\.csv"));
}

TEST_F(TestDASCtDefSerialize, test_scan_ctdef_version_gate_and_expr_ref_context_restore)
{
  ObArenaAllocator alloc;
  ObArray<ObExpr> exprs;
  ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(3));
  ExprSerCtxGuard ctx_guard(&exprs);
  ObDASScanCtDef src(alloc);
  src.ref_table_id_ = 500125;
  src.schema_version_ = 5678;
  src.is_external_table_ = false;
  src.lake_table_format_ = share::ObLakeTableFormat::HIVE;
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.init(2));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(16));
  ASSERT_EQ(OB_SUCCESS, src.access_column_ids_.push_back(17));
  ASSERT_EQ(OB_SUCCESS, src.external_file_location_.store_str(ObString::make_string("legacy-only-location")));
  ASSERT_EQ(OB_SUCCESS, src.external_file_access_info_.store_str(ObString::make_string("legacy-only-access")));
  ASSERT_EQ(OB_SUCCESS, src.external_file_format_str_.store_str(ObString::make_string("legacy-only-format")));
  ASSERT_EQ(OB_SUCCESS, src.external_file_pattern_.store_str(ObString::make_string("legacy-only-pattern")));
  ASSERT_EQ(OB_SUCCESS, src.result_output_.init(exprs.count()));
  for (int64_t i = 0; i < exprs.count(); ++i) { ASSERT_EQ(OB_SUCCESS, src.result_output_.push_back(&exprs.at(i))); }

  // The class codec is permanently legacy. Raising the compact capability
  // gate must not change one byte of its output.
  char legacy_old[64 * 1024];
  char legacy_new[64 * 1024];
  int64_t legacy_old_pos = 0;
  int64_t legacy_new_pos = 0;
  {
    ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_2);
    ASSERT_EQ(OB_SUCCESS, src.serialize(legacy_old, sizeof(legacy_old), legacy_old_pos));
  }
  {
    ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_3);
    ASSERT_EQ(OB_SUCCESS, src.serialize(legacy_new, sizeof(legacy_new), legacy_new_pos));
  }
  ASSERT_EQ(legacy_old_pos, legacy_new_pos);
  ASSERT_EQ(0, MEMCMP(legacy_old, legacy_new, legacy_old_pos));
  ASSERT_EQ(src.get_serialize_size(), legacy_old_pos);
  ASSERT_FALSE(serialization::using_v4_encoding());

  ObDASScanCtDef legacy_dst(alloc);
  int64_t legacy_dpos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy_dst.deserialize(legacy_old, legacy_old_pos, legacy_dpos));
  ASSERT_EQ(legacy_old_pos, legacy_dpos);
  ASSERT_EQ(0, legacy_dst.external_file_location_.str_.compare("legacy-only-location"));
  ASSERT_EQ(share::ObLakeTableFormat::HIVE, legacy_dst.lake_table_format_);
  ASSERT_EQ(3, legacy_dst.result_output_.count());

  // The caller-selected compact codec is self-delimited and still roundtrips.
  char compact_buf[64 * 1024];
  int64_t compact_pos = 0;
  int64_t compact_size = 0;
  {
    compact_size = das_compact::scan_ctdef_size(src);
    ASSERT_EQ(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, compact_buf, sizeof(compact_buf), compact_pos));
  }
  ASSERT_EQ(compact_size, compact_pos);
  static const unsigned char STANDARD_UNIS_V4_PREFIX_GOLDEN[] = {0x04, 0xb3, 0x82, 0x80, 0x80, 0x00};
  ASSERT_EQ(313, compact_pos);
  ASSERT_EQ(0, MEMCMP(STANDARD_UNIS_V4_PREFIX_GOLDEN, compact_buf, sizeof(STANDARD_UNIS_V4_PREFIX_GOLDEN)));
  int64_t envelope_pos = 0;
  int64_t compact_version = 0;
  int64_t compact_body_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(compact_buf, compact_pos, envelope_pos, &compact_version));
  ASSERT_EQ(serialization::SER_COMPACT_OPT_VERSION, compact_version);
  ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(compact_buf, compact_pos, envelope_pos, &compact_body_len));
  ASSERT_EQ(compact_pos - envelope_pos, compact_body_len);
  ASSERT_LT(compact_pos, legacy_old_pos);
  ASSERT_FALSE(serialization::using_v4_encoding());

  ObDASScanCtDef compact_dst(alloc);
  int64_t compact_dpos = 0;
  {
    ASSERT_EQ(OB_SUCCESS,
              das_compact::deserialize_scan_ctdef(compact_dst, compact_buf, compact_pos, compact_dpos, nullptr));
  }
  ASSERT_EQ(compact_pos, compact_dpos);
  ASSERT_EQ(0, compact_dst.external_file_location_.str_.length());
  ASSERT_EQ(share::ObLakeTableFormat::INVALID, compact_dst.lake_table_format_);
  ASSERT_EQ(3, compact_dst.result_output_.count());
  for (int64_t i = 0; i < exprs.count(); ++i) { ASSERT_EQ(&exprs.at(i), compact_dst.result_output_.at(i)); }

  ObDASScanCtDef truncated_dst(alloc);
  int64_t truncated_pos = 0;
  ASSERT_NE(OB_SUCCESS,
            das_compact::deserialize_scan_ctdef(truncated_dst, compact_buf, compact_pos - 1, truncated_pos, nullptr));
  ASSERT_LE(truncated_pos, compact_pos - 1);

  ObDASScanCtDef empty_dst(alloc);
  int64_t empty_pos = compact_pos;
  ASSERT_EQ(OB_DESERIALIZE_ERROR,
            das_compact::deserialize_scan_ctdef(empty_dst, compact_buf, compact_pos, empty_pos, nullptr));

  SerCompactState state = SerCompactState::UNKNOWN;
  ObDASScanCtDef dispatched_legacy(alloc);
  int64_t dispatched_pos = 0;
  ASSERT_EQ(OB_SUCCESS, das_compact::deserialize_scan_ctdef(dispatched_legacy, legacy_old, legacy_old_pos,
                                                            dispatched_pos, nullptr, &state));
  ASSERT_EQ(SerCompactState::LEGACY, state);
  ASSERT_EQ(legacy_old_pos, dispatched_pos);
  ObDASScanCtDef mixed_family(alloc);
  dispatched_pos = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, das_compact::deserialize_scan_ctdef(mixed_family, compact_buf, compact_pos,
                                                                  dispatched_pos, nullptr, &state));
  ASSERT_EQ(SerCompactState::LEGACY, state);

  char malformed[64 * 1024];
  MEMCPY(malformed, compact_buf, compact_pos);
  malformed[0] = 3;
  ObDASScanCtDef bad_version_dst(alloc);
  int64_t bad_version_pos = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED,
            das_compact::deserialize_scan_ctdef(bad_version_dst, malformed, compact_pos, bad_version_pos, nullptr));

  MEMCPY(malformed, compact_buf, compact_pos);
  int64_t length_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(malformed + 1, sizeof(malformed) - 1, length_pos,
                                                              compact_body_len + 1));
  malformed[compact_pos] = 0;
  ObDASScanCtDef trailing_dst(alloc);
  int64_t trailing_pos = 0;
  {
    ASSERT_EQ(OB_DESERIALIZE_ERROR,
              das_compact::deserialize_scan_ctdef(trailing_dst, malformed, compact_pos + 1, trailing_pos, nullptr));
  }

  ObDASScanCtDef replay_dst(alloc);
  das_compact::ObDASScanCtDefCompact compact_record(replay_dst);
  int64_t replay_pos = 0;
  {
    ASSERT_EQ(OB_SUCCESS, serialization::decode(compact_buf, compact_pos, replay_pos, compact_record));
  }
  das_compact::ObScanCtDefReplayCtx replay_ctx;
  ASSERT_EQ(OB_SUCCESS, compact_record.replay_to(replay_dst, replay_ctx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, compact_record.replay_to(compact_dst, replay_ctx));

  int64_t error_pos = 0;
  {
    ASSERT_NE(OB_SUCCESS, das_compact::serialize_scan_ctdef(src, compact_buf, compact_size - 1, error_pos));
  }
  ASSERT_FALSE(serialization::using_v4_encoding());
}

TEST_F(TestDASCtDefSerialize, test_expr_ref_varint_size_is_compact_only)
{
  ObArenaAllocator alloc;
  ObArray<ObExpr> exprs;
  ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(3));
  ExprSerCtxGuard ctx_guard(&exprs);
  ObDASScanCtDef empty(alloc);
  ObDASScanCtDef with_refs(alloc);
  ASSERT_EQ(OB_SUCCESS, with_refs.result_output_.init(exprs.count()));
  for (int64_t i = 0; i < exprs.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, with_refs.result_output_.push_back(&exprs.at(i)));
  }

  const int64_t legacy_empty_size = empty.get_serialize_size();
  const int64_t legacy_ref_size = with_refs.get_serialize_size();
  int64_t compact_empty_size = 0;
  int64_t compact_ref_size = 0;
  {
    compact_empty_size = das_compact::scan_ctdef_size(empty);
    compact_ref_size = das_compact::scan_ctdef_size(with_refs);
  }
  ASSERT_EQ(3 * static_cast<int64_t>(sizeof(uint32_t)), legacy_ref_size - legacy_empty_size);
  ASSERT_EQ(3, compact_ref_size - compact_empty_size);
  ASSERT_FALSE(serialization::using_v4_encoding());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
