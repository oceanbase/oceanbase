/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX SQL_ENG
#include "../engine/basic/utils/expr_maker.h"
#include "share/external_table/ob_external_table_part_info.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_cluster_version.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#define private public
#include "sql/optimizer/ob_lake_table_partition_info.h"
#undef private
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace sql
{
namespace unittest
{

using namespace common;
using namespace storage;

class TestExternalTableRowIterator : public ObExternalTableRowIterator
{
public:
  void set_scan_param(const ObTableScanParam *scan_param)
  {
    scan_param_ = scan_param;
  }

  int load_iceberg_file_partition_value(const ObFileScanTask *scan_task,
                                        ObExternalIteratorState &state)
  {
    return fill_iceberg_file_partition_value(scan_task, state);
  }

  int find_iceberg_identity_partition_value(const int64_t source_field_id,
                                            const ObExternalIteratorState &state,
                                            const ObObj *&partition_value) const
  {
    return get_iceberg_identity_partition_value(source_field_id, state, partition_value);
  }

  static int materialize_partition_value(ObExpr *expr,
                                         ObEvalCtx &eval_ctx,
                                         const ObObj &partition_value)
  {
    return load_partition_value_to_expr(expr, eval_ctx, partition_value);
  }

  int get_next_row(ObNewRow *&row) override
  {
    UNUSED(row);
    return OB_ITER_END;
  }

  void reset() override
  {
  }
};

TEST(TestIcebergPartitionValue, ResolveBySourceId)
{
  ObTableScanParam scan_param;
  scan_param.lake_table_format_ = share::ObLakeTableFormat::ICEBERG;
  TestExternalTableRowIterator iter;
  iter.set_scan_param(&scan_param);

  ObObj partition_values[4];
  partition_values[0].set_int32(31);
  partition_values[1].set_int(310);
  partition_values[2].set_int32(17);
  partition_values[3].set_null();

  ObExternalIteratorState state;
  state.part_list_val_.assign(partition_values, ARRAYSIZEOF(partition_values));

  const ObObj *partition_value = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.find_iceberg_identity_partition_value(31, state, partition_value));
  ASSERT_NE(nullptr, partition_value);
  EXPECT_EQ(310, partition_value->get_int());

  partition_value = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.find_iceberg_identity_partition_value(17, state, partition_value));
  ASSERT_NE(nullptr, partition_value);
  EXPECT_TRUE(partition_value->is_null());

  partition_value = reinterpret_cast<const ObObj *>(1);
  ASSERT_EQ(OB_SUCCESS, iter.find_iceberg_identity_partition_value(99, state, partition_value));
  EXPECT_EQ(nullptr, partition_value);
}

TEST(TestIcebergPartitionValue, RejectMisalignedPartitionTuple)
{
  ObTableScanParam scan_param;
  scan_param.lake_table_format_ = share::ObLakeTableFormat::ICEBERG;
  TestExternalTableRowIterator iter;
  iter.set_scan_param(&scan_param);

  ObObj partition_values[1];
  partition_values[0].set_int32(17);

  ObExternalIteratorState state;
  state.part_list_val_.assign(partition_values, ARRAYSIZEOF(partition_values));

  const ObObj *partition_value = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            iter.find_iceberg_identity_partition_value(17, state, partition_value));
  EXPECT_EQ(nullptr, partition_value);
}

TEST(TestIcebergPartitionValue, MaterializeUniformConstant)
{
  static constexpr int64_t BATCH_SIZE = 8;
  ObArenaAllocator allocator("IcebergPartVal");
  ObExecContext exec_ctx(allocator);
  ObEvalCtx eval_ctx(exec_ctx);
  ConstIntExprMaker expr_maker(0, BATCH_SIZE);
  ExprMaker *maker = &expr_maker;
  maker->set_frame_idx(0);

  eval_ctx.frames_ = static_cast<char **>(allocator.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, eval_ctx.frames_);
  eval_ctx.frames_[0] = static_cast<char *>(allocator.alloc(maker->frame_mem_size()));
  ASSERT_NE(nullptr, eval_ctx.frames_[0]);
  MEMSET(eval_ctx.frames_[0], 0, maker->frame_mem_size());
  exec_ctx.set_frames(eval_ctx.frames_);
  exec_ctx.set_frame_cnt(1);
  eval_ctx.max_batch_size_ = BATCH_SIZE;

  ObExpr expr;
  maker->make(expr, eval_ctx);

  ObObj partition_value;
  partition_value.set_int(42);
  ASSERT_EQ(
      OB_SUCCESS,
      TestExternalTableRowIterator::materialize_partition_value(&expr, eval_ctx, partition_value));
  EXPECT_EQ(VEC_UNIFORM_CONST, expr.get_format(eval_ctx));
  EXPECT_EQ(42, expr.locate_batch_datums(eval_ctx)[0].get_int());

  partition_value.set_null();
  ASSERT_EQ(
      OB_SUCCESS,
      TestExternalTableRowIterator::materialize_partition_value(&expr, eval_ctx, partition_value));
  EXPECT_TRUE(expr.locate_batch_datums(eval_ctx)[0].is_null());
}

TEST(TestIcebergPartitionValue, NormalizePromotedFloatPartitionValue)
{
  ObArenaAllocator allocator("IcebergPartCast");
  ObRawExprResType target_type;
  target_type.set_type(ObDoubleType);
  target_type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObDoubleType]);
  target_type.set_collation_type(CS_TYPE_BINARY);

  ObObj source_value;
  source_value.set_float(1.5f);
  ObObj target_value;
  ASSERT_EQ(OB_SUCCESS,
            ObLakeTablePartitionInfo::cast_iceberg_partition_value(source_value,
                                                                   target_type,
                                                                   allocator,
                                                                   target_value));
  ASSERT_TRUE(target_value.is_double());
  EXPECT_DOUBLE_EQ(1.5, target_value.get_double());
}

TEST(TestIcebergPartitionValue, GateManifestPartitionValueByClusterVersion)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  cluster_version.update_cluster_version(CLUSTER_VERSION_5_0_1_0);
  EXPECT_FALSE(iceberg::ObIcebergUtils::is_manifest_partition_value_supported());

  ObTableScanParam scan_param;
  scan_param.lake_table_format_ = share::ObLakeTableFormat::ICEBERG;
  TestExternalTableRowIterator iter;
  iter.set_scan_param(&scan_param);
  ObObj legacy_partition_values[1];
  legacy_partition_values[0].set_int(17);
  ObExternalIteratorState state;
  state.part_list_val_.assign(legacy_partition_values, ARRAYSIZEOF(legacy_partition_values));
  const ObObj *partition_value = reinterpret_cast<const ObObj *>(1);
  EXPECT_EQ(OB_SUCCESS, iter.find_iceberg_identity_partition_value(17, state, partition_value));
  EXPECT_EQ(nullptr, partition_value);

  cluster_version.update_cluster_version(CLUSTER_VERSION_5_0_2_0);
  EXPECT_TRUE(iceberg::ObIcebergUtils::is_manifest_partition_value_supported());
}

TEST(TestIcebergPartitionValue, PreserveManifestPartitionIdDuringGranuleConversion)
{
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  ObArenaAllocator allocator("IcebergTaskPart");

  cluster_version.update_cluster_version(CLUSTER_VERSION_5_0_2_0);
  ObIcebergScanTask manifest_task(allocator);
  manifest_task.part_id_ = 2;
  ASSERT_EQ(OB_SUCCESS,
            share::ObExternalTableUtils::convert_lake_table_scan_task(7, 42, &manifest_task));
  EXPECT_EQ(7, manifest_task.file_id_);
  EXPECT_EQ(2, manifest_task.part_id_);

  cluster_version.update_cluster_version(CLUSTER_VERSION_5_0_1_0);
  ObIcebergScanTask legacy_task(allocator);
  legacy_task.part_id_ = 2;
  ASSERT_EQ(OB_SUCCESS,
            share::ObExternalTableUtils::convert_lake_table_scan_task(8, 42, &legacy_task));
  EXPECT_EQ(8, legacy_task.file_id_);
  EXPECT_EQ(42, legacy_task.part_id_);

  cluster_version.update_cluster_version(CLUSTER_VERSION_5_0_2_0);
}

TEST(TestIcebergPartitionValue, ScanTaskAndPartitionInfoSerialization)
{
  ObArenaAllocator source_allocator("IcebergTaskSrc");
  ObIcebergScanTask source_task(source_allocator);
  source_task.part_id_ = 0;
  source_task.partition_spec_id_ = 23;

  const int64_t task_serialize_size = source_task.get_serialize_size();
  char *task_buf = static_cast<char *>(source_allocator.alloc(task_serialize_size));
  ASSERT_NE(nullptr, task_buf);
  int64_t encode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, source_task.serialize(task_buf, task_serialize_size, encode_pos));
  ASSERT_EQ(task_serialize_size, encode_pos);

  ObArenaAllocator target_allocator("IcebergTaskDst");
  ObIcebergScanTask target_task(target_allocator);
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target_task.deserialize(task_buf, encode_pos, decode_pos));
  ASSERT_EQ(encode_pos, decode_pos);
  ASSERT_EQ(0, target_task.part_id_);
  ASSERT_EQ(23, target_task.partition_spec_id_);

  share::ObExternalTablePartInfoArray source_partition_infos(source_allocator);
  ASSERT_EQ(OB_SUCCESS, source_partition_infos.reserve(2));
  ObObj partition_values[4];
  partition_values[0].set_int32(31);
  partition_values[1].set_int(310);
  partition_values[2].set_int32(17);
  partition_values[3].set_null();
  share::ObExternalTablePartInfo part_info;
  part_info.part_id_ = 0;
  part_info.list_row_value_.assign(partition_values, ARRAYSIZEOF(partition_values));
  ASSERT_EQ(OB_SUCCESS, source_partition_infos.set_part_pair_by_idx(0, part_info));
  share::ObExternalTablePartInfo empty_part_info;
  empty_part_info.part_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, source_partition_infos.set_part_pair_by_idx(1, empty_part_info));

  const int64_t info_serialize_size = source_partition_infos.get_serialize_size();
  char *info_buf = static_cast<char *>(source_allocator.alloc(info_serialize_size));
  ASSERT_NE(nullptr, info_buf);
  encode_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            source_partition_infos.serialize(info_buf, info_serialize_size, encode_pos));
  ASSERT_EQ(info_serialize_size, encode_pos);

  share::ObExternalTablePartInfoArray target_partition_infos(target_allocator);
  decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target_partition_infos.deserialize(info_buf, encode_pos, decode_pos));
  ASSERT_EQ(encode_pos, decode_pos);
  ASSERT_EQ(2, target_partition_infos.count());
  EXPECT_EQ(1, target_partition_infos.at(1).part_id_);
  EXPECT_EQ(0, target_partition_infos.at(1).list_row_value_.get_count());

  ObTableScanParam scan_param;
  scan_param.lake_table_format_ = share::ObLakeTableFormat::ICEBERG;
  scan_param.partition_infos_ = &target_partition_infos;
  TestExternalTableRowIterator iter;
  iter.set_scan_param(&scan_param);
  ObExternalIteratorState state;
  ASSERT_EQ(OB_SUCCESS, iter.load_iceberg_file_partition_value(&target_task, state));
  EXPECT_EQ(0, state.part_id_);

  const ObObj *partition_value = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.find_iceberg_identity_partition_value(31, state, partition_value));
  ASSERT_NE(nullptr, partition_value);
  EXPECT_EQ(310, partition_value->get_int());

  partition_value = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.find_iceberg_identity_partition_value(17, state, partition_value));
  ASSERT_NE(nullptr, partition_value);
  EXPECT_TRUE(partition_value->is_null());
}

TEST(TestIcebergPartitionValue, BuildDistinctPartitionIds)
{
  ObArenaAllocator allocator("IcebergPartIds");
  iceberg::ManifestEntry cn_entry(allocator);
  iceberg::ManifestEntry us_entry(allocator);
  iceberg::ManifestEntry unpartitioned_entry(allocator);
  cn_entry.partition_spec_id = 0;
  us_entry.partition_spec_id = 0;
  unpartitioned_entry.partition_spec_id = 1;

  ASSERT_EQ(OB_SUCCESS, cn_entry.data_file.partition.init(1));
  ASSERT_EQ(OB_SUCCESS, us_entry.data_file.partition.init(1));
  ObObj country;
  country.set_varchar(ObString::make_string("CN"));
  country.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  country.set_collation_level(CS_LEVEL_IMPLICIT);
  ASSERT_EQ(OB_SUCCESS, cn_entry.data_file.partition.push_back(country));
  country.set_varchar(ObString::make_string("US"));
  ASSERT_EQ(OB_SUCCESS, us_entry.data_file.partition.push_back(country));

  ObIcebergFileDesc cn_file(allocator);
  ObIcebergFileDesc us_file(allocator);
  ObIcebergFileDesc unpartitioned_file(allocator);
  cn_file.entry_ = &cn_entry;
  us_file.entry_ = &us_entry;
  unpartitioned_file.entry_ = &unpartitioned_entry;
  ObSEArray<ObIcebergFileDesc *, 3> file_descs;
  ASSERT_EQ(OB_SUCCESS, file_descs.push_back(&cn_file));
  ASSERT_EQ(OB_SUCCESS, file_descs.push_back(&us_file));
  ASSERT_EQ(OB_SUCCESS, file_descs.push_back(&unpartitioned_file));

  ObLakeTablePartitionInfo partition_info(allocator);
  ObLakeTablePartKeyMap part_key_map;
  ASSERT_EQ(OB_SUCCESS, partition_info.build_iceberg_part_ids(part_key_map, file_descs));
  EXPECT_EQ(0, cn_file.part_idx_);
  EXPECT_EQ(1, us_file.part_idx_);
  EXPECT_EQ(2, unpartitioned_file.part_idx_);
  EXPECT_EQ(3, part_key_map.size());
  ASSERT_EQ(OB_SUCCESS, part_key_map.destroy());

  ASSERT_EQ(OB_SUCCESS, partition_info.iceberg_file_descs_.assign(file_descs));
  ObCandiTabletLoc *tablet_loc = partition_info.get_phy_tbl_location_info_for_update()
                                     .get_phy_part_loc_info_list_for_update()
                                     .alloc_place_holder();
  ASSERT_NE(nullptr, tablet_loc);
  ObOptIcebergFile cn_opt_file(allocator);
  ObOptIcebergFile us_opt_file(allocator);
  ObOptIcebergFile unpartitioned_opt_file(allocator);
  cn_opt_file.file_desc_idx_ = 0;
  us_opt_file.file_desc_idx_ = 1;
  unpartitioned_opt_file.file_desc_idx_ = 2;
  ASSERT_EQ(OB_SUCCESS, tablet_loc->get_opt_lake_table_files_for_update().push_back(&cn_opt_file));
  ASSERT_EQ(OB_SUCCESS, tablet_loc->get_opt_lake_table_files_for_update().push_back(&us_opt_file));
  ASSERT_EQ(OB_SUCCESS,
            tablet_loc->get_opt_lake_table_files_for_update().push_back(&unpartitioned_opt_file));
  ASSERT_EQ(OB_SUCCESS, partition_info.update_iceberg_file_part_ids());
  EXPECT_EQ(0, cn_opt_file.part_id_);
  EXPECT_EQ(1, us_opt_file.part_id_);
  EXPECT_EQ(2, unpartitioned_opt_file.part_id_);

  ObIcebergScanTask cn_task(allocator);
  ObIcebergScanTask us_task(allocator);
  ObIcebergScanTask unpartitioned_task(allocator);
  ASSERT_EQ(OB_SUCCESS, cn_task.init_with_opt_lake_table_file(allocator, cn_opt_file));
  ASSERT_EQ(OB_SUCCESS, us_task.init_with_opt_lake_table_file(allocator, us_opt_file));
  ASSERT_EQ(OB_SUCCESS,
            unpartitioned_task.init_with_opt_lake_table_file(allocator, unpartitioned_opt_file));
  EXPECT_EQ(0, cn_task.part_id_);
  EXPECT_EQ(1, us_task.part_id_);
  EXPECT_EQ(2, unpartitioned_task.part_id_);
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().update_cluster_version(
      CLUSTER_CURRENT_VERSION);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
