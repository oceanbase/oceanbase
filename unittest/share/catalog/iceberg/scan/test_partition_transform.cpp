/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#define private public
#include "sql/optimizer/file_prune/ob_iceberg_file_pruner.h"
#undef private

#include <gmock/gmock.h>
#include <gtest/gtest.h>


using namespace oceanbase;

class TestPartitionTransform : public ::testing::Test {

};

TEST_F(TestPartitionTransform, bucket_hash) {
  ObNewRange range;
  ObObj start;
  ObObj end;
  range.start_key_ = ObRowkey(&start, 1);
  range.end_key_ = ObRowkey(&end, 1);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  // int
  {
    // hash value 1392991556
    start.set_int32(1);
    end.set_int32(1);
    ASSERT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(1, start.get_int32());
  }

  // date
  {
    // hash value 1176478452
    start.set_date(2);
    end.set_date(2);
    ASSERT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(2, start.get_int32());
  }

  // long
  {
    // hash value 2017239379
    start.set_int(34);
    end.set_int(34);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(4, start.get_int32());
  }

  // time
  {
    // hash value 1066781525
    start.set_time(35);
    end.set_time(35);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(0, start.get_int32());
  }

  // datetime
  {
    // hash value 1497025390
    start.set_datetime(36);
    end.set_datetime(36);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(0, start.get_int32());
  }

  // timestamp
  {
    // hash value 978785182
    start.set_time(37);
    end.set_time(37);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(2, start.get_int32());
  }

  // binary
  {
    // hash value 1210000089
    ObString s = "iceberg";
    start.set_varchar(s);
    end.set_varchar(s);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(4, start.get_int32());
  }

  // decimal(14.20)
  {
    // hash value 1646729059
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    buf[0] = 0x8c;
    buf[1] = 0x05;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(4, start.get_int32());
  }

  // decimal(0.00)
  {
    // hash value 1364076727
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    // buf[0] = 0x8c;
    // buf[1] = 0x05;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(2, start.get_int32());
  }

  // decimal(-14.20)
  {
    // hash value 667775751
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    buf[0] = 0x74;
    buf[1] = 0xFA;
    buf[2] = 0xFF;
    buf[3] = 0xFF;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(1, start.get_int32());
  }

  {
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    buf[0] = 0x00;
    buf[1] = 0xFF;
    buf[2] = 0xFF;
    buf[3] = 0xFF;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(0, start.get_int32());
  }
}

TEST_F(TestPartitionTransform, identity_partition_exact_filter)
{
  ObArenaAllocator allocator;
  sql::ObIcebergFilePrunner pruner(allocator);
  sql::ObConstRawExpr filter_expr;
  const uint64_t column_id = 123;

  ASSERT_EQ(OB_SUCCESS, pruner.part_bound_.init(2));
  for (int32_t spec_id = 0; spec_id < 2; ++spec_id) {
    sql::ObIcebergPartBound *part_bound =
        OB_NEWx(sql::ObIcebergPartBound, &allocator, allocator);
    sql::ObPartFieldBound *field_bound =
        OB_NEWx(sql::ObPartFieldBound, &allocator, allocator);
    ASSERT_NE(nullptr, part_bound);
    ASSERT_NE(nullptr, field_bound);
    ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.init(1));
    ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.init(1));
    field_bound->column_id_ = column_id;
    field_bound->transform_type_ = sql::iceberg::TransformType::Identity;
    ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.push_back(&filter_expr));
    ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.push_back(field_bound));
    ASSERT_EQ(OB_SUCCESS, pruner.part_bound_.push_back(std::make_pair(spec_id, part_bound)));
  }

  ObSEArray<uint64_t, 2> part_column_ids;
  ObSEArray<sql::ObRawExpr *, 2> range_exprs;
  ASSERT_EQ(OB_SUCCESS, pruner.get_part_id_and_range_exprs(part_column_ids, range_exprs));
  ASSERT_EQ(1, part_column_ids.count());
  ASSERT_EQ(column_id, part_column_ids.at(0));
  ASSERT_EQ(1, range_exprs.count());
  ASSERT_EQ(&filter_expr, range_exprs.at(0));
}

TEST_F(TestPartitionTransform, single_identity_spec_survives_pruner_copy)
{
  ObArenaAllocator allocator;
  sql::ObIcebergFilePrunner source_pruner(allocator);
  sql::ObIcebergFilePrunner copied_pruner(allocator);
  sql::ObConstRawExpr filter_expr;
  const uint64_t column_id = 123;
  sql::ObIcebergPartBound *part_bound =
      OB_NEWx(sql::ObIcebergPartBound, &allocator, allocator);
  sql::ObPartFieldBound *field_bound =
      OB_NEWx(sql::ObPartFieldBound, &allocator, allocator);

  ASSERT_NE(nullptr, part_bound);
  ASSERT_NE(nullptr, field_bound);
  ASSERT_EQ(OB_SUCCESS, source_pruner.part_bound_.init(1));
  ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.init(1));
  ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.init(1));
  field_bound->column_id_ = column_id;
  field_bound->transform_type_ = sql::iceberg::TransformType::Identity;
  ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.push_back(&filter_expr));
  ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.push_back(field_bound));
  ASSERT_EQ(OB_SUCCESS, source_pruner.part_bound_.push_back(std::make_pair(0, part_bound)));
  ASSERT_EQ(OB_SUCCESS, copied_pruner.assign(source_pruner));

  ObSEArray<uint64_t, 2> part_column_ids;
  ObSEArray<sql::ObRawExpr *, 2> range_exprs;
  ASSERT_EQ(OB_SUCCESS,
            copied_pruner.get_part_id_and_range_exprs(part_column_ids, range_exprs));
  ASSERT_EQ(1, part_column_ids.count());
  ASSERT_EQ(column_id, part_column_ids.at(0));
  ASSERT_EQ(1, range_exprs.count());
  ASSERT_EQ(&filter_expr, range_exprs.at(0));
}

TEST_F(TestPartitionTransform, mixed_partition_spec_keeps_filter)
{
  ObArenaAllocator allocator;
  sql::ObIcebergFilePrunner pruner(allocator);
  sql::ObConstRawExpr filter_expr;
  const uint64_t column_id = 123;

  ASSERT_EQ(OB_SUCCESS, pruner.part_bound_.init(2));
  for (int32_t spec_id = 0; spec_id < 2; ++spec_id) {
    sql::ObIcebergPartBound *part_bound =
        OB_NEWx(sql::ObIcebergPartBound, &allocator, allocator);
    sql::ObPartFieldBound *field_bound =
        OB_NEWx(sql::ObPartFieldBound, &allocator, allocator);
    ASSERT_NE(nullptr, part_bound);
    ASSERT_NE(nullptr, field_bound);
    ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.init(1));
    ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.init(1));
    field_bound->column_id_ = column_id;
    field_bound->transform_type_ = spec_id == 0
        ? sql::iceberg::TransformType::Identity
        : sql::iceberg::TransformType::Day;
    ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.push_back(&filter_expr));
    ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.push_back(field_bound));
    ASSERT_EQ(OB_SUCCESS, pruner.part_bound_.push_back(std::make_pair(spec_id, part_bound)));
  }

  ObSEArray<uint64_t, 2> part_column_ids;
  ObSEArray<sql::ObRawExpr *, 2> range_exprs;
  ASSERT_EQ(OB_SUCCESS, pruner.get_part_id_and_range_exprs(part_column_ids, range_exprs));
  ASSERT_TRUE(part_column_ids.empty());
  ASSERT_TRUE(range_exprs.empty());
}

TEST_F(TestPartitionTransform, different_partition_column_keeps_filter)
{
  ObArenaAllocator allocator;
  sql::ObIcebergFilePrunner pruner(allocator);
  sql::ObConstRawExpr filter_expr;
  const uint64_t column_id = 123;
  const uint64_t other_column_id = 456;

  ASSERT_EQ(OB_SUCCESS, pruner.part_bound_.init(2));
  for (int32_t spec_id = 0; spec_id < 2; ++spec_id) {
    sql::ObIcebergPartBound *part_bound = OB_NEWx(sql::ObIcebergPartBound, &allocator, allocator);
    sql::ObPartFieldBound *field_bound = OB_NEWx(sql::ObPartFieldBound, &allocator, allocator);
    ASSERT_NE(nullptr, part_bound);
    ASSERT_NE(nullptr, field_bound);
    ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.init(1));
    ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.init(1));
    field_bound->column_id_ = spec_id == 0 ? column_id : other_column_id;
    field_bound->transform_type_ = sql::iceberg::TransformType::Identity;
    ASSERT_EQ(OB_SUCCESS, field_bound->range_exprs_.push_back(&filter_expr));
    ASSERT_EQ(OB_SUCCESS, part_bound->part_field_bounds_.push_back(field_bound));
    ASSERT_EQ(OB_SUCCESS, pruner.part_bound_.push_back(std::make_pair(spec_id, part_bound)));
  }

  ObSEArray<uint64_t, 2> part_column_ids;
  ObSEArray<sql::ObRawExpr *, 2> range_exprs;
  ASSERT_EQ(OB_SUCCESS, pruner.get_part_id_and_range_exprs(part_column_ids, range_exprs));
  ASSERT_TRUE(part_column_ids.empty());
  ASSERT_TRUE(range_exprs.empty());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
