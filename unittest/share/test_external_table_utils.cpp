/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/external_table/ob_external_table_utils.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace share
{
using namespace common;

class TestExternalTableUtils : public ::testing::Test
{
protected:
  void check_csv_row_metadata(const bool expected_is_parallel,
                              const int64_t expected_file_id,
                              const int64_t expected_chunk_id,
                              const int64_t expected_line_number,
                              const int64_t expected_row_start_offset)
  {
    int128_t encoded = 0;
    ASSERT_EQ(OB_SUCCESS,
              ObExternalTableUtils::encode_csv_row_metadata(expected_is_parallel,
                                                            expected_file_id,
                                                            expected_chunk_id,
                                                            expected_line_number,
                                                            expected_row_start_offset,
                                                            encoded));

    bool is_parallel = false;
    int64_t file_id = OB_INVALID_INDEX;
    int64_t chunk_id = OB_INVALID_INDEX;
    int64_t line_number = OB_INVALID_INDEX;
    int64_t row_start_offset = OB_INVALID_INDEX;
    ASSERT_EQ(OB_SUCCESS,
              ObExternalTableUtils::decode_csv_row_metadata(encoded,
                                                            is_parallel,
                                                            file_id,
                                                            chunk_id,
                                                            line_number,
                                                            row_start_offset));
    ASSERT_EQ(expected_is_parallel, is_parallel);
    ASSERT_EQ(expected_file_id, file_id);
    ASSERT_EQ(expected_is_parallel ? expected_chunk_id : OB_INVALID_INDEX, chunk_id);
    ASSERT_EQ(expected_line_number, line_number);
    ASSERT_EQ(expected_row_start_offset, row_start_offset);
  }
};

TEST_F(TestExternalTableUtils, encode_decode_parallel_csv_row_metadata)
{
  const int64_t file_id = 12345;
  const int64_t chunk_id = 678;
  const int64_t line_number = 9876543;
  const int64_t row_start_offset = 1234567890123;
  int128_t encoded = 0;
  const uint64_t expected_low_word = ObExternalTableUtils::ROW_METADATA_PARALLEL_FLAG
      | (static_cast<uint64_t>(file_id) << ObExternalTableUtils::ROW_METADATA_FILE_ID_SHIFT)
      | (static_cast<uint64_t>(chunk_id) << ObExternalTableUtils::ROW_METADATA_CHUNK_ID_SHIFT)
      | (static_cast<uint64_t>(line_number)
         << ObExternalTableUtils::ROW_METADATA_PARALLEL_LINE_NUMBER_SHIFT);

  ASSERT_EQ(OB_SUCCESS,
            ObExternalTableUtils::encode_csv_row_metadata(true,
                                                          file_id,
                                                          chunk_id,
                                                          line_number,
                                                          row_start_offset,
                                                          encoded));
  ASSERT_EQ(expected_low_word, static_cast<uint64_t>(static_cast<int64_t>(encoded)));
  ASSERT_EQ(static_cast<uint64_t>(row_start_offset),
            static_cast<uint64_t>(static_cast<int64_t>(
                encoded >> ObExternalTableUtils::ROW_METADATA_OFFSET_SHIFT)));
  check_csv_row_metadata(true, file_id, chunk_id, line_number, row_start_offset);

  check_csv_row_metadata(true,
                         ObExternalTableUtils::ROW_METADATA_FILE_ID_MASK,
                         ObExternalTableUtils::ROW_METADATA_CHUNK_ID_MASK,
                         ObExternalTableUtils::ROW_METADATA_PARALLEL_LINE_NUMBER_MASK,
                         ObExternalTableUtils::ROW_METADATA_OFFSET_MASK);
}

TEST_F(TestExternalTableUtils, encode_decode_non_parallel_csv_row_metadata)
{
  const int64_t file_id = 54321;
  const int64_t line_number = 123456789;
  const int64_t row_start_offset = 9876543210123;
  int128_t encoded = 0;
  const uint64_t expected_low_word =
      (static_cast<uint64_t>(file_id) << ObExternalTableUtils::ROW_METADATA_FILE_ID_SHIFT)
      | (static_cast<uint64_t>(line_number)
         << ObExternalTableUtils::ROW_METADATA_NON_PARALLEL_LINE_NUMBER_SHIFT);

  ASSERT_EQ(OB_SUCCESS,
            ObExternalTableUtils::encode_csv_row_metadata(false,
                                                          file_id,
                                                          0,
                                                          line_number,
                                                          row_start_offset,
                                                          encoded));
  ASSERT_EQ(expected_low_word, static_cast<uint64_t>(static_cast<int64_t>(encoded)));
  ASSERT_EQ(static_cast<uint64_t>(row_start_offset),
            static_cast<uint64_t>(static_cast<int64_t>(
                encoded >> ObExternalTableUtils::ROW_METADATA_OFFSET_SHIFT)));
  check_csv_row_metadata(false, file_id, 0, line_number, row_start_offset);

  check_csv_row_metadata(false,
                         ObExternalTableUtils::ROW_METADATA_FILE_ID_MASK,
                         0,
                         ObExternalTableUtils::ROW_METADATA_NON_PARALLEL_LINE_NUMBER_MASK,
                         ObExternalTableUtils::ROW_METADATA_OFFSET_MASK);
}

TEST_F(TestExternalTableUtils, reject_out_of_range_csv_row_metadata)
{
  int128_t encoded = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(false, -1, 0, 0, 0, encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(
                false, ObExternalTableUtils::ROW_METADATA_FILE_ID_MASK + 1, 0, 0, 0, encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(true, 0, -1, 0, 0, encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(
                true, 0, ObExternalTableUtils::ROW_METADATA_CHUNK_ID_MASK + 1, 0, 0, encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(true, 0, 0, -1, 0, encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(
                true,
                0,
                0,
                ObExternalTableUtils::ROW_METADATA_PARALLEL_LINE_NUMBER_MASK + 1,
                0,
                encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(
                false,
                0,
                0,
                ObExternalTableUtils::ROW_METADATA_NON_PARALLEL_LINE_NUMBER_MASK + 1,
                0,
                encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(false, 0, 0, 0, -1, encoded));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObExternalTableUtils::encode_csv_row_metadata(
                false,
                0,
                0,
                0,
                ObExternalTableUtils::ROW_METADATA_OFFSET_MASK + 1,
                encoded));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
