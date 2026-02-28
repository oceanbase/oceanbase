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

#define USING_LOG_PREFIX SQL_EXE
#include <gtest/gtest.h>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include "sql/ob_sql_init.h"
#include "sql/engine/px/ob_granule_parallel_task_gen.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestGamblingBound : public ::testing::Test
{
public:
  TestGamblingBound() = default;
  virtual ~TestGamblingBound() = default;
  virtual void SetUp() {};
  virtual void TearDown() {};

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestGamblingBound);
};

static int read_file_to_buffer(const char* file_path, char*& buffer, int64_t& file_size)
{
  int ret = OB_SUCCESS;
  buffer = nullptr;
  file_size = 0;

  if (OB_ISNULL(file_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file_path is null", K(ret));
  } else {
    std::ifstream file_stream(file_path, std::ios::binary | std::ios::ate);
    if (!file_stream.is_open()) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("Failed to open file", K(ret), K(file_path));
    } else {
      std::streamsize size = file_stream.tellg();
      file_stream.seekg(0, std::ios::beg);

      if (size <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("File size is invalid", K(ret), K(size));
      } else {
        buffer = static_cast<char*>(ob_malloc(size, ObModIds::OB_SQL_EXECUTOR));
        if (nullptr == buffer) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Failed to allocate buffer", K(ret), K(size));
        } else {
          file_stream.read(buffer, size);
          std::streamsize bytes_read = file_stream.gcount();
          if (bytes_read != size) {
            ob_free(buffer);
            buffer = nullptr;
            ret = OB_IO_ERROR;
            LOG_WARN("Failed to read all file content", K(ret), K(bytes_read), K(size));
          } else {
            file_size = static_cast<int64_t>(size);
          }
        }
      }
      file_stream.close();
    }
  }

  return ret;
}

static int init_csv_file_format(ObArenaAllocator &allocator,
                                 ObExternalFileFormat &file_format,
                                 const ObString &field_term_str,
                                 const ObString &line_term_str,
                                 int64_t field_enclosed_char,
                                 int64_t field_escaped_char,
                                 int64_t max_row_length)
{
  int ret = OB_SUCCESS;

  file_format.format_type_ = ObExternalFileFormat::CSV_FORMAT;

  if (OB_FAIL(ob_write_string(allocator, field_term_str, file_format.csv_format_.field_term_str_, true))) {
    LOG_WARN("Failed to write field_term_str", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, line_term_str, file_format.csv_format_.line_term_str_, true))) {
    LOG_WARN("Failed to write line_term_str", K(ret));
  } else {
    file_format.csv_format_.field_enclosed_char_ = field_enclosed_char;
    file_format.csv_format_.field_escaped_char_ = field_escaped_char;
    file_format.csv_format_.max_row_length_ = max_row_length;
  }

  return ret;
}

TEST_F(TestGamblingBound, test_gambling_functor)
{
  int ret = OB_SUCCESS;

  const char* test_file_path = std::getenv("TEST_GAMBLING_BOUND_FILE");
  if (nullptr == test_file_path || strlen(test_file_path) == 0) {
    // do nothing
  } else {
    char* buffer = nullptr;
    int64_t file_size = 0;
    if (OB_SUCC(ret) && OB_FAIL(read_file_to_buffer(test_file_path, buffer, file_size))) {
      LOG_WARN("Failed to read file", K(ret), K(test_file_path));
    }

    ObArenaAllocator allocator(ObModIds::OB_SQL_EXECUTOR);
    ObExternalFileFormat file_format;
    ObString field_term_str("|");
    ObString line_term_str("\n");
    int64_t field_enclosed_char = '\'';
    int64_t field_escaped_char = '\\';
    int64_t max_row_length = 500; // 2MB
    if (OB_SUCC(ret) && OB_FAIL(init_csv_file_format(allocator, file_format, field_term_str, line_term_str,
                                                    field_enclosed_char, field_escaped_char, max_row_length))) {
      LOG_WARN("Failed to init CSV file format", K(ret));
    }

    if (OB_SUCC(ret)) {
      GamblingFunctor functor;
      for (int64_t i = 0; i < file_size && OB_SUCC(ret); ++i) {
        if (OB_FAIL(functor(file_format, buffer + i, 1))) {
          LOG_WARN("Failed to call GamblingFunctor", K(ret));
        } else {
          LOG_INFO("gambling state", K(i));
          functor.print_state();
        }
      }
    }

    // Clean up
    if (nullptr != buffer) {
      ob_free(buffer);
      buffer = nullptr;
    }
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
