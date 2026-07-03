/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/ai_service/ob_ai_batch_file_manager.h"
#undef protected
#undef private
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ai_service/ob_ai_func_provider.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

class TestAiBatchFileManager : public ::testing::Test
{
public:
  // Generate a single valid JSONL line for embedding result
  // Format: {"id":"batch_req_N","custom_id":"N","response":{"status_code":200,"body":{"data":[{"embedding":[0.0,0.1,...]}]}}}
  static int64_t generate_jsonl_line(char *buf, int64_t buf_size, int index, int dim)
  {
    int64_t pos = 0;
    int ret = databuff_printf(buf, buf_size, pos,
        "{\"id\":\"batch_req_%d\",\"custom_id\":\"%d\","
        "\"response\":{\"status_code\":200,"
        "\"body\":{\"data\":[{\"embedding\":[", index, index);
    if (ret < 0) return -1;

    for (int i = 0; i < dim; ++i) {
      if (i > 0) {
        if (databuff_printf(buf, buf_size, pos, ",") < 0) return -1;
      }
      if (databuff_printf(buf, buf_size, pos, "%.6f",
              static_cast<double>(index * dim + i) * 0.001) < 0) return -1;
    }

    if (databuff_printf(buf, buf_size, pos, "]}]}},\"error\":null}\n") < 0) return -1;
    return pos;
  }

  // Generate buffer with N JSONL lines
  static int generate_jsonl_buffer(
      common::ObArenaAllocator &allocator,
      int num_lines,
      int dim,
      char *&out_buf,
      int64_t &out_size)
  {
    int ret = OB_SUCCESS;
    int64_t estimated = static_cast<int64_t>(num_lines) * (dim * 20 + 200);
    char *buf = static_cast<char*>(allocator.alloc(estimated + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      int64_t pos = 0;
      for (int i = 0; i < num_lines && pos < estimated; ++i) {
        int64_t written = generate_jsonl_line(buf + pos, estimated - pos, i, dim);
        if (written < 0) {
          ret = OB_SIZE_OVERFLOW;
          break;
        }
        pos += written;
      }
      buf[pos] = '\0';
      out_buf = buf;
      out_size = pos;
    }
    return ret;
  }
};

// Basic: valid JSONL with a few lines
TEST_F(TestAiBatchFileManager, parse_jsonl_basic)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator allocator("TestBatch");
  ObArray<ObAiBatchLineResult> results;
  ObArenaAllocator result_alloc("TestResult");

  const int dim = 16;
  const int num_lines = 5;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, generate_jsonl_buffer(allocator, num_lines, dim, buf, size));
  ASSERT_GT(size, 0);

  ASSERT_EQ(OB_SUCCESS, manager.parse_jsonl_results_from_buffer(result_alloc, buf, size, results));
  ASSERT_EQ(num_lines, results.count());

  for (int i = 0; i < num_lines; ++i) {
    char expected_id[32];
    snprintf(expected_id, sizeof(expected_id), "%d", i);
    EXPECT_STREQ(expected_id, results.at(i).custom_id_.ptr());
    EXPECT_EQ(200, results.at(i).response_status_);
    EXPECT_TRUE(results.at(i).error_detail_.empty());
    // Verify body is non-empty (contains embedding data)
    EXPECT_GT(results.at(i).response_body_.length(), 0);
  }
}

// Empty buffer → error
TEST_F(TestAiBatchFileManager, parse_jsonl_empty_buffer)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator result_alloc("TestResult");
  ObArray<ObAiBatchLineResult> results;

  int ret = manager.parse_jsonl_results_from_buffer(result_alloc, "", 0, results);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// NULL buffer → error
TEST_F(TestAiBatchFileManager, parse_jsonl_null_buffer)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator result_alloc("TestResult");
  ObArray<ObAiBatchLineResult> results;

  int ret = manager.parse_jsonl_results_from_buffer(result_alloc, nullptr, 10, results);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// Empty lines (just \n) should be skipped
TEST_F(TestAiBatchFileManager, parse_jsonl_empty_lines)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator allocator("TestBatch");
  ObArenaAllocator result_alloc("TestResult");
  ObArray<ObAiBatchLineResult> results;

  const char *input =
      "\n"
      "{\"id\":\"r1\",\"custom_id\":\"1\",\"response\":{\"status_code\":200,\"body\":{\"data\":[{\"embedding\":[0.1]}]}}}\n"
      "\n"
      "{\"id\":\"r2\",\"custom_id\":\"2\",\"response\":{\"status_code\":200,\"body\":{\"data\":[{\"embedding\":[0.2]}]}}}\n"
      "\n";

  int64_t size = strlen(input);
  char *buf = static_cast<char*>(allocator.alloc(size + 1));
  MEMCPY(buf, input, size + 1);

  ASSERT_EQ(OB_SUCCESS, manager.parse_jsonl_results_from_buffer(result_alloc, buf, size, results));
  // Only 2 non-empty lines
  ASSERT_EQ(2, results.count());
  EXPECT_STREQ("1", results.at(0).custom_id_.ptr());
  EXPECT_STREQ("2", results.at(1).custom_id_.ptr());
}

// Last line without trailing newline → still parsed
TEST_F(TestAiBatchFileManager, parse_jsonl_no_trailing_newline)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator allocator("TestBatch");
  ObArenaAllocator result_alloc("TestResult");
  ObArray<ObAiBatchLineResult> results;

  const char *input =
      "{\"id\":\"r1\",\"custom_id\":\"1\",\"response\":{\"status_code\":200,\"body\":{\"data\":[{\"embedding\":[0.1]}]}}}\n"
      "{\"id\":\"r2\",\"custom_id\":\"2\",\"response\":{\"status_code\":200,\"body\":{\"data\":[{\"embedding\":[0.2]}]}}}";

  int64_t size = strlen(input);
  char *buf = static_cast<char*>(allocator.alloc(size + 1));
  MEMCPY(buf, input, size + 1);

  ASSERT_EQ(OB_SUCCESS, manager.parse_jsonl_results_from_buffer(result_alloc, buf, size, results));
  EXPECT_EQ(2, results.count());
  EXPECT_STREQ("1", results.at(0).custom_id_.ptr());
  EXPECT_STREQ("2", results.at(1).custom_id_.ptr());
}

// Large buffer: 10K lines (~5MB) → verify count, no crash
TEST_F(TestAiBatchFileManager, parse_jsonl_large_10k)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator allocator("TestBatch");
  ObArenaAllocator result_alloc("TestResult");
  ObArray<ObAiBatchLineResult> results;

  const int dim = 16;
  const int num_lines = 10000;
  char *buf = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, generate_jsonl_buffer(allocator, num_lines, dim, buf, size));
  ASSERT_GT(size, 1000000);  // should be >1MB

  LOG_INFO("LARGE_PARSE_TEST", K(num_lines), K(size));

  ASSERT_EQ(OB_SUCCESS, manager.parse_jsonl_results_from_buffer(result_alloc, buf, size, results));
  ASSERT_EQ(num_lines, results.count());

  // Spot-check a few entries
  EXPECT_STREQ("0", results.at(0).custom_id_.ptr());
  EXPECT_EQ(200, results.at(0).response_status_);
  EXPECT_STREQ("9999", results.at(9999).custom_id_.ptr());
  EXPECT_EQ(200, results.at(9999).response_status_);
}

// Malformed JSON lines: parsing stops at first error, but doesn't crash
TEST_F(TestAiBatchFileManager, parse_jsonl_malformed_lines)
{
  ObAiBatchFileManager manager;
  ObArenaAllocator allocator("TestBatch");
  ObArenaAllocator result_alloc("TestResult");
  ObArray<ObAiBatchLineResult> results;

  const char *input =
      "{\"id\":\"r1\",\"custom_id\":\"1\",\"response\":{\"status_code\":200,\"body\":{\"data\":[{\"embedding\":[0.1]}]}}}\n"
      "this is not json\n";   // malformed line causes parse to fail

  int64_t size = strlen(input);
  char *buf = static_cast<char*>(allocator.alloc(size + 1));
  MEMCPY(buf, input, size + 1);

  // First line parses OK, second line fails → function returns error
  int ret = manager.parse_jsonl_results_from_buffer(result_alloc, buf, size, results);
  ASSERT_NE(OB_SUCCESS, ret);
  // First valid line should have been parsed before the error
  ASSERT_GE(results.count(), 1);
  EXPECT_STREQ("1", results.at(0).custom_id_.ptr());
}

TEST_F(TestAiBatchFileManager, batch_status_mapping)
{
  EXPECT_EQ(OB_AI_BATCH_FILE_STATUS_UPLOADED,
            ObAiBatchFileManagerUtils::str_to_batch_status(ObString::make_string("validating")));
  EXPECT_EQ(OB_AI_BATCH_FILE_STATUS_IN_PROGRESS,
            ObAiBatchFileManagerUtils::str_to_batch_status(ObString::make_string("cancelling")));
  EXPECT_EQ(OB_AI_BATCH_FILE_STATUS_CANCELLED,
            ObAiBatchFileManagerUtils::str_to_batch_status(ObString::make_string("cancelled")));
}

// ==================== to_json tests ====================

TEST_F(TestAiBatchFileManager, to_json_basic)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString::make_string("42");
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_SUCCESS, line.to_json(allocator, json_str));
  ASSERT_GT(json_str.length(), 0);

  ObString expected = ObString::make_string(
      "{\"custom_id\":\"42\",\"method\":\"POST\","
      "\"url\":\"/v1/embeddings\","
      "\"body\":{\"model\":\"m\",\"input\":\"hello\"}}");
  EXPECT_EQ(0, expected.compare(json_str));
}

TEST_F(TestAiBatchFileManager, to_json_empty_body)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString::make_string("1");
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings");
  // body_ left empty

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

TEST_F(TestAiBatchFileManager, to_json_injection_custom_id_with_quote)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString::make_string("1\",\"evil\":\"x");
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

TEST_F(TestAiBatchFileManager, to_json_injection_custom_id_with_backslash)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString::make_string("1\\\"evil");
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

TEST_F(TestAiBatchFileManager, to_json_injection_method_with_quote)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString::make_string("1");
  line.method_ = ObString::make_string("POST\",\"injected\":\"true");
  line.url_ = ObString::make_string("/v1/embeddings");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

TEST_F(TestAiBatchFileManager, to_json_injection_url_with_quote)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString::make_string("1");
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings\",\"url\":\"/admin/drop");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

TEST_F(TestAiBatchFileManager, to_json_control_char_in_custom_id)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  line.custom_id_ = ObString(4, "ab\nc");
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

TEST_F(TestAiBatchFileManager, to_json_custom_id_exceeds_max_length)
{
  ObArenaAllocator allocator("TestToJson");
  ObAiBatchFileLine line;
  char long_id[300];
  memset(long_id, 'a', sizeof(long_id));
  line.custom_id_ = ObString(sizeof(long_id), long_id);
  line.method_ = ObString::make_string("POST");
  line.url_ = ObString::make_string("/v1/embeddings");
  line.body_ = ObString::make_string("{\"model\":\"m\",\"input\":\"hello\"}");

  ObString json_str;
  ASSERT_EQ(OB_INVALID_ARGUMENT, line.to_json(allocator, json_str));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ai_batch_file_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
