// owner: maochongxin.mcx
// owner group: shenzhen

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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "mittest/env/ob_simple_server_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "share/ai_service/ob_batch_file_jsonl_writer.h"
#include "share/ai_service/ob_batch_file_jsonl_iterator.h"
#include "share/ai_service/ob_ai_batch_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::tmp_file;

namespace oceanbase
{
namespace unittest
{

class TestAiBatchFileDownload : public ObSimpleClusterTestBase
{
public:
  TestAiBatchFileDownload() : ObSimpleClusterTestBase("test_ai_batch_file_download") {}
  virtual ~TestAiBatchFileDownload() {}

  virtual void SetUp() override
  {
    bool tenant_exist = false;
    if (OB_SUCCESS != check_tenant_exist(tenant_exist) || !tenant_exist) {
      ObSimpleClusterTestBase::SetUp();
      ASSERT_EQ(OB_SUCCESS, create_tenant());
      ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    }
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  }

  // Generate a single JSONL line (same format as real batch results)
  static int64_t generate_result_line(char *buf, int64_t buf_size, int index, int dim)
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

    if (databuff_printf(buf, buf_size, pos, "]}]}},\"error\":null}") < 0) return -1;
    return pos;
  }

protected:
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(TestAiBatchFileDownload);
};

// L2-1: Write N lines via ObBatchFileJsonlWriter, read back via Iterator, parse and verify
TEST_F(TestAiBatchFileDownload, write_and_read_back_small)
{
  const int num_lines = 100;
  const int dim = 16;

  // Switch to tenant context for TmpFileManager access
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  // Allocate a TmpFile directory
  int64_t dir_id = -1;
  ASSERT_EQ(OB_SUCCESS,
      FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id));
  ASSERT_GE(dir_id, 0);

  // Write lines via ObBatchFileJsonlWriter
  ObBatchFileJsonlWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(dir_id, tenant_id_, "TestBatchDL"));
  ASSERT_TRUE(writer.is_inited());

  char line_buf[4096];
  for (int i = 0; i < num_lines; ++i) {
    int64_t len = generate_result_line(line_buf, sizeof(line_buf), i, dim);
    ASSERT_GT(len, 0);
    ObString line(len, line_buf);
    ASSERT_EQ(OB_SUCCESS, writer.write_line(line));
  }
  EXPECT_EQ(num_lines, writer.get_line_count());

  // Finish writing → get segment metadata
  ObBatchFileDataSegment segment;
  ASSERT_EQ(OB_SUCCESS, writer.finish(segment));
  ASSERT_TRUE(segment.is_valid());
  ASSERT_EQ(num_lines, segment.line_count_);
  ASSERT_GT(segment.size_, 0);

  // Read back via ObBatchFileJsonlIterator
  ObBatchFileJsonlIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(segment.fd_, segment.start_offset_,
                                   segment.size_, tenant_id_));

  ObAiBatchFileManager manager; // only used to call parse_jsonl_line_
  ObArenaAllocator parse_alloc("TestJsonParse");
  int read_count = 0;
  for (int i = 0; i < num_lines; ++i) {
    ObString line;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_line(line));
    ASSERT_FALSE(line.empty());

    // Parse the line via the production parser
    ObArenaAllocator json_alloc("TestJsonLine");
    ObAiBatchLineResult result;
    ASSERT_EQ(OB_SUCCESS, manager.parse_jsonl_line_(parse_alloc, json_alloc,
                                                     line.ptr(), line.length(), result));
    ASSERT_TRUE(result.is_success());
    ASSERT_EQ(200, result.response_status_);

    char expected_id[32];
    snprintf(expected_id, sizeof(expected_id), "%d", read_count);
    EXPECT_STREQ(expected_id, result.custom_id_.ptr());
    ++read_count;
  }
  EXPECT_EQ(num_lines, read_count);

  // Verify no more lines
  ObString extra_line;
  EXPECT_EQ(OB_ITER_END, iter.get_next_line(extra_line));

  writer.destroy();
  iter.destroy();
}

// L2-2: 写大量行 (10K)，验证不丢数据、不 OOM
TEST_F(TestAiBatchFileDownload, write_and_read_back_large)
{
  const int num_lines = 10000;
  const int dim = 16;

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  int64_t dir_id = -1;
  ASSERT_EQ(OB_SUCCESS,
      FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id));

  ObBatchFileJsonlWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(dir_id, tenant_id_, "TestBatchDL"));
  LOG_INFO("LARGE_FILE_TEST: writing lines", K(num_lines));

  char *line_buf = static_cast<char*>(ob_malloc(8192, "TestLineBuf"));
  ASSERT_NE(nullptr, line_buf);
  for (int i = 0; i < num_lines; ++i) {
    int64_t len = generate_result_line(line_buf, 8192, i, dim);
    ASSERT_GT(len, 0);
    ObString line(len, line_buf);
    ASSERT_EQ(OB_SUCCESS, writer.write_line(line));
  }
  EXPECT_EQ(num_lines, writer.get_line_count());

  ObBatchFileDataSegment segment;
  ASSERT_EQ(OB_SUCCESS, writer.finish(segment));
  ASSERT_EQ(num_lines, segment.line_count_);
  LOG_INFO("LARGE_FILE_TEST: finished writing", K(segment));

  // Read back and verify count
  ObBatchFileJsonlIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(segment.fd_, segment.start_offset_,
                                   segment.size_, tenant_id_));

  int ret = OB_SUCCESS;
  int read_count = 0;
  ObString line;
  while (OB_SUCC(iter.get_next_line(line))) {
    ++read_count;
  }
  EXPECT_EQ(num_lines, read_count);
  LOG_INFO("LARGE_FILE_TEST: verified", K(read_count));

  ob_free(line_buf);
  writer.destroy();
  iter.destroy();
}

// L2-3: 模拟 curl chunk 写入 (write_chunk 随机分片)
TEST_F(TestAiBatchFileDownload, chunk_write_and_read_back)
{
  const int num_lines = 500;
  const int dim = 16;

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  int64_t dir_id = -1;
  ASSERT_EQ(OB_SUCCESS,
      FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(tenant_id_, dir_id));

  // Build full JSONL buffer first
  ObArenaAllocator buf_alloc("TestChunkBuf");
  int64_t est_size = num_lines * (dim * 20 + 200);
  char *full_buf = static_cast<char*>(buf_alloc.alloc(est_size + 1));
  ASSERT_NE(nullptr, full_buf);

  int64_t buf_pos = 0;
  char line_buf[4096];
  for (int i = 0; i < num_lines; ++i) {
    int64_t len = generate_result_line(line_buf, sizeof(line_buf), i, dim);
    // append line + \n
    MEMCPY(full_buf + buf_pos, line_buf, len);
    buf_pos += len;
    full_buf[buf_pos++] = '\n';
  }
  int64_t full_size = buf_pos;

  // Write via random-sized chunks (simulate TCP fragmentation)
  ObBatchFileJsonlWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(dir_id, tenant_id_, "TestChunkWr"));

  int64_t offset = 0;
  unsigned int seed = 42;
  while (offset < full_size) {
    int64_t remaining = full_size - offset;
    // Random chunk size between 1 and 4095 bytes
    int64_t chunk_size = (rand_r(&seed) % 4095) + 1;
    if (chunk_size > remaining) chunk_size = remaining;

    ASSERT_EQ(OB_SUCCESS, writer.write_chunk(full_buf + offset, chunk_size));
    offset += chunk_size;
  }

  ObBatchFileDataSegment segment;
  ASSERT_EQ(OB_SUCCESS, writer.finish(segment));
  ASSERT_EQ(num_lines, segment.line_count_);

  // Read back and verify
  ObBatchFileJsonlIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(segment.fd_, segment.start_offset_,
                                   segment.size_, tenant_id_));

  ObAiBatchFileManager manager;
  ObArenaAllocator parse_alloc("TestJsonP");
  int ret = OB_SUCCESS;
  int read_count = 0;
  ObString line;
  while (OB_SUCC(iter.get_next_line(line))) {
    if (line.empty()) continue; // skip empty lines between chunks
    ObArenaAllocator json_alloc("TestJLine");
    ObAiBatchLineResult result;
    ASSERT_EQ(OB_SUCCESS, manager.parse_jsonl_line_(parse_alloc, json_alloc,
                                                     line.ptr(), line.length(), result));
    ASSERT_TRUE(result.is_success());

    char expected_id[32];
    snprintf(expected_id, sizeof(expected_id), "%d", read_count);
    EXPECT_STREQ(expected_id, result.custom_id_.ptr());
    ++read_count;
  }
  EXPECT_EQ(num_lines, read_count);
  LOG_INFO("CHUNK_TEST: verified", K(num_lines), K(read_count));

  writer.destroy();
  iter.destroy();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
