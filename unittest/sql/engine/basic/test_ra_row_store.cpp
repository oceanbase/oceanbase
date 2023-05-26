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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_macro_file.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestEnv : public ::testing::Environment
{
public:
  virtual void SetUp() override
  {
    GCONF.enable_sql_operator_dump.set_value("True");
    lib::AChunkMgr::instance().set_max_chunk_cache_size(0);
    int ret = OB_SUCCESS;
    lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
    ret = malloc_allocator->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    int s = (int)time(NULL);
    LOG_INFO("initial setup random seed", K(s));
    srandom(s);
  }

  virtual void TearDown() override
  {
  }
};

#define CALL(func, ...) func(__VA_ARGS__); ASSERT_FALSE(HasFatalFailure());

class TestRARowStore : public blocksstable::TestDataFilePrepare
{
public:
  TestRARowStore() : blocksstable::TestDataFilePrepare("TestDiskIR", 2<<20, 1000)
  {
  }

  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    blocksstable::TestDataFilePrepare::SetUp();
		ret = blocksstable::ObMacroFileManager::get_instance().init();
		ASSERT_EQ(OB_SUCCESS, ret);

    row_.count_ = COLS;
    row_.cells_ = cells_;
    cells_[1].set_null();

    ret = rs_.init(0, tenant_id_, ctx_id_, label_);
    ASSERT_EQ(OB_SUCCESS, ret);

    memset(str_buf_, 'a', BUF_SIZE);
  }

  virtual void TearDown() override
  {
    rs_.reset();

    blocksstable::ObMacroFileManager::get_instance().destroy();
    blocksstable::TestDataFilePrepare::TearDown();
    common::ObModItem mod;
  }

  ObNewRow &gen_row(int64_t row_id)
  {
    cells_[0].set_int(row_id);
    int64_t max_size = 512;
    if (enable_big_row_ && row_id > 0 && random() % 100000 < 5) {
      max_size = 1 << 20;
    }
    int64_t size = 10 + random() % max_size;
    cells_[2].set_varchar(str_buf_, (int)size);
    return row_;
  }

  template <typename T>
  void verify_row(T &reader, int64_t id, bool verify_all = false)
  {
    const ObNewRow *r = NULL;
    int ret = reader.get_row(id, r);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t v;
    r->get_cell(0).get_int(v);
    if (verify_all) {
      r->get_cell(1).is_null();
      ObString s = r->get_cell(2).get_varchar();
      ASSERT_EQ(0, strncmp(str_buf_, s.ptr(), s.length()));
    }
    ASSERT_EQ(id, v);
  }

  void verify_row(int64_t id, bool verify_all = false)
  {
    return verify_row(rs_, id, verify_all);
  }

  void verify_rows(int64_t begin, int64_t end,
      const char *mode =  "scan", int64_t cnt = 0, bool verify_all = false)
  {
    if (strcmp(mode, "scan") == 0) {
      for (int64_t i = begin; i < end; i++) {
        CALL(verify_row, i, verify_all);
      }
    } else if (strcmp(mode, "rscan") == 0) {
      for (int64_t i = begin; i < end; i++) {
        CALL(verify_row, end + begin - i - 1, verify_all);
      }
    } else if (strcmp(mode, "get") == 0) {
      for (int64_t i = 0; i < cnt; i++) {
        CALL(verify_row, random() % (end - begin) + begin, verify_all);
      }
    }
  }

  void append_rows(int64_t cnt)
  {
    int64_t ret = OB_SUCCESS;
    int64_t base = rs_.get_row_cnt();
    for (int64_t i = 0; i < cnt; i++) {
      ret = rs_.add_row(gen_row(base + i));
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(base + cnt, rs_.get_row_cnt());
  }

protected:

  const static int64_t COLS = 3;
  bool enable_big_row_ = false;
  ObObj cells_[COLS];
  ObNewRow row_;
  ObRARowStore rs_;

  int64_t tenant_id_ = OB_SYS_TENANT_ID;
  int64_t ctx_id_ = ObCtxIds::WORK_AREA;
  const char *label_ = ObModIds::OB_SQL_ROW_STORE;

  const static int64_t BUF_SIZE = 2 << 20;
  char str_buf_[BUF_SIZE];
};

TEST_F(TestRARowStore, basic)
{
  CALL(append_rows, 3000); // approximate 1MB, index block not needed
  CALL(verify_rows, 0, rs_.get_row_cnt(), "scan", 0, true);
  CALL(verify_rows, 0, rs_.get_row_cnt(), "rscan", 0, true);
  CALL(verify_rows, 0, rs_.get_row_cnt(), "get", rs_.get_row_cnt(), true);

  CALL(append_rows, 10000);
  LOG_INFO("mem", K(rs_.get_mem_hold()));
  ASSERT_EQ(13000, rs_.get_row_cnt());
  for (int64_t i = 0; i < rs_.get_row_cnt(); i++) {
    CALL(verify_row, i, true);
  }

  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  ret = rs_.get_row(-1, row);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = rs_.get_row(rs_.get_row_cnt(), row);
  ASSERT_NE(OB_SUCCESS, ret);

  enable_big_row_ = true;
  CALL(append_rows, 20000);
  LOG_INFO("mem", K(rs_.get_mem_hold()));
  ASSERT_EQ(33000, rs_.get_row_cnt());
  CALL(verify_rows, 0, rs_.get_row_cnt(), "get", 1000, true);

  rs_.reset();
  ret = rs_.init(5L << 20, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);
  CALL(append_rows, 100000);
  ASSERT_GT(rs_.get_mem_hold(), 0);
  ASSERT_GT(rs_.get_file_size(), 0);
  LOG_WARN("mem and disk", K(rs_.get_mem_hold()), K(rs_.get_file_size()));
  CALL(verify_rows, 0, rs_.get_row_cnt(), "scan", 0, true);
  CALL(verify_rows, 0, rs_.get_row_cnt(), "rscan", 0, true);
  CALL(verify_rows, 0, rs_.get_row_cnt(), "get", 500, true);
}

TEST_F(TestRARowStore, mem_perf)
{
  int64_t rows = 2000000;
  int64_t begin = ObTimeUtil::current_time();
  CALL(append_rows, rows);
  LOG_WARN("write time:", K(rows), K(ObTimeUtil::current_time() - begin));
  for (int64_t i = 0; i < 10000; i++) {
    CALL(verify_row, random() % rs_.get_row_cnt(), true);
  }
  LOG_INFO("mem", K(rs_.get_mem_hold()));

  begin = ObTimeUtil::current_time();
  CALL(verify_rows, 0, rows, "scan");
  LOG_WARN("scan time:", K(rows), K(ObTimeUtil::current_time() - begin));

  begin = common::ObTimeUtil::current_time();
  CALL(verify_rows, 0, rows, "rscan");
  LOG_WARN("reverse scan time:", K(rows), K(ObTimeUtil::current_time() - begin));

  begin = common::ObTimeUtil::current_time();
  CALL(verify_rows, 0, rows, "get", rows);
  LOG_WARN("random access time:", K(rows), K(ObTimeUtil::current_time() - begin));
}

TEST_F(TestRARowStore, disk)
{
  int64_t begin = 0;
  int64_t write_time = 0;
  int64_t round = 500;
  rs_.set_mem_limit(100L << 20);
  for (int64_t i = 0; i < round; i++) {
    if (i == round / 2) {
      enable_big_row_ = true;
    }
    begin = common::ObTimeUtil::current_time();
    CALL(append_rows, 10000);
    write_time += common::ObTimeUtil::current_time() - begin;
    CALL(verify_rows, 0, rs_.get_row_cnt(), "get", 10);
  }
  int64_t rows = round * 10000;
  LOG_WARN("mem and disk", K(rows), K(rs_.get_mem_hold()), K(rs_.get_file_size()));
  LOG_INFO("disk write:", K(rows), K(write_time));

  begin = ObTimeUtil::current_time();
  CALL(verify_rows, 0, rs_.get_row_cnt(), "rscan");
  LOG_WARN("disk rscan time:", K(rows), K(ObTimeUtil::current_time() - begin));
}

TEST_F(TestRARowStore, finish_add_row)
{
  int ret = OB_SUCCESS;

  rs_.set_mem_limit(100L << 20);
  CALL(append_rows, 10000);
  int64_t before_mem = rs_.get_mem_hold();
  ASSERT_EQ(OB_SUCCESS, rs_.finish_add_row());
  int64_t after_mem = rs_.get_mem_hold();
  ASSERT_EQ(before_mem, after_mem);
  CALL(verify_rows, 0, rs_.get_row_cnt(), "scan");
  CALL(verify_rows, 0, rs_.get_row_cnt(), "get", 1000);


  rs_.reset();
  ret = rs_.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  rs_.set_mem_limit(50L << 20);
  enable_big_row_ = true;

  for (int64_t i = 0; i < 1000; i++) {
    CALL(append_rows, 1000);
    if (rs_.get_file_size() > 0) {
      break;
    }
  }

  before_mem = rs_.get_mem_hold();
  ASSERT_EQ(OB_SUCCESS, rs_.finish_add_row());
  after_mem = rs_.get_mem_hold();
  ASSERT_GT(before_mem, after_mem);

  CALL(verify_rows, 0, rs_.get_row_cnt(), "rscan");
  CALL(verify_rows, 0, rs_.get_row_cnt(), "get", 1000);
}

TEST_F(TestRARowStore, multi_reader)
{
  rs_.set_mem_limit(50L << 20);
  CALL(append_rows, 10000);

  ObRARowStore::Reader reader0(rs_);
  ObRARowStore::Reader reader1(rs_);
  ObRARowStore::Reader reader2(rs_);

  ObRARowStore::Reader *readers[] = { &reader0, &reader1, &reader2 };
  for (int64_t i = 0; i < 10000; ++i) {
    int64_t row_id = random() % rs_.get_row_cnt();
    CALL(verify_row, rs_, row_id);
    for (int64_t j = 0; j < sizeof(readers) / sizeof(readers[0]); ++j) {
      row_id = random() % rs_.get_row_cnt();
      CALL(verify_row, *readers[j], row_id);
    }
  }

  int round = 1;
  while (round > 0) {
    append_rows(10000);
    if (rs_.get_file_size() <= 0) {
      round++;
    } else {
      round--;
    }
    if (round > 1000) {
      break;
    }
  }

  ASSERT_GT(rs_.get_file_size(), 0);

  for (int64_t i = 0; i < 1000; ++i) {
    int64_t row_id = random() % rs_.get_row_cnt();
    CALL(verify_row, rs_, row_id);
    for (int64_t j = 0; j < sizeof(readers) / sizeof(readers[0]); ++j) {
      row_id = random() % rs_.get_row_cnt();
      CALL(verify_row, *readers[j], row_id);
    }
  }

  for (int64_t i = 0; i < rs_.get_row_cnt(); ++i) {
    CALL(verify_row, *readers[0], i);
    CALL(verify_row, *readers[1], rs_.get_row_cnt() - 1 - i);
  }
}

TEST_F(TestRARowStore, reuse)
{
  rs_.set_mem_limit(50L << 20);
  rs_.reuse();
  CALL(append_rows, 10000);

  ObRARowStore::Reader reader(rs_);

  for (int64_t i = 0; i < 10000; ++i) {
    int64_t row_id = random() % rs_.get_row_cnt();
    CALL(verify_row, rs_, row_id);
    row_id = random() % rs_.get_row_cnt();
    if (random() & 1) {
      CALL(verify_row, reader, row_id);
    } else {
      CALL(verify_row, rs_, row_id);
    }
  }

  rs_.reuse();
  reader.reuse();
  ASSERT_EQ(0, rs_.get_row_cnt());
  ASSERT_LT(16 << 10, rs_.get_mem_hold());
  CALL(append_rows, 10000);
  for (int64_t i = 0; i < 10000; ++i) {
    int64_t row_id = random() % rs_.get_row_cnt();
    CALL(verify_row, rs_, row_id);
    row_id = random() % rs_.get_row_cnt();
    if (random() & 1) {
      CALL(verify_row, reader, row_id);
    } else {
      CALL(verify_row, rs_, row_id);
    }
  }

  int round = 1;
  while (round > 0) {
    append_rows(10000);
    if (rs_.get_file_size() <= 0) {
      round++;
    } else {
      round--;
    }
    if (round > 1000) {
      break;
    }
  }

  for (int64_t i = 0; i < 1000; ++i) {
    int64_t row_id = random() % rs_.get_row_cnt();
    CALL(verify_row, rs_, row_id);
    row_id = random() % rs_.get_row_cnt();
    if (random() & 1) {
      CALL(verify_row, reader, row_id);
    } else {
      CALL(verify_row, rs_, row_id);
    }
  }

  rs_.reuse();
  reader.reuse();
  ASSERT_EQ(0, rs_.get_file_size());
  ASSERT_EQ(0, rs_.get_row_cnt());

  CALL(append_rows, 10000);

  for (int64_t i = 0; i < 1000; ++i) {
    int64_t row_id = random() % rs_.get_row_cnt();
    CALL(verify_row, rs_, row_id);
    row_id = random() % rs_.get_row_cnt();
    if (random() & 1) {
      CALL(verify_row, reader, row_id);
    } else {
      CALL(verify_row, rs_, row_id);
    }
  }
}

TEST_F(TestRARowStore, start_dump_by_total_mem_used)
{
  int ret = OB_SUCCESS;
  append_rows(500000);
  int64_t avg_row_size = rs_.get_mem_hold() / rs_.get_row_cnt();
  LOG_INFO("average row size", K(avg_row_size));

  lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
  malloc_allocator->set_tenant_limit(OB_SYS_TENANT_ID, 1L << 30);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 50MB for work area
  ret = lib::set_wa_limit(OB_SYS_TENANT_ID, 5);
  ASSERT_EQ(OB_SUCCESS, ret);

  rs_.reset();
  ret = rs_.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);


  // case1: trigger dump by memory mod usage (60% of limit, 30MB)
  // write 28MB, all in memory
  CALL(append_rows, (28L << 20) / avg_row_size);
  ASSERT_EQ(rs_.get_file_size(), 0);

  // append 10MB, need dump
  CALL(append_rows, (10L << 20) / avg_row_size);
  ASSERT_GT(rs_.get_file_size(), 0);

  rs_.reset();
  ret = rs_.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // case2: trigger dump by memory ctx usage (80% of limit, 40MB)
  ObMemAttr attr = default_memattr;
  attr.tenant_id_ = tenant_id_;
  attr.ctx_id_ = ctx_id_;
  // memory ctx hold 20MB
  void *mem = ob_malloc(20L << 20, attr);

  // write 15MB, in memory
  CALL(append_rows, (15L << 20) / avg_row_size);
  ASSERT_EQ(rs_.get_file_size(), 0);

  // append 10MB, need dump
  CALL(append_rows, (10L << 20) / avg_row_size);
  ASSERT_GT(rs_.get_file_size(), 0);

  ob_free(mem);

  // case3: write to disk disabled
  // write 40MB, all in memmory
  GCONF.enable_sql_operator_dump.set_value("False");
  rs_.reset();
  ret = rs_.init(0, tenant_id_, ctx_id_, label_);
  ASSERT_EQ(OB_SUCCESS, ret);

  CALL(append_rows, (40L << 20) / avg_row_size);
  ASSERT_EQ(rs_.get_file_size(), 0);

  // append 20MB, exceed work area
  int64_t rows = (100L << 20) / avg_row_size;
  int64_t idx = 0;
  int64_t base = rs_.get_row_cnt();
  for (; idx < rows; idx++) {
    if (OB_SUCCESS != rs_.add_row(gen_row(base + idx))) {
      break;
    }
  }
  ASSERT_LT(idx, rows);

  lib::ObMallocAllocator::get_instance()->print_tenant_ctx_memory_usage(tenant_id_);
  lib::ObMallocAllocator::get_instance()->print_tenant_memory_usage(tenant_id_);

  ASSERT_EQ(rs_.get_file_size(), 0);
}

} // end namespace sql
} // end namespace oceanbase


void ignore_sig(int sig)
{
  UNUSED(sig);
}

int main(int argc, char **argv)
{
  signal(49, ignore_sig);
  oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  auto *env = new (oceanbase::sql::TestEnv);
  testing::AddGlobalTestEnvironment(env);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
