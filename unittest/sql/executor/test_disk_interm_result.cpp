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
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_macro_file.h"
#include "sql/executor/ob_interm_result.h"
#include "sql/executor/ob_interm_result_pool.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_interm_result_item.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestDiskIntermResult : public blocksstable::TestDataFilePrepare
{
public:
  TestDiskIntermResult() : blocksstable::TestDataFilePrepare("TestDiskIR", 2<<20, 1000)
  {
  }

  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    blocksstable::TestDataFilePrepare::SetUp();
		ret = blocksstable::ObMacroFileManager::get_instance().init();
		ASSERT_EQ(OB_SUCCESS, ret);

    GCONF.enable_sql_operator_dump.set_value("True");
  }

  virtual void TearDown() override
  {
    blocksstable::TestDataFilePrepare::TearDown();
  }

protected:
};

TEST_F(TestDiskIntermResult, disk_write_read)
{
  int ret = OB_SUCCESS;

  ObObj cells[2];
  cells[0].set_int(1);
  const char *str = __FILE__;
  cells[1].set_varchar(str, (int32_t)strlen(str));

  ObNewRow row;
  row.count_ = 2;
  row.cells_ = cells;

  ObIntermResult *ir = NULL;
  ret = ObIntermResultManager::get_instance()->alloc_result(ir);
  ASSERT_EQ(OB_SUCCESS, ret);
  lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
  ret = malloc_allocator->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  malloc_allocator->set_tenant_limit(OB_SYS_TENANT_ID, 1L << 30);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 50MB for work area
  ret = lib::set_wa_limit(OB_SYS_TENANT_ID, 5);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write 100MB
  static int64_t max_row_cnt = 1000000000;
  int64_t row_cnt = 0;
  for (;row_cnt < max_row_cnt; row_cnt++) {
    ret = ir->add_row(OB_SYS_TENANT_ID, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (row_cnt % 10000 == 0) {
      int64_t size = 0;
      ret = ir->get_all_data_size(size);
      ASSERT_EQ(OB_SUCCESS, ret);
      if (size > (100L << 20)) {
        row_cnt++;
        break;
      } else {
        LOG_INFO("write progress", K(row_cnt), K(size));
      }
    }
  }

  ret = ir->complete_add_rows(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t mem_size = 0;
  int64_t disk_size = 0;
  ret = ir->get_data_size_detail(mem_size, disk_size);
  ASSERT_GT(mem_size + disk_size, 100L << 20);
  ASSERT_GT(mem_size, 0);
  ASSERT_GT(disk_size, 0);

  ObAddr server;
  server.set_ip_addr("127.0.0.1", 80);
  ObSliceID slice_id;
  slice_id.set_server(server);
  slice_id.set_execution_id(1);
  slice_id.set_job_id(1);
  slice_id.set_task_id(1);
  slice_id.set_slice_id(1);
  ObIntermResultInfo iraddr;
  iraddr.init(slice_id);

  ret = ObIntermResultManager::get_instance()->add_result(iraddr, ir, 1000000000L);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObIntermResultIterator iter;
  ret = ObIntermResultManager::get_instance()->get_result(iraddr, iter);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = new char[ObScanner::DEFAULT_MAX_SERIALIZE_SIZE * 2];
  int64_t read_row_cnt = 0;
  ObScanner scanner;
  while (true) {
    ObIIntermResultItem *it = NULL;
    ret = iter.get_next_interm_result_item(it);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(it != NULL);

    int64_t len = it->get_data_len();
    ret = it->copy_data(buf, len);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t pos = 0;
    ret = scanner.deserialize(buf, len, pos);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObScanner::Iterator row_iter = scanner.begin();
    while (OB_SUCCESS == row_iter.get_next_row(row)) {
      read_row_cnt++;
    }
  }

  ASSERT_EQ(row_cnt, read_row_cnt);
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
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::sql::ObIntermResultItemPool::build_instance();
  oceanbase::sql::ObIntermResultPool::build_instance();
  oceanbase::sql::ObIntermResultManager::build_instance();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
