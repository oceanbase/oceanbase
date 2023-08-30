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

#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/multi_data_source/mds_writer.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/multi_data_source/compile_utility/map_type_index_in_tuple.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_node.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/tablet/ob_tablet_meta.h"
#define MIT_TESTCASE_LABEL
#include "mtlenv/mock_tenant_module_env.h"
#include "observer/ob_server.h"
namespace oceanbase {
namespace storage
{
namespace mds
{
void *MdsAllocator::alloc(const int64_t size)
{
  void *ptr = ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  ob_free(ptr);
}
}
}
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;
using namespace transaction;

class TestBufferCtxNode: public ::testing::Test
{
public:
  TestBufferCtxNode() {};
  virtual ~TestBufferCtxNode() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
  static void SetUpTestCase() { ASSERT_EQ(OB_SUCCESS, storage::MockTenantModuleEnv::get_instance().init()); }
  static void TearDownTestCase() { storage::MockTenantModuleEnv::get_instance().destroy(); }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestBufferCtxNode);
};

TEST_F(TestBufferCtxNode, test_mds_ctx_serialize_deserialize) {
  BufferCtxNode node;
  BufferCtx *buffer_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, MdsFactory::create_buffer_ctx(transaction::ObTxDataSourceType::TEST1, ObTransID(1), buffer_ctx));
  node.set_ctx(buffer_ctx);
  ObTxBufferCtxArray buffer_ctx_array;
  ASSERT_EQ(OB_SUCCESS, buffer_ctx_array.push_back(node));
  ASSERT_EQ(OB_SUCCESS, buffer_ctx_array.push_back(node));
  constexpr int64_t buffer_len = 2048;
  char stack_buffer[buffer_len] = {0};
  int64_t pos1 = 0;
  ASSERT_EQ(OB_SUCCESS, buffer_ctx_array.serialize(stack_buffer, buffer_len, pos1));
  int64_t pos2 = 0;
  ASSERT_EQ(OB_SUCCESS, buffer_ctx_array.deserialize(stack_buffer, buffer_len, pos2));
  ASSERT_EQ(pos1, pos2);
  for (int i = 0; i < buffer_ctx_array.count(); ++i) {
    buffer_ctx_array[i].destroy_ctx();
  }
  node.destroy_ctx();
}

TEST_F(TestBufferCtxNode, test_exec_info_serialize_deserialize) {
  BufferCtx *buffer_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, MdsFactory::create_buffer_ctx(transaction::ObTxDataSourceType::TEST1, ObTransID(1), buffer_ctx));

  constexpr int64_t buffer_size = 1024;
  ObTxExecInfo exec_info;
  ObTxBufferNode node;
  char buffer1[buffer_size] = {1};
  node.data_.assign_buffer(buffer1, buffer_size);
  node.buffer_ctx_node_.set_ctx(buffer_ctx);
  ASSERT_EQ(OB_SUCCESS, exec_info.multi_data_source_.push_back(node));
  ASSERT_EQ(OB_SUCCESS, exec_info.multi_data_source_.push_back(node));

  char serialize_buffer[4096];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, exec_info.generate_mds_buffer_ctx_array());
  ASSERT_EQ(OB_SUCCESS, exec_info.serialize(serialize_buffer, 4096, pos));
  pos = 0;
  ObTxExecInfo exec_info_new;
  ASSERT_EQ(OB_SUCCESS, exec_info_new.deserialize(serialize_buffer, 4096, pos));
  ASSERT_EQ(exec_info_new.mds_buffer_ctx_array_.count(), 2);
  exec_info_new.mrege_buffer_ctx_array_to_multi_data_source();
  for (int i = 0; i < exec_info_new.multi_data_source_.count(); ++i) {
    exec_info_new.multi_data_source_[i].buffer_ctx_node_.destroy_ctx();
  }
  node.buffer_ctx_node_.destroy_ctx();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_buffer_ctx_node.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_buffer_ctx_node.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    ret = -1;
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}
