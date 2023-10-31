/*
 * test_compaction_mem_context.cpp
 * Author:
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "lib/utility/ob_print_utils.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/blocksstable/ob_data_buffer.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace compaction;


class TestCompactionMemCtx : public ::testing::Test
{
public:
  TestCompactionMemCtx();
  virtual ~TestCompactionMemCtx() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();


private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
};


TestCompactionMemCtx::TestCompactionMemCtx()
  : tenant_id_(1),
    tenant_base_(tenant_id_)
{
}

void TestCompactionMemCtx::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestCompactionMemCtx::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestCompactionMemCtx::SetUp()
{
  int ret = OB_SUCCESS;

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(tenant_id_, MTL_ID());
}

void TestCompactionMemCtx::TearDown()
{
  ObTenantEnv::set_tenant(nullptr);
}


namespace unittest
{

TEST_F(TestCompactionMemCtx, basic_test)
{
  ObLocalArena arena("TestArena", OB_MALLOC_NORMAL_BLOCK_SIZE);

  void *buf = arena.alloc(128);
  ASSERT_TRUE(nullptr != buf);
  EXPECT_EQ(144, arena.used());
  EXPECT_EQ(true, arena.total() <= 8064);

  arena.free(buf);
  arena.clear();
  EXPECT_EQ(0, arena.used());
  EXPECT_EQ(0, arena.total());
}


TEST_F(TestCompactionMemCtx, monitor_arena_test)
{
  ObArenaAllocator inner_arena;
  ObTabletMergeDagParam param;
  ObCompactionMemoryContext mem_ctx(param, inner_arena);

  ObLocalArena arena("TestArena");
  arena.bind_mem_ctx(mem_ctx);
  EXPECT_EQ(&mem_ctx, arena.ref_mem_ctx_);

  void *buf = arena.alloc(128);
  ASSERT_TRUE(nullptr != buf);
  EXPECT_EQ(144, arena.used());
  EXPECT_EQ(true, arena.total() <= 8064);

  mem_ctx.mem_click();
  EXPECT_EQ(true, mem_ctx.get_total_mem_peak() <= 8064);

  arena.free(buf);
  arena.clear();
  EXPECT_EQ(0, arena.used());
  EXPECT_EQ(0, arena.total());

  // only stat the peak val
  mem_ctx.mem_click();
  EXPECT_EQ(true, mem_ctx.get_total_mem_peak() <= 8064);

  arena.~ObLocalArena();
  EXPECT_EQ(nullptr, arena.ref_mem_ctx_);
}


TEST_F(TestCompactionMemCtx, mem_ctx_test)
{
  ObArenaAllocator inner_arena;
  ObTabletMergeDagParam param;
  ObCompactionMemoryContext mem_ctx(param, inner_arena);

  void *buf1 = mem_ctx.alloc(128);
  ASSERT_TRUE(nullptr != buf1);

  void *buf2 = mem_ctx.local_alloc(128);
  ASSERT_TRUE(nullptr != buf2);

  mem_ctx.mem_click();
  ASSERT_TRUE(0 != mem_ctx.get_total_mem_peak());
}


} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -f test_compaction_memory_context.log*");
  OB_LOGGER.set_file_name("test_compaction_memory_context.log", true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  return RUN_ALL_TESTS();
}
