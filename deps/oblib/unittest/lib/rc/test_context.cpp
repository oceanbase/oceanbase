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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/rc/context.h"
#undef private
#undef protected
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/alloc/memory_dump.h"
#include "lib/thread/thread_mgr.h"
#include "lib/allocator/ob_mem_leak_checker.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

static bool has_unfree = false;
void has_unfree_callback(char *)
{
  has_unfree = true;
}

class TestContext: public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestContext, Basic)
{
  // There must be a Flow pointing to root on each thread
  auto &context = Flow::current_ctx();
  auto &flow = Flow::current_flow();
  ASSERT_EQ(MemoryContext::root(), context);
  ASSERT_TRUE(flow.prev_ == flow.next_ && flow.prev_ == nullptr);
  ASSERT_TRUE(context->tree_node_.parent_ == context->tree_node_.child_ &&
              context->tree_node_.parent_ == context->tree_node_.next_ &&
              context->tree_node_.parent_ == nullptr);
  uint64_t tenant_id = 1001;
  uint64_t ctx_id = ObCtxIds::WORK_AREA;
  ObMallocAllocator *ma = ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, ma->create_and_add_tenant_allocator(tenant_id));

  ObPageManager g_pm;
  ObPageManager::set_thread_local_instance(g_pm);
  g_pm.set_tenant_ctx(tenant_id, ctx_id);
  g_pm.set_max_chunk_cache_size(0);
  MemoryContext &root = MemoryContext::root();
  ContextParam param;
  param.set_mem_attr(tenant_id, "Context", ctx_id);
  ContextTLOptGuard guard(true);
  param.properties_ = USE_TL_PAGE_OPTIONAL;
  MemoryContext mem_context;
  int ret = root->CREATE_CONTEXT(mem_context, param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(&mem_context->get_allocator(), &mem_context->get_arena_allocator());
  int64_t used = g_pm.used_;
  ASSERT_EQ(0, used);
  void *ptr = nullptr;
  ObMemAttr attr(OB_SERVER_TENANT_ID, ObNewModIds::TEST);
  WITH_CONTEXT(mem_context) {
    ptr = ctxalp(100);
    ASSERT_NE(ptr, nullptr);
    MEMSET(ptr, 0, 100);
    ASSERT_GT(g_pm.used_, used);

    auto &P_MCTX = CURRENT_CONTEXT;
    auto &P_MFLOW = Flow::current_flow();
    CREATE_WITH_TEMP_CONTEXT_P(false, param) {
      ASSERT_EQ(&CURRENT_CONTEXT, &P_MCTX);
    } else {
      ASSERT_TRUE(false);
    }
    CREATE_WITH_TEMP_CONTEXT_P(true, param) {
      ASSERT_NE(&CURRENT_CONTEXT, &P_MCTX);
      ASSERT_EQ(Flow::current_flow().prev_, &P_MFLOW);
      ASSERT_EQ(Flow::current_flow().next_, nullptr);
      int64_t p_hold = P_MCTX->hold();
      int64_t hold = CURRENT_CONTEXT->hold();
      for (int i = 0; i < 64; ++i) {
        ptr = ctxalp(100);
        ASSERT_NE(ptr, nullptr);
      }
      ASSERT_GT(g_pm.used_, used);
      ASSERT_EQ(p_hold, P_MCTX->hold());
      ASSERT_LT(hold, CURRENT_CONTEXT->hold());
      int64_t orig_pm_used = g_pm.used_;
      has_unfree = false;
      CREATE_WITH_TEMP_CONTEXT(param) {
        for (int i = 0; i < 64; ++i) {
          ptr = ctxalp(1024);
          ASSERT_NE(ptr, nullptr);
          ptr = ctxalf(100, attr);
          ASSERT_NE(ptr, nullptr);
          ObArenaAllocator &arena_alloc = CURRENT_CONTEXT->get_arena_allocator();
          ptr = arena_alloc.alloc(1024);
          ASSERT_NE(ptr, nullptr);
          ObIAllocator &alloc = CURRENT_CONTEXT->get_malloc_allocator();
          int64_t ori_used = CURRENT_CONTEXT->used();
          ptr = alloc.alloc(1024);
          ASSERT_NE(ptr, nullptr);
          ASSERT_GT(CURRENT_CONTEXT->used(), ori_used);
          alloc.free(ptr);
          ASSERT_EQ(CURRENT_CONTEXT->used(), ori_used);
        }
      } else {
        ASSERT_TRUE(false);
      }
      ASSERT_TRUE(has_unfree);
      ASSERT_EQ(orig_pm_used, g_pm.used_);
      CREATE_WITH_TEMP_CONTEXT(param) {
        {
          // In order to allow the object_set inside current_ctx to allocate free_list in advance
          // Don't let the memory occupied by free_list affect subsequent verification
          ctxalf(8192 - 1000, attr);
          ptr = ctxalf(2000, attr);
          ASSERT_NE(ptr, nullptr);
          ctxfree(ptr);
        }
        int sub_cnt = 8;
        MemoryContext subs[sub_cnt];
        for (int i = 0; i < sub_cnt; ++i) {
          param.properties_ = USE_TL_PAGE_OPTIONAL |
            RETURN_MALLOC_DEFAULT;
          ret = CURRENT_CONTEXT->CREATE_CONTEXT(subs[i], param);
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_EQ(&subs[i]->get_allocator(), &subs[i]->get_malloc_allocator());
          ptr = subs[i]->allocp(100);
          ASSERT_NE(ptr, nullptr);
          ptr = subs[i]->get_arena_allocator().alloc(100);
          ASSERT_NE(ptr, nullptr);
          WITH_CONTEXT(subs[i]) {
            ASSERT_EQ(subs[i], CURRENT_CONTEXT);
            ptr = ctxalp(100);
            ASSERT_NE(ptr, nullptr);
            ptr = CURRENT_CONTEXT->get_arena_allocator().alloc(100);
            ASSERT_NE(ptr, nullptr);
          } else {
            ASSERT_TRUE(false);
          }
        }
        ASSERT_GT(g_pm.used_, orig_pm_used);
        for (int i = 0; i < sub_cnt/2; ++i) {
          DESTROY_CONTEXT(subs[i]);
        }
        ASSERT_GT(g_pm.used_, orig_pm_used);
        // check child num
        int child_cnt = 0;
        for (auto cur = CURRENT_CONTEXT->tree_node_.child_;cur;cur=cur->next_,child_cnt++);
        ASSERT_EQ(child_cnt, sub_cnt/2);
      } else {
        ASSERT_TRUE(false);
      }
      ASSERT_EQ(g_pm.used_, orig_pm_used);
    } else {
      ASSERT_TRUE(false);
    }
  } else {
    ASSERT_TRUE(false);
  }
  ASSERT_GT(g_pm.used_, used);
  DESTROY_CONTEXT(mem_context);
  ASSERT_EQ(g_pm.used_, used);

  {
    get_mem_leak_checker().init();
    reset_mem_leak_checker_label("test1");
    ob_malloc(100, "test1");
    ob_malloc(100, common::ObNewModIds::OB_COMMON_ARRAY);
    get_mem_leak_checker().print();

    reset_mem_leak_checker_label("OB_COMMON_ARRAY");
    ob_malloc(100, "test1");
    void *ptr = ob_malloc(100, common::ObNewModIds::OB_COMMON_ARRAY);
    get_mem_leak_checker().print();
    ob_free(ptr);
    get_mem_leak_checker().print();

    reset_mem_leak_checker_label("");
    ob_malloc(100, "test1");
    ob_malloc(100, common::ObNewModIds::OB_COMMON_ARRAY);
    get_mem_leak_checker().print();

    reset_mem_leak_checker_label("NONE");
    ob_malloc(100, "test1");
    ob_malloc(100, common::ObNewModIds::OB_COMMON_ARRAY);
    get_mem_leak_checker().print();

    // test rate
    reset_mem_leak_checker_label("test2");
    int i = 20;
    while (i--) {
      ob_malloc(100, "test2");
    }
    get_mem_leak_checker().print();

    usleep(1000 * 1000);
    i = 20;
    while (i--) {
      ob_malloc(100, "test2");
    }
    get_mem_leak_checker().print();
  }

  // Based on testing needs, this code is temporarily retained
  ob_malloc(10000000, ObNewModIds::OB_COMMON_ARRAY);
  ObMemoryDump::get_instance().init();
  auto task = ObMemoryDump::get_instance().alloc_task();
  task->type_ = DUMP_CHUNK;
  task->dump_all_ = true;
  ObMemoryDump::get_instance().push(task);

  task = ObMemoryDump::get_instance().alloc_task();
  task->type_ = STAT_LABEL;
  ObMemoryDump::get_instance().push(task);
  usleep(1000000);
  ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(500, ObCtxIds::DEFAULT_CTX_ID)->print_memory_usage();
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
