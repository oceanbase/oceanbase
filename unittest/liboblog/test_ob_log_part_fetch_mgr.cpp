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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "ob_log_utils.h"
#define private public
#include "ob_log_part_fetch_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;

namespace oceanbase
{
namespace unittest
{
class TestObLogPartFetchMgr: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const int64_t MAX_CACHED_PART_FETCH_CTX_COUNT = 10 * 1000;
  static const int64_t PART_FETCH_CTX_POOL_BLOCK_SIZE = 1L << 24;
  static const uint64_t DEFAULT_TENANT_ID = common::OB_SERVER_TENANT_ID;

  static const int64_t PART_FETCH_CTX__COUNT = 10 * 1000;
  static const int64_t SINGLE_PART_FETCH_CTX__COUNT = 1;
  static int64_t g_slowest_part_num;
};

int64_t TestObLogPartFetchMgr::g_slowest_part_num =
ObLogConfig::default_print_fetcher_slowest_part_num;
typedef common::ObSmallObjPool<PartFetchCtx> PartFetchCtxPool;
PartFetchCtxPool ctx_pool;

void generate_ctx(const int64_t part_fetch_ctx_count,
    PartTransResolver &part_trans_resolver,
    PartFetchCtx *pctx_array[],
    ObLogPartFetchMgr::PartFetchCtxArray &part_fetch_ctx_array)
{
  int ret = OB_SUCCESS;

  for (int64_t idx = 0; idx < part_fetch_ctx_count; ++idx) {
    PartFetchCtx *&ctx = pctx_array[idx];
    if (OB_FAIL(ctx_pool.alloc(ctx)) || OB_ISNULL(ctx)) {
      LOG_ERROR("alloc PartFetchCtx fail", K(ret), K(idx), KPC(ctx));
    } else {
      // Initialising the fetch logging context
      ctx->reset(ObPartitionKey(1000U, idx, part_fetch_ctx_count),
          get_timestamp(), idx, idx, part_trans_resolver);
      // Manually assigning values to partition progress, for testing purposes
      ctx->progress_.progress_ = part_fetch_ctx_count - idx;
      if (OB_FAIL(part_fetch_ctx_array.push_back(ctx))) {
        LOG_ERROR("part_fetch_ctx_array push back fail", K(ret), K(idx), KPC(ctx));
      } else {
        LOG_DEBUG("data", K(idx), "progress", ctx->get_progress());
      }
    }
  }
}

void free_all_ctx(const int64_t array_cnt,
    PartFetchCtx *pctx_array[])
{
  for (int64_t idx = 0; idx < array_cnt; ++idx) {
    PartFetchCtx *&ctx = pctx_array[idx];
    if (NULL != ctx) {
      ctx->reset();
      ctx_pool.free(ctx);
      ctx = NULL;
    }
  }
}

int do_top_k(const ObLogPartFetchMgr::PartFetchCtxArray &part_fetch_ctx_array,
    const int64_t g_slowest_part_num)
{
  int ret = OB_SUCCESS;

  ObLogPartFetchMgr::SlowestPartArray slow_part_array;
  ObLogPartFetchMgr part_fetch_mgr;
  int64_t start_time = get_timestamp();
  int64_t end_time = 0;
  if (OB_FAIL(part_fetch_mgr.find_k_slowest_partition_(part_fetch_ctx_array,
          g_slowest_part_num,
          slow_part_array))) {
    LOG_ERROR("find_the_k_slowest_partition_ fail", K(ret));
  } else {
    end_time = get_timestamp();
    LOG_INFO("top-k cost time", "time", TVAL_TO_STR(end_time - start_time));

    int64_t array_cnt = slow_part_array.count();
    for (int64_t idx = 0; idx < array_cnt; ++idx) {
      const PartFetchCtx *ctx = slow_part_array.at(idx);
      EXPECT_EQ(idx + 1, ctx->get_progress());
      LOG_INFO("slow part", K(idx), "pkey", ctx->get_pkey(),
          "progress", ctx->get_progress());
    }
  }

  return ret;
}

//  TEST find_k_slowest_partition
TEST_F(TestObLogPartFetchMgr, top_k)
{
  int ret = OB_SUCCESS;

  ObLogPartFetchMgr::PartFetchCtxArray part_fetch_ctx_array;
  // Storage Context Pointer
  PartFetchCtx *pctx_array[PART_FETCH_CTX__COUNT];
  PartTransResolver part_trans_resolver;

  // PartFetchCtxPool
  if (OB_FAIL(ctx_pool.init(MAX_CACHED_PART_FETCH_CTX_COUNT,
      ObModIds::OB_LOG_PART_FETCH_CTX_POOL,
      DEFAULT_TENANT_ID,
      PART_FETCH_CTX_POOL_BLOCK_SIZE))) {
    LOG_ERROR("init PartFetchCtxPool fail", K(ret), LITERAL_K(MAX_CACHED_PART_FETCH_CTX_COUNT),
        LITERAL_K(PART_FETCH_CTX_POOL_BLOCK_SIZE));
  }

  // case-1:
  // Test 100,000 partitions
  // Generate ctx
  generate_ctx(PART_FETCH_CTX__COUNT, part_trans_resolver, pctx_array, part_fetch_ctx_array);
  // Execute top-k
  EXPECT_EQ(OB_SUCCESS, do_top_k(part_fetch_ctx_array, g_slowest_part_num));
  // free
  free_all_ctx(PART_FETCH_CTX__COUNT, pctx_array);


  // case-2
  // Test 0 partitions
  part_fetch_ctx_array.reset();
  //  Generate ctx
  generate_ctx(0, part_trans_resolver, pctx_array, part_fetch_ctx_array);
  // Execute top-k
  EXPECT_EQ(OB_SUCCESS, do_top_k(part_fetch_ctx_array, g_slowest_part_num));
  // free
  free_all_ctx(0, pctx_array);


  // case-3
  //Test 1 partitions
  part_fetch_ctx_array.reset();
  //  Generate ctx
  generate_ctx(SINGLE_PART_FETCH_CTX__COUNT, part_trans_resolver, pctx_array, part_fetch_ctx_array);
  // Execute top-k
  EXPECT_EQ(OB_SUCCESS, do_top_k(part_fetch_ctx_array, g_slowest_part_num));
  // free
  free_all_ctx(SINGLE_PART_FETCH_CTX__COUNT, pctx_array);


  // case-4
  // Test 2 partitions, one of which is NULL
  part_fetch_ctx_array.reset();
  //  Generate ctx
  generate_ctx(SINGLE_PART_FETCH_CTX__COUNT, part_trans_resolver, pctx_array, part_fetch_ctx_array);
  // push NULL
  EXPECT_EQ(OB_SUCCESS, part_fetch_ctx_array.push_back(NULL));
  // Execute top-k
  EXPECT_EQ(OB_ERR_UNEXPECTED, do_top_k(part_fetch_ctx_array, g_slowest_part_num));
  // free
  free_all_ctx(SINGLE_PART_FETCH_CTX__COUNT, pctx_array);


  // ctx pool destory
  ctx_pool.destroy();
}


}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;

  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_part_fetch_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();

  return ret;
}
