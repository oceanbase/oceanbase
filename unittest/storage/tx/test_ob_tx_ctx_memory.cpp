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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX TRANS
#define private public
#define protected public

#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_hotspot_define.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

// Pre-optimization baselines (ObSEArray<ObTxRedoExtractArg, 4> inlined ~17.5KB).
// After optimization (raw pointer + on-demand allocation), sizes should be SMALLER.
// These upper bounds prevent unintended memory growth.
static constexpr int64_t PRE_OPT_PART_TRANS_CTX_SIZE = 37312;
static constexpr int64_t PRE_OPT_HOTSPOT_REDO_CACHE_SIZE = 18336;
static constexpr int64_t PRE_OPT_TX_REDO_EXTRACT_ARG_SIZE = 4392;

class ObTxCtxMemoryTest : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
  virtual void TearDown() override {}
};

TEST_F(ObTxCtxMemoryTest, hotspot_cache_smaller_than_pre_opt)
{
  const int64_t cache_size = sizeof(ObTxHotspotRedoCache);
  LOG_INFO("ObTxHotspotRedoCache sizeof", K(cache_size),
           "pre_opt", PRE_OPT_HOTSPOT_REDO_CACHE_SIZE);

  EXPECT_LT(cache_size, PRE_OPT_HOTSPOT_REDO_CACHE_SIZE)
      << "ObTxHotspotRedoCache should be smaller after replacing ObSEArray with raw pointer. "
      << "Current: " << cache_size << ", Pre-opt: " << PRE_OPT_HOTSPOT_REDO_CACHE_SIZE;
}

TEST_F(ObTxCtxMemoryTest, part_trans_ctx_no_growth)
{
  const int64_t ctx_size = sizeof(ObPartTransCtx);
  LOG_INFO("ObPartTransCtx sizeof", K(ctx_size),
           "pre_opt", PRE_OPT_PART_TRANS_CTX_SIZE);

  EXPECT_LE(ctx_size, PRE_OPT_PART_TRANS_CTX_SIZE)
      << "ObPartTransCtx should not grow beyond pre-optimization baseline. "
      << "Current: " << ctx_size << ", Baseline: " << PRE_OPT_PART_TRANS_CTX_SIZE;
}

TEST_F(ObTxCtxMemoryTest, extract_arg_size_stable)
{
  const int64_t arg_size = sizeof(ObTxRedoExtractArg);
  LOG_INFO("ObTxRedoExtractArg sizeof", K(arg_size),
           "pre_opt", PRE_OPT_TX_REDO_EXTRACT_ARG_SIZE);

  EXPECT_EQ(arg_size, PRE_OPT_TX_REDO_EXTRACT_ARG_SIZE)
      << "ObTxRedoExtractArg element size changed unexpectedly. "
      << "Current: " << arg_size << ", Expected: " << PRE_OPT_TX_REDO_EXTRACT_ARG_SIZE;
}

TEST_F(ObTxCtxMemoryTest, hotspot_percentage_reduced)
{
  const int64_t ctx_size = sizeof(ObPartTransCtx);
  const int64_t handle_size = sizeof(ObTxHotspotRedoCacheHandle);
  const double percent = (handle_size * 100.0) / ctx_size;
  LOG_INFO("hotspot handle percentage of ObPartTransCtx", "percent", percent,
           K(handle_size), K(ctx_size));

  EXPECT_LT(percent, 49.0)
      << "Hotspot handle should occupy less than 49% of ObPartTransCtx after optimization. "
      << "Current: " << percent << "%";
}

TEST_F(ObTxCtxMemoryTest, basic_type_sizes)
{
  EXPECT_EQ(sizeof(ObTransID), 8);
  EXPECT_EQ(sizeof(ObTxSEQ), 8);
  EXPECT_EQ(sizeof(share::SCN), 8);
}

TEST_F(ObTxCtxMemoryTest, disallow_copy)
{
  EXPECT_FALSE(std::is_copy_constructible<ObTxHotspotRedoCache>::value)
      << "ObTxHotspotRedoCache must not be copyable (manages raw pointer)";
  EXPECT_FALSE(std::is_copy_assignable<ObTxHotspotRedoCache>::value)
      << "ObTxHotspotRedoCache must not be copy-assignable";
}

TEST_F(ObTxCtxMemoryTest, memory_report)
{
  LOG_INFO("=== Memory Optimization Report ===");
  LOG_INFO("ObPartTransCtx", "size", sizeof(ObPartTransCtx));
  LOG_INFO("ObTxHotspotRedoCacheHandle", "size", sizeof(ObTxHotspotRedoCacheHandle));
  LOG_INFO("ObTxHotspotRedoCache", "size", sizeof(ObTxHotspotRedoCache));
  LOG_INFO("ObTxRedoExtractArg", "size", sizeof(ObTxRedoExtractArg));
  LOG_INFO("ObTxHotspotRedoStat", "size", sizeof(ObTxHotspotRedoStat));
  LOG_INFO("ObHotspotSchedulerResponseTask", "size", sizeof(ObHotspotSchedulerResponseTask));

  const int64_t resident_saved = PRE_OPT_PART_TRANS_CTX_SIZE - static_cast<int64_t>(sizeof(ObPartTransCtx));
  const int64_t core_saved = PRE_OPT_HOTSPOT_REDO_CACHE_SIZE - static_cast<int64_t>(sizeof(ObTxHotspotRedoCache));
  LOG_INFO("Resident memory saved per ObPartTransCtx", "bytes", resident_saved);
  LOG_INFO("On-demand hotspot core memory saved", "bytes", core_saved);
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_tx_ctx_memory.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_tx_ctx_memory.log", true, false,
                       "test_ob_tx_ctx_memory.log",
                       "test_ob_tx_ctx_memory.log");
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
