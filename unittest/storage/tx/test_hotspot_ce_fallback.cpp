/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX TRANS
#define private public
#define protected public

#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_log.h"
#include "share/ob_errno.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;

class ObTestHotspotCeFallback : public ::testing::Test
{
public:
  void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }
};

#ifndef OB_HOTSPOT_GROUP_COMMIT
TEST_F(ObTestHotspotCeFallback, empty_facade_has_no_hotspot_work)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  int64_t free_cb_cnt = -1;
  int64_t busy_cb_cnt = -1;
  int64_t idle_cb_cnt = -1;
  share::SCN log_ts;
  palf::LSN lsn;

  EXPECT_EQ(0, cache.get_hotspot_cache_count());
  EXPECT_EQ(0, cache.get_free_cb_count());
  EXPECT_EQ(0, cache.get_busy_cb_count());
  cache.get_cb_list_count(free_cb_cnt, busy_cb_cnt, idle_cb_cnt);
  EXPECT_EQ(0, free_cb_cnt);
  EXPECT_EQ(0, busy_cb_cnt);
  EXPECT_EQ(0, idle_cb_cnt);
  EXPECT_FALSE(cache.need_rollback_primary_tx());
  EXPECT_TRUE(cache.all_redo_flushed());
  EXPECT_TRUE(cache.all_redo_synced());
  EXPECT_TRUE(cache.all_redo_frozen_flushed());
  EXPECT_FALSE(cache.get_min_busy_log_ts().is_valid());
  EXPECT_FALSE(cache.get_max_busy_log_ts().is_valid());
  EXPECT_FALSE(cache.get_max_busy_lsn().is_valid());
  cache.get_max_busy_log_ts_and_lsn(log_ts, lsn);
  EXPECT_FALSE(log_ts.is_valid());
  EXPECT_FALSE(lsn.is_valid());
  EXPECT_EQ(nullptr, cache.cache_);
  EXPECT_EQ(OB_SUCCESS, cache.reuse());
  EXPECT_EQ(nullptr, cache.cache_);
  EXPECT_EQ(OB_SUCCESS, cache.try_release_idle_log_cb(nullptr));
  cache.reset();
}

TEST_F(ObTestHotspotCeFallback, progression_apis_fail_explicitly)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  ObTransID primary_id(1);
  ObTxRedoLog redo_log(DATA_CURRENT_VERSION);
  ObTxSEQ redo_seq;
  ObSecondaryTxRedoRange range;
  int64_t fill_redo_data_size = 0;
  int64_t need_remove_count = 0;

  EXPECT_EQ(OB_NOT_SUPPORTED, cache.insert_into(nullptr));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.assign_remapped_seq_ranges(ObTxSEQ(1, 0), ObTxSEQ(1, 1)));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.extract_hotspot_redo(0));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.get_secondary_tx_redo_range(0, range, fill_redo_data_size));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.try_flush_hotspot_redo(0, redo_log, redo_seq));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.after_flush_hotspot_redo(0, share::SCN::min_scn(), OB_SUCCESS));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.after_sync_hotspot_redo(0, true, share::SCN::min_scn()));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.remove_synced_hotspot_redo(0, need_remove_count));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.response_scheduler(OB_SUCCESS, share::SCN::invalid_scn()));
  EXPECT_EQ(OB_NOT_SUPPORTED, cache.abort_secondary_txs(OB_TRANS_KILLED));
}

TEST_F(ObTestHotspotCeFallback, compare_hotspot_cb_prefers_normal_cb)
{
  TransModulePageAllocator allocator;
  ObTxHotspotRedoCacheHandle cache(allocator);
  ObTxLogCb *normal_cb = reinterpret_cast<ObTxLogCb *>(0x1);
  ObTxLogCb *hotspot_cb = nullptr;
  bool is_hotspot_larger = false;

  cache.compare_hotspot_cb(normal_cb, hotspot_cb, is_hotspot_larger);

  EXPECT_TRUE(is_hotspot_larger);
  EXPECT_EQ(reinterpret_cast<ObTxLogCb *>(0x1), normal_cb);
  EXPECT_EQ(nullptr, hotspot_cb);
}
#endif

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_hotspot_ce_fallback.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_hotspot_ce_fallback.log", true, false,
                       "test_hotspot_ce_fallback.log",
                       "test_hotspot_ce_fallback.log");
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
