//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "share/compaction/ob_schedule_batch_size_mgr.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace compaction
{
void ObScheduleBatchSizeMgr::set_tablet_batch_size(const int64_t tablet_batch_size)
{
  if (tablet_batch_size != tablet_batch_size_ && tablet_batch_size > 0) {
    LOG_INFO("succeeded to reload new merge schedule tablet batch cnt", K(tablet_batch_size));
    tablet_batch_size_ = tablet_batch_size;
  }
}

int64_t ObScheduleBatchSizeMgr::get_checker_batch_size() const
{
  return MAX(DEFAULT_CHECKER_BATCH_SIZE, tablet_batch_size_ / 100);
}

void ObScheduleBatchSizeMgr::get_rs_check_batch_size(
    const int64_t table_cnt,
    int64_t &table_id_batch_size) const
{
  table_id_batch_size = TABLE_ID_BATCH_CHECK_SIZE;
  if (table_cnt > TOTAL_TABLE_CNT_THREASHOLD) {
    int64_t factor = (table_cnt / TOTAL_TABLE_CNT_THREASHOLD) * 2;
    table_id_batch_size *= factor;
  }
}

int64_t ObScheduleBatchSizeMgr::get_inner_table_scan_batch_size() const
{
  return MAX(1, (tablet_batch_size_ / DEFAULT_TABLET_BATCH_CNT)) * DEFAULT_INNER_TABLE_SCAN_BATCH_SIZE;
}

bool ObScheduleBatchSizeMgr::need_rebuild_map(
  const int64_t default_map_bucket_cnt,
  const int64_t item_cnt,
  const int64_t cur_bucket_cnt,
  int64_t &recommend_map_bucked_cnt)
{
  bool rebuild_map_flag = false;
  int64_t map_cnt = MAX(item_cnt / 3, default_map_bucket_cnt);
  recommend_map_bucked_cnt = MIN(map_cnt, default_map_bucket_cnt * 30);
  if ((cur_bucket_cnt == 0)
    || (recommend_map_bucked_cnt < map_cnt / 2)
    || (recommend_map_bucked_cnt > map_cnt * 3)) {
    rebuild_map_flag = true;
  }
  return rebuild_map_flag;
}

} // namespace compaction
} // namespace oceanbase
