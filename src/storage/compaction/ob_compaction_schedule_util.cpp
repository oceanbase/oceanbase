/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_compaction_schedule_util.h"

namespace oceanbase
{
namespace compaction
{

/**
 * ObCompactionScheduleTimeGuard Impl
 */
const char *ObCompactionScheduleTimeGuard::CompactionEventStr[] = {
    "GET_TABLET",
    "UPDATE_TABLET_REPORT_STATUS",
    "READ_MEDIUM_INFO",
    "SCHEDULE_NEXT_MEDIUM",
    "SCHEDULE_TABLET_MEDIUM",
    "FAST_FREEZE",
    "SEARCH_META_TABLE",
    "CHECK_META_TABLE",
    "SEARCH_CHECKSUM",
    "CHECK_CHECKSUM",
    "SCHEDULER_NEXT_ROUND"
};

const char *ObCompactionScheduleTimeGuard::get_comp_event_str(enum CompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(CompactionEventStr), "events str len is mismatch");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < GET_TABLET) {
    str = "invalid_type";
  } else {
    str = CompactionEventStr[event];
  }
  return str;
}

int64_t ObCompactionScheduleTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (int64_t idx = 0; idx < idx_; ++idx) {
    if (0 < click_poinsts_[idx]) {
      fmt_ts_to_meaningful_str(buf, buf_len, pos, get_comp_event_str((CompactionEvent)line_array_[idx]), click_poinsts_[idx]);
    }
  }
  return pos;
}

} // namespace compaction
} // namespace oceanbase