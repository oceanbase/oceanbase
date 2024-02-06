//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "rootserver/freeze/ob_major_merge_progress_util.h"

namespace oceanbase
{
namespace compaction
{

ObTableCompactionInfo &ObTableCompactionInfo::operator=(const ObTableCompactionInfo &other)
{
  table_id_ = other.table_id_;
  tablet_cnt_ = other.tablet_cnt_;
  status_ = other.status_;
  unfinish_index_cnt_ = other.unfinish_index_cnt_;
  return *this;
}

const char *ObTableCompactionInfo::TableStatusStr[] = {
  "INITIAL",
  "COMPACTED",
  "CAN_SKIP_VERIFYING",
  "INDEX_CKM_VERIFIED",
  "VERIFIED"
};

const char *ObTableCompactionInfo::status_to_str(const Status &status)
{
  STATIC_ASSERT(static_cast<int64_t>(TB_STATUS_MAX) == ARRAYSIZEOF(TableStatusStr), "table status str len is mismatch");
  const char *str = "";
  if (status < INITIAL || status >= TB_STATUS_MAX) {
    str = "invalid_status";
  } else {
    str = TableStatusStr[status];
  }
  return str;
}

ObTableCompactionInfo::ObTableCompactionInfo()
  : table_id_(OB_INVALID_ID),
    tablet_cnt_(0),
    unfinish_index_cnt_(INVALID_INDEX_CNT),
    status_(Status::INITIAL)
{
}
/**
 * -------------------------------------------------------------------ObMergeProgress-------------------------------------------------------------------
 */
int64_t ObMergeProgress::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    const bool merge_finish = is_merge_finished();
    if (merge_finish) {
      J_KV(K(merge_finish), K_(total_table_cnt));
    } else {
      J_KV(KP(this), K(merge_finish), K_(unmerged_tablet_cnt), K_(merged_tablet_cnt), K_(total_table_cnt));
      for (int64_t i = 0; i < RECORD_TABLE_TYPE_CNT; ++i) {
        J_COMMA();
        J_KV(ObTableCompactionInfo::TableStatusStr[i], table_cnt_[i]);
      }
    }
    J_OBJ_END();
  }
  return pos;
}
/**
 * -------------------------------------------------------------------ObRSCompactionTimeGuard-------------------------------------------------------------------
 */
const char *ObRSCompactionTimeGuard::CompactionEventStr[] = {
    "PREPARE_UNFINISH_TABLE_IDS",
    "GET_TABLET_LS_PAIRS",
    "GET_TABLET_META_TABLE",
    "CKM_VERIFICATION"
};

const char *ObRSCompactionTimeGuard::get_comp_event_str(enum CompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(CompactionEventStr), "events str len is mismatch");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < PREPARE_UNFINISH_TABLE_IDS) {
    str = "invalid_type";
  } else {
    str = CompactionEventStr[event];
  }
  return str;
}

int64_t ObRSCompactionTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  for (int64_t idx = 0; idx < idx_; ++idx) {
    fmt_ts_to_meaningful_str(buf, buf_len, pos, get_comp_event_str((CompactionEvent)line_array_[idx]), click_poinsts_[idx]);
  }
  return pos;
}

} // namespace compaction
} // namespace oceanbase
