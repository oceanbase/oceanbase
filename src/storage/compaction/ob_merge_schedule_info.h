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

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_MERGE_SCHEDULE_INFO_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_MERGE_SCHEDULE_INFO_H_

#include "lib/queue/ob_dedup_queue.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

struct ObMergeStatEntry
{
  ObMergeStatEntry();
  void reset();
  int64_t frozen_version_;
  int64_t start_time_;
  int64_t finish_time_;
};

// Major Merge History
class ObMajorMergeHistory
{
public:
  ObMajorMergeHistory();
  virtual ~ObMajorMergeHistory();
  int notify_major_merge_start(const int64_t frozen_version);
  int notify_major_merge_finish(const int64_t frozen_version);
  int get_entry(const int64_t frozen_version, ObMergeStatEntry &entry);
private:
  int search_entry(const int64_t frozen_version, ObMergeStatEntry *&pentry);
  static const int64_t MAX_KEPT_HISTORY = 16;
  obsys::ObRWLock lock_;
  ObMergeStatEntry stats_[MAX_KEPT_HISTORY];
private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorMergeHistory);
};

class ObMinorMergeHistory
{
public:
  explicit ObMinorMergeHistory(const uint64_t tenant_id);
  virtual ~ObMinorMergeHistory();
  int notify_minor_merge_start(const int64_t snapshot_version);
  int notify_minor_merge_finish(const int64_t snapshot_version);
private:
  static const int64_t MAX_MINOR_HISTORY = 16;
  lib::ObMutex mutex_;
  int64_t count_;
  uint64_t tenant_id_;
  int64_t snapshot_history_[MAX_MINOR_HISTORY];
private:
  DISALLOW_COPY_AND_ASSIGN(ObMinorMergeHistory);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_COMPACTION_OB_MERGE_INFO_H_ */
