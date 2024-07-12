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

#ifndef OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_TASK
#define OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_TASK

#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace compaction
{
class ObTabletMergeCtx;
}

namespace storage
{
class ObTabletHandle;
class ObTablet;
class ObTableHandleV2;

namespace mds
{
class ObMdsTableMergeDag;

class ObMdsTableMergeTask : public share::ObITask
{
public:
  ObMdsTableMergeTask();
  virtual ~ObMdsTableMergeTask() = default;
  ObMdsTableMergeTask(const ObMdsTableMergeTask&) = delete;
  ObMdsTableMergeTask &operator=(const ObMdsTableMergeTask&) = delete;
public:
  virtual int process() override;

  int init();
private:
  void try_schedule_compaction_after_mds_mini(compaction::ObTabletMergeCtx &ctx, ObTabletHandle &tablet_handle);
  void set_merge_finish_time(compaction::ObTabletMergeCtx &ctx);
  static int build_mds_sstable(
      compaction::ObTabletMergeCtx &ctx,
      const int64_t mds_construct_sequence,
      ObTableHandleV2 &table_handle);
private:
  bool is_inited_;
  ObMdsTableMergeDag *mds_merge_dag_;
};
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_TASK
