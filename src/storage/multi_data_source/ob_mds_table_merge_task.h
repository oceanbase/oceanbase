/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
class ObTabletMdsMiniMergeDag;

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
  int check_tablet_status_for_empty_mds_table_(const ObTablet& tablet) const;
  static int build_mds_sstable(
      compaction::ObTabletMergeCtx &ctx,
      const int64_t mds_construct_sequence,
      ObTableHandleV2 &table_handle);
private:
  bool is_inited_;
  ObTabletMdsMiniMergeDag *mds_merge_dag_;
};
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_TASK
