//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_BATCH_FREEZE_TABLETS_DAG_H_
#define OB_STORAGE_COMPACTION_BATCH_FREEZE_TABLETS_DAG_H_
#include "share/compaction/ob_batch_exec_dag.h"
namespace oceanbase
{
namespace compaction
{
struct ObTabletSchedulePair
{
public:
  ObTabletSchedulePair()
    : tablet_id_(),
      schedule_merge_scn_(0)
  { }
  ObTabletSchedulePair(
      const common::ObTabletID &tablet_id,
      const int64_t schedule_merge_scn)
    : tablet_id_(tablet_id),
      schedule_merge_scn_(schedule_merge_scn)
  { }
  bool is_valid() const { return tablet_id_.is_valid() && schedule_merge_scn_ > 0; }
  bool need_force_freeze() const { return schedule_merge_scn_ > 0; }
  void reset() { tablet_id_.reset(); schedule_merge_scn_ = 0; }
  TO_STRING_KV(K_(tablet_id), K_(schedule_merge_scn));
public:
  common::ObTabletID tablet_id_;
  int64_t schedule_merge_scn_;
};

struct ObBatchFreezeTabletsParam : public ObBatchExecParam<ObTabletSchedulePair>
{
  ObBatchFreezeTabletsParam()
    : ObBatchExecParam(BATCH_FREEZE)
  {}
  ObBatchFreezeTabletsParam(
    const share::ObLSID &ls_id,
    const int64_t merge_version)
    : ObBatchExecParam(BATCH_FREEZE, ls_id, merge_version, DEFAULT_BATCH_SIZE)
  {}
  virtual ~ObBatchFreezeTabletsParam() = default;
  static constexpr int64_t DEFAULT_BATCH_SIZE = 32;
};
class ObBatchFreezeTabletsTask;
class ObBatchFreezeTabletsDag : public ObBatchExecDag<ObBatchFreezeTabletsTask, ObBatchFreezeTabletsParam>
{
public:
  ObBatchFreezeTabletsDag()
    : ObBatchExecDag(share::ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS)
  {}
  virtual ~ObBatchFreezeTabletsDag() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchFreezeTabletsDag);
};

class ObBatchFreezeTabletsTask : public ObBatchExecTask<ObBatchFreezeTabletsTask, ObBatchFreezeTabletsParam>
{
public:
  ObBatchFreezeTabletsTask();
  virtual ~ObBatchFreezeTabletsTask();
  virtual int inner_process() override;
private:
  int schedule_tablet_major_after_freeze(
    ObLS &ls,
    const ObTabletSchedulePair &cur_pair);
  DISALLOW_COPY_AND_ASSIGN(ObBatchFreezeTabletsTask);
};


} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_BATCH_FREEZE_TABLETS_DAG_H_
