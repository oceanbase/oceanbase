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
      schedule_merge_scn_(0),
      co_major_merge_type_(ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE)
  { }
  ObTabletSchedulePair(
      const common::ObTabletID &tablet_id,
      const int64_t schedule_merge_scn,
      const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type)
    : tablet_id_(tablet_id),
      schedule_merge_scn_(schedule_merge_scn),
      co_major_merge_type_(co_major_merge_type)
  { }
  bool is_valid() const { return tablet_id_.is_valid() && schedule_merge_scn_ > 0; }
  bool need_force_freeze() const { return schedule_merge_scn_ > 0; }
  void reset() { tablet_id_.reset(); schedule_merge_scn_ = 0; co_major_merge_type_ = ObCOMajorMergePolicy::INVALID_CO_MAJOR_MERGE_TYPE; }
  TO_STRING_KV(K_(tablet_id), K_(schedule_merge_scn), K_(co_major_merge_type));
public:
  common::ObTabletID tablet_id_;
  int64_t schedule_merge_scn_;
  ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type_;
};

struct ObBatchFreezeTabletsParam : public ObBatchExecParam<ObTabletSchedulePair>
{
  ObBatchFreezeTabletsParam()
    : ObBatchExecParam(BATCH_FREEZE),
      loop_cnt_(0)
  {}
  ObBatchFreezeTabletsParam(
    const share::ObLSID &ls_id,
    const int64_t merge_version,
    const int64_t loop_cnt = 0)
    : ObBatchExecParam(BATCH_FREEZE, ls_id, merge_version, DEFAULT_BATCH_SIZE),
      loop_cnt_(loop_cnt)
  {}
  virtual ~ObBatchFreezeTabletsParam() = default;
  static constexpr int64_t DEFAULT_BATCH_SIZE = 32;
  int64_t loop_cnt_;
  int64_t to_string(char *buf, const int64_t buf_len) const override;
};
class ObBatchFreezeTabletsTask;
class ObBatchFreezeTabletsDag : public ObBatchExecDag<ObBatchFreezeTabletsTask, ObBatchFreezeTabletsParam>
{
public:
  ObBatchFreezeTabletsDag()
    : ObBatchExecDag(share::ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS)
  {}
  virtual ~ObBatchFreezeTabletsDag() = default;
  virtual int inner_init();
  virtual bool operator == (const ObIDag &other) const override;
public:
  static constexpr int64_t MAX_CONCURRENT_FREEZE_TASK_CNT = 2;
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
