/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_WINDOW_SCHEDULE_TABLET_FUNC_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_WINDOW_SCHEDULE_TABLET_FUNC_H_

#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/compaction/ob_window_compaction_utils.h"

namespace oceanbase
{
namespace compaction
{

class ObWindowScheduleTabletFunc : public ObScheduleTabletFunc
{
public:
  ObWindowScheduleTabletFunc() : ObScheduleTabletFunc(0, ObAdaptiveMergePolicy::WINDOW_COMPACTION, 0 /*loop_cnt*/, COMPACTION_WINDOW_MODE), window_decision_log_info_() {}
  virtual ~ObWindowScheduleTabletFunc() {}
public:
  int refresh_window_tablet(const ObTabletCompactionScore &candidate);
  int process_ready_candidate(
    ObTabletCompactionScore &candidate,
    storage::ObTabletHandle &tablet_handle);
  int process_log_submitted_candidate(
    ObTabletCompactionScore &candidate,
    storage::ObTabletHandle &tablet_handle);
  virtual const ObWindowCompactionDecisionLogInfo *get_window_decision_log_info() const override { return &window_decision_log_info_; }
private:
  ObWindowCompactionDecisionLogInfo window_decision_log_info_;
  DISALLOW_COPY_AND_ASSIGN(ObWindowScheduleTabletFunc);
};

} // namespace compaction
} // namespace oceanbase

#endif