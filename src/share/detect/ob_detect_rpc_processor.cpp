/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/detect/ob_detect_rpc_processor.h"
#include "share/detect/ob_detect_manager.h"

namespace oceanbase {
namespace obrpc {

int ObDetectRpcP::process()
{
  int ret = OB_SUCCESS;
  const ObTaskStateDetectReq &req = arg_;
  ObTaskStateDetectResp &response = result_;
  ARRAY_FOREACH_NORET(req.uniq_ids_, i) {
    const ObDetectableId &detectable_id = req.uniq_ids_.at(i);
    MTL_SWITCH(detectable_id.tenant_id_) {
      ObDetectManager* dm = MTL(ObDetectManager*);
      if (OB_ISNULL(dm)) {
        // ignore ret
        LIB_LOG(WARN, "[DM] dm is null", K(detectable_id.tenant_id_));
        continue;
      }
      bool task_alive = dm->is_task_alive(detectable_id);
      if (!task_alive) {
        LIB_LOG(DEBUG, "[DM] tast has already exit", K(detectable_id));
      }
      TaskInfo task_info;
      task_info.task_id_ = detectable_id;
      task_info.task_state_ = task_alive ? common::ObTaskState::RUNNING : common::ObTaskState::FINISHED;
      if (OB_FAIL(response.task_infos_.push_back(task_info))) {
        LIB_LOG(WARN, "[DM] failed to push_back", K(detectable_id), K(task_alive));
      }
    }
  }
  return ret;
}

} // end namespace obrpc
} // end namespace oceanbase
