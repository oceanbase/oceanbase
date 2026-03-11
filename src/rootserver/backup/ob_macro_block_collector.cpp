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

#ifdef OB_BUILD_SHARED_STORAGE

#define USING_LOG_PREFIX RS

#include "ob_macro_block_collector.h"
#include <algorithm>

namespace oceanbase
{
using namespace common;
using namespace sqlclient;
using namespace share;
namespace rootserver
{

int ObMacroBlockCollector::collect_macro_blocks_with_generator(
    const ObSSHAMacroTaskType &task_type,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const share::ObBackupDest &backup_dest,
    const share::SCN &snapshot_scn,
    const common::ObIArray<share::ObLSID> &ls_id_array,
    share::ObHATransEndHelper *trans_end_helper,
    int64_t &total_task_cnt,
    int64_t &total_macro_cnt,
    int64_t &total_bytes)
{
  int ret = OB_SUCCESS;
  total_macro_cnt = 0;
  total_bytes = 0;

  if (OB_INVALID_TENANT_ID == tenant_id || !backup_dest.is_valid() || !snapshot_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(backup_dest), K(snapshot_scn));
  } else {
    ObSSHAMacroCopyTaskGeneratorInitParam init_param;
    init_param.task_type_ = task_type;
    init_param.tenant_id_ = tenant_id;
    init_param.parent_task_id_ = parent_task_id;
    init_param.snapshot_ = snapshot_scn;
    if (OB_FAIL(init_param.backup_set_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
    } else {
      // Assign log stream ID array to init param
      if (OB_FAIL(init_param.ls_id_array_.assign(ls_id_array))) {
        LOG_WARN("failed to assign ls id array", K(ret));
      } else {
        storage::ObSSHAMacroCopyTaskGenerator generator;

        if (OB_FAIL(generator.init(init_param, trans_end_helper))) {
          LOG_WARN("failed to init generator", K(ret), K(init_param));
        } else if (OB_FAIL(generator.generate(total_task_cnt, total_macro_cnt, total_bytes))) {
          LOG_WARN("failed to process generator", K(ret));
        } else {
          LOG_INFO("collect macro blocks with generator success",
                  K(tenant_id), K(ls_id_array), K(total_macro_cnt), K(total_task_cnt),
                  K(total_bytes), K(snapshot_scn));
        }
      }
    }
  }

  return ret;
}

} // namespace rootserver
} // namespace oceanbase

#endif // OB_BUILD_SHARED_STORAGE
