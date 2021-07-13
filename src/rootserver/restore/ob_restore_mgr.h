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

#ifndef __OB_RS_RESTORE_MGR_H__
#define __OB_RS_RESTORE_MGR_H__

#include "rootserver/restore/ob_restore_stat.h"
#include "rootserver/restore/ob_restore_info.h"

namespace oceanbase {
namespace rootserver {
class ObRestoreMgr {
public:
  ObRestoreMgr(const volatile bool& is_stop);
  ~ObRestoreMgr();
  int init(ObRestoreMgrCtx* restore_ctx);
  int restore();

private:
  /* functions */
  int check_stop()
  {
    return is_stop_ ? common::OB_CANCELED : common::OB_SUCCESS;
  }
  int get_jobs(common::ObIArray<RestoreJob>& jobs);
  int update_job_status(int64_t job_id, RestoreTaskStatus status);
  int try_update_job_status();

  int restore_meta(RestoreJob& job_info);
  int restore_replica(RestoreJob& job_info);
  int restore_success(RestoreJob& job_info);
  int restore_stop(RestoreJob& job_info);
  int restore_fail(RestoreJob& job_info);

  /* variables */
  bool inited_;
  const volatile bool& is_stop_;
  ObRestoreMgrCtx* ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreMgr);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif /* __OB_RS_RESTORE_MGR_H__ */
//// end of header file
