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

#ifndef OCEANBASE_ROOTSERVER_OB_CANCEL_VALIDATE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_CANCEL_VALIDATE_SCHEDULER_H_

#include <stdint.h>
#include "rootserver/backup/ob_root_validate.h"
#include "share/backup/ob_validate_task_updater.h"

namespace oceanbase {
namespace rootserver {

class ObCancelValidateScheduler {
public:
  ObCancelValidateScheduler();
  virtual ~ObCancelValidateScheduler();

  int init(const uint64_t tenant_id, const int64_t job_id, common::ObMySQLProxy& sql_proxy,
      rootserver::ObRootValidate& root_validate);
  int start_schedule_cancel_validate();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t job_id_;
  share::ObBackupValidateTaskUpdater updater_;
  rootserver::ObRootValidate* root_validate_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif
