/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_DEVICE_OB_DEVICE_CREDENTIAL_TASK_H_
#define OCEANBASE_SHARE_DEVICE_OB_DEVICE_CREDENTIAL_TASK_H_

#include "lib/restore/ob_storage_info.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace share
{
// To ensure that the temporary credentials in the credential map are always valid,
// the credentials are refreshed every 20 minutes.

class ObDeviceCredentialTask : public common::ObTimerTask
{
public:
  ObDeviceCredentialTask();
  virtual ~ObDeviceCredentialTask();
  int init(const int64_t interval_us);
  void reset();
  virtual void runTimerTask() override;
  TO_STRING_KV(K_(is_inited), K_(schedule_interval_us));

private:
  int do_work_();

private:
  bool is_inited_;
  int64_t schedule_interval_us_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_DEVICE_OB_DEVICE_CREDENTIAL_TASK_H_