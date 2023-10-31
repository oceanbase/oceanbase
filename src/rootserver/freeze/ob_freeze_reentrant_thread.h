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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_REENTRANT_THREAD_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_REENTRANT_THREAD_H_

#include "rootserver/ob_rs_reentrant_thread.h"
#include "common/ob_role.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace rootserver
{

class ObFreezeReentrantThread : public ObRsReentrantThread
{
public:
  ObFreezeReentrantThread(const uint64_t tenant_id);
  virtual ~ObFreezeReentrantThread() {}

  virtual void pause();
  virtual void resume();
  bool is_paused() const { return is_paused_; }

  int64_t get_epoch() const { return epoch_; }
  int set_epoch(const int64_t epoch);

protected:
  virtual int try_idle(const int64_t idle_time_us, const int exe_ret);
  int obtain_proposal_id_from_ls(const bool is_primary_service, int64_t &proposal_id, common::ObRole &role);

protected:
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;

private:
  bool is_paused_;
  // @epoch, is used to solve 'multi-freeze_service' may operate inner table concurrently.
  //
  // For solving switching-role slowly, we keep the tenant major_freeze_service, just
  // mark it as 'paused' state, not destroy it.
  // It is not a perfect way cuz it may occur that 'new freeze_service' start to work
  // while the 'old freeze_service' is still working before changing to 'paused' state.
  //
  // So we add a column in __all_service_epoch table, named 'freeze_service_epoch'. We use epoch_
  // to update it only when epoch_ is greater than it.
  // If epoch_ is changing during one round execution, we should mark this round execution as
  // failed, and retry in next round.
  int64_t epoch_;
};

} // rootserver
} // oceanbase
#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_REENTRANT_THREAD_H_