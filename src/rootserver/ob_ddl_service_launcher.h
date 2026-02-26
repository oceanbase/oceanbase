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

#ifndef _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_LAUNCHER_H_
#define _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_LAUNCHER_H_

#include "common/ob_role.h"      // for ObRole
#include "lib/utility/ob_macro_utils.h"  // for DISALLOW_COPY_AND_ASSIGN
//#include "lib/lock/ob_spin_rwlock.h" // for SpinRWLock
#include "logservice/ob_log_base_type.h" // for ObIRoleChangeSubHandler etc.
#include "share/scn.h"                   // for SCN
#include "rootserver/ob_tenant_thread_helper.h" // for DEFINE_MTL_FUNC

namespace oceanbase
{
namespace common
{
class SpinRWLock;
}
namespace rootserver
{
class ObDDLServiceLauncher : public logservice::ObIRoleChangeSubHandler,
                             public logservice::ObICheckpointSubHandler,
                             public logservice::ObIReplaySubHandler
{
public:
  ObDDLServiceLauncher();
  virtual ~ObDDLServiceLauncher() {}

  int init();
  void destroy();

  bool is_inited() const { return inited_; }
  static bool is_ddl_service_started() { return ATOMIC_LOAD(&is_ddl_service_started_); }

  // for ObIRoleChangeSubHandler
  virtual int switch_to_leader() override;
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;

  int start_ddl_service_with_old_logic(
      const int64_t new_rs_epoch,
      const int64_t proposal_id_to_check);
  static int get_sys_palf_role_and_epoch(
         common::ObRole &role,
         int64_t &proposal_id);

  // for MTL related
  static int mtl_init(ObDDLServiceLauncher *&ddl_service_launcher);

  // for ObICheckpointSubHandler
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &rec_scn) override { return OB_SUCCESS; }

  // for ObIReplaySubHandler
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(buffer, nbytes, lsn, scn);
    return ret;
  }
private:
  int inner_start_ddl_service_with_lock_(
      bool with_new_mode,
      const int64_t proposal_id_to_check,
      const int64_t new_rs_epoch);
  int init_sequence_id_(
      bool with_new_mode,
      const int64_t proposal_id,
      const int64_t new_rs_epoch);
private:
  bool inited_;
  static bool is_ddl_service_started_;
  common::SpinRWLock rw_lock_; // used for update for is_ddl_service_started_
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLServiceLauncher);
};
} // end namespace rootserver
} // end namespace oceanbase
#endif // _OCEANBASE_ROOTSERVER_OB_DDL_SERVICE_LAUNCHER_H_
