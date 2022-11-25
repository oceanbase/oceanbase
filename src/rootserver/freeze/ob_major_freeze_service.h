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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_SERVICE_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_SERVICE_

#include "share/ob_ls_id.h"
#include "logservice/ob_log_base_type.h"
#include "lib/lock/ob_recursive_mutex.h"

namespace oceanbase
{
namespace rootserver
{
class ObTenantMajorFreeze;

class ObMajorFreezeService : public logservice::ObIReplaySubHandler,
                             public logservice::ObICheckpointSubHandler,
                             public logservice::ObIRoleChangeSubHandler
{
public:
  ObMajorFreezeService() 
    : is_inited_(false), tenant_id_(common::OB_INVALID_ID), 
      is_launched_(false), lock_(), rw_lock_(), switch_lock_(),
      tenant_major_freeze_(nullptr)
  {}
  virtual ~ObMajorFreezeService();

  int init(uint64_t tenant_id);

  // clog checkpoint, do nothing
  int flush(int64_t rec_log_ts)
  {
    UNUSED(rec_log_ts);
    return OB_SUCCESS;
  }
  int64_t get_rec_log_ts() { return INT64_MAX; }
 
  // for replay, do nothing
  int replay(const void *buffer,
             const int64_t buf_size,
             const palf::LSN &lsn,
             const int64_t log_ts) 
  { 
    UNUSED(buffer);
    UNUSED(buf_size);
    UNUSED(lsn);
    UNUSED(log_ts);
    return OB_SUCCESS; 
  }

  // switch leader
  void switch_to_follower_forcedly(); 
  int switch_to_leader(); 

  int switch_to_follower_gracefully();
  int resume_leader() { return switch_to_leader(); }

  int launch_major_freeze();
  int suspend_merge();
  int resume_merge();
  int clear_merge_error();

  uint64_t get_tenant_id() const { return tenant_id_; }

  static int mtl_init(ObMajorFreezeService *&service);
  int start() { return common::OB_SUCCESS; };
  void stop();
  void wait();
  void destroy();

private:
  int alloc_tenant_major_freeze();
  int delete_tenant_major_freeze();
  int inner_switch_to_follower();
  int check_inner_stat();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_launched_;
  // lock_: used for avoiding launching, suspend, resume, etc. ops concurrently execute
  common::ObRecursiveMutex lock_;
  // rw_lock_: used for switch_role, not use lock_ in switch_role. Otherwise, if major_freeze
  // hang, switch_role may also hang
  // https://work.aone.alibaba-inc.com/issue/44368193
  common::SpinRWLock rw_lock_;
  // switch_lock_: used for avoiding switch_to_leader, switch_to_follower concurrently execute. 
  common::ObRecursiveMutex switch_lock_;
  ObTenantMajorFreeze *tenant_major_freeze_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_FREEZE_SERVICE_
