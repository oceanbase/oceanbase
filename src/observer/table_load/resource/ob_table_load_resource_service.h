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

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "logservice/ob_log_base_type.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/resource/ob_table_load_resource_manager.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadResourceManager;

class ObTableLoadResourceService : public logservice::ObIReplaySubHandler,
						 public logservice::ObICheckpointSubHandler,
                                   public logservice::ObIRoleChangeSubHandler
{
public:
	ObTableLoadResourceService()
    : switch_lock_(common::ObLatchIds::RESOURCE_SERVICE_SWITCH_LOCK),
      rw_lock_(common::ObLatchIds::RESOURCE_SERVICE_LOCK),
      resource_manager_(nullptr),
      tenant_id_(common::OB_INVALID_ID),
      is_inited_(false)
  {
  }
  virtual ~ObTableLoadResourceService();
	int init(const uint64_t tenant_id);
  static int mtl_init(ObTableLoadResourceService *&service);
	int start() { return common::OB_SUCCESS; };
  void stop();
  void wait();
  void destroy();

	// for replay, do nothing
	int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn)
  {
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  // for checkpoint, do nothing
  share::SCN get_rec_scn() { return share::SCN::max_scn(); }

  int flush(share::SCN &scn)
  {
    UNUSED(scn);
    return OB_SUCCESS;
  }

  int resume_leader() { return switch_to_leader(); }
	int switch_to_leader();
	int switch_to_follower_gracefully();
	void switch_to_follower_forcedly();

  static int check_tenant();
	uint64_t get_tenant_id() const { return tenant_id_; }
  static int apply_resource(ObDirectLoadResourceApplyArg &arg, ObDirectLoadResourceOpRes &res);
  static int release_resource(ObDirectLoadResourceReleaseArg &arg);
  static int update_resource(ObDirectLoadResourceUpdateArg &arg);
private:
	int alloc_resource_manager();
	int delete_resource_manager();
	int inner_switch_to_follower();
	int check_inner_stat();
private:
  common::ObRecursiveMutex switch_lock_;
  common::SpinRWLock rw_lock_;
	ObTableLoadResourceManager *resource_manager_;
  uint64_t tenant_id_;
	bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
