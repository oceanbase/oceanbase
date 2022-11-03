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

#include "ob_leader_coordinator.h"
#include <utility>
#include "lib/ob_errno.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_occam_time_guard.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "table_accessor.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{

using namespace common;
using namespace share;

ObLeaderCoordinator::ObLeaderCoordinator()
: all_ls_election_reference_info_(nullptr), is_inited_(false) {}

int ObLeaderCoordinator::mtl_init(ObLeaderCoordinator *&p_coordinator)
{
  LC_TIME_GUARD(1_s);
  return p_coordinator->init_and_start();
}

struct AllLsElectionReferenceInfoFactory
{
  static ObArray<LsElectionReferenceInfo> *create_new()
  {
    LC_TIME_GUARD(1_s);
    ObArray<LsElectionReferenceInfo> *new_all_ls_election_reference_info = nullptr;
    if (nullptr == (new_all_ls_election_reference_info = (ObArray<LsElectionReferenceInfo>*)mtl_malloc(sizeof(ObArray<LsElectionReferenceInfo>), "Coordinator"))) {
      COORDINATOR_LOG(ERROR, "alloc memory failed", K(MTL_ID()));
    } else {
      new(new_all_ls_election_reference_info) ObArray<LsElectionReferenceInfo>();
    }
    return new_all_ls_election_reference_info;
  }
  static void delete_obj(ObArray<LsElectionReferenceInfo> *obj) {
    LC_TIME_GUARD(1_s);
    if (OB_NOT_NULL(obj)) {
      obj->~ObArray();
      mtl_free(obj);
    }
  }
};

int ObLeaderCoordinator::init_and_start()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if(is_inited_) {
    ret = OB_INIT_TWICE;
    COORDINATOR_LOG(ERROR, "has been inited alread]y", KR(ret), K(MTL_ID()));
  } else if (CLICK_FAIL(timer_.init_and_start(1, 1_ms, "CoordTimer"))) {
    COORDINATOR_LOG(ERROR, "fail to init and start timer", KR(ret), K(MTL_ID()));
  } else if (nullptr == (all_ls_election_reference_info_ = AllLsElectionReferenceInfoFactory::create_new())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COORDINATOR_LOG(ERROR, "fail to new all_ls_election_reference_info_", KR(ret), K(MTL_ID()));
  } else {
    new(all_ls_election_reference_info_) ObArray<LsElectionReferenceInfo>();
    if (CLICK_FAIL(timer_.schedule_task_ignore_handle_repeat(500_ms,
                                                          [this](){ refresh(); return false; }))) {
      COORDINATOR_LOG(ERROR, "schedule repeat task failed", KR(ret), K(MTL_ID()));
    } else {
      is_inited_ = true;
      COORDINATOR_LOG(INFO, "init leader coordinator success", KR(ret), K(MTL_ID()), K(lbt()));
    }
  }
  return ret;
}

void ObLeaderCoordinator::stop_and_wait()
{
  LC_TIME_GUARD(1_s);
  timer_.stop_and_wait();
  ObSpinLockGuard guard(lock_);
  is_inited_ = false;
}

void ObLeaderCoordinator::refresh()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObArray<LsElectionReferenceInfo> *new_all_ls_election_reference_info = AllLsElectionReferenceInfoFactory::create_new();
  if (nullptr == new_all_ls_election_reference_info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COORDINATOR_LOG(WARN, "alloc new_all_ls_election_reference_info failed", KR(ret), K(MTL_ID()));
  } else if (CLICK_FAIL(TableAccessor::get_all_ls_election_reference_info(*new_all_ls_election_reference_info))) {
    COORDINATOR_LOG(WARN, "get all ls election reference info failed", KR(ret), K(MTL_ID()));
  } else {
    COORDINATOR_LOG(INFO, "refresh __all_ls_election_reference_info success", KR(ret), K(MTL_ID()), K(*new_all_ls_election_reference_info));
    ObSpinLockGuard guard(lock_);
    std::swap(new_all_ls_election_reference_info, all_ls_election_reference_info_);
  }
  AllLsElectionReferenceInfoFactory::delete_obj(new_all_ls_election_reference_info);
}

int ObLeaderCoordinator::get_ls_election_reference_info(const share::ObLSID &ls_id,
                                                        LsElectionReferenceInfo &reference_info) const {
  LC_TIME_GUARD(1_s);
  ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    COORDINATOR_LOG(ERROR, "call before init", KR(ret));
  } else {
    int64_t idx = 0;
    for (; idx < all_ls_election_reference_info_->count(); ++idx) {
      if (all_ls_election_reference_info_->at(idx).element<0>() == ls_id.id()) {
        if (CLICK_FAIL(reference_info.assign(all_ls_election_reference_info_->at(idx)))) {
          COORDINATOR_LOG(WARN, "fail to assign reference info",
                                KR(ret), K(MTL_ID()), KPC_(all_ls_election_reference_info));
        }
        break;
      }
    }
    if (idx == all_ls_election_reference_info_->count()) {
      ret = OB_ENTRY_NOT_EXIST;
      COORDINATOR_LOG(WARN, "can not find this ls_id in all_ls_election_reference_info_",
                            KR(ret), K(MTL_ID()), K(ls_id), KPC_(all_ls_election_reference_info));
    }
  }
  return ret;
}

}
}
}
