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
    : all_ls_election_reference_info_(nullptr),
      is_running_(false),
      lock_(common::ObLatchIds::ELECTION_LOCK)
{}

ObLeaderCoordinator::~ObLeaderCoordinator() {}

struct AllLsElectionReferenceInfoFactory
{
  static ObArray<LsElectionReferenceInfo> *create_new()
  {
    LC_TIME_GUARD(1_s);
    ObArray<LsElectionReferenceInfo> *new_all_ls_election_reference_info = nullptr;
    if (nullptr == (new_all_ls_election_reference_info = (ObArray<LsElectionReferenceInfo>*)mtl_malloc(sizeof(ObArray<LsElectionReferenceInfo>), "Coordinator"))) {
      COORDINATOR_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "alloc memory failed");
    } else {
      new(new_all_ls_election_reference_info) ObArray<LsElectionReferenceInfo>();
      new_all_ls_election_reference_info->set_attr(ObMemAttr(MTL_ID(), "LsElectRefInfo"));
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

void ObLeaderCoordinator::destroy()
{
  LC_TIME_GUARD(1_s);
  recovery_detect_timer_.destroy();
  failure_detect_timer_.destroy();
  AllLsElectionReferenceInfoFactory::delete_obj(all_ls_election_reference_info_);
  all_ls_election_reference_info_ = NULL;
  COORDINATOR_LOG(INFO, "ObLeaderCoordinator mtl destroy");
}

int ObLeaderCoordinator::mtl_init(ObLeaderCoordinator *&p_coordinator)// init timer
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(p_coordinator->lock_);
  if(p_coordinator->is_running_) {
    ret = OB_INIT_TWICE;
    COORDINATOR_LOG(ERROR, "has been inited alread]y", KR(ret));
  } else if (CLICK_FAIL(p_coordinator->recovery_detect_timer_.init_and_start(1, 100_ms, "CoordTR"))) {
    COORDINATOR_LOG(ERROR, "fail to init and start recovery_detect_timer", KR(ret));
  } else if (CLICK_FAIL(p_coordinator->failure_detect_timer_.init_and_start(1, 100_ms, "CoordTF"))) {
    COORDINATOR_LOG(ERROR, "fail to init and start failure_detect_timer", KR(ret));
  } else {
    COORDINATOR_LOG(INFO, "ObLeaderCoordinator mtl init success", KR(ret));
  }
  return ret;
}

int ObLeaderCoordinator::mtl_start(ObLeaderCoordinator *&p_coordinator)// start run timer task
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  if (nullptr == (p_coordinator->all_ls_election_reference_info_ = AllLsElectionReferenceInfoFactory::create_new())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COORDINATOR_LOG(ERROR, "fail to new all_ls_election_reference_info_", KR(ret));
  } else {
    new(p_coordinator->all_ls_election_reference_info_) ObArray<LsElectionReferenceInfo>();
    if (CLICK_FAIL(p_coordinator->recovery_detect_timer_.schedule_task_repeat(
        p_coordinator->refresh_priority_task_handle_,
        5000_ms,
        [p_coordinator](){ p_coordinator->refresh(); return false; }))) {
      COORDINATOR_LOG(ERROR, "schedule repeat task failed", KR(ret));
    } else {
      p_coordinator->is_running_ = true;
      COORDINATOR_LOG(INFO, "ObLeaderCoordinator mtl start success", KR(ret));
    }
  }
  return ret;
}

void ObLeaderCoordinator::mtl_stop(ObLeaderCoordinator *&p_coordinator)// stop timer task
{
  if (OB_ISNULL(p_coordinator)) {
    COORDINATOR_LOG_RET(WARN, OB_INVALID_ARGUMENT, "p_coordinator is NULL");
  } else {
    p_coordinator->is_running_ = false;
    p_coordinator->refresh_priority_task_handle_.stop();
    COORDINATOR_LOG(INFO, "ObLeaderCoordinator mtl stop");
  }
}

void ObLeaderCoordinator::mtl_wait(ObLeaderCoordinator *&p_coordinator)// wait timer task
{
  if (OB_ISNULL(p_coordinator)) {
    COORDINATOR_LOG_RET(WARN, OB_INVALID_ARGUMENT, "p_coordinator is NULL");
  } else {
    p_coordinator->refresh_priority_task_handle_.wait();
    COORDINATOR_LOG(INFO, "ObLeaderCoordinator mtl wait");
  }
}

void ObLeaderCoordinator::refresh()
{
  LC_TIME_GUARD(1_s);
  int ret = OB_SUCCESS;
  ObArray<LsElectionReferenceInfo> *new_all_ls_election_reference_info = AllLsElectionReferenceInfoFactory::create_new();
  if (nullptr == new_all_ls_election_reference_info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COORDINATOR_LOG(WARN, "alloc new_all_ls_election_reference_info failed", KR(ret));
  } else if (CLICK_FAIL(TableAccessor::get_all_ls_election_reference_info(*new_all_ls_election_reference_info))) {
    COORDINATOR_LOG(WARN, "get all ls election reference info failed", KR(ret));
  } else {
    COORDINATOR_LOG(INFO, "refresh __all_ls_election_reference_info success", KR(ret), K(*new_all_ls_election_reference_info));
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
  if (!is_running_) {
    ret = OB_NOT_RUNNING;
    COORDINATOR_LOG(WARN, "not running", KR(ret));
  } else {
    int64_t idx = 0;
    for (; idx < all_ls_election_reference_info_->count(); ++idx) {
      if (all_ls_election_reference_info_->at(idx).element<0>() == ls_id.id()) {
        if (CLICK_FAIL(reference_info.assign(all_ls_election_reference_info_->at(idx)))) {
          COORDINATOR_LOG(WARN, "fail to assign reference info",
                                KR(ret), KPC_(all_ls_election_reference_info));
        }
        break;
      }
    }
    if (idx == all_ls_election_reference_info_->count()) {
      ret = OB_ENTRY_NOT_EXIST;
      COORDINATOR_LOG(WARN, "can not find this ls_id in all_ls_election_reference_info_",
                            KR(ret), K(ls_id), KPC_(all_ls_election_reference_info));
    }
  }
  return ret;
}

int ObLeaderCoordinator::schedule_refresh_priority_task()
{
  // the parameter is set to 1us to pass the checking
  return refresh_priority_task_handle_.reschedule_after(1);
}

}
}
}
