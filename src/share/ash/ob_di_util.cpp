/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "lib/stat/ob_diagnostic_info_container.h"
#include "share/ash/ob_di_util.h"
#include "lib/container/ob_vector.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "common/ob_smart_var.h"

namespace oceanbase
{
namespace share
{

int ObDiagnosticInfoUtil::get_the_diag_info(int64_t session_id, ObDISessionCollect &diag_info)
{
  int ret = OB_ENTRY_NOT_EXIST;
  common::ObVector<uint64_t> ids;
  if (MTL_ID() == OB_SYS_TENANT_ID) {
    GCTX.omt_->get_tenant_ids(ids);
  } else {
    ids.push_back(MTL_ID());
  }
  bool is_break = false;
  for (int64_t i = 0; i < ids.size() && !is_break; ++i) {
    uint64_t tenant_id = ids[i];
    if (!is_virtual_tenant_id(tenant_id)) {
      MTL_SWITCH(tenant_id)
      {
        if (OB_FAIL(
                MTL(ObDiagnosticInfoContainer *)->get_session_diag_info(session_id, diag_info))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("failed to get diagnostic session info", K(ret), K(session_id));
            is_break = true;
            break;
          } else {
            // iter next possible tenant
          }
        } else {
          is_break = true;
          // until success.
          break;
        }
      }
    }
  }
  if (ret == OB_ENTRY_NOT_EXIST) {
    ObDiagnosticInfoContainer *c = ObDiagnosticInfoContainer::get_global_di_container();
    if (OB_FAIL(c->get_session_diag_info(session_id, diag_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get diagnostic info from global container", K(ret), K(session_id));
      }
    }
  }
  return ret;
}

int ObDiagnosticInfoUtil::get_all_diag_info(
    ObIArray<std::pair<uint64_t, ObDISessionCollect>> &diag_infos, int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObVector<uint64_t> ids;
  GCTX.omt_->get_tenant_ids(ids);
  std::function<bool(const SessionID &, ObDiagnosticInfo *)> fn =
      [&diag_infos, tenant_id](const SessionID &session_id, ObDiagnosticInfo *di) {
        int ret = OB_SUCCESS;
        typedef std::pair<uint64_t, common::ObDISessionCollect> DiPair;
        HEAP_VAR(DiPair, pair)
        {
          const uint64_t cur_tenant_id = di->get_tenant_id();
          if (tenant_id == OB_SYS_TENANT_ID || cur_tenant_id == tenant_id) {
            pair.first = di->get_session_id();
            pair.second.session_id_ = di->get_session_id();
            pair.second.base_value_.set_tenant_id(di->get_tenant_id());
            pair.second.base_value_.set_curr_wait(di->get_curr_wait());
            pair.second.base_value_.get_add_stat_stats().add(di->get_add_stat_stats());
            di->get_event_stats().accumulate_to(pair.second.base_value_.get_event_stats());
            if (OB_FAIL(diag_infos.push_back(pair))) {
              LOG_WARN("failed to insert into diagnostic infos", K(ret));
            }
          }
        }
        return OB_SUCCESS == ret;
      };
  for (int64_t i = 0; OB_SUCC(ret) && i < ids.size(); ++i) {
    const uint64_t cur_tenant_id = ids[i];
    if (tenant_id == OB_SYS_TENANT_ID || cur_tenant_id == tenant_id) {
      if (!is_virtual_tenant_id(cur_tenant_id)) {
        MTL_SWITCH(cur_tenant_id)
        {
          if (OB_FAIL(MTL(ObDiagnosticInfoContainer *)->for_each_running_di(fn))) {
            LOG_WARN("failed to get all diag info", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDiagnosticInfoContainer *c = ObDiagnosticInfoContainer::get_global_di_container();
    if (OB_FAIL(c->for_each_running_di(fn))) {
      LOG_WARN("failed to get all diag info from global di", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObDiagnosticInfoUtil::get_the_diag_info(uint64_t tenant_id, ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  std::function<bool(const SessionID &, ObDiagnosticInfo *)> fn =
      [&diag_info, tenant_id](const SessionID &session_id, ObDiagnosticInfo *di) {
        const uint64_t cur_tenant_id = di->get_tenant_id();
        if (cur_tenant_id == tenant_id) {
          diag_info.get_add_stat_stats().add(di->get_add_stat_stats());
          const_cast<ObDiagnosticInfo *>(di)->get_event_stats().accumulate_to(
              diag_info.get_event_stats());
        }
        return true;
      };
  MTL_SWITCH(tenant_id)
  {
    ObDiagnosticInfoContainer *c = MTL(ObDiagnosticInfoContainer *);
    if (OB_FAIL(c->for_each_running_di(fn))) {
      LOG_WARN("failed to get tenant diagnostic info", K(ret), KPC(c));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(c->get_base_summary().get_tenant_add_stats(
              tenant_id, diag_info.get_add_stat_stats()))) {
        LOG_WARN("failed to get base summary", K(ret));
      } else if (OB_FAIL(c->get_base_summary().get_tenant_event(tenant_id, diag_info.get_event_stats()))) {
        LOG_WARN("failed to get base summary", K(ret));
        // latch stat only record in global stat.
        // } else if (c->get_base_summary().get_tenant_latch_stat(
        //                tenant_id, diag_info.get_latch_stats())) {
        //   LOG_WARN("failed to get base summary", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDiagnosticInfoContainer *c = ObDiagnosticInfoContainer::get_global_di_container();
    if (OB_FAIL(c->for_each_running_di(fn))) {
      LOG_WARN("failed to get tenant diagnostic info", K(ret), KPC(c));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(c->get_base_summary().get_tenant_add_stats(
              tenant_id, diag_info.get_add_stat_stats()))) {
        LOG_WARN("failed to get base summary", K(ret));
      } else if (OB_FAIL(c->get_base_summary().get_tenant_event(tenant_id, diag_info.get_event_stats()))) {
        LOG_WARN("failed to get base summary", K(ret));
      } else if (OB_FAIL(c->get_base_summary().get_tenant_latch_stat(
                     tenant_id, diag_info.get_latch_stats()))) {
        LOG_WARN("failed to get base summary", K(ret));
      }
    }
  }
  return ret;
}

inline int get_group_di(int64_t group_id,
    ObArray<std::pair<int64_t, common::ObDiagnoseTenantInfo>> &diag_infos,
    common::ObIAllocator *alloc, ObDiagnoseTenantInfo *&di)
{
  int ret = OB_SUCCESS;
  di = nullptr;
  for (int i = 0; i < diag_infos.size(); i++) {
    if (diag_infos.at(i).first == group_id) {
      di = &diag_infos.at(i).second;
      break;
    }
  }
  if (OB_ISNULL(di)) {
    typedef std::pair<int64_t, ObDiagnoseTenantInfo> ResType;
    HEAP_VAR(ResType, tinfo, group_id, alloc)
    {
      if (OB_FAIL(diag_infos.push_back(tinfo))) {
        LOG_WARN("failed to create group diagnose info", K(ret), K(group_id));
      } else {
        di = &(diag_infos.at(diag_infos.size() - 1).second);
        OB_ASSERT(diag_infos.at(diag_infos.size() - 1).first == group_id);
      }
    }
  }
  return ret;
}

int ObDiagnosticInfoUtil::get_group_diag_info(uint64_t tenant_id,
    ObArray<std::pair<int64_t, common::ObDiagnoseTenantInfo>> &diag_infos,
    common::ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  std::function<bool(const SessionID &, ObDiagnosticInfo *)> fn = [&diag_infos, &alloc, tenant_id](
                                                                      const SessionID &session_id,
                                                                      ObDiagnosticInfo *di) {
    ObDiagnoseTenantInfo *tdi = nullptr;
    int ret = OB_SUCCESS;
    const uint64_t cur_tenant_id = di->get_tenant_id();
    if (cur_tenant_id == tenant_id) {
      if (OB_FAIL(get_group_di(di->get_group_id(), diag_infos, alloc, tdi))) {
        LOG_WARN("failed to acquire di", K(ret), K(di));
      } else {
        OB_ASSERT(tdi != nullptr);
        tdi->get_add_stat_stats().add(di->get_add_stat_stats());
        const_cast<ObDiagnosticInfo *>(di)->get_event_stats().accumulate_to(tdi->get_event_stats());
      }
    }
    return OB_SUCCESS == ret;
  };

  std::function<void(int64_t, const ObDiagnoseTenantInfo &)> summary_fn =
      [&diag_infos, &alloc](int64_t group_id, const ObDiagnoseTenantInfo &di) {
        ObDiagnoseTenantInfo *tdi = nullptr;
        int ret = OB_SUCCESS;
        if (OB_FAIL(get_group_di(group_id, diag_infos, alloc, tdi))) {
          LOG_WARN("failed to acquire di", K(ret), K(di));
        } else {
          OB_ASSERT(tdi != nullptr);
          tdi->get_add_stat_stats().add(
              const_cast<ObDiagnoseTenantInfo &>(di).get_add_stat_stats());
          tdi->get_event_stats().add(const_cast<ObDiagnoseTenantInfo &>(di).get_event_stats());
        }
      };
  MTL_SWITCH(tenant_id)
  {
    ObDiagnosticInfoContainer *c = MTL(ObDiagnosticInfoContainer *);
    if (OB_FAIL(c->for_each_running_di(fn))) {
      LOG_WARN("failed to get tenant diagnostic info", K(ret), KPC(c));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(c->get_base_summary().for_each_group(tenant_id, summary_fn))) {
        LOG_WARN("failed to get base summary", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDiagnosticInfoContainer *c = ObDiagnosticInfoContainer::get_global_di_container();
    if (OB_FAIL(c->for_each_running_di(fn))) {
      LOG_WARN("failed to get tenant diagnostic info", K(ret), KPC(c));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(c->get_base_summary().for_each_group(tenant_id, summary_fn))) {
        LOG_WARN("failed to get base summary", K(ret));
      }
    }
  }
  return ret;
}

} /* namespace share */
} /* namespace oceanbase */
