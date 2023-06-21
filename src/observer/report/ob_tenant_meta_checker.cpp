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

#define USING_LOG_PREFIX SERVER

#include "observer/report/ob_tenant_meta_checker.h"
#include "observer/ob_server_struct.h" // GCTX
#include "share/ob_thread_define.h" // TenantLSMetaChecker, TenantTabletMetaChecker
#include "share/ls/ob_ls_operator.h" // ObLSOperator
#include "share/ls/ob_ls_table_iterator.h" // ObLSTableIterator
#include "storage/tablet/ob_tablet_iterator.h" // ObLSTabletIterator
#include "share/tablet/ob_tablet_table_operator.h" // ObTabletTableOperator
#include "share/tablet/ob_tablet_table_iterator.h" // ObTenantTabletTableIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService, ObLSIterator
#include "storage/tx_storage/ob_ls_handle.h" // ObLSHandle
#include "share/ob_tablet_replica_checksum_operator.h" // ObTabletReplicaChecksumItem
#include "storage/tablet/ob_tablet.h" // ObTablet

namespace oceanbase
{
using namespace share;
using namespace common;

namespace observer
{
ObTenantLSMetaTableCheckTask::ObTenantLSMetaTableCheckTask(ObTenantMetaChecker &checker)
    : checker_(checker)
{
}

void ObTenantLSMetaTableCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checker_.check_ls_table())) {
    LOG_WARN("fail to check ls meta table", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(checker_.schedule_ls_meta_check_task())) {
    LOG_WARN("fail to schedule ls meta check task", KR(ret));
  }
}

ObTenantTabletMetaTableCheckTask::ObTenantTabletMetaTableCheckTask(
    ObTenantMetaChecker &checker)
    : checker_(checker)
{
}

void ObTenantTabletMetaTableCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checker_.check_tablet_table())) {
    LOG_WARN("fail to check tablet meta table", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(checker_.schedule_tablet_meta_check_task())) {
    LOG_WARN("fail to schedule tablet meta check task", KR(ret));
  }
}

ObTenantMetaChecker::ObTenantMetaChecker()
    : inited_(false),
      stopped_(true),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_checker_tg_id_(OB_INVALID_INDEX),
      tablet_checker_tg_id_(OB_INVALID_INDEX),
      lst_operator_(NULL),
      tt_operator_(NULL),
      ls_meta_check_task_(*this),
      tablet_meta_check_task_(*this)
{
}

int ObTenantMetaChecker::mtl_init(ObTenantMetaChecker *&checker)
{
  const uint64_t tenant_id = MTL_ID();
  ObLSTableOperator *lst_operator = GCTX.lst_operator_;
  ObTabletTableOperator *tt_operator = GCTX.tablet_operator_;
  return checker->init(tenant_id, lst_operator, tt_operator);
}

int ObTenantMetaChecker::init(
    const uint64_t tenant_id,
    share::ObLSTableOperator *lst_operator,
    share::ObTabletTableOperator *tt_operator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))
      || OB_ISNULL(lst_operator)
      || OB_ISNULL(tt_operator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), KP(lst_operator), KP(tt_operator));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantLSMetaChecker, ls_checker_tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT ls meta checker failed", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TenantTabletMetaChecker, tablet_checker_tg_id_))) {
    LOG_WARN("TG_CREATE_TENANT tablet meta checker failed", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    lst_operator_ = lst_operator;
    tt_operator_ = tt_operator;
    inited_ = true;
  }
  return ret;
}

int ObTenantMetaChecker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    stopped_ = false;
    if (OB_FAIL(TG_START(ls_checker_tg_id_))) {
      LOG_WARN("TG_START ls_checker_tg_id failed", KR(ret), K_(ls_checker_tg_id));
    } else if (OB_FAIL(TG_START(tablet_checker_tg_id_))) {
      LOG_WARN("TG_START tablet_checker_tg_id failed", KR(ret), K_(tablet_checker_tg_id));
    } else if (OB_FAIL(schedule_ls_meta_check_task())) {
      LOG_WARN("schedule ls meta check task failed", KR(ret), K_(ls_checker_tg_id));
    } else if (OB_FAIL(schedule_tablet_meta_check_task())) {
      LOG_WARN("schedule tablet meta check task failed", KR(ret), K_(tablet_checker_tg_id));
    } else {
      LOG_INFO("ObTenantMetaChecker start success",
          K_(tenant_id), K_(ls_checker_tg_id), K_(tablet_checker_tg_id));
    }
  }
  return ret;
}

void ObTenantMetaChecker::stop()
{
  if (OB_LIKELY(inited_)) {
    stopped_ = true;
    TG_STOP(ls_checker_tg_id_);
    TG_STOP(tablet_checker_tg_id_);
    LOG_INFO("ObTenantMetaChecker stop finished",
        K_(tenant_id), K_(ls_checker_tg_id), K_(tablet_checker_tg_id));
  }
}

void ObTenantMetaChecker::wait()
{
  if (OB_LIKELY(inited_)) {
    TG_WAIT(ls_checker_tg_id_);
    TG_WAIT(tablet_checker_tg_id_);
    LOG_INFO("ObTenantMetaChecker wait finished",
        K_(tenant_id), K_(ls_checker_tg_id), K_(tablet_checker_tg_id));
  }
}

void ObTenantMetaChecker::destroy()
{
  if (OB_LIKELY(inited_)) {
    tenant_id_ = OB_INVALID_TENANT_ID;
    lst_operator_ = nullptr;
    tt_operator_ = nullptr;
    inited_ = false;
    stopped_ = true;
    TG_DESTROY(ls_checker_tg_id_);
    TG_DESTROY(tablet_checker_tg_id_);
    LOG_INFO("ObTenantMetaChecker destroy finished",
        K_(tenant_id), K_(ls_checker_tg_id), K_(tablet_checker_tg_id));
  }
}

int ObTenantMetaChecker::check_ls_table()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  share::ObLSTable::Mode mode = share::ObLSTable::DEFAULT_MODE;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(check_ls_table_(mode))) {
      LOG_WARN("check ls table failed", KR(ret), K(mode));
    }
    // Additionally, check sys tenant's inner table
    if (is_sys_tenant(tenant_id_)) { // overwrite ret
      mode = share::ObLSTable::INNER_TABLE_ONLY_MODE;
      if (OB_FAIL(check_ls_table_(mode))) {
        LOG_WARN("check ls table failed", KR(ret), K(mode));
      }
    }
  }
  return ret;
}

int ObTenantMetaChecker::check_ls_table_(
    const share::ObLSTable::Mode mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    ObLSReplicaMap replica_map;
    int64_t dangling_count = 0;  // replica only in ls meta table
    int64_t report_count = 0;    // replica not in/match ls meta table
    if (OB_FAIL(build_replica_map_(replica_map, mode))) {
      LOG_WARN("build replica map from ls table failed", KR(ret), K(mode));
    } else if (OB_FAIL(check_dangling_replicas_(replica_map, dangling_count))) {
      LOG_WARN("check replicas exist in ls table but not in local failed", KR(ret));
    } else if (OB_FAIL(check_report_replicas_(replica_map, report_count))) {
      LOG_WARN("check replicas not in/match ls table failed", KR(ret));
    } else if (dangling_count != 0 || report_count != 0) {
      LOG_INFO("checker found and corrected dangling or to report replicas for ls meta table",
        KR(ret), K_(tenant_id), K(dangling_count), K(report_count), K_(ls_checker_tg_id));
    }
    LOG_TRACE("finish checking ls table", KR(ret), K_(tenant_id),
        K(dangling_count), K(report_count), K_(ls_checker_tg_id),
        K(start_time), "cost_time", ObTimeUtility::current_time() - start_time);
  }
  return ret;
}

int ObTenantMetaChecker::check_tablet_table()
{
  int ret = OB_SUCCESS;
  int64_t dangling_count = 0;  // replica only in tablet meta table
  int64_t report_count = 0;  // replica not in/match tablet meta table
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    ObTabletReplicaMap replica_map;
    if (OB_FAIL(build_replica_map_(replica_map))) {
      LOG_WARN("build replica map from tablet table failed", KR(ret));
    } else if (OB_FAIL(check_dangling_replicas_(replica_map, dangling_count))) {
      LOG_WARN("check replicas exist in tablet table but not in local failed", KR(ret));
    } else if (OB_FAIL(check_report_replicas_(replica_map, report_count))) {
      LOG_WARN("check replicas not in/match tablet table failed", KR(ret));
    } else if (dangling_count != 0 || report_count != 0) {
      LOG_INFO("checker found and corrected dangling or to report replicas for tablet meta table",
        KR(ret), K_(tenant_id), K(dangling_count), K(report_count), K_(tablet_checker_tg_id));
    }
    LOG_TRACE("finish checking tablet table", KR(ret), K_(tenant_id),
        K(dangling_count), K(report_count), K_(tablet_checker_tg_id),
        K(start_time), "cost_time", ObTimeUtility::current_time() - start_time);
  }
  return ret;
}

int ObTenantMetaChecker::schedule_ls_meta_check_task()
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_INTERVAL = GCONF.ls_meta_table_check_interval;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(
      ls_checker_tg_id_,
      ls_meta_check_task_,
      CHECK_INTERVAL,
      false/*repeat*/))) {
    LOG_WARN("TG_SCHEDULE ls meta check task failed", KR(ret), K_(ls_checker_tg_id), K(CHECK_INTERVAL));
  } else {
    LOG_TRACE("schedule ls meta check task success", K_(tenant_id), K_(ls_checker_tg_id));
  }
  return ret;
}

int ObTenantMetaChecker::schedule_tablet_meta_check_task()
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_INTERVAL = GCONF.tablet_meta_table_check_interval;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(
      tablet_checker_tg_id_,
      tablet_meta_check_task_,
      CHECK_INTERVAL,
      false/*repeat*/))) {
    LOG_WARN("TG_SCHEDULE tablet meta check task failed",
        KR(ret), K_(tablet_checker_tg_id), K(CHECK_INTERVAL));
  } else {
    LOG_TRACE("schedule tablet meta check task success", K_(tenant_id), K_(tablet_checker_tg_id));
  }
  return ret;
}

int ObTenantMetaChecker::build_replica_map_(
    ObLSReplicaMap &replica_map,
    const share::ObLSTable::Mode mode)
{
  int ret = OB_SUCCESS;
  ObLSTableIterator lst_iter;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
  } else if (OB_FAIL(replica_map.create(
      hash::cal_next_prime(LS_REPLICA_MAP_BUCKET_NUM),
      "LSCheckMap"))) {
    LOG_WARN("fail to create replica_map", KR(ret));
  } else if (OB_FAIL(lst_iter.init(*lst_operator_, tenant_id_, mode))) {
    LOG_WARN("fail to init ls meta table iter", KR(ret), K_(tenant_id), K(mode));
  } else if (OB_FAIL(lst_iter.get_filters().set_reserved_server(GCONF.self_addr_))) {
    LOG_WARN("fail to set server for filter", KR(ret), "server", GCONF.self_addr_);
  } else {
    ObLSInfo ls_info;
    while (OB_SUCC(ret)) {
      ls_info.reset();
      if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
      } else if (OB_FAIL(lst_iter.next(ls_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("ls table iterator next failed", KR(ret));
        }
      } else if (0 == ls_info.replica_count()) {
        continue;
      } else if (1 == ls_info.replica_count()) {
        const ObLSReplica &replica = ls_info.get_replicas().at(0);
        if (OB_FAIL(replica_map.set_refactored(replica.get_ls_id(), replica))) {
          LOG_WARN("fail to set_refactored", KR(ret), K(replica));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls_info should have one local replica at most", KR(ret), K(ls_info));
      }
    } // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTenantMetaChecker::build_replica_map_(ObTabletReplicaMap &replica_map)
{
  int ret = OB_SUCCESS;
  ObTenantTabletTableIterator tt_iter;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(tt_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
  } else if (OB_FAIL(replica_map.create(
      hash::cal_next_prime(TABLET_REPLICA_MAP_BUCKET_NUM),
      "TabletCheckMap"))) {
    LOG_WARN("fail to create replica_map", KR(ret));
  } else if (OB_FAIL(tt_iter.init(*tt_operator_, tenant_id_))) {
    LOG_WARN("fail to init tablet meta table iter", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(tt_iter.get_filters().set_reserved_server(GCONF.self_addr_))) {
    LOG_WARN("fail to set server for filter", KR(ret), "server", GCONF.self_addr_);
  } else {
    ObTabletInfo tablet_info;
    while (OB_SUCC(ret)) {
      tablet_info.reset();
      if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
      } else if (OB_FAIL(tt_iter.next(tablet_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("tablet table iterator next failed", KR(ret));
        }
      } else if (0 == tablet_info.replica_count()) {
        continue;
      } else if (1 == tablet_info.replica_count()) {
        const ObTabletReplica &replica = tablet_info.get_replicas().at(0);
        if (OB_FAIL(replica_map.set_refactored(
            ObTabletLSPair(replica.get_tablet_id(), replica.get_ls_id()),
            replica))) {
          LOG_WARN("fail to set_refactored", KR(ret), K(replica));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_info should have one local replica at most", KR(ret), K(tablet_info));
      }
    } // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTenantMetaChecker::check_dangling_replicas_(
    ObLSReplicaMap &replica_map,
    int64_t &dangling_count)
{
  int ret = OB_SUCCESS;
  dangling_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
  } else {
    FOREACH_X(it, replica_map, OB_SUCC(ret)) {
      ObLSHandle ls_handle;
      if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(
          it->first,
          ls_handle,
          ObLSGetMod::OBSERVER_MOD))) {
        if (OB_LS_NOT_EXIST == ret) { // not exist in local, remove it from table
          ret = OB_SUCCESS;
          ++dangling_count;
          if (OB_ISNULL(GCTX.ob_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ob_service is null", KR(ret));
          } else if (OB_FAIL(GCTX.ob_service_->submit_ls_update_task(tenant_id_, it->first))) {
            LOG_WARN("fail to submit ls update task", KR(ret), K_(tenant_id), "ls_id", it->first);
          } else {
            LOG_INFO("add async task to remove replica from ls table",
                K_(tenant_id), "replica", it->second);
          }
        } else {
          LOG_WARN("get ls handle failed", KR(ret), "ls_id", it->first);
        }
      } else if (OB_ISNULL(ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls_handle.get_ls() is nullptr", KR(ret));
      }
    } // end for
  }
  return ret;
}

int ObTenantMetaChecker::check_dangling_replicas_(
    ObTabletReplicaMap &replica_map,
    int64_t &dangling_count)
{
  int ret = OB_SUCCESS;
  dangling_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service is null", KR(ret));
  } else {
    bool not_exist = false;
    FOREACH_X(it, replica_map, OB_SUCC(ret)) {
      const ObLSID &ls_id = it->first.get_ls_id();
      const ObTabletID &tablet_id = it->first.get_tablet_id();
      if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
      } else if (OB_FAIL(check_tablet_not_exist_in_local_(ls_id, tablet_id, not_exist))) {
        LOG_WARN("fail to check tablet whether exist in local", KR(ret), K(ls_id), K(tablet_id));
      } else if (not_exist) {
        ++dangling_count;
        if (OB_FAIL(GCTX.ob_service_->submit_tablet_update_task(tenant_id_, ls_id, tablet_id))) {
          LOG_WARN("fail to submit tablet update task",
              KR(ret), K_(tenant_id), K(ls_id), K(tablet_id));
        } else {
          LOG_INFO("add async task to remove replica from tablet table",
              K_(tenant_id), "replica", it->second);
        }
      }
    } // end for
  }
  return ret;
}

int ObTenantMetaChecker::check_tablet_not_exist_in_local_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    bool &not_exist)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  not_exist = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id), K(tablet_id));
  } else if (tablet_id.is_reserved_tablet()) {
    // skip reserved tablet
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(
      ls_id,
      ls_handle,
      ObLSGetMod::OBSERVER_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      not_exist = true;
    } else {
      LOG_WARN("fail to get sys_ls handle", KR(ret));
    }
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle of sys ls is null", KR(ret));
  } else if (OB_ISNULL(ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet_svr of sys ls is null", KR(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      not_exist = true;
    } else {
      LOG_WARN("fail to get tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObTenantMetaChecker::check_report_replicas_(
    ObLSReplicaMap &replica_map,
    int64_t &report_count)
{
  int ret = OB_SUCCESS;
  report_count = 0;
  ObSharedGuard<ObLSIterator> ls_iter;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service is null", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(
      ls_iter,
      ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls iter", KR(ret));
  } else {
    ObLS *ls = NULL;
    while(OB_SUCC(ret)) {
      if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(ls_checker_tg_id));
      } else if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("scan next ls failed.", KR(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ls", KR(ret));
      } else {
        const ObLSID &ls_id = ls->get_ls_id();
        ObLSReplica table_replica; // replica from meta table
        ObLSReplica local_replica; // replica from local
        if (OB_FAIL(replica_map.get_refactored(ls_id, table_replica))) {
          if (OB_HASH_NOT_EXIST == ret) { // not exist in table while exist in local
            ret = OB_SUCCESS;
            if (OB_FAIL(GCTX.ob_service_->submit_ls_update_task(tenant_id_, ls_id))) {
              LOG_WARN("fail to submit ls update task", KR(ret), K_(tenant_id), K(ls_id));
            } else {
              ++report_count;
              LOG_INFO("add missing replica to ls meta table success",
                  KR(ret), K_(tenant_id), K(ls_id));
            }
          } else {
            LOG_WARN("get replica from hashmap failed", KR(ret), K_(tenant_id), K(ls_id));
          }
        } else if (OB_FAIL(GCTX.ob_service_->fill_ls_replica(
            tenant_id_,
            ls_id,
            local_replica))) {
          LOG_WARN("fail to fill ls replica", KR(ret), K_(tenant_id), K(ls_id));
        } else if (table_replica.is_equal_for_report(local_replica)) {
          continue;
        } else { // not equal
          if (OB_FAIL(GCTX.ob_service_->submit_ls_update_task(tenant_id_, ls_id))) {
            LOG_WARN("fail to submit ls update task", KR(ret), K_(tenant_id), K(ls_id));
          } else {
            ++report_count;
            LOG_INFO("modify replica success", KR(ret), K(local_replica), K(table_replica));
          }
        }
      }
    } // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTenantMetaChecker::check_report_replicas_(
    ObTabletReplicaMap &replica_map,
    int64_t &report_count)
{
  int ret = OB_SUCCESS;
  report_count = 0;
  ObSharedGuard<ObLSIterator> ls_iter;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ob_service is null", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(
      ls_iter,
      ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls iter", KR(ret));
  } else {
    ObLS *ls = NULL;
    ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_READABLE_COMMITED);
    while(OB_SUCC(ret)) {
      if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObTenantMetaChecker is stopped", KR(ret), K_(tenant_id), K_(tablet_checker_tg_id));
      } else if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("scan next ls failed.", KR(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ls", KR(ret));
      } else if (OB_ISNULL(ls->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablet svr", KR(ret));
      } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
        LOG_WARN("failed to build ls tablet iter", KR(ret));
      } else {
        ObTabletHandle tablet_handle;
        ObTabletID tablet_id;
        ObTabletReplica local_replica; // replica from local
        ObTabletReplica table_replica; // replica from meta table
        share::ObTabletReplicaChecksumItem tablet_checksum; // TODO(@donglou.zl) check tablet_replica_checksum
        const bool need_checksum = false;
        const ObLSID &ls_id = ls->get_ls_id();
        while (OB_SUCC(ret)) {
          if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next tablet", KR(ret));
            }
          } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle));
          } else if (FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_)) {
          } else if (tablet_id.is_reserved_tablet()) {
            continue;
          } else if (OB_FAIL(replica_map.get_refactored(
              ObTabletLSPair(tablet_id, ls_id),
              table_replica))) {
            if (OB_HASH_NOT_EXIST == ret) { // not exist in table while exist in local
              ret = OB_SUCCESS;
              if (OB_FAIL(GCTX.ob_service_->submit_tablet_update_task(tenant_id_, ls_id, tablet_id))) {
                LOG_WARN("fail to submit tablet update task",
                    KR(ret), K_(tenant_id), K(ls_id), K(tablet_id));
              } else {
                ++report_count;
                LOG_INFO("add missing replica to tablet meta table success",
                    KR(ret), K_(tenant_id), K(ls_id), K(tablet_id));
              }
            } else {
              LOG_WARN("get replica from hashmap failed",
                  KR(ret), K_(tenant_id), K(ls_id), K(tablet_id));
            }
          } else if (OB_FAIL(GCTX.ob_service_->fill_tablet_report_info(
              tenant_id_,
              ls_id,
              tablet_id,
              local_replica,
              tablet_checksum,
              need_checksum))) {
            LOG_WARN("fail to fill tablet replica", KR(ret), K_(tenant_id), K(ls_id), K(tablet_id));
          } else if (table_replica.is_equal_for_report(local_replica)) {
            continue;
          } else { // not equal
            if (OB_FAIL(GCTX.ob_service_->submit_tablet_update_task(tenant_id_, ls_id, tablet_id))) {
              LOG_WARN("fail to submit tablet update task",
                  KR(ret), K_(tenant_id), K(ls_id), K(tablet_id));
            } else {
              ++report_count;
              LOG_INFO("modify replica success", KR(ret), K(local_replica), K(table_replica));
            }
          }
        } // end while for tablet_iter
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    } // end while for ls_iter
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase
