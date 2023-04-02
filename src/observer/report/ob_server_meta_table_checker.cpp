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

#include "observer/report/ob_server_meta_table_checker.h"
#include "observer/ob_server_struct.h" // GCTX
#include "share/ob_thread_define.h" // ServerMetaChecker
#include "share/ls/ob_ls_table_operator.h" // ObLSTableOperator
#include "share/tablet/ob_tablet_table_operator.h" // ObTabletTableOperator
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "observer/omt/ob_multi_tenant.h" // ObMultiTenant
#include "share/tablet/ob_tablet_info.h" // ObTabletInfo
#include "share/ob_tablet_replica_checksum_operator.h" // for ObTabletReplicaChecksumItem
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMySQLTransaction

namespace oceanbase
{
using namespace share;
using namespace common;

namespace observer
{
ObServerLSMetaTableCheckTask::ObServerLSMetaTableCheckTask(
    ObServerMetaTableChecker &checker)
    : checker_(checker)
{
}

void ObServerLSMetaTableCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checker_.check_meta_table(
      ObServerMetaTableChecker::ObMetaTableCheckType::CHECK_LS_META_TABLE))) {
    LOG_WARN("fail to check ls meta table", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(checker_.schedule_ls_meta_check_task())) {
    LOG_WARN("fail to schedule ls meta check task", KR(ret));
  }
}

ObServerTabletMetaTableCheckTask::ObServerTabletMetaTableCheckTask(
    ObServerMetaTableChecker &checker)
    : checker_(checker)
{
}

void ObServerTabletMetaTableCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checker_.check_meta_table(
      ObServerMetaTableChecker::ObMetaTableCheckType::CHECK_TABLET_META_TABLE))) {
    LOG_WARN("fail to check tablet meta table", KR(ret));
  }
  // ignore ret
  if (OB_FAIL(checker_.schedule_tablet_meta_check_task())) {
    LOG_WARN("fail to schedule tablet meta check task", KR(ret));
  }
}

ObServerMetaTableChecker::ObServerMetaTableChecker()
    : inited_(false),
      stopped_(true),
      ls_tg_id_(OB_INVALID_INDEX),
      tablet_tg_id_(OB_INVALID_INDEX),
      ls_meta_check_task_(*this),
      tablet_meta_check_task_(*this),
      lst_operator_(NULL),
      tt_operator_(NULL),
      omt_(NULL),
      schema_service_(NULL)
{
}

int ObServerMetaTableChecker::init(
    share::ObLSTableOperator *lst_operator,
    share::ObTabletTableOperator *tt_operator,
    omt::ObMultiTenant *omt,
    share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(lst_operator)
      || OB_ISNULL(tt_operator)
      || OB_ISNULL(omt)
      || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::ServerMetaChecker, ls_tg_id_))) {
    LOG_WARN("TG_CREATE ls_tg_id_ failed", KR(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::ServerMetaChecker, tablet_tg_id_))) {
    LOG_WARN("TG_CREATE tablet_tg_id_ failed", KR(ret));
  } else {
    lst_operator_ = lst_operator;
    tt_operator_ = tt_operator;
    omt_ = omt;
    schema_service_ = schema_service;
    inited_ = true;
  }
  return ret;
}

int ObServerMetaTableChecker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    stopped_ = false;
    if (OB_FAIL(TG_START(ls_tg_id_))) {
      LOG_WARN("TG_START ls_tg_id_ failed", KR(ret), K_(ls_tg_id));
    } else if (OB_FAIL(TG_START(tablet_tg_id_))) {
      LOG_WARN("TG_START tablet_tg_id_ failed", KR(ret), K_(tablet_tg_id));
    } else if (OB_FAIL(schedule_ls_meta_check_task())) {
      LOG_WARN("schedule ls meta check task failed", KR(ret), K_(ls_tg_id));
    } else if (OB_FAIL(schedule_tablet_meta_check_task())) {
      LOG_WARN("schedule tablet meta check task failed", KR(ret), K_(tablet_tg_id));
    } else {
      LOG_INFO("ObServerMetaTableChecker start success", K_(ls_tg_id), K_(tablet_tg_id));
    }
  }
  return ret;
}

void ObServerMetaTableChecker::stop()
{
  if (OB_LIKELY(inited_)) {
    stopped_ = true;
    TG_STOP(ls_tg_id_);
    TG_STOP(tablet_tg_id_);
    LOG_INFO("ObServerMetaTableChecker stop finished", K_(ls_tg_id), K_(tablet_tg_id));
  }
}

void ObServerMetaTableChecker::wait()
{
  if (OB_LIKELY(inited_)) {
    TG_WAIT(ls_tg_id_);
    TG_WAIT(tablet_tg_id_);
    LOG_INFO("ObServerMetaTableChecker wait finished", K_(ls_tg_id), K_(tablet_tg_id));
  }
}

void ObServerMetaTableChecker::destroy()
{
  if (OB_LIKELY(inited_)) {
    lst_operator_ = nullptr;
    tt_operator_ = nullptr;
    omt_ = nullptr;
    schema_service_ = nullptr;
    inited_ = false;
    stopped_ = true;
    TG_DESTROY(ls_tg_id_);
    TG_DESTROY(tablet_tg_id_);
    LOG_INFO("ObServerMetaTableChecker destroy finished", K_(ls_tg_id), K_(tablet_tg_id));
  }
}

int ObServerMetaTableChecker::check_meta_table(const ObMetaTableCheckType check_type)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> schema_tenant_ids;
  ObArray<uint64_t> omt_tenant_ids;
  ObArray<uint64_t> nonlocal_tenant_ids;
  if (OB_UNLIKELY(!inited_)
      || OB_ISNULL(omt_)
      || OB_ISNULL(schema_service_)
      || OB_ISNULL(tt_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerMetaTableChecker is stopped", KR(ret), K_(ls_tg_id), K_(tablet_tg_id));
  } else if (OB_UNLIKELY(CHECK_LS_META_TABLE != check_type && CHECK_TABLET_META_TABLE != check_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid check type", KR(ret), K(check_type), K_(ls_tg_id), K_(tablet_tg_id));
  } else if (OB_FAIL(schema_service_->get_tenant_ids(schema_tenant_ids))) {
    LOG_WARN("fail to get tenant ids from schema", KR(ret));
  } else if (OB_FAIL(omt_->get_mtl_tenant_ids(omt_tenant_ids))) {
    LOG_WARN("fail to get tenant_ids from omt", KR(ret));
  } else if (OB_FAIL(get_difference(schema_tenant_ids, omt_tenant_ids, nonlocal_tenant_ids))) {
    LOG_WARN("fail to get difference from schema_tenant_ids and omt_tenant_ids",
        KR(ret), K(schema_tenant_ids), K(omt_tenant_ids));
  } else {
    ARRAY_FOREACH_NORET(nonlocal_tenant_ids, idx) { // ignore ret between each tenant
      int64_t ls_residual_count = 0;
      int64_t meta_residual_count = 0;
      int64_t checksum_residual_count = 0;
      const uint64_t tenant_id = nonlocal_tenant_ids.at(idx);
      if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
          || is_virtual_tenant_id(tenant_id))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
      } else if (is_sys_tenant(tenant_id)) {
        // skip hidden sys_tenant, residual ls/tablet will be processed by ObTenantMetaChecker
      } else if (CHECK_LS_META_TABLE == check_type) {
        if (OB_FAIL(check_ls_table_(tenant_id, ls_residual_count))) {
          LOG_WARN("fail to check ls meta table", KR(ret), K(tenant_id));
        } else if (ls_residual_count != 0) {
          LOG_INFO("ObServerMetaTableChecker found residual ls and corrected ls meta table for a tenant",
              KR(ret), K(tenant_id), K(ls_residual_count));
        }
      } else if (CHECK_TABLET_META_TABLE == check_type) {
        if (OB_FAIL(check_tablet_table_(tenant_id, meta_residual_count, checksum_residual_count))) {
          LOG_WARN("fail to check tablet meta table", KR(ret), K(tenant_id));
        } else if ((0 != meta_residual_count) || (0 != checksum_residual_count)) {
          LOG_INFO("ObServerMetaTableChecker found residual tablet, and corrected tablet"
            " meta table and tablet replica checksum table for a tenant", KR(ret), K(tenant_id),
            K(meta_residual_count), K(checksum_residual_count));
        }
      } else { // can't be here
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid check type", KR(ret), K(check_type));
      }
    }
    LOG_TRACE("ObServerMetaTableChecker finish check meta table", KR(ret), K(nonlocal_tenant_ids));
  }
  return ret;
}

int ObServerMetaTableChecker::check_ls_table_(
    const uint64_t tenant_id,
    int64_t &residual_count)
{
  int ret = OB_SUCCESS;
  residual_count  = 0;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(lst_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerMetaTableChecker is stopped", KR(ret), K_(ls_tg_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(is_sys_tenant(tenant_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support check sys tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(lst_operator_->remove_residual_ls(
      tenant_id,
      GCONF.self_addr_,
      residual_count))) {
    LOG_WARN("fail to remove residual ls by operator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObServerMetaTableChecker::check_tablet_table_(
    const uint64_t tenant_id,
    int64_t &meta_residual_count,
    int64_t &checksum_residual_count)
{
  int ret = OB_SUCCESS;
  int trans_ret = OB_SUCCESS;
  meta_residual_count = 0;
  checksum_residual_count = 0;
  int64_t affected_rows_meta = 0;
  int64_t affected_rows_checksum = 0;
  const int64_t limit = 1024;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(tt_operator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerMetaTableChecker is stopped", KR(ret), K_(tablet_tg_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else {
    do {
      common::ObMySQLTransaction trans;
      const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
        LOG_WARN("fail to start transaction", KR(ret), K(tenant_id), K(meta_tenant_id));
      } else if (OB_UNLIKELY(stopped_)) {
        ret = OB_CANCELED;
        LOG_WARN("ObServerMetaTableChecker is stopped", KR(ret), K_(tablet_tg_id));
      } else if (OB_FAIL(tt_operator_->remove_residual_tablet(trans, tenant_id, GCONF.self_addr_,
                 limit, affected_rows_meta))) {
        LOG_WARN("fail to remove residual tablet by operator", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObTabletReplicaChecksumOperator::remove_residual_checksum(trans,
                 tenant_id, GCONF.self_addr_, limit, affected_rows_checksum))) {
        LOG_WARN("fail to remove residual checksum by operator", KR(ret), K(tenant_id));
      } else {
        meta_residual_count += affected_rows_meta;
        checksum_residual_count += affected_rows_checksum;
      }
      if (OB_UNLIKELY(affected_rows_meta != affected_rows_checksum)) {
        LOG_WARN("affected_rows_meta is not equal to affected_rows_checksum, may due to cluster"
          "upgrade", K(tenant_id), K(affected_rows_meta), K(affected_rows_checksum));
      }
      if (trans.is_started()) {
        trans_ret = trans.end(OB_SUCCESS == ret);
        if (OB_UNLIKELY(OB_SUCCESS != trans_ret)) {
          LOG_WARN("fail to end transaction", KR(trans_ret));
          ret = ((OB_SUCCESS == ret) ? trans_ret : ret);
        }
      }
    } while (OB_SUCC(ret) && ((limit == affected_rows_meta) || (limit == affected_rows_checksum)));
  }
  return ret;
}

int ObServerMetaTableChecker::schedule_ls_meta_check_task()
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_INTERVAL = GCONF.ls_meta_table_check_interval;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerMetaTableChecker is stopped", KR(ret), K_(ls_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(ls_tg_id_, ls_meta_check_task_, CHECK_INTERVAL, false/*repeat*/))) {
    LOG_WARN("TG_SCHEDULE ls meta check task failed", KR(ret), K_(ls_tg_id), K(CHECK_INTERVAL));
  } else {
    LOG_TRACE("schedule ls meta check task success", K_(ls_tg_id));
  }
  return ret;
}

int ObServerMetaTableChecker::schedule_tablet_meta_check_task()
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_INTERVAL = GCONF.tablet_meta_table_check_interval;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("ObServerMetaTableChecker is stopped", KR(ret), K_(tablet_tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tablet_tg_id_, tablet_meta_check_task_, CHECK_INTERVAL, false/*repeat*/))) {
    LOG_WARN("TG_SCHEDULE tablet meta check task failed", KR(ret), K_(tablet_tg_id), K(CHECK_INTERVAL));
  } else {
    LOG_TRACE("schedule tablet meta check task success", K_(tablet_tg_id));
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase
