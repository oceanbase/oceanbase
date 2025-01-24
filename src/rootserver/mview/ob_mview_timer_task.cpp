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

#define USING_LOG_PREFIX RS

#include "rootserver/mview/ob_mview_timer_task.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/ob_global_stat_proxy.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;

int ObMViewTimerTask::schedule_task(const int64_t delay, bool repeate, bool immediate)
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, delay, repeate, immediate))) {
    LOG_WARN("fail to schedule mview timer task", KR(ret), KP(this), K(delay), K(repeate),
             K(immediate));
  }
  return ret;
}

void ObMViewTimerTask::cancel_task()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
  }
}

void ObMViewTimerTask::wait_task()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer *);
  if (OB_ISNULL(timer)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("shared timer is NULL", KR(ret));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
  }
}

int ObMViewTimerTask::need_schedule_major_refresh_mv_task(const uint64_t tenant_id,
                                                          bool &need_schedule)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  bool contains_major_refresh_mview = false;
  share::ObSnapshotTableProxy snapshot_proxy;
  need_schedule = false;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_4_0)) {
  } else if (tenant_id == OB_SYS_TENANT_ID) {
    // skip sys tenant
  } else if (OB_UNLIKELY(OB_ISNULL(sql_proxy))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(snapshot_proxy.check_snapshot_exist(*sql_proxy, tenant_id,
                                                         share::SNAPSHOT_FOR_MAJOR_REFRESH_MV,
                                                         contains_major_refresh_mview))) {
    LOG_WARN("fail to check if tenant contains major refresh snapshot", KR(ret), K(tenant_id));
  } else if (contains_major_refresh_mview) {
    need_schedule = true;
  }

  return ret;
}

int ObMViewTimerTask::need_push_major_mv_merge_scn(const uint64_t tenant_id,
                                                   bool &need_push,
                                                   share::SCN &lastest_merge_scn,
                                                   share::SCN &major_mv_merge_scn)
{
  int ret = OB_SUCCESS;
  need_push = false;
  bool need_schedule = false;
  ObGlobalStatProxy global_proxy(*GCTX.sql_proxy_, tenant_id);
  compaction::ObTenantTabletScheduler* tablet_scheduler = MTL(compaction::ObTenantTabletScheduler*);

  if (OB_FAIL(need_schedule_major_refresh_mv_task(tenant_id, need_schedule))) {
    LOG_WARN("failed to check need schedule", KR(ret), K(tenant_id));
  } else if (!need_schedule) {
    // do nothing
  } else if (OB_ISNULL(tablet_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj is null", KR(ret), KP(tablet_scheduler));
  } else if (OB_FAIL(lastest_merge_scn.convert_for_gts(tablet_scheduler->get_inner_table_merged_scn()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert_for_gts", KR(ret));
  } else if (OB_FAIL(global_proxy.get_major_refresh_mv_merge_scn(false /*select for update*/,
                                                                 major_mv_merge_scn))) {
    LOG_WARN("fail to get major_refresh_mv_merge_scn", KR(ret), K(tenant_id));
  }

  if (OB_FAIL(ret)) {
  } else if (!need_schedule) {
  } else if (!lastest_merge_scn.is_valid() || !major_mv_merge_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_scn is not valid", KR(ret), K(lastest_merge_scn), K(major_mv_merge_scn));
  } else if (lastest_merge_scn < major_mv_merge_scn) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lastest merge_scn less than major_mv_merge_scn", KR(ret), K(lastest_merge_scn), K(major_mv_merge_scn));
  } else if (lastest_merge_scn == major_mv_merge_scn) {
  } else {
    need_push = true;
  }
  return ret;
}

int ObMViewTimerTask::check_mview_last_refresh_scn(const uint64_t tenant_id,
                                                   const share::SCN &tenant_mv_merge_scn)
{
  int ret = OB_SUCCESS;
  share::SCN last_refresh_scn = share::SCN::min_scn();
  ObSqlString sql;
  const int64_t refresh_mode = (int64_t)ObMVRefreshMode::MAJOR_COMPACTION;   // new mview mode
  const int64_t last_refresh_type = (int64_t)ObMVRefreshType::FAST; // inc refresh type
  const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy should not null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (!is_valid_tenant_id(user_tenant_id) || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(user_tenant_id), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT MIN(LAST_REFRESH_SCN) AS MIN_LAST_REFRESH_SCN FROM \
                                     `%s`.`%s` \
                                     WHERE (REFRESH_MODE = %ld) AND (LAST_REFRESH_TYPE = %ld)",
                                     OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_TNAME,
                                     refresh_mode, last_refresh_type))) {
    LOG_WARN("fail to format sql", KR(ret), K(sql), K(user_tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *mysql_result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res,
                                        user_tenant_id,
                                        sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret), K(sql), K(user_tenant_id));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), KP(mysql_result));
      } else if (OB_FAIL(mysql_result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next", K(ret), KP(mysql_result));
        }
      } else {
        uint64_t min_last_refresh_scn = 0;
        EXTRACT_UINT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
        *mysql_result, "MIN_LAST_REFRESH_SCN", min_last_refresh_scn, uint64_t,
        true/*skip null*/, false /*skip column*/, 0/*default value*/);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(last_refresh_scn.convert_for_inner_table_field(min_last_refresh_scn))){
          LOG_WARN("failed to convert for inner table filed", K(ret),
                    K(min_last_refresh_scn), K(last_refresh_scn));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (last_refresh_scn.is_min() || last_refresh_scn.is_base_scn()) {
      // major mv merge scn is zero or one
    } else if (last_refresh_scn == tenant_mv_merge_scn) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mv min last_refresh_scn not equal to major_mv_merge_scn",
                K(ret), K(tenant_id), K(tenant_mv_merge_scn), K(last_refresh_scn));
    }
    LOG_INFO("get mview last refresh scn", KR(ret), K(sql), K(user_tenant_id),
              K(tenant_mv_merge_scn), K(last_refresh_scn));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
