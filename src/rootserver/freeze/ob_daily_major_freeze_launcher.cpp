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

#define USING_LOG_PREFIX RS

#include "rootserver/freeze/ob_daily_major_freeze_launcher.h"

#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "rootserver/freeze/ob_freeze_info_manager.h"
#include "share/ob_debug_sync.h"
#include "share/config/ob_server_config.h"
#include "share/ob_tablet_checksum_operator.h"
#include "observer/ob_srv_network_frame.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace share::schema;
namespace rootserver
{
ObDailyMajorFreezeLauncher::ObDailyMajorFreezeLauncher()
  : ObFreezeReentrantThread(),
    is_inited_(false),
    already_launch_(false),
    config_(nullptr),
    gc_freeze_info_last_timestamp_(0),
    freeze_info_mgr_(nullptr)
{
}

int ObDailyMajorFreezeLauncher::init(
    const uint64_t tenant_id,
    ObServerConfig &config,
    ObMySQLProxy &proxy,
    ObFreezeInfoManager &freeze_info_manager)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    config_ = &config;
    gc_freeze_info_last_timestamp_ = ObTimeUtility::current_time();
    freeze_info_mgr_ = &freeze_info_manager;
    sql_proxy_ = &proxy;
    already_launch_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::start()
{
  int ret = OB_SUCCESS;
  lib::Threads::set_run_wrapper(MTL_CTX());
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDailyMajorFreezeLauncher not init", KR(ret));
  } else if (OB_FAIL(create(MAJOR_FREEZE_LAUNCHER_THREAD_CNT, "MFLaunch"))) {
    LOG_WARN("fail to create major_freeze_launch thread", K_(tenant_id), KR(ret));
  } else if (OB_FAIL(ObRsReentrantThread::start())) {
    LOG_WARN("fail to start major_freeze_launch thread", K_(tenant_id), KR(ret));
  } else {
    LOG_INFO("ObDailyMajorFreezeLauncher start succ", K_(tenant_id));
  }
  return ret;
}

void ObDailyMajorFreezeLauncher::run3()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to run, not init", KR(ret));
  } else {
    LOG_INFO("start daily major_freeze_launcher", K_(tenant_id));
    ObThreadCondGuard guard(get_cond());

    while (!stop_) {
      update_last_run_timestamp();
      LOG_TRACE("run daily major freeze launcher", K_(tenant_id));

      if (OB_FAIL(try_launch_major_freeze())) {
        LOG_WARN("fail to try_launch_major_freeze", KR(ret), K_(tenant_id));
      }
      // ignore ret
      if (OB_FAIL(try_gc_freeze_info())) {
        LOG_WARN("fail to try_gc_freeze_info", KR(ret), K_(tenant_id));
      }
      // ignore ret
      if (OB_FAIL(try_gc_tablet_checksum())) {
        LOG_WARN("fail to try_gc_tablet_checksum", KR(ret), K_(tenant_id));
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_idle(LAUNCHER_INTERVAL_US, ret))) {
        LOG_WARN("fail to try_idle", KR(ret), KR(tmp_ret));
      }
    }
    LOG_INFO("daily major_freeze_launcher stopped", K_(tenant_id));
  }
}

int ObDailyMajorFreezeLauncher::try_launch_major_freeze()
{
  int ret = OB_SUCCESS;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is not valid", KR(ret), K_(tenant_id));
  } else if (tenant_config->major_freeze_duty_time.disable()) {
    LOG_INFO("major_freeze_duty_time is disabled, can not launch major freeze by duty", K_(tenant_id));
  } else {
    const int hour = tenant_config->major_freeze_duty_time.hour();
    const int minute = tenant_config->major_freeze_duty_time.minute();
    time_t cur_time = -1;
    time(&cur_time);
    struct tm human_time;
    struct tm *human_time_ptr = nullptr;
    if (nullptr == (human_time_ptr = (localtime_r(&cur_time, &human_time)))) {
      ret = OB_ERR_SYS;
      LOG_WARN("fail to get localtime", KR(ret), K(errno));
    } else if ((human_time_ptr->tm_hour == hour) && (human_time_ptr->tm_min == minute)) {
      if (!already_launch_) {
        do {
          ObMajorFreezeParam param;
          param.transport_ = GCTX.net_frame_->get_req_transport();
          if (OB_FAIL(param.add_freeze_info(tenant_id_))) {
            LOG_WARN("fail to push_back", KR(ret), K_(tenant_id));
          } else if (OB_FAIL(ObMajorFreezeHelper::major_freeze(param))) {
            LOG_WARN("fail to major freeze", K(param), KR(ret));
          } else {
            already_launch_ = true;
            LOG_INFO("launch major freeze by duty time", K_(tenant_id),
                     "duty_time", tenant_config->major_freeze_duty_time);
          }

          // launcher will retry when error code is OB_EAGAIN
          // maybe use a new err code is better(OB_MAJRO_FREEZE_EAGAIN)
          if (OB_EAGAIN == ret) {
            int64_t usleep_cnt = 0;
            update_last_run_timestamp();
            while (!stop_ && (usleep_cnt < MAJOR_FREEZE_RETRY_LIMIT)) {
              ++usleep_cnt;
              ob_usleep(MAJOR_FREEZE_RETRY_INTERVAL_US);
            }
            ret = OB_SUCCESS;
          }
        } while (!stop_ && (OB_EAGAIN == ret));
      } else {
        LOG_INFO("major_freeze has been already launched, no need to do again", K_(tenant_id));
      }
    } else {
      already_launch_ = false;
    }
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::try_gc_freeze_info()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if ((now - gc_freeze_info_last_timestamp_) < MODIFY_GC_INTERVAL) {
    // nothing
  } else if (OB_FAIL(freeze_info_mgr_->try_gc_freeze_info())) {
    LOG_WARN("fail to gc_freeze_info", KR(ret), K_(tenant_id));
  } else {
    gc_freeze_info_last_timestamp_ = now;
  }
  return ret;
}

int ObDailyMajorFreezeLauncher::try_gc_tablet_checksum()
{
  int ret = OB_SUCCESS;
  const int64_t MIN_RESERVED_COUNT = 8;
  int64_t now = ObTimeUtility::current_time();
  const static int64_t BATCH_DELETE_CNT = 2000;
  
  if (OB_UNLIKELY(IS_NOT_INIT || OB_ISNULL(sql_proxy_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    ObMySQLTransaction trans;
    SMART_VAR(ObArray<SCN>, all_compaction_scn) {
      if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
        LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(ObTabletChecksumOperator::load_all_compaction_scn(trans,
                tenant_id_, all_compaction_scn))) {
        LOG_WARN("fail to load all compaction scn", KR(ret), K_(tenant_id));
      } else if (all_compaction_scn.count() > MIN_RESERVED_COUNT) {
        const int64_t snapshot_ver_cnt = all_compaction_scn.count();
        const SCN &gc_snapshot_scn = all_compaction_scn.at(snapshot_ver_cnt - MIN_RESERVED_COUNT - 1);

        if (OB_FAIL(ObTabletChecksumOperator::delete_tablet_checksum_items(trans, tenant_id_, gc_snapshot_scn,
            BATCH_DELETE_CNT))) {
          LOG_WARN("fail to delete tablet checksum items", KR(ret), K_(tenant_id), K(gc_snapshot_scn));
        }
      }
    }

    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        ret = ((OB_SUCC(ret)) ? tmp_ret : ret);
        LOG_WARN("fail to end trans", "is_commit", OB_SUCCESS == ret, KR(tmp_ret));
      }
    }
  }
  return ret;
}

int64_t ObDailyMajorFreezeLauncher::get_schedule_interval() const
{
  int64_t schedule_interval = MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT;
  if (OB_UNLIKELY(LAUNCHER_INTERVAL_US > MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT)) {
    schedule_interval = LAUNCHER_INTERVAL_US;
  }
  return schedule_interval;
}

}//end namespace rootserver
}//end namespace oceanbase
