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

#define USING_LOG_PREFIX SHARE

#include "observer/ob_srv_network_frame.h"
#include "share/ash/ob_active_sess_hist_task.h"
#include "share/ash/ob_active_sess_hist_list.h"
#include "share/ash/ob_ash_refresh_task.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "observer/omt/ob_th_worker.h"
#include "deps/oblib/src/rpc/obmysql/ob_mysql_packet.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "lib/stat/ob_diagnostic_info_container.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

// a sample would be taken place up to 20ms after ash iteration begins.
// if sample time is above this threshold, mean ash execution too slow
constexpr int64_t ash_iteration_time = 40000;   // 40ms

#define GET_OTHER_TSI_ADDR(var_name, addr) \
const int64_t var_name##_offset = ((int64_t)addr - (int64_t)pthread_self()); \
decltype(*addr) var_name = *(decltype(addr))(thread_base + var_name##_offset);

ObActiveSessHistTask &ObActiveSessHistTask::get_instance()
{
  static ObActiveSessHistTask the_one;
  return the_one;
}

int ObActiveSessHistTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }
  return ret;
}
const static int64_t REFRESH_INTERVAL = 1 * 1000L * 1000L;
int ObActiveSessHistTask::start()
{
  int ret = OB_SUCCESS;
  // refresh sess info every 1 second
  if (OB_FAIL(TG_START(lib::TGDefIDs::ActiveSessHist))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ActiveSessHist,
                                 *this,
                                 REFRESH_INTERVAL,
                                 false /* repeat */))) {
    LOG_WARN("fail define timer schedule", K(ret));
  } else if (OB_FAIL(ObAshRefreshTask::get_instance().start())) {
    LOG_WARN("failed to start ash refresh task", K(ret));
  } else {
    LOG_INFO("ASH init OK");
  }
  return ret;
}

void ObActiveSessHistTask::wait()
{
  TG_WAIT(lib::TGDefIDs::ActiveSessHist);
}

void ObActiveSessHistTask::stop()
{
  TG_STOP(lib::TGDefIDs::ActiveSessHist);
}

void ObActiveSessHistTask::destroy()
{
  TG_DESTROY(lib::TGDefIDs::ActiveSessHist);
}
ERRSIM_POINT_DEF(OB_ASH_SCHEDULE_TIME);
void ObActiveSessHistTask::runTimerTask()
{
  common::ObTimeGuard time_guard(__func__, ash_iteration_time);
  int64_t current_time = ObTimeUtility::current_time();
  common::ObBKGDSessInActiveGuard inactive_guard;
  int ret = OB_SUCCESS;
  ObActiveSessHistList &ash_list = ObActiveSessHistList::get_instance();
  ash_list.lock();
  if (true == GCONF._ob_ash_enable) {
    WR_STAT_GUARD(ASH_SCHEDULAR);
    sample_time_ = ObTimeUtility::current_time();
    tsc_sample_time_ = rdtsc();
    int64_t begin_write_pos = ash_list.write_pos();
    ash_list.check_if_need_compress();

    std::function<bool(const SessionID&, ObDiagnosticInfo *)> fn = std::bind(&ObActiveSessHistTask::process_running_di, this, std::placeholders::_1, std::placeholders::_2);

    common::ObVector<uint64_t> ids;
    GCTX.omt_->get_tenant_ids(ids);
    for (int64_t i = 0; i < ids.size(); ++i) {
      uint64_t tenant_id = ids[i];
      if (!is_virtual_tenant_id(tenant_id)) {
        MTL_SWITCH(tenant_id)
        {
          if (MTL(ObDiagnosticInfoContainer *)->is_inited() && OB_FAIL(MTL(ObDiagnosticInfoContainer *)->for_each_running_di(fn))) {
            LOG_WARN("fail to process tenant ash stat, prceed anyway", K(ret), K(tenant_id));
            ret = OB_SUCCESS;
          } else if (ash_list.need_compress()) {
            MTL(ObDiagnosticInfoContainer *)->copy_wait_request_to_ash_buffer_and_reset(tenant_id);
          }
        } else {
          LOG_WARN("failed to switch to current tenant, prceed anyway", K(ret), K(tenant_id));
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_FAIL(ObDiagnosticInfoContainer::get_global_di_container()->for_each_running_di(fn))) {
      LOG_WARN("failed to get global diagnostic info", K(ret), KPC(ObDiagnosticInfoContainer::get_global_di_container()));
    } else if (ash_list.need_compress()) {
      ObDiagnosticInfoContainer::get_global_di_container()->copy_wait_request_to_ash_buffer_and_reset(OB_SYS_TENANT_ID);
    }
    int64_t write_num = ash_list.write_pos() - begin_write_pos;
    ash_list.set_current_write_num(write_num);
    ash_list.check_if_can_reset_compress_flag();
    ash_list.reset_compress_num();
  }
  ash_list.unlock();
  int64_t duration = ObTimeUtility::current_time() - current_time;
  int64_t next_schedule_time = REFRESH_INTERVAL - duration;
  if (next_schedule_time < 0) {
    next_schedule_time = 0;
  }
  if (OB_ASH_SCHEDULE_TIME) {
    next_schedule_time = -OB_ASH_SCHEDULE_TIME - 1;
    LOG_INFO("ash simulate schedule time", K(next_schedule_time));
  }
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ActiveSessHist,
                                 *this,
                                 next_schedule_time,
                                 false /* repeat */))) {
    // TG_SCHEDULE only fails when timer task queue is full.
    // But in ASH sample thread, there is only one task. So it would never happen.
    LOG_ERROR("fail to schedule next ASH timer task", K(ret), K(duration));
  }
}

bool ObActiveSessHistTask::process_running_di(const SessionID &session_id, ObDiagnosticInfo *di)
{
  if (di->is_active_session() ||
      ObWaitEventIds::NETWORK_QUEUE_WAIT == di->get_ash_stat().event_no_ ||
      di->get_ash_stat().is_in_row_lock_wait()) {
    ObActiveSessHistList &ash_list = ObActiveSessHistList::get_instance();
    if (OB_UNLIKELY(ash_list.need_compress() &&
                    ObWaitEventIds::NETWORK_QUEUE_WAIT == di->get_ash_stat().event_no_)) {
      MTL(ObDiagnosticInfoContainer *)->calculate_wait_in_request_queue(di);
    } else {
      di->get_ash_stat().sample_time_ = sample_time_;
      ObActiveSessionStat::calc_db_time(di, sample_time_, tsc_sample_time_);
      ObActiveSessionStat::calc_retry_wait_event(di->get_ash_stat(), sample_time_);
      ObActiveSessionStat::cal_delta_io_data(di);
      ash_list.add(di->get_ash_stat());
    }
  } else {
    // inactive session
// #ifdef ENABLE_DEBUG_LOG
//     if (di->get_ash_stat().is_in_row_lock_wait()) {
//       LOG_ERROR_RET(OB_ERR_UNEXPECTED, "inactive session enter row lock conflict stat", KPC(di));
//     }
// #endif
  }

  return true;
}