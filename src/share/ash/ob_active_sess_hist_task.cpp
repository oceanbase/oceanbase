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

#include "lib/oblog/ob_log.h"
#include "lib/thread/thread_mgr.h"
#include "share/ob_thread_mgr.h"
#include "share/ash/ob_active_sess_hist_task.h"
#include "share/ash/ob_active_sess_hist_list.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/time/ob_time_utility.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "observer/omt/ob_th_worker.h"

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

int ObActiveSessHistTask::start()
{
  int ret = OB_SUCCESS;
  // refresh sess info every 1 second
  const static int64_t REFRESH_INTERVAL = 1 * 1000L * 1000L;
  if (OB_FAIL(TG_START(lib::TGDefIDs::ActiveSessHist))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ActiveSessHist,
                                 *this,
                                 REFRESH_INTERVAL,
                                 true /* repeat */))) {
    LOG_WARN("fail define timer schedule", K(ret));
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

void ObActiveSessHistTask::runTimerTask()
{
  common::ObTimeGuard time_guard(__func__, ash_iteration_time);
  ObActiveSessHistList::get_instance().lock();
  if (OB_NOT_NULL(GCTX.session_mgr_) && (true == GCONF._ob_ash_enable)) {
    WR_STAT_GUARD(ASH_SCHEDULAR);
    // iter over session mgr
    sample_time_ = ObTimeUtility::current_time();
    GCTX.session_mgr_->for_each_session(*this);
    // iter over each thread
    StackMgr::Guard guard(g_stack_mgr);
    for (auto* header = *guard; OB_NOT_NULL(header); header = guard.next()) {
      auto* thread_base = (char*)(header->pth_);
      if (OB_NOT_NULL(thread_base)) {
        GET_OTHER_TSI_ADDR(tid, &get_tid_cache());
        {
          char path[64];
          IGNORE_RETURN snprintf(path, 64, "/proc/self/task/%ld", tid);
          if (-1 == access(path, F_OK)) {
            // thread not exist, may have exited.
            continue;
          }
        }

        if (GETTID() == tid) {
          // ASH sampling thread does not participate in the background thread collection of ASH
          continue;
        }

        GET_OTHER_TSI_ADDR(ash_stat, &ObActiveSessionGuard::thread_local_stat_);
        GET_OTHER_TSI_ADDR(serving_id, &omt::ObThWorker::serving_tenant_id_);
        ash_stat.program_[0] = '\0';
        ash_stat.module_[0] = '\0';
        // das remote execution is foreground session
        if (ash_stat.in_das_remote_exec_ == true) {
          GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
#ifdef NDEBUG
          sprintf(ash_stat.program_, "DAS REMOTE EXEC (%.*s)",
                                            OB_THREAD_NAME_BUF_LEN,
                                            thread_base + tname_offset);
#else
          sprintf(ash_stat.program_, "DAS REMOTE EXEC (%.*s)_%7ld",
                                            OB_THREAD_NAME_BUF_LEN,
                                            thread_base + tname_offset,
                                            ash_stat.tid_);
#endif
          ash_stat.sample_time_ = sample_time_;
          ObActiveSessionStat::calc_db_time(ash_stat, sample_time_);
          ObActiveSessHistList::get_instance().add(ash_stat);
        }

        // iter over each active background thread
        if (0 == serving_id && ash_stat.is_bkgd_active_ ) {
          if ( '\0' == ash_stat.program_[0]) {
            GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
            MEMCPY(ash_stat.program_, thread_base + tname_offset, OB_THREAD_NAME_BUF_LEN);
          }
          if (ash_stat.inner_sql_wait_type_id_ != ObInnerSqlWaitTypeId::NULL_INNER_SQL) {
            sprintf(ash_stat.module_, "MODULE (%s)", inner_sql_wait_to_string(ash_stat.inner_sql_wait_type_id_));
          }
          if (OB_LIKELY(0 != ash_stat.session_id_ && ash_stat.is_bkgd_active_)) {
            ObActiveSessionStat::calc_db_time_for_background_session(ash_stat, sample_time_);
            ash_stat.sample_time_ = sample_time_;
            ObActiveSessHistList::get_instance().add(ash_stat);
          }
        } else if (ash_stat.is_bkgd_active_ && ash_stat.pcode_ != 0) {
          // tenant worker thread scheduling without session --> rpc process
          GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
          sprintf(ash_stat.program_, "RPC PROCESS (%.*s)_%4d",
                                  OB_THREAD_NAME_BUF_LEN,
                                  thread_base + tname_offset,
                                  ash_stat.pcode_);
          if (ash_stat.inner_sql_wait_type_id_ != ObInnerSqlWaitTypeId::NULL_INNER_SQL) {
            sprintf(ash_stat.module_, "MODULE (%s)", inner_sql_wait_to_string(ash_stat.inner_sql_wait_type_id_));
          }
          ObActiveSessionStat::calc_db_time_for_background_session(ash_stat, sample_time_);
          ash_stat.sample_time_ = sample_time_;
          ObActiveSessHistList::get_instance().add(ash_stat);
        }
      }
    } // end for
  }
  ObActiveSessHistList::get_instance().unlock();
}

bool ObActiveSessHistTask::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  if (OB_ISNULL(sess_info)) {
  } else if (ObSQLSessionState::QUERY_ACTIVE == sess_info->get_session_state()) {
    ObActiveSessionStat &stat = sess_info->get_ash_stat();
    if (OB_UNLIKELY(&stat == &ObActiveSessionGuard::thread_local_stat_)) {
      LOG_WARN_RET(OB_SUCCESS, "session active but set to dummy stat", KPC(sess_info));
    } else {
      stat.sample_time_ = sample_time_;
      if (OB_UNLIKELY(stat.tenant_id_ == 0 || stat.user_id_ == 0 || stat.session_id_ == 0)) {
        LOG_WARN_RET(OB_SUCCESS, "foreground session stat empty!", K(stat), KPC(sess_info));
      }
      stat.plan_id_ = sess_info->get_current_plan_id();
      if (OB_INVALID_ID != stat.plsql_entry_object_id_
          && 1 == stat.in_sql_execution_
          && 0 == stat.in_plsql_execution_
          && '\0' != stat.top_level_sql_id_[0]) {
        // do nothing, already set in pl
      } else {
        sess_info->get_cur_sql_id(stat.top_level_sql_id_, sizeof(stat.top_level_sql_id_));
      }
      sess_info->get_cur_sql_id(stat.sql_id_, sizeof(stat.sql_id_));
      ObActiveSessionStat::calc_db_time(stat, sample_time_);

      stat.program_[0] = '\0';
      stat.module_[0] = '\0';
      // fill program
      if (sess_info->get_user_at_client_ip().empty()) {
        if (stat.is_remote_inner_sql_) {
          sprintf(stat.program_, "INNER SQL REMOTE EXEC (%.*s)",
                              OB_THREAD_NAME_BUF_LEN,
                              sess_info->get_thread_name());
        } else {
          sprintf(stat.program_, "LOCAL INNER SQL EXEC (%.*s)",
                              OB_THREAD_NAME_BUF_LEN,
                              sess_info->get_thread_name());
        }
        stat.session_type_ = true;  // BACKGROUND
      } else if (sess_info->is_remote_session()) {
        sprintf(stat.program_, "REMOTE SQL EXEC %.*s (%.*s)", sess_info->get_user_at_client_ip().length(),
                                              sess_info->get_user_at_client_ip().ptr(),
                                              OB_THREAD_NAME_BUF_LEN,
                                              sess_info->get_thread_name());
      } else {
        sprintf(stat.program_, "%.*s (%.*s)", sess_info->get_user_at_client_ip().length(),
                                              sess_info->get_user_at_client_ip().ptr(),
                                              OB_THREAD_NAME_BUF_LEN,
                                              sess_info->get_thread_name());
      }
      // fill module
      if (sess_info->is_inner() && OB_NOT_NULL(stat.get_prev_stat()) && stat.prev_inner_sql_wait_type_id_ != ObInnerSqlWaitTypeId::NULL_INNER_SQL) {
        sprintf(stat.module_, "INNER SQL EXEC (%s)", inner_sql_wait_to_string(stat.prev_inner_sql_wait_type_id_));
      // DO NOT mark foreground module. Leave it for customers.
      // } else if (stat.inner_sql_wait_type_id_ != ObInnerSqlWaitTypeId::NULL_INNER_SQL) {
      //   sprintf(stat.module_, "MODULE (%s)", inner_sql_wait_to_string(stat.inner_sql_wait_type_id_));
      }
      ObActiveSessHistList::get_instance().add(stat);
    }
  }
  return true;
}
