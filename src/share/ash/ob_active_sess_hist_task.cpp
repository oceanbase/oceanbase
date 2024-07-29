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
#include "observer/ob_srv_network_frame.h"
#include "share/ob_thread_mgr.h"
#include "share/ash/ob_active_sess_hist_task.h"
#include "share/ash/ob_active_sess_hist_list.h"
#include "share/ash/ob_ash_refresh_task.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/time/ob_time_utility.h"
#include "share/wr/ob_wr_stat_guard.h"
#include "observer/omt/ob_th_worker.h"
#include "deps/oblib/src/rpc/obmysql/ob_mysql_packet.h"

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

void ObActiveSessHistTask::runTimerTask()
{
  common::ObTimeGuard time_guard(__func__, ash_iteration_time);
  ObActiveSessHistList::get_instance().lock();
  if (OB_NOT_NULL(GCTX.session_mgr_) && (true == GCONF._ob_ash_enable)) {
    WR_STAT_GUARD(ASH_SCHEDULAR);
    prev_sample_time_ = sample_time_;
    sample_time_ = ObTimeUtility::current_time();
    // 1. iter over session mgr
    GCTX.session_mgr_->for_each_session(*this);

    // 2. iter over net queue
    std::function<void(const ObNetInfo &)> net_recorder =
        std::bind(&ObActiveSessHistTask::process_net_request, this, std::placeholders::_1);
    rpc::ObNetTraverProcessAutoDiag net_travaler(net_recorder);
    rpc::ObNetQueueTraver net_traver;
    net_traver.traverse(net_travaler);

    // 3. iter over each thread
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
        bool is_finish_push = false;
        // das remote execution is foreground session
        if (ash_stat.in_das_remote_exec_ == true) {
          GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
#ifdef NDEBUG
          snprintf(ash_stat.program_, ASH_PROGRAM_STR_LEN, "DAS REMOTE EXEC (%.*s)",
                                            OB_THREAD_NAME_BUF_LEN,
                                            thread_base + tname_offset);
#else
          snprintf(ash_stat.program_, ASH_PROGRAM_STR_LEN, "DAS REMOTE EXEC (%.*s)_%7ld",
                                            OB_THREAD_NAME_BUF_LEN,
                                            thread_base + tname_offset,
                                            ash_stat.tid_);
#endif
          ash_stat.sample_time_ = sample_time_;
          ObActiveSessionStat::calc_db_time(ash_stat, sample_time_);
          ObActiveSessHistList::get_instance().add(ash_stat);
          is_finish_push = true;
        }

        // iter over each active background thread
        if (0 == serving_id && ash_stat.is_bkgd_active_ ) {
          if ( '\0' == ash_stat.program_[0]) {
            GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
            MEMCPY(ash_stat.program_, thread_base + tname_offset, OB_THREAD_NAME_BUF_LEN);
          }
          if (ash_stat.inner_sql_wait_type_id_ != ObInnerSqlWaitTypeId::NULL_INNER_SQL) {
            snprintf(ash_stat.module_, ASH_MODULE_STR_LEN, "MODULE (%.*s)", ASH_MODULE_STR_LEN - 10/*MODULE ()*/,
                inner_sql_wait_to_string(ash_stat.prev_inner_sql_wait_type_id_));
          }
          if (OB_LIKELY(0 != ash_stat.session_id_ && ash_stat.is_bkgd_active_)) {
            ObActiveSessionStat::calc_db_time(ash_stat, sample_time_);
            ash_stat.sample_time_ = sample_time_;
            ObActiveSessHistList::get_instance().add(ash_stat);
          }
        } else if (ash_stat.is_bkgd_active_ && ash_stat.pcode_ != 0 && !is_finish_push) {
          // tenant worker thread scheduling without session --> rpc process
          GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
          snprintf(ash_stat.program_, ASH_PROGRAM_STR_LEN, "RPC PROCESS (%.*s)_%4d",
                                  OB_THREAD_NAME_BUF_LEN,
                                  thread_base + tname_offset,
                                  ash_stat.pcode_);
          if (ash_stat.inner_sql_wait_type_id_ != ObInnerSqlWaitTypeId::NULL_INNER_SQL) {
            snprintf(ash_stat.module_, ASH_MODULE_STR_LEN, "MODULE (%s)", inner_sql_wait_to_string(ash_stat.inner_sql_wait_type_id_));
          }
          ObActiveSessionStat::calc_db_time(ash_stat, sample_time_);
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

      stat.stmt_type_ = sess_info->get_stmt_type();
      stat.plan_hash_ = sess_info->get_current_plan_hash();
      stat.tx_id_ = sess_info->get_tx_id_with_thread_data_lock();
      ObActiveSessionStat::calc_db_time(stat, sample_time_);

      stat.program_[0] = '\0';
      stat.module_[0] = '\0';
      stat.action_[0] = '\0';
      stat.client_id_[0] = '\0';
      // fill program
      if (!sess_info->get_user_at_client_ip().empty()) {
        if (sess_info->is_remote_session()) {
          snprintf(stat.program_, ASH_PROGRAM_STR_LEN, "REMOTE SQL CMD %s EXEC %.*s (%.*s)", sess_info->get_mysql_cmd_str(),
              sess_info->get_user_at_client_ip().length(), sess_info->get_user_at_client_ip().ptr(),
              OB_THREAD_NAME_BUF_LEN, sess_info->get_thread_name());
        } else {
          snprintf(stat.program_, ASH_PROGRAM_STR_LEN, "SQL CMD %s EXEC %.*s (%.*s)", sess_info->get_mysql_cmd_str(),
              sess_info->get_user_at_client_ip().length(), sess_info->get_user_at_client_ip().ptr(),
              OB_THREAD_NAME_BUF_LEN, sess_info->get_thread_name());
        }
      }

      // fill module
      if (sess_info->is_real_inner_session()) {
        snprintf(stat.module_, ASH_MODULE_STR_LEN, "LOCAL INNER SQL EXEC (%.*s)", ASH_MODULE_STR_LEN - 18/*INNER SQL EXEC ()*/,
            inner_sql_wait_to_string(stat.prev_inner_sql_wait_type_id_));
      } else {
        if (!sess_info->get_module_name().empty()) {
          int64_t size = sess_info->get_module_name().length() > ASH_MODULE_STR_LEN
                             ? ASH_MODULE_STR_LEN
                             : sess_info->get_module_name().length();
          MEMCPY(stat.module_, sess_info->get_module_name().ptr(), size);
          stat.module_[size] = '\0';
        }

        // fill action for user session
        if (!sess_info->get_action_name().empty()) {
          int64_t size = sess_info->get_action_name().length() > ASH_ACTION_STR_LEN
                     ? ASH_ACTION_STR_LEN
                     : sess_info->get_module_name().length();
          MEMCPY(stat.action_, sess_info->get_action_name().ptr(), size);
          stat.action_[size] = '\0';
        }

        // fill client id for user session
        if (!sess_info->get_client_identifier().empty()) {
          int64_t size = sess_info->get_client_identifier().length() > ASH_CLIENT_ID_STR_LEN
                     ? ASH_CLIENT_ID_STR_LEN
                     : sess_info->get_module_name().length();
          MEMCPY(stat.client_id_, sess_info->get_client_identifier().ptr(), size);
          stat.client_id_[size] = '\0';
        }
      }
      ObActiveSessHistList::get_instance().add(stat);
    }
  }
  return true;
}

void ObActiveSessHistTask::process_net_request(
    const ObNetInfo &net_info)
{
  ObActiveSessionStat stat;
  if (net_info.type_ == rpc::ObRequest::OB_RPC || net_info.type_ == rpc::ObRequest::OB_MYSQL) {
    stat.trace_id_ = net_info.trace_id_;
    stat.tenant_id_ = net_info.tenant_id_;
    stat.group_id_ = net_info.group_id_;
    stat.sample_time_ = sample_time_;
    stat.p2_ = net_info.retry_times_;
    if (net_info.type_ == rpc::ObRequest::OB_RPC) {
      stat.session_type_ = true;  // BACKGROUND
      snprintf(stat.program_, ASH_PROGRAM_STR_LEN, "RPC PROCESS");
      stat.p1_ = net_info.pcode_;
      stat.p2_ = net_info.req_level_;
      stat.p3_ = net_info.priority_;
    } else if (net_info.type_ == rpc::ObRequest::OB_MYSQL) {
      stat.session_type_ = false;  // FOREGROUND
      stat.session_id_ = net_info.sql_session_id_;
      int ret = OB_SUCCESS;
      MTL_SWITCH(stat.tenant_id_) {
        ObSQLSessionInfo *session_info = nullptr;
        if (OB_FAIL(GCTX.session_mgr_->get_session(static_cast<uint32_t>(stat.session_id_), session_info))) {
          // it's normal for a queueing network request doesn't have session object.
        } else {
          if (session_info->is_remote_session()) {
            snprintf(stat.program_, ASH_PROGRAM_STR_LEN, "REMOTE SQL CMD %s EXEC %.*s (%.*s)",
                obmysql::get_mysql_cmd_str(net_info.mysql_cmd_),
                session_info->get_user_at_client_ip().length(),
                session_info->get_user_at_client_ip().ptr(), OB_THREAD_NAME_BUF_LEN,
                session_info->get_thread_name());
          } else {
            snprintf(stat.program_, ASH_PROGRAM_STR_LEN, "SQL CMD %s EXEC %.*s (%.*s)",
                obmysql::get_mysql_cmd_str(net_info.mysql_cmd_),
                session_info->get_user_at_client_ip().length(),
                session_info->get_user_at_client_ip().ptr(), OB_THREAD_NAME_BUF_LEN,
                session_info->get_thread_name());
          }
          GCTX.session_mgr_->revert_session(session_info);
        }
      }
    }
    if (net_info.enqueue_timestamp_ >= prev_sample_time_) {
      // network request enqueued after last ash sample.
      stat.last_ts_ = net_info.enqueue_timestamp_;
      stat.wait_event_begin_ts_ = net_info.enqueue_timestamp_;
    } else {
      stat.last_ts_ = prev_sample_time_;
      stat.wait_event_begin_ts_ = prev_sample_time_;
    }
    stat.event_no_ = ObWaitEventIds::NETWORK_QUEUE_WAIT;
    ObActiveSessionStat::calc_db_time(stat, sample_time_);
    ObActiveSessHistList::get_instance().add(stat);
  } else {
    LOG_WARN_RET(
        OB_SUCCESS, "unsupport net packet type for active session history sampling", K(net_info));
  }
}