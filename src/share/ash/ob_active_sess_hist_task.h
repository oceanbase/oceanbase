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

#ifndef _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_
#define _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_

#include "lib/task/ob_timer.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/net/ob_net_queue_traver.h"

namespace oceanbase
{
namespace share
{

using ObNetInfo = rpc::ObNetTraverProcessAutoDiag::ObNetQueueTraRes;

class ObActiveSessHistTask : public common::ObTimerTask
{
public:
  ObActiveSessHistTask()
      : is_inited_(false), elapsed_secs_(0), prev_write_pos_(0), last_regular_snapshot_time_(OB_INVALID_TIMESTAMP), sample_time_(OB_INVALID_TIMESTAMP), prev_sample_time_(OB_INVALID_TIMESTAMP) {}
  virtual ~ObActiveSessHistTask() = default;
  static ObActiveSessHistTask &get_instance();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
  void process_net_request(const ObNetInfo &net_info);
  bool do_snapshot_ahead();
private:
  bool is_inited_;
  int elapsed_secs_;
  int64_t prev_write_pos_;
  int64_t last_regular_snapshot_time_;
  int64_t sample_time_;
  int64_t prev_sample_time_;  // record prev sample time for rpc db time calculation.
};

}
}
#endif /* _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_ */
//// end of header file
