/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_
#define _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_

#include "lib/task/ob_timer.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/net/ob_net_queue_traver.h"
#include "share/wr/ob_wr_snapshot_rpc_processor.h"

namespace oceanbase
{
namespace share
{

using ObNetInfo = rpc::ObNetTraverProcessAutoDiag::ObNetQueueTraRes;

class ObActiveSessHistTask : public common::ObTimerTask
{
public:
  ObActiveSessHistTask()
      : is_inited_(false), sample_time_(OB_INVALID_TIMESTAMP), tsc_sample_time_(0) {}
  virtual ~ObActiveSessHistTask() = default;
  static ObActiveSessHistTask &get_instance();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;
private:
  bool process_running_di(const SessionID &session_id, ObDiagnosticInfo *di);
  bool is_inited_;
  int64_t sample_time_;
  int64_t tsc_sample_time_;
};

}
}
#endif /* _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_ */
//// end of header file
