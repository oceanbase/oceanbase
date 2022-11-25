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

namespace oceanbase
{
namespace share
{

class ObActiveSessHistTask : public common::ObTimerTask
{
public:
  ObActiveSessHistTask()
      : is_inited_(false) {}
  virtual ~ObActiveSessHistTask() = default;
  static ObActiveSessHistTask &get_instance();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;
  bool operator()(sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo *sess_info);
private:
  bool is_inited_;
  int64_t sample_time_;
};

}
}
#endif /* _OB_SHARE_ASH_ACTIVE_SESSION_TASK_H_ */
//// end of header file
