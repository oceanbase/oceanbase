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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_SERVER_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_SERVER_TASK_H_

#include "lib/net/ob_addr.h"
#include "lib/thread/ob_async_task_queue.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObServerConfig;
}
namespace share
{
struct ObServerStatus;
}

namespace rootserver
{
class ObServerManager;
class ObDRTaskMgr;
class ObAllServerTask : public share::ObAsyncTask
{
public:
  ObAllServerTask(ObServerManager &server_manager,
                  ObDRTaskMgr &disaster_recovery_task_mgr,
                  const common::ObAddr &server,
                  bool with_rootserver);
  virtual ~ObAllServerTask();

  int process();
  int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
private:
  ObServerManager &server_manager_;
  ObDRTaskMgr &disaster_recovery_task_mgr_;
  common::ObAddr server_;
  bool with_rootserver_;

  DISALLOW_COPY_AND_ASSIGN(ObAllServerTask);
};

}//end namespace rootserver
}//end namespace oceanbase
#endif
