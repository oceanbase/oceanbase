/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
class ObAllServerTask : public share::ObAsyncTask
{
public:
  ObAllServerTask(ObServerManager &server_manager,
                  const common::ObAddr &server,
                  bool with_rootserver);
  virtual ~ObAllServerTask();

  int process();
  int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
private:
  ObServerManager &server_manager_;
  common::ObAddr server_;
  bool with_rootserver_;

  DISALLOW_COPY_AND_ASSIGN(ObAllServerTask);
};

}//end namespace rootserver
}//end namespace oceanbase
#endif
