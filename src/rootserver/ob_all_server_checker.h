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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_SERVER_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_SERVER_CHECKER_H_

#include "lib/container/ob_array.h"
#include "lib/thread/ob_work_queue.h"
#include "share/ob_server_status.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObServerConfig;

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace rootserver
{
class ObServerManager;
class ObAllServerChecker
{
public:
  ObAllServerChecker();
  virtual ~ObAllServerChecker();
  int init(ObServerManager &server_manager,
           const common::ObAddr &rs_addr);

  int check_all_server();
private:
  int check_status_same(const share::ObServerStatus &left,
                        const share::ObServerStatus &right,
                        bool &same) const;

private:
  bool inited_;
  ObServerManager *server_manager_;
  common::ObAddr rs_addr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllServerChecker);
};

class ObCheckServerTask : public common::ObAsyncTimerTask
{
public:
  ObCheckServerTask(common::ObWorkQueue &work_queue,
                    ObAllServerChecker &checker);
  virtual ~ObCheckServerTask() {}

  // interface of AsyncTask
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObAllServerChecker &checker_;
};

}//end namespace root server
}//end namespace oceanbase
#endif
