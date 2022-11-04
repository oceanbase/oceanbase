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

#ifndef OB_ADMIN_TRX_EXECUTOR_H_
#define OB_ADMIN_TRX_EXECUTOR_H_

#include "../ob_admin_executor.h"
#include "lib/string/ob_string.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_net_client.h"

namespace oceanbase
{
namespace tools
{
class ObAdminTransExecutor : public ObAdminExecutor
{
public:
  ObAdminTransExecutor();
  virtual ~ObAdminTransExecutor();
  virtual int execute(int argc, char *argv[]);
private:
  void print_usage();
  int parse_options(int argc, char *argv[]);
  int modify_trans();
  int dump_trans();
  int kill_trans();
 
private:
  bool inited_;
  obrpc::ObNetClient client_;
  obrpc::ObSrvRpcProxy srv_proxy_;

  common::ObAddr dst_server_;
  transaction::ObTransID trans_id_;
  int64_t status_;
  int64_t trans_version_;
  int64_t end_log_ts_;
  int32_t cmd_;
  int64_t timeout_;
  static const int64_t DEFAULT_TIMEOUT;
};

}
}

#endif /* OB_ADMIN_TRX_EXECUTOR_H_ */


