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

#ifndef OCEANBASE_OBSERVER_OBSM_CONN_CALLBACK_H_
#define OCEANBASE_OBSERVER_OBSM_CONN_CALLBACK_H_
#include "rpc/obmysql/ob_i_sm_conn_callback.h"

namespace oceanbase
{
namespace obmysql
{

class ObSMConnectionCallback: public ObISMConnectionCallback
{
public:
  ObSMConnectionCallback() {}
  virtual ~ObSMConnectionCallback() {}
  int init(ObSqlSockSession& sess, observer::ObSMConnection& conn) override;
  void destroy(observer::ObSMConnection& conn) override;
  int on_disconnect(observer::ObSMConnection& conn) override;
};
extern ObSMConnectionCallback global_sm_conn_callback;
}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OBSM_CONN_CALLBACK_H_ */
