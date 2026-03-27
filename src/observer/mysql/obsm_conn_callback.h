/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
