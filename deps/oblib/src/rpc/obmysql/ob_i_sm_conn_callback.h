/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBMYSQL_OB_I_SM_CONN_CALLBACK_H_
#define OCEANBASE_OBMYSQL_OB_I_SM_CONN_CALLBACK_H_
namespace oceanbase
{
namespace observer
{
class ObSMConnection;
};

namespace obmysql
{
class ObSqlSockSession;
class ObISMConnectionCallback
{
public:
  ObISMConnectionCallback() {}
  virtual ~ObISMConnectionCallback() {}
  virtual int init(ObSqlSockSession& sess, observer::ObSMConnection& conn) = 0;
  virtual void destroy(observer::ObSMConnection& conn) = 0;
  virtual int on_disconnect(observer::ObSMConnection& conn) = 0;
};

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_I_SM_CONN_CALLBACK_H_ */

