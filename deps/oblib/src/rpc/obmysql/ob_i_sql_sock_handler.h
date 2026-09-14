/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBMYSQL_OB_I_SQL_SOCK_HANDLER_H_
#define OCEANBASE_OBMYSQL_OB_I_SQL_SOCK_HANDLER_H_
namespace oceanbase
{
namespace obmysql
{
class ObISqlSockHandler {
public:
  ObISqlSockHandler() {}
  virtual ~ObISqlSockHandler() {}
  virtual int on_readable(void* sess) = 0;
  // A successful handoff keeps the socket alive until the QUIT worker finishes.
  // Otherwise the caller must release its read ownership and close the socket.
  virtual int on_disconnect_readable(void *sess, bool &quit_delivered) = 0;
  virtual void on_close(void* sess, int err) = 0;
  virtual void on_flushed(void* sess) = 0;
  virtual int on_connect(void* sess, int fd) = 0;
};

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_I_SQL_SOCK_HANDLER_H_ */

