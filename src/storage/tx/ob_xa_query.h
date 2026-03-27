// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_TX_OB_XA_QUERY_H
#define OCEANBASE_TX_OB_XA_QUERY_H

#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "storage/tx/ob_xa_define.h"

namespace oceanbase
{
namespace transaction
{
class ObXAQuery
{
public:
  ObXAQuery() {}
  virtual ~ObXAQuery() {}
public:
  virtual int xa_start(const ObXATransID &xid, const int64_t flags) = 0;
  virtual int xa_end(const ObXATransID &xid, const int64_t flags) = 0;
  virtual int xa_prepare(const ObXATransID &xid) = 0;
  virtual int xa_commit(const ObXATransID &xid, const int64_t flags) = 0;
  virtual int xa_rollback(const ObXATransID &xid) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAQuery);
};

// this is used to executing xa subprogram for oceanbase
class ObXAQueryObImpl : public ObXAQuery
{
public:
  ObXAQueryObImpl() : is_inited_(false), conn_(NULL) {}
  ~ObXAQueryObImpl() { destroy(); }
  int init(common::sqlclient::ObISQLConnection *conn_);
  void reset();
  void destroy() { reset(); }
public:
  virtual int xa_start(const ObXATransID &xid, const int64_t flags) override;
  virtual int xa_end(const ObXATransID &xid, const int64_t flags) override;
  virtual int xa_prepare(const ObXATransID &xid) override;
  virtual int xa_commit(const ObXATransID &xid, const int64_t flags) override;
  virtual int xa_rollback(const ObXATransID &xid) override;
private:
  int execute_query_(const ObSqlString &sql, int &xa_result);
private:
  bool is_inited_;
  common::sqlclient::ObISQLConnection *conn_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAQueryObImpl);
};

// this is used to execute xa subprogram for oracle
class ObXAQueryOraImpl : public ObXAQuery
{
public:
  ObXAQueryOraImpl() : is_inited_(false), conn_(NULL) {}
  ~ObXAQueryOraImpl() { destroy(); }
  int init(common::sqlclient::ObISQLConnection *conn_);
  void reset();
  void destroy() { reset(); }
public:
  virtual int xa_start(const ObXATransID &xid, const int64_t flags) override;
  virtual int xa_end(const ObXATransID &xid, const int64_t flags) override;
  virtual int xa_prepare(const ObXATransID &xid) override;
  virtual int xa_commit(const ObXATransID &xid, const int64_t flags) override;
  virtual int xa_rollback(const ObXATransID &xid) override;
private:
  int convert_flag_(const int64_t xa_flag, const int64_t xa_req_type, uint32_t &oci_flag);
private:
  bool is_inited_;
  // oci connection to oracle
  // common::ObOciConnection *conn_;
  common::sqlclient::ObISQLConnection *conn_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObXAQueryOraImpl);
};

} // end of namespace transaction
} // end of nemespace oceanbase

#endif // OCEANBASE_TX_OB_XA_QUERY_H
