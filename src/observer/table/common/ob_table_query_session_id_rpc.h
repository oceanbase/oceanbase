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

#ifndef _OB_TABLE_QUERY_SESSION_ID_RPC_H
#define _OB_TABLE_QUERY_SESSION_ID_RPC_H
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
namespace oceanbase
{
namespace observer
{
class ObTableSessIDRequest
{
  OB_UNIS_VERSION(1);
public:
  ObTableSessIDRequest()
    : tenant_id_(0),
      range_(0)
  {}
  ~ObTableSessIDRequest() {}
public:
  int init(const uint64_t tenant_id, const int64_t range);
  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_range() const { return range_; }
  TO_STRING_KV(K_(tenant_id), K_(range));
private:
  uint64_t tenant_id_;
  int64_t range_;
};
} // namespace observer

namespace obrpc
{
class ObTableSessIDRpcResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableSessIDRpcResult()
    : tenant_id_(0),
      status_(0),
      start_id_(0),
      end_id_(0)
  {}
  ~ObTableSessIDRpcResult() {}
public:
  int init(const uint64_t tenant_id, const int status, const int64_t start_id, const int64_t end_id);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int get_status() const { return status_; }
  int64_t get_start_id() const { return start_id_; }
  int64_t get_end_id() const { return end_id_; }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(status), K_(start_id), K_(end_id));
public:
  static const int64_t SESS_ID_RPC_TIMEOUT = 1 * 1000 * 1000;
private:
  uint64_t tenant_id_;
  int status_;
  int64_t start_id_;
  int64_t end_id_;
};

class ObTableSessIDRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObTableSessIDRpcProxy);
  RPC_S(PR1 sync_fetch_sess_id, OB_TABLE_SYC_FETCH_SESS_ID, (observer::ObTableSessIDRequest), ObTableSessIDRpcResult);
};

class ObTableSessIDP : public ObRpcProcessor<obrpc::ObTableSessIDRpcProxy::ObRpc<OB_TABLE_SYC_FETCH_SESS_ID>>
{
public:
  ObTableSessIDP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableSessIDP);
};
} // namespace obrpc

namespace observer
{
class ObTableSessIDRequestRpc
{
public:
  ObTableSessIDRequestRpc()
    : is_inited_(false),
      rpc_proxy_(nullptr),
      self_()
  {}
  ~ObTableSessIDRequestRpc() { destroy(); }
  int init(obrpc::ObTableSessIDRpcProxy *rpc_proxy,
           const common::ObAddr &self);
  void destroy();
  int fetch_new_range(const ObTableSessIDRequest &msg,
                      obrpc::ObTableSessIDRpcResult &res);
private:
  bool is_inited_;
  obrpc::ObTableSessIDRpcProxy *rpc_proxy_;
  common::ObAddr self_;
};
} // namespace observer
} // namespace oceanbase

#endif // _OB_TABLE_QUERY_SESSION_ID_RPC_H