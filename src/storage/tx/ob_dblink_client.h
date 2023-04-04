// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_OB_DBLINK_CLIENT_H
#define OCEANBASE_TRANSACTION_OB_DBLINK_CLIENT_H

#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_xa_query.h"

namespace oceanbase
{
namespace transaction
{
enum class ObDBLinkClientState
{
  IDLE = 0,
  START,
  END,
  PREPARING,
  PREPARED,
  RDONLY_PREPARED,
  COMMITTING,
  COMMITTED,
  ROLLBACKING,
  ROLLBACKED,
};

// this class is used to maintain trans state of dblink client
// 1. commit (only two phase)
//    the state switch of a committed dblink trans is as follows.
//    1) IDLE => START => END => PREPARING => PREPARED => COMMITTING => COMMITTED
//    2) IDLE => START => END => PREPARING => RDONLY_PREPARED
// 2. two phase rollback
//    the state switch of a committed dblink trans is as follows.
//    1) IDLE => START => END => PREPARING => PREPARED => ROLLBACKING => ROLLBACKED
//    2) IDLE => START => END => PREPARING => RDONLY_PREPARED
// 3. one phase rollback
//    1) IDLE => START => END => ROLLBACKING => ROLLBACKED
//
// exception
// 1. if START and xa end fails, the cooresponding connection should be disconnected
// 2. if PREPARING, xa prepare fails and xa rollback succeeds, the connection can be reused
class ObDBLinkClient
{
public:
  explicit ObDBLinkClient() : lock_(), is_inited_(false), index_(0), xid_(),
      state_(ObDBLinkClientState::IDLE),
      dblink_type_(common::sqlclient::DblinkDriverProto::DBLINK_UNKNOWN), dblink_conn_(NULL),
      impl_(NULL), tx_timeout_us_(-1)
  {}
  ~ObDBLinkClient() { destroy(); }
  void reset();
  void destroy() { reset(); }
  int init(const uint32_t index,
           const common::sqlclient::DblinkDriverProto dblink_type,
           const int64_t tx_timeout_us,
           common::sqlclient::ObISQLConnection *dblink_conn);
public:
  int rm_xa_start(const transaction::ObXATransID &xid, const ObTxIsolationLevel isolation);
  int rm_xa_end();
  int rm_xa_prepare();
  int rm_xa_commit();
  int rm_xa_rollback();
public:
  const transaction::ObXATransID &get_xid() const { return xid_; }
  uint32_t get_index() const { return index_; }
  bool is_inited() const { return is_inited_; }
  bool is_started(const transaction::ObXATransID &xid);
  bool equal(common::sqlclient::ObISQLConnection *dblink_conn);
public:
  static bool is_valid_dblink_type(const common::sqlclient::DblinkDriverProto dblink_type);
private:
  int rm_xa_end_();
  int init_query_impl_(const ObTxIsolationLevel isolation);
public:
  TO_STRING_KV(KP(this), K_(is_inited), K_(index), K_(xid), K_(state),
      K_(dblink_type), KP_(dblink_conn), K_(tx_timeout_us));
protected:
  common::ObSpinLock lock_;
  bool is_inited_;
  uint32_t index_;
  transaction::ObXATransID xid_;
  ObDBLinkClientState state_;
  common::sqlclient::DblinkDriverProto dblink_type_;
  common::sqlclient::ObISQLConnection *dblink_conn_;
  transaction::ObXAQuery *impl_;
  int64_t tx_timeout_us_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDBLinkClient);
};

static const int64_t OB_DEFAULT_DBLINK_CLIENT_COUNT = 4;
typedef ObSEArray<ObDBLinkClient *, OB_DEFAULT_DBLINK_CLIENT_COUNT> ObDBLinkClientArray;

} // end of namespace transaction
} // end of namespace oceanbase

#endif // OCEANBASE_TRANSACTION_OB_DBLINK_CLIENT_H
