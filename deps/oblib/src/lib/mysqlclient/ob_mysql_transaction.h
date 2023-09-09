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

#ifndef OCEANBASE_MYSQL_TRANSACTION_H_
#define OCEANBASE_MYSQL_TRANSACTION_H_

#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
class ObISQLConnectionPool;
}

// query stash desc for query batch
class ObSqlTransQueryStashDesc
{
public:
  ObSqlTransQueryStashDesc() : tenant_id_(OB_INVALID_TENANT_ID),stash_query_row_cnt_(0) {}
  ~ObSqlTransQueryStashDesc() { reset(); }
  void reset() {
    tenant_id_ = OB_INVALID_TENANT_ID;
    stash_query_row_cnt_ = 0;
    stash_query_.reuse();
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() { return tenant_id_; }
  void add_row_cnt(int64_t row_cnt) { stash_query_row_cnt_ += row_cnt; }
  int64_t get_row_cnt() { return stash_query_row_cnt_; }
  ObSqlString &get_stash_query() { return stash_query_; }
  TO_STRING_KV(K_(tenant_id), K_(stash_query_row_cnt), K_(stash_query));
private:
  uint64_t tenant_id_;
  int64_t stash_query_row_cnt_;
  ObSqlString stash_query_;
};

// not thread safe sql transaction execution
// use one connection
class ObMySQLTransaction : public ObSingleConnectionProxy
{
public:
  ObMySQLTransaction(bool enable_query_stash = false);
  virtual ~ObMySQLTransaction();
public:
  // start transaction
  virtual int start(ObISQLClient *proxy,
                    const uint64_t tenant_id,
                    bool with_snapshot = false,
                    const int32_t group_id = 0);
  virtual int start(ObISQLClient *proxy,
                    const uint64_t &tenant_id,
                    const int64_t &refreshed_schema_version,
                    bool with_snapshot = false);
  // end the transaction
  virtual int end(const bool commit);
  virtual bool is_started() const { return in_trans_; }

  // get_stash_query for query batch buf
  int get_stash_query(uint64_t tenant_id, const char* table_name, ObSqlTransQueryStashDesc *&desc);
  bool get_enable_query_stash() {
    return enable_query_stash_;
  }
  // do stash query in batch
  int do_stash_query_batch() {
    return do_stash_query(QUERY_MIN_BATCH_CNT);
  }
  constexpr static int QUERY_MIN_BATCH_CNT = 200;
  // do stash query all
  int do_stash_query(int min_batch_cnt = 1);
protected:
  int start_transaction(const uint64_t &tenant_id, bool with_snap_shot);
  int end_transaction(const bool commit);
protected:
  int64_t start_time_;
  bool in_trans_;
  // inner sql now not support multi query，enable_query_stash now just enable for batch insert values
  bool enable_query_stash_;
  hash::ObHashMap<const char*, ObSqlTransQueryStashDesc*> query_stash_desc_;
};

} // end namespace commmon
} // end namespace oceanbase

#endif // OCEANBASE_MYSQL_TRANSACTION_H_
