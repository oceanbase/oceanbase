/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_TABLE_CONNECTION_MGR_H_
#define OB_TABLE_CONNECTION_MGR_H_
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "rpc/ob_request.h"

namespace oceanbase
{
namespace table
{
class ObTableConnection
{
public:
  ObTableConnection() {}
  ~ObTableConnection() {}
  int init(const common::ObAddr &addr, int64_t tenant_id, int64_t database_id, int64_t user_id);
  void update_last_active_time(int64_t last_active_time);
  void update_all_ids(int64_t tenant_id, int64_t database_id, int64_t user_id);
  const common::ObAddr &get_addr() { return client_addr_; }
  const int64_t &get_tenant_id() { return tenant_id_; }
  const int64_t &get_database_id() { return database_id_; }
  const int64_t &get_user_id() { return user_id_; }
  const int64_t &get_first_active_time() { return first_active_time_; }
  const int64_t &get_last_active_time() { return last_active_time_; }
  TO_STRING_KV(K_(client_addr),
               K_(tenant_id),
               K_(database_id),
               K_(user_id),
               K_(first_active_time),
               K_(last_active_time));
private:
  common::ObAddr client_addr_;
  int64_t tenant_id_;
  int64_t database_id_;
  int64_t user_id_;
  int64_t first_active_time_;
  int64_t last_active_time_;
};

class ObTableConnectionMgr
{
using ObTableConnectionMap = common::hash::ObHashMap<common::ObAddr, ObTableConnection>;
public:
  static ObTableConnectionMgr &get_instance();
  int update_table_connection(const common::ObAddr &client_addr, int64_t tenant_id, int64_t database_id, int64_t user_id);
  int update_table_connection(const rpc::ObRequest *req, int64_t tenant_id, int64_t database_id, int64_t user_id);
  void on_conn_close(easy_connection_t *c);
  template <typename Function>
  int for_each_connection(Function &fn);
private:
  ObTableConnectionMgr();
  int init();
private:
  static const int64_t CONN_INFO_MAP_BUCKET_SIZE = 1024;
  ObTableConnectionMap connection_map_;
  static ObTableConnectionMgr *instance_;
  static int64_t once_;  // for singleton instance creating
};

template <typename Function>
int ObTableConnectionMgr::for_each_connection(Function &fn)
{
  return connection_map_.foreach_refactored(fn);
}

class ObTableConnUpdater
{
public:
  explicit ObTableConnUpdater(int64_t tenant_id, int64_t database_id, int64_t user_id)
  : tenant_id_(tenant_id),
    database_id_(database_id),
    user_id_(user_id)
  {}
  virtual ~ObTableConnUpdater() {}
  void operator() (common::hash::HashMapPair<common::ObAddr, ObTableConnection> &entry);
private:
  int64_t tenant_id_;
  int64_t database_id_;
  int64_t user_id_;
  DISALLOW_COPY_AND_ASSIGN(ObTableConnUpdater);
};

} // end namespace observer
} // end namespace oceanbase

#endif /* OB_TABLE_CONNECTION_MGR_H_ */