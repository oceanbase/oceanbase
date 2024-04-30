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

#ifndef OCEANBASE_SQL_USER_RESOURCE_MGR_H_
#define OCEANBASE_SQL_USER_RESOURCE_MGR_H_

#include "lib/hash/ob_link_hashmap.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/task/ob_timer.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
namespace sql {
class ObSQLSessionInfo;
class ObTenantUserKey {
public:
  ObTenantUserKey() : tenant_id_(0), user_id_(0)
  {}
  ObTenantUserKey(const uint64_t tenant_id, const uint64_t user_id) :
    tenant_id_(tenant_id), user_id_(user_id)
  {}
  uint64_t hash() const
  {
    return common::murmurhash(&user_id_, sizeof(user_id_), tenant_id_);
  };
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  };
  int compare(const ObTenantUserKey& r) const
  {
    int cmp = 0;
    if (tenant_id_ < r.tenant_id_) {
      cmp = -1;
    } else if (tenant_id_ == r.tenant_id_) {
      if (user_id_ < r.user_id_) {
        cmp = -1;
      } else if (user_id_ == r.user_id_) {
        cmp = 0;
      } else {
        cmp = 1;
      }
    } else {
      cmp = 1;
    }
    return cmp;
  }
  bool operator== (const ObTenantUserKey &other) const { return 0 == compare(other); }
  bool operator!=(const ObTenantUserKey &other) const { return !operator==(other); }
  bool operator<(const ObTenantUserKey &other) const { return -1 == compare(other); }
  TO_STRING_KV(K_(tenant_id), K(user_id_));

public:
  uint64_t tenant_id_;
  uint64_t user_id_;
};

typedef common::LinkHashNode<ObTenantUserKey> ObConnectResHashNode;
typedef common::LinkHashValue<ObTenantUserKey> ObConnectResHashValue;

class ObConnectResource : public ObConnectResHashValue {
public:
  ObConnectResource()
      : rwlock_(), cur_connections_(0), history_connections_(0), start_time_(0),
        tenant_id_(OB_SERVER_TENANT_ID)
  {
  }
  ObConnectResource(uint64_t cur_connections, uint64_t history_connections, int64_t cur_time,
                    int64_t tenant_id)
      : rwlock_(), cur_connections_(cur_connections),
        history_connections_(history_connections),
        start_time_(cur_time),
        tenant_id_(tenant_id)
  {
  }
  virtual ~ObConnectResource()
  {}
  TO_STRING_KV(K_(cur_connections), K_(history_connections), K_(start_time));
  common::ObLatch rwlock_;
  uint64_t cur_connections_;
  uint64_t history_connections_;
  // From start_time_ to now, count of connections is history_connections_.
  // According to MySQL, we don't have to record time of all connections in the previous hour.
  // At start_time_ + 1 hour, history_connections_ is reset to zero.
  // For example, max_connections_per_hour is 3,
  // we create one connection at 1:00, 1:10 and 1:20, then we can't create a connection until 2:00.
  // At 2:00, we can create 3 connections, so we only have to record start_time_ = 1:00 and
  // number of connections from this time, and don't have to record 1:10 or 1:20.
  int64_t start_time_;
  // TODO: count of update and query in one hour.
  int64_t tenant_id_;
};

class ObConnectResAlloc {
public:
  ObConnectResAlloc()
  {}
  ~ObConnectResAlloc()
  {}
  ObConnectResource* alloc_value();
  void free_value(ObConnectResource* conn_res);

  ObConnectResHashNode* alloc_node(ObConnectResource* value);
  void free_node(ObConnectResHashNode* node);
};

typedef common::ObLinkHashMap<ObTenantUserKey, ObConnectResource, ObConnectResAlloc> ObConnResMap;

class ObConnectResourceMgr {
public:
  ObConnectResourceMgr();
  virtual ~ObConnectResourceMgr();
  int init(share::schema::ObMultiVersionSchemaService &schema_service);
  // ask for tenant connection resource.
  int apply_for_tenant_conn_resource(const uint64_t tenant_id, const ObPrivSet &priv,
                     const uint64_t max_tenant_connections);
  void release_tenant_conn_resource(const uint64_t tenant_id);
  int get_tenant_cur_connections(const uint64_t tenant_id,
                                 bool &tenant_exists,
                                 uint64_t &cur_connections);
  int get_or_insert_user_resource(
      const uint64_t tenant_id,
      const uint64_t user_id,
      const uint64_t max_user_connections,
      const uint64_t max_connections_per_hour,
      ObConnectResource *&user_res);
  int increase_user_connections_count(
      const uint64_t max_user_connections,
      const uint64_t max_connections_per_hour,
      const ObString &user_name,
      ObConnectResource *user_res,
      bool &user_conn_increased);
  int on_user_connect(const uint64_t tenant_id,
                      const uint64_t user_id,
                      const ObPrivSet &priv,
                      const ObString &user_name,
                      const uint64_t max_connections_per_hour,
                      const uint64_t max_user_connections,
                      const uint64_t max_global_connections,
                      ObSQLSessionInfo& session);
  int on_user_disconnect(ObSQLSessionInfo &session);
  int erase_tenant_conn_res_map(int64_t tenant_id);
private:
  struct EraseTenantMapFunc
  {
    EraseTenantMapFunc(int64_t tenant_id)
      : tenant_id_(tenant_id), erase_cnt_(0) {}
    ~EraseTenantMapFunc() {}
    bool operator()(const ObTenantUserKey &key, const ObConnectResource *value) {
      bool res = key.tenant_id_ == tenant_id_;
      erase_cnt_ += res ? 1 : 0;
      return res;
    }
    int64_t tenant_id_;
    int64_t erase_cnt_;
  };
  class CleanUpConnResourceFunc
  {
  public:
    CleanUpConnResourceFunc(share::schema::ObSchemaGetterGuard &schema_guard,
      ObConnResMap &conn_res_map)
    : schema_guard_(schema_guard), conn_res_map_(conn_res_map)
    {}
    bool operator() (ObTenantUserKey key, ObConnectResource *user_res);
  private:
    share::schema::ObSchemaGetterGuard &schema_guard_;
    ObConnResMap &conn_res_map_;
  };
  class ConnResourceCleanUpTask : public common::ObTimerTask
  {
  public:
    ConnResourceCleanUpTask(ObConnectResourceMgr &conn_res_mgr)
      : conn_res_mgr_(conn_res_mgr) {}
    int init(ObConnectResourceMgr *tz_mgr);
    virtual ~ConnResourceCleanUpTask() {}
    void runTimerTask(void) override;

    ObConnectResourceMgr &conn_res_mgr_;
    static const uint64_t SLEEP_USECONDS = 3600000000;  // one hour
  };
  friend class ConnResourceCleanUpTask;
  friend class CleanUpUserResourceFunc;
private:
  bool inited_;
  ObConnResMap user_res_map_;
  ObConnResMap tenant_res_map_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ConnResourceCleanUpTask cleanup_task_;
  DISALLOW_COPY_AND_ASSIGN(ObConnectResourceMgr);
};



}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_USER_RESOURCE_MGR_H_ */
