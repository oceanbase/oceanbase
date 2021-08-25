// Copyright 2021 Alibaba Inc. All Rights Reserved.
// Author:
//     shanting <dachuan.sdc@antgroup.com>
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
  ObTenantUserKey() : id_(0)
  {}
  ObTenantUserKey(const int64_t id_) : id_(id_)
  {}
  uint64_t hash() const
  {
    return common::murmurhash(&id_, sizeof(id_), 0);
  };
  int compare(const ObTenantUserKey& r)
  {
    int cmp = 0;
    if (id_ < r.id_) {
      cmp = -1;
    } else if (id_ == r.id_) {
      cmp = 0;
    } else {
      cmp = 1;
    }
    return cmp;
  }
  TO_STRING_KV(K_(id));

public:
  uint64_t id_;
};

typedef common::LinkHashNode<ObTenantUserKey> ObConnectResHashNode;
typedef common::LinkHashValue<ObTenantUserKey> ObConnectResHashValue;

class ObConnectResource : public ObConnectResHashValue {
public:
  ObConnectResource() : rwlock_(), cur_connections_(0), history_connections_(0), start_time_(0)
  {}
  ObConnectResource(uint64_t cur_connections, uint64_t history_connections, int64_t cur_time)
      : rwlock_(), cur_connections_(cur_connections), history_connections_(history_connections), start_time_(cur_time)
  {}
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
  int init(share::schema::ObMultiVersionSchemaService& schema_service);
  // ask for tenant connection resource.
  int apply_for_tenant_conn_resource(
      const uint64_t tenant_id, const ObPrivSet& priv, const uint64_t max_tenant_connections);
  void release_tenant_conn_resource(const uint64_t tenant_id);
  int get_or_insert_user_resource(const uint64_t user_id, const uint64_t max_user_connections,
      const uint64_t max_connections_per_hour, ObConnectResource*& user_res, bool& has_insert);
  int increase_user_connections_count(const uint64_t max_user_connections, const uint64_t max_connections_per_hour,
      const ObString& user_name, ObConnectResource* user_res);
  int on_user_connect(const uint64_t tenant_id, const uint64_t user_id, const ObPrivSet& priv,
      const ObString& user_name, const uint64_t max_connections_per_hour, const uint64_t max_user_connections,
      const uint64_t max_global_connections, ObSQLSessionInfo& session);
  int on_user_disconnect(ObSQLSessionInfo& session);

private:
  class CleanUpConnResourceFunc {
  public:
    CleanUpConnResourceFunc(
        share::schema::ObSchemaGetterGuard& schema_guard, ObConnResMap& conn_res_map, const bool is_user)
        : schema_guard_(schema_guard), conn_res_map_(conn_res_map), is_user_(is_user)
    {}
    bool operator()(ObTenantUserKey key, ObConnectResource* user_res);

  private:
    share::schema::ObSchemaGetterGuard& schema_guard_;
    ObConnResMap& conn_res_map_;
    const bool is_user_;
  };
  class ConnResourceCleanUpTask : public common::ObTimerTask {
  public:
    ConnResourceCleanUpTask(ObConnectResourceMgr& conn_res_mgr) : conn_res_mgr_(conn_res_mgr)
    {}
    int init(ObConnectResourceMgr* tz_mgr);
    virtual ~ConnResourceCleanUpTask()
    {}
    void runTimerTask(void) override;

    ObConnectResourceMgr& conn_res_mgr_;
    const uint64_t SLEEP_USECONDS = 3600000000;  // one hour
  };
  friend class ConnResourceCleanUpTask;
  friend class CleanUpUserResourceFunc;

private:
  bool inited_;
  ObConnResMap user_res_map_;
  ObConnResMap tenant_res_map_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ConnResourceCleanUpTask cleanup_task_;
  DISALLOW_COPY_AND_ASSIGN(ObConnectResourceMgr);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_USER_RESOURCE_MGR_H_ */
