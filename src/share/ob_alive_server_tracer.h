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

#ifndef OCEANBASE_SHARE_OB_ALIVE_SERVER_TRACER_H_
#define OCEANBASE_SHARE_OB_ALIVE_SERVER_TRACER_H_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_latch.h"
#include "lib/net/ob_addr.h"
#include "lib/task/ob_timer.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace obrpc {
class ObCommonRpcProxy;
}
namespace share {
class ObILocalityManager {
public:
  virtual int is_local_zone_read_only(bool& is_readonly) = 0;
  virtual int is_local_server(const common::ObAddr& server, bool& is_local) = 0;
};

class ObIAliveServerTracer {
public:
  virtual int is_alive(const common::ObAddr& addr, bool& alive, int64_t& trace_time) const = 0;
  virtual int get_server_status(
      const common::ObAddr& addr, bool& alive, bool& is_server_exist, int64_t& trace_time) const = 0;

  virtual int get_primary_cluster_id(int64_t& cluster_id) const = 0;
};

class ObAliveServerMap : public ObIAliveServerTracer {
public:
  const static int64_t HASH_SERVER_CNT = 2048;

  ObAliveServerMap();
  virtual ~ObAliveServerMap();

  int init();

  virtual int is_alive(const common::ObAddr& addr, bool& alive, int64_t& trace_time) const;
  virtual int get_server_status(
      const common::ObAddr& addr, bool& alive, bool& is_server_exist, int64_t& trace_time) const;
  virtual int refresh(
      common::ObIArray<common::ObAddr>& active_server_list, common::ObIArray<common::ObAddr>& inactive_server_list);
  virtual int get_primary_cluster_id(int64_t& cluster_id) const
  {
    UNUSED(cluster_id);
    return common::OB_OP_NOT_ALLOW;
  }

private:
  virtual int refresh_server_list(const common::ObIArray<common::ObAddr>& server_list,
      common::hash::ObHashSet<common::ObAddr, common::hash::NoPthreadDefendMode>& servers);

private:
  bool is_inited_;
  mutable common::ObLatch lock_;
  int64_t trace_time_;
  common::hash::ObHashSet<common::ObAddr, common::hash::NoPthreadDefendMode> active_servers_;
  common::hash::ObHashSet<common::ObAddr, common::hash::NoPthreadDefendMode> inactive_servers_;

  DISALLOW_COPY_AND_ASSIGN(ObAliveServerMap);
};

class ObAliveServerTracer;
class ObAliveServerRefreshTask : public common::ObTimerTask {
public:
  const static int64_t REFRESH_INTERVAL_US = 5L * 1000 * 1000;  // 5 second
  explicit ObAliveServerRefreshTask(ObAliveServerTracer& tracker);
  virtual ~ObAliveServerRefreshTask();

  int init();
  virtual void runTimerTask();

private:
  ObAliveServerTracer& tracer_;
  bool is_inited_;
  ;

  DISALLOW_COPY_AND_ASSIGN(ObAliveServerRefreshTask);
};

class ObAliveServerTracer : public ObIAliveServerTracer {
public:
  ObAliveServerTracer();
  virtual ~ObAliveServerTracer();

  int init(obrpc::ObCommonRpcProxy& rpc_porxy, common::ObMySQLProxy& sql_proxy);

  virtual int is_alive(const common::ObAddr& addr, bool& alive, int64_t& trace_time) const;
  virtual int get_server_status(
      const common::ObAddr& addr, bool& alive, bool& is_server_exist, int64_t& trace_time) const;
  virtual int get_primary_cluster_id(int64_t& cluster_id) const;
  virtual int refresh();

private:
  int refresh_primary_cluster_id();

private:
  const static int64_t SERVER_MAP_CNT = 2;

  bool is_inited_;
  ObAliveServerMap server_maps_[SERVER_MAP_CNT];
  ObAliveServerMap* volatile cur_map_;
  ObAliveServerMap* volatile last_map_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  ObAliveServerRefreshTask task_;
  int64_t primary_cluster_id_;
  common::ObMySQLProxy* sql_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObAliveServerTracer);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_ALIVE_SERVER_TRACER_H_
