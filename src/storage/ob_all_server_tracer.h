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

#ifndef OB_ALL_SERVER_TRACER_H_
#define OB_ALL_SERVER_TRACER_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/hash/ob_hashset.h"
#include "share/partition_table/ob_iserver_trace.h"
#include "rootserver/ob_server_table_operator.h"

namespace oceanbase {
namespace storage {

class ObServerTraceMap : public share::ObIServerTrace {
public:
  ObServerTraceMap();
  virtual ~ObServerTraceMap();
  int init();
  virtual int is_server_exist(const common::ObAddr& server, bool& exist) const;
  virtual int check_server_alive(const common::ObAddr& server, bool& is_alive) const;
  virtual int check_in_service(const common::ObAddr& addr, bool& service_started) const;
  virtual int check_migrate_in_blocked(const common::ObAddr& addr, bool& is_block) const;
  virtual int check_server_permanent_offline(const common::ObAddr& server, bool& is_offline) const;
  virtual int is_server_stopped(const common::ObAddr& server, bool& is_stopped) const;
  int refresh();

private:
  static const int64_t DEFAULT_SERVER_COUNT = 2048;
  bool is_inited_;
  mutable common::SpinRWLock lock_;
  common::hash::ObHashSet<common::ObAddr, common::hash::NoPthreadDefendMode> servers_;
  rootserver::ObServerTableOperator server_table_operator_;
};

class ObServerTraceTask : public common::ObTimerTask {
public:
  ObServerTraceTask();
  virtual ~ObServerTraceTask();
  int init(ObServerTraceMap* trace_map, int tg_id);
  virtual void runTimerTask();
  TO_STRING_KV(KP_(trace_map));

private:
  const static int64_t REFRESH_INTERVAL_US = 5L * 1000 * 1000;
  ObServerTraceMap* trace_map_;
  bool is_inited_;
};

class ObAllServerTracer : public share::ObIServerTrace {
public:
  static ObAllServerTracer& get_instance();
  int init(int tg_id, ObServerTraceTask& trace_task);
  virtual int is_server_exist(const common::ObAddr& server, bool& exist) const;
  virtual int check_server_alive(const common::ObAddr& server, bool& is_alive) const;
  virtual int check_in_service(const common::ObAddr& addr, bool& service_started) const;
  virtual int check_server_permanent_offline(const common::ObAddr& server, bool& is_offline) const;
  virtual int is_server_stopped(const common::ObAddr& server, bool& is_stopped) const;
  virtual int check_migrate_in_blocked(const common::ObAddr& addr, bool& is_block) const;

private:
  ObAllServerTracer();
  virtual ~ObAllServerTracer();

private:
  bool is_inited_;
  ObServerTraceMap trace_map_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_ALL_SERVER_TRACER_H_
