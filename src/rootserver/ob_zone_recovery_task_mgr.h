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

#ifndef OCEANBASE_ROOTSERVER_OB_ZONE_RECOVERY_TASK_MGR_H_
#define OCEANBASE_ROOTSERVER_OB_ZONE_RECOVERY_TASK_MGR_H_

#include "lib/lock/ob_thread_cond.h"
#include "lib/hash/ob_refered_map.h"
#include "rootserver/ob_rs_reentrant_thread.h"
#include "rootserver/ob_rebalance_task.h"
#include "rootserver/ob_zone_server_recovery_machine.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
}

namespace rootserver {
class ObServerManager;
class ObUnitManager;
class ObZoneRecoveryTaskMgr;
class ObZoneServerRecoveryMachine;

class ObZoneRecoveryTaskKey {
public:
  ObZoneRecoveryTaskKey() : tenant_id_(OB_INVALID_ID), file_id_(-1), hash_value_(0)
  {}
  virtual ~ObZoneRecoveryTaskKey()
  {}

public:
  bool is_valid() const;
  bool operator==(const ObZoneRecoveryTaskKey& that) const;
  bool operator!=(const ObZoneRecoveryTaskKey& that) const
  {
    return !(*this == that);
  }
  ObZoneRecoveryTaskKey& operator=(const ObZoneRecoveryTaskKey& that);
  uint64_t hash() const;
  int init(const uint64_t tenant_id, const int64_t file_id, const common::ObAddr& server);
  TO_STRING_KV(K_(tenant_id), K_(file_id), K_(server));

private:
  uint64_t inner_hash() const;

private:
  uint64_t tenant_id_;
  int64_t file_id_;
  common::ObAddr server_;
  uint64_t hash_value_;
};

typedef common::hash::ObReferedMap<common::ObAddr, ObServerTaskStat> ObServerTaskStatMap;

typedef common::hash::ObReferedMap<uint64_t, ObTenantTaskStat> ObTenantTaskStatMap;

class ObZoneRecoveryTask : public common::ObDLinkBase<ObZoneRecoveryTask> {
public:
  typedef common::hash::ObHashMap<ObZoneRecoveryTaskKey, ObZoneRecoveryTask*, common::hash::NoPthreadDefendMode>
      TaskMap;
  friend class ObZoneRecoveryTaskMgr;

public:
  ObZoneRecoveryTask()
      : task_id_(),
        task_key_(),
        src_server_stat_(nullptr),
        dest_server_stat_(nullptr),
        tenant_stat_(nullptr),
        generate_time_(common::ObTimeUtility::current_time()),
        schedule_time_(-1),
        execute_time_(-1),
        src_server_(),
        src_svr_seq_(OB_INVALID_SVR_SEQ),
        dest_server_(),
        dest_svr_seq_(OB_INVALID_SVR_SEQ),
        tenant_id_(OB_INVALID_ID),
        file_id_(-1),
        dest_unit_id_(OB_INVALID_ID)
  {}
  virtual ~ObZoneRecoveryTask()
  {}

public:
  int build(const common::ObAddr& src_server, const int64_t src_svr_seq, const common::ObAddr& dest_server,
      const int64_t dest_svr_seq, const uint64_t tenant_id, const int64_t file_id, const uint64_t dest_unit_id);
  int build_by_task_result(const obrpc::ObFastRecoveryTaskReplyBatchArg& arg, int& ret_code);
  const ObZoneRecoveryTaskKey& get_task_key() const
  {
    return task_key_;
  }
  int set_server_and_tenant_stat(ObServerTaskStatMap& server_stat_map, ObTenantTaskStatMap& tenant_stat_map);
  const ObServerTaskStatMap::Item* get_src_server_stat() const
  {
    return src_server_stat_;
  }
  const ObServerTaskStatMap::Item* get_dest_server_stat() const
  {
    return dest_server_stat_;
  }
  void clean_server_and_tenant_ref(TaskMap& task_map);
  void clean_task_map(TaskMap& task_map);
  void set_schedule();
  bool in_schedule() const
  {
    return schedule_time_ > 0;
  }
  int64_t get_executor_time() const
  {
    return execute_time_;
  }
  void set_executor_time(const int64_t executor_time)
  {
    execute_time_ = executor_time;
  }
  int64_t get_generate_time() const
  {
    return generate_time_;
  }
  void set_generate_time(const int64_t generate_time)
  {
    generate_time_ = generate_time;
  }
  int64_t get_schedule_time() const
  {
    return schedule_time_;
  }
  void set_schedule_time(const int64_t schedule_time)
  {
    schedule_time_ = schedule_time;
  }
  int64_t get_deep_copy_size() const
  {
    return sizeof(ObZoneRecoveryTask);
  }
  int clone(void* input_ptr, ObZoneRecoveryTask*& output_task) const;
  int execute(obrpc::ObSrvRpcProxy& rpc_proxy) const;
  int build_fast_recovery_rpc_batch_arg(obrpc::ObFastRecoveryTaskBatchArg& batch_arg) const;
  const common::ObAddr& get_dest() const
  {
    return dest_server_;
  }
  const common::ObAddr& get_src() const
  {
    return src_server_;
  }
  int64_t get_src_svr_seq() const
  {
    return src_svr_seq_;
  }
  int64_t get_dest_svr_seq() const
  {
    return dest_svr_seq_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_file_id() const
  {
    return file_id_;
  }
  uint64_t get_unit_id() const
  {
    return dest_unit_id_;
  }
  TO_STRING_KV(K(task_id_), K(task_key_), K(generate_time_), K(schedule_time_), K(execute_time_), K(src_server_),
      K(src_svr_seq_), K(dest_server_), K(dest_svr_seq_), K(tenant_id_), K(file_id_), K(dest_unit_id_));

private:
  int set_task_id(const common::ObAddr& addr);

private:
  share::ObTaskId task_id_;
  ObZoneRecoveryTaskKey task_key_;
  ObServerTaskStatMap::Item* src_server_stat_;
  ObServerTaskStatMap::Item* dest_server_stat_;
  ObTenantTaskStatMap::Item* tenant_stat_;
  int64_t generate_time_;
  int64_t schedule_time_;
  int64_t execute_time_;
  common::ObAddr src_server_;
  int64_t src_svr_seq_;
  common::ObAddr dest_server_;
  int64_t dest_svr_seq_;
  uint64_t tenant_id_;
  int64_t file_id_;
  uint64_t dest_unit_id_;
};

class ObZoneRecoveryTaskQueue {
public:
  typedef common::ObDList<ObZoneRecoveryTask> TaskList;
  typedef common::hash::ObHashMap<ObZoneRecoveryTaskKey, ObZoneRecoveryTask*, common::hash::NoPthreadDefendMode>
      TaskMap;

  ObZoneRecoveryTaskQueue();
  virtual ~ObZoneRecoveryTaskQueue();

  int init(common::ObServerConfig& config, ObTenantTaskStatMap& tenant_stat, ObServerTaskStatMap& server_stat,
      const int64_t bucket_num);

  int push_task(ObZoneRecoveryTaskMgr& task_mgr, const ObZoneRecoveryTask& task);

  int pop_task(ObZoneRecoveryTask*& task);

  int get_schedule_task(const ObZoneRecoveryTask& input_task, ObZoneRecoveryTask*& task);

  int get_discard_in_schedule_task(
      const common::ObAddr& server, const int64_t start_service_time, ObZoneRecoveryTask*& task);

  int discard_waiting_task(const common::ObAddr& server, const int64_t start_service_time);

  int finish_schedule(ObZoneRecoveryTask* task);

  int has_in_schedule_task(const ObZoneRecoveryTaskKey& pkey, bool& has) const;

  int get_not_in_progress_task(obrpc::ObSrvRpcProxy* rpc_proxy, ObServerManager* server_mgr, ObZoneRecoveryTask*& task,
      ZoneFileRecoveryStatus& status);

  int check_fast_recovery_task_status(
      const common::ObZone& zone, const ObZoneRecoveryTask& task, ZoneFileRecoveryStatus& status);

  int do_check_datafile_recovery_status(const common::ObZone& zone, const common::ObAddr& server, const int64_t svr_seq,
      const common::ObAddr& dest_server, const int64_t dest_svr_seq, const uint64_t tenant_id, const int64_t file_id,
      ZoneFileRecoveryStatus& status);

  int notify_zone_server_recovery_machine(const ObZoneRecoveryTask& task, const int ret_code);

  int64_t wait_task_cnt() const
  {
    return wait_list_.get_size();
  }
  int64_t in_schedule_task_cnt() const
  {
    return schedule_list_.get_size();
  }
  int64_t task_cnt()
  {
    return task_map_.size();
  }
  TaskList& get_wait_list()
  {
    return wait_list_;
  }
  TaskList& get_schedule_list()
  {
    return schedule_list_;
  }
  void reuse();

private:
  int do_push_task(ObZoneRecoveryTaskMgr& task_mgr, const ObZoneRecoveryTask& task);

private:
  bool is_inited_;
  common::ObServerConfig* config_;
  ObTenantTaskStatMap* tenant_stat_map_;
  ObServerTaskStatMap* server_stat_map_;
  common::ObFIFOAllocator task_alloc_;
  TaskList wait_list_;
  TaskList schedule_list_;
  TaskMap task_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObZoneRecoveryTaskQueue);
};

class ObZoneRecoveryTaskMgr : public ObRsReentrantThread {
public:
  const static int64_t TASK_QUEUE_LIMIT = 1 << 16;
  const static int64_t QUEUE_LOW_TASK_CNT = TASK_QUEUE_LIMIT / 4;
  const static int64_t ONCE_ADD_TASK_CNT = TASK_QUEUE_LIMIT / 2;
  const static int64_t SAFE_DISCARD_TASK_INTERVAL = 200 * 1000;            // 200ms
  const static int64_t DATA_IN_CLEAR_INTERVAL = 20 * 60 * 1000000;         // 60 min;
  const static int64_t CHECK_IN_PROGRESS_INTERVAL_PER_TASK = 5 * 1000000;  // task 5s every task

public:
  ObZoneRecoveryTaskMgr();

public:
  virtual void run3() override;
  virtual int blocking_run()
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  void stop();

public:
  int init(common::ObServerConfig& config, ObServerManager* server_mgr, ObUnitManager* unit_mgr,
      obrpc::ObSrvRpcProxy* rpc_proxy, ObZoneServerRecoveryMachine* server_recovery_machine);

  int add_task(const ObZoneRecoveryTask& task);

  int add_task(const ObZoneRecoveryTask& task, int64_t& task_cnt);

  int execute_over(const ObZoneRecoveryTask& task, const int ret_code);

  int discard_task(const common::ObAddr& server, const int64_t start_service_time);

  void reuse();
  int set_self(const common::ObAddr& self);
  bool get_reach_concurrency_limit() const
  {
    return reach_concurrency_limited_;
  }
  void clear_reach_concurrency_limit()
  {
    reach_concurrency_limited_ = false;
  }
  void set_reach_concurrency_limit()
  {
    reach_concurrency_limited_ = true;
  }
  int get_all_tasks(common::ObIAllocator& allocator, common::ObIArray<ObZoneRecoveryTask*>& tasks);

private:
  common::ObThreadCond& get_cond()
  {
    return cond_;
  }
  ObZoneRecoveryTaskQueue& get_task_queue()
  {
    return task_queue_;
  }
  int pop_task(ObZoneRecoveryTask*& task);
  int execute_task(const ObZoneRecoveryTask& task);
  int dump_statistics() const;
  int do_execute_over(
      const ObZoneRecoveryTask& task, const int ret_code, const bool need_try_clear_data_in_limit = true);
  int get_task_cnt(int64_t& wait_cnt, int64_t& in_schedule_cnt);
  int check_task_in_progress(int64_t& last_check_task_in_progress_ts);
  int do_check_task_in_progress();
  int64_t task_cnt();
  int set_server_data_in_limit(const ObAddr& addr);
  void try_clear_server_data_in_limit(const ObAddr& addr);
  int notify_server_recovery_machine(const ObZoneRecoveryTask& task, const int ret_code);

private:
  bool inited_;
  volatile bool reach_concurrency_limited_;
  common::ObServerConfig* config_;
  mutable common::ObThreadCond cond_;
  ObTenantTaskStatMap tenant_stat_;
  ObServerTaskStatMap server_stat_;
  ObZoneRecoveryTaskQueue task_queue_;
  common::ObAddr self_;
  ObServerManager* server_mgr_;
  ObUnitManager* unit_mgr_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  ObZoneServerRecoveryMachine* server_recovery_machine_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObZoneRecoveryTaskMgr);
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_ZONE_RECOVERY_TASK_MGR_H_
