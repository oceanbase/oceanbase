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

#ifndef OCEANBASE_STORAGE_OB_BUILD_INDEX_SCHEDULER_H_
#define OCEANBASE_STORAGE_OB_BUILD_INDEX_SCHEDULER_H_

#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "share/ob_ddl_task_executor.h"
#include "lib/wait_event/ob_wait_event.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/ob_build_index_task.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace storage {
class ObPartitionService;
class ObBuildIndexDag;
class ObIPartitionGroupGuard;

class ObBuildIndexBaseTask : public share::ObIDDLTask {
public:
  ObBuildIndexBaseTask(const share::ObIDDLTaskType task_type);
  virtual ~ObBuildIndexBaseTask();
  static int report_index_status(const uint64_t index_table_id, const int64_t partition_id,
      const share::schema::ObIndexStatus index_status, const int build_index_ret, const ObRole role);

protected:
  int check_partition_need_build_index(const common::ObPartitionKey& pkey,
      const share::schema::ObTableSchema& index_schema, const share::schema::ObTableSchema& data_table_schema,
      storage::ObIPartitionGroupGuard& guard, bool& need_build);

private:
  int check_partition_exist_in_current_server(const share::schema::ObTableSchema& index_schema,
      const share::schema::ObTableSchema& data_table_schema, const ObPartitionKey& pkey, ObIPartitionGroupGuard& guard,
      bool& exist);
  int check_restore_need_retry(const uint64_t tenant_id, bool& need_retry);
  int check_partition_split_finish(const ObPartitionKey& pkey, bool& is_split_finished);

protected:
  using ObIDDLTask::is_inited_;
  using ObIDDLTask::need_retry_;
  using ObIDDLTask::task_id_;
};

class ObTenantDDLCheckSchemaTask : public ObBuildIndexBaseTask {
public:
  ObTenantDDLCheckSchemaTask();
  virtual ~ObTenantDDLCheckSchemaTask();
  int init(const uint64_t tenant_id, const int64_t base_version, const int64_t refreshed_version);
  virtual bool operator==(const ObIDDLTask& other) const;
  virtual int64_t hash() const;
  virtual int process();
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  virtual ObIDDLTask* deep_copy(char* buf, const int64_t size) const override;
  TO_STRING_KV(K_(tenant_id), K_(base_version), K_(refreshed_version));
  static int generate_schedule_index_task(const common::ObPartitionKey& pkey, const uint64_t index_id,
      const int64_t schema_version, const bool is_unique_index);

private:
  int find_build_index_partitions(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& guard, common::ObIArray<common::ObPartitionKey>& partition_keys);
  int get_candidate_tables(common::ObIArray<uint64_t>& table_ids);
  static int create_index_partition_table_store(
      const common::ObPartitionKey& pkey, const uint64_t index_id, const int64_t schema_version);
  int get_candidate_tenants(common::ObIArray<uint64_t>& tenant_ids);
  int process_schedule_build_index_task();
  int process_tenant_memory_task();

private:
  int64_t base_version_;
  int64_t refreshed_version_;
  uint64_t tenant_id_;
};

class ObBuildIndexScheduleTask : public ObBuildIndexBaseTask {
public:
  enum State {
    WAIT_TRANS_END = 0,
    WAIT_SNAPSHOT_READY,
    CHOOSE_BUILD_INDEX_REPLICA,
    WAIT_CHOOSE_OR_BUILD_INDEX_END,
    COPY_BUILD_INDEX_DATA,
    UNIQUE_INDEX_CHECKING,
    WAIT_REPORT_STATUS,
    END,
  };
  ObBuildIndexScheduleTask();
  virtual ~ObBuildIndexScheduleTask();
  int init(const common::ObPartitionKey& pkey, const uint64_t index_id, const int64_t schema_version,
      const bool is_unique_index);
  virtual bool operator==(const ObIDDLTask& other) const;
  virtual int64_t hash() const;
  virtual int process();
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  virtual ObIDDLTask* deep_copy(char* buf, const int64_t size) const override;
  TO_STRING_KV(K_(pkey), K_(index_id), K_(schema_version), K_(state));

private:
  int check_trans_end(bool& is_trans_end, int64_t& snapshot_version);
  int report_trans_status(const int trans_status, const int64_t snapshot_version);
  int wait_trans_end(const bool is_leader);
  int check_rs_snapshot_elapsed(const int64_t snapshot_version, bool& is_elapsed);
  int wait_snapshot_ready(const bool is_leader);
  int choose_build_index_replica(const bool is_leader);
  int check_need_choose_replica(bool& need);
  int check_need_schedule_dag(bool& need_schedule_dag);
  int check_build_index_end(bool& build_index_end, bool& need_copy);
  int wait_choose_or_build_index_end(const bool is_leader);
  int check_all_replica_report_build_index_end(const bool is_leader, bool& is_end);
  int wait_report_status(const bool is_leader);
  int get_snapshot_version(int64_t& snapshot_version);
  int get_role(common::ObRole& role);
  int schedule_dag();
  int alloc_index_prepare_task(ObBuildIndexDag* dag);
  int copy_build_index_data(const bool is_leader);
  int send_copy_replica_rpc();
  int get_candidate_source_replica(const bool need_refresh = false);
  int unique_index_checking(const bool is_leader);
  int rollback_state(const int state);

private:
  static const int64_t COPY_BUILD_INDEX_DATA_TIMEOUT = 10 * 1000 * 1000LL;  // 10s
  static const int64_t REFRESH_CANDIDATE_REPLICA_COUNT = 6;                 // 60s
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  int64_t schema_version_;
  int state_;
  bool is_dag_scheduled_;
  common::ObRole last_role_;
  int64_t last_active_timestamp_;
  bool is_unique_index_;
  bool is_copy_request_sent_;
  int64_t retry_cnt_;
  int64_t build_snapshot_version_;
  ObAddr candidate_replica_;
};

class ObCheckTenantSchemaTask : public common::ObTimerTask {
public:
  ObCheckTenantSchemaTask();
  virtual ~ObCheckTenantSchemaTask();
  int init();
  virtual void runTimerTask() override;
  void destroy();

private:
  static const int64_t DEFAULT_TENANT_BUCKET_NUM = 100;
  bool is_inited_;
  lib::ObMutex lock_;
  // key: tenant_id, value: schema_version
  common::hash::ObHashMap<uint64_t, int64_t> tenant_refresh_map_;
};

class ObBuildIndexScheduler {
public:
  static const int64_t DEFAULT_THREAD_CNT = 4;
  static const int64_t MINI_MODE_THREAD_CNT = 1;

public:
  int init();
  static ObBuildIndexScheduler& get_instance();
  int push_task(ObBuildIndexScheduleTask& task);
  int add_tenant_ddl_task(const ObTenantDDLCheckSchemaTask& task);
  void destroy();
  void stop();
  void wait();

private:
  ObBuildIndexScheduler();
  virtual ~ObBuildIndexScheduler();

private:
  static const int64_t DEFAULT_DDL_BUCKET_NUM = 100000;
  static const int64_t CHECK_TENANT_SCHEMA_INTERVAL_US = 100 * 1000L;
  bool is_inited_;
  share::ObDDLTaskExecutor task_executor_;
  common::ObSpinLock lock_;
  ObCheckTenantSchemaTask check_tenant_schema_task_;
  bool is_stop_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_BUILD_INDEX_SCHEDULER_H_
