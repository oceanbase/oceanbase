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

#ifndef OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_

#include "lib/container/ob_array.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {

namespace common {
class ObMySQLProxy;
}

namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share

namespace obrpc {
class ObSrvRpcProxy;
}

namespace rootserver {
class ObServerManager;
class ObZoneManager;
class ObDDLService;

class ObIndexBuildStatus {
public:
  class PartitionIndexStatus {
  public:
    PartitionIndexStatus();
    ~PartitionIndexStatus()
    {}

    void reset();
    bool is_valid() const;
    bool operator<(const PartitionIndexStatus& o) const;
    bool operator==(const PartitionIndexStatus& o) const;
    bool operator!=(const PartitionIndexStatus& o) const
    {
      return !operator==(o);
    }

    TO_STRING_KV(K_(partition_id), K_(server), K_(index_status), K_(ret_code));

    int64_t partition_id_;
    common::ObAddr server_;
    share::schema::ObIndexStatus index_status_;
    int ret_code_;
  };

  class PartitionIndexStatusOrder {
  public:
    explicit PartitionIndexStatusOrder(int& ret) : ret_(ret)
    {}
    ~PartitionIndexStatusOrder()
    {}
    bool operator()(const PartitionIndexStatus& left, const PartitionIndexStatus& right) const;

  private:
    int& ret_;
  };

  ObIndexBuildStatus() : loaded_(false), all_status_()
  {}
  virtual ~ObIndexBuildStatus()
  {}
  int load_all(const uint64_t index_table_id, const int64_t partition_id, common::ObMySQLProxy& sql_proxy);
  int find(const int64_t partition_id, const common::ObAddr& server, PartitionIndexStatus& status) const;
  TO_STRING_KV(K_(loaded), K_(all_status));

private:
  bool loaded_;
  common::ObArray<PartitionIndexStatus> all_status_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexBuildStatus);
};

class ObIndexWaitTransStatus {
public:
  class PartitionWaitTransStatus {
  public:
    PartitionWaitTransStatus();
    virtual ~PartitionWaitTransStatus();
    bool operator<(const PartitionWaitTransStatus& other) const;
    bool operator==(const PartitionWaitTransStatus& other) const;
    bool operator!=(const PartitionWaitTransStatus& other) const;
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(partition_id), K_(trans_status), K_(snapshot_version), K_(schema_version));
    int64_t partition_id_;
    int trans_status_;
    int64_t snapshot_version_;
    int64_t schema_version_;
  };

  class PartitionWaitTransStatusComparator {
  public:
    explicit PartitionWaitTransStatusComparator(int& ret) : ret_(ret)
    {}
    virtual ~PartitionWaitTransStatusComparator()
    {}
    bool operator()(const PartitionWaitTransStatus& lhs, const PartitionWaitTransStatus& rhs) const;

  private:
    int& ret_;
  };
  ObIndexWaitTransStatus();
  virtual ~ObIndexWaitTransStatus();
  int check_wait_trans_end(const uint64_t index_id, ObMySQLProxy& sql_proxy, bool& is_end);
  int get_wait_trans_status(const uint64_t index_id, ObMySQLProxy& sql_proxy);
  int find_status(const int64_t partition_id, PartitionWaitTransStatus& status) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexWaitTransStatus);
  bool loaded_;
  common::ObArray<PartitionWaitTransStatus> all_wait_trans_status_;
};

class ObRSBuildIndexTask : public share::ObIDDLTask {
public:
  enum TaskState {
    WAIT_TRANS_END = 0,
    WAIT_BUILD_INDEX_END,
    BUILD_INDEX_FINISH,
  };
  ObRSBuildIndexTask();
  virtual ~ObRSBuildIndexTask();
  int init(
      const uint64_t index_id, const uint64_t data_table_id, const int64_t schema_version, ObDDLService* ddl_service);
  virtual int64_t hash() const;
  virtual int process();
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  virtual ObRSBuildIndexTask* deep_copy(char* buf, const int64_t size) const;
  bool operator==(const ObIDDLTask& other) const;
  int report_index_status(const share::schema::ObIndexStatus index_status);
  int generate_index_build_stat_record();
  TO_STRING_KV(K_(state), K_(index_id), K_(data_table_id), K_(schema_version), KP_(ddl_service));
  int64_t get_tenant_id() const
  {
    return extract_tenant_id(index_id_);
  }
  int64_t get_data_table_id() const
  {
    return data_table_id_;
  }

private:
  int wait_trans_end(bool& is_end);
  int wait_build_index_end(bool& is_end);
  bool need_print_log();
  int calc_snapshot_version(const int64_t max_commit_version, int64_t &snapshot_version);
  int acquire_snapshot(const int64_t snapshot_version, const int64_t data_table_id, const int64_t schema_version,
      common::ObMySQLTransaction &trans);
  int release_snapshot();
  int remove_index_build_stat_record();

private:
  static const int64_t PRINT_LOG_INTERVAL = 600 * 1000000;
  static const int64_t INDEX_SNAPSHOT_VERSION_DIFF = 100 * 1000;  // 100ms
  TaskState state_;
  int64_t data_table_id_;
  uint64_t index_id_;
  int64_t schema_version_;
  ObDDLService* ddl_service_;
  int64_t last_log_timestamp_;
};

class ObRSBuildIndexScheduler {
public:
  static const int64_t DEFAULT_THREAD_CNT = 1;

public:
  int init(ObDDLService* ddl_service);
  static ObRSBuildIndexScheduler& get_instance();
  int push_task(ObRSBuildIndexTask& task);
  void destroy();

private:
  ObRSBuildIndexScheduler();
  virtual ~ObRSBuildIndexScheduler();
  void stop();
  void wait();

private:
  static const int64_t DEFAULT_BUCKET_NUM = 10000;
  bool is_inited_;
  share::ObDDLTaskExecutor task_executor_;
  bool is_stop_;
  ObDDLService* ddl_service_;
};

class ObIndexBuilder {
public:
  explicit ObIndexBuilder(ObDDLService& ddl_service);
  virtual ~ObIndexBuilder();

  int create_index(const obrpc::ObCreateIndexArg& arg, const int64_t frozen_version);
  int drop_index(const obrpc::ObDropIndexArg& arg);

  // Check and update local index status.
  // if not all index table updated return OB_EAGAIN.
  int update_local_index_status(const volatile bool& stop, const int64_t merged_version);
  int do_create_index(const obrpc::ObCreateIndexArg& arg, const int64_t frozen_version);
  int do_create_global_index(share::schema::ObSchemaGetterGuard& schema_guard, const obrpc::ObCreateIndexArg& arg,
      const share::schema::ObTableSchema& table_schema, const int64_t frozen_version);
  int do_create_local_index(const obrpc::ObCreateIndexArg& arg, const share::schema::ObTableSchema& table_schema,
      const int64_t frozen_version);
  int generate_schema(const obrpc::ObCreateIndexArg& arg, const int64_t frozen_version,
      share::schema::ObTableSchema& data_schema, const bool global_index_without_column_info,
      share::schema::ObTableSchema& index_schema);
  int submit_build_global_index_task(const share::schema::ObTableSchema& index_schema);
  int submit_build_local_index_task(const share::schema::ObTableSchema& index_schema);

private:
  typedef common::ObArray<std::pair<int64_t, common::ObString> > OrderFTColumns;
  class FulltextColumnOrder {
  public:
    FulltextColumnOrder()
    {}
    ~FulltextColumnOrder()
    {}

    bool operator()(
        const std::pair<int64_t, common::ObString>& left, const std::pair<int64_t, common::ObString>& right) const
    {
      return left.first < right.first;
    }
  };

  int set_basic_infos(const obrpc::ObCreateIndexArg& arg, const int64_t frozen_version,
      const share::schema::ObTableSchema& data_schema, share::schema::ObTableSchema& schema);
  int set_index_table_columns(const obrpc::ObCreateIndexArg& arg, const share::schema::ObTableSchema& data_schema,
      share::schema::ObTableSchema& schema);
  int set_index_table_options(const obrpc::ObCreateIndexArg& arg, const share::schema::ObTableSchema& data_schema,
      share::schema::ObTableSchema& schema);

  bool is_final_index_status(const share::schema::ObIndexStatus index_status, const bool is_dropped_schema) const;

  int update_local_index_status(const volatile bool& stop, const int64_t merged_version, const uint64_t index_table_id);

private:
  ObDDLService& ddl_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexBuilder);
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_
