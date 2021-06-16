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

#ifndef OCEANBASE_SQL_OB_INDEX_SSTABLE_BUILDER_H_
#define OCEANBASE_SQL_OB_INDEX_SSTABLE_BUILDER_H_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "sql/executor/ob_determinate_task_transmit.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share
namespace sql {

class ObSqlCtx;
class ObResultSet;
class ObPhyOperator;
class ObPhysicalPlan;
class ObExecutorRpcImpl;
class ObExecutorRpcCtx;

// Build global index data from snapshot of data table.
class ObIndexSSTableBuilder {
public:
  typedef common::ObArray<common::ObArray<common::ObNewRange>> RangeArrayArray;
  struct BuildIndexJob {
    int64_t job_id_;  // system job id
    int64_t schema_version_;
    int64_t snapshot_version_;
    uint64_t data_table_id_;
    uint64_t index_table_id_;
    int64_t degree_of_parallelism_;

    BuildIndexJob()
        : job_id_(0),
          schema_version_(0),
          snapshot_version_(0),
          data_table_id_(OB_INVALID_ID),
          index_table_id_(OB_INVALID_ID),
          degree_of_parallelism_(0)
    {}

    bool is_valid() const
    {
      return job_id_ >= 0 && schema_version_ > 0 && snapshot_version_ > 0 && OB_INVALID_ID != data_table_id_ &&
             OB_INVALID_ID != index_table_id_ && degree_of_parallelism_ > 0;
    }

    TO_STRING_KV(K(job_id_), K(schema_version_), K(snapshot_version_), K(data_table_id_), K(index_table_id_),
        K(degree_of_parallelism_));
  };

  class ReplicaPicker : public ObDeterminateTaskTransmit::ITaskRouting {
  public:
    ReplicaPicker(){};
    virtual ~ReplicaPicker()
    {}

    virtual int route(Policy policy, const ObTaskInfo& task, const common::ObIArray<common::ObAddr>& previous,
        ObAddr& server) override;

    virtual int pick_data_replica(
        const common::ObPartitionKey& pkey, const ObIArray<ObAddr>& previous, ObAddr& server) = 0;

    virtual int pick_index_replica(
        const common::ObPartitionKey& pkey, const ObIArray<ObAddr>& previous, ObAddr& server) = 0;
  };

  // routeing task && record task location
  class TaskRoutingPorxy : public ObDeterminateTaskTransmit::ITaskRouting {
  public:
    TaskRoutingPorxy(ObIndexSSTableBuilder& builder) : routing_(NULL), builder_(builder)
    {}
    virtual int route(Policy policy, const ObTaskInfo& task, const common::ObIArray<common::ObAddr>& previous,
        ObAddr& server) override;
    void set_routing(ObDeterminateTaskTransmit::ITaskRouting* routing)
    {
      routing_ = routing;
    }

  private:
    ObDeterminateTaskTransmit::ITaskRouting* routing_;
    ObIndexSSTableBuilder& builder_;
  };

  // for serialize to store param in inner table.
  class BuildParam;

  class ObBuildExecutor;
  class ObClearExecutor;

  ObIndexSSTableBuilder();
  virtual ~ObIndexSSTableBuilder();

  int init(common::ObMySQLProxy& sql_proxy, common::ObOracleSqlProxy& oracle_sql_proxy, const BuildIndexJob& job,
      ReplicaPicker& picker, int64_t const abs_timeout_us);
  bool is_inited() const
  {
    return inited_;
  }

  int build();
  static int reclaim_intermediate_result(const int64_t job_id);

  // clear intermediate result by job
  static int clear_interm_result(common::ObMySQLProxy& sql_proxy, const int64_t job_id);
  static int clear_interm_result(
      const int64_t job_id, common::ObMySQLProxy& sql_proxy, ObExecutorRpcCtx& ctx, ObExecutorRpcImpl& rpc_proxy);

  static int query_execution_id(
      uint64_t& execution_id, const int64_t job_id, const int64_t snapshot_version, common::ObMySQLProxy& sql_proxy);

private:
  class BuildIndexGuard;
  int build(ObSql& sql_engine, ObSqlCtx& sql_ctx, ObResultSet& result);

  // generate physical plan
  int gen_data_scan(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op);
  int gen_build_macro(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op);
  int gen_build_sstable(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op);

  int gen_data_exchange(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op);
  int gen_macro_exchange(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op);
  int gen_sstable_exchange(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op);

  int alloc_determinate_transmit(ObPhysicalPlan& phy_plan, ObDeterminateTaskTransmit*& transmit);

  int column_ids2desc(const share::schema::ObTableSchema& table, const common::ObIArray<uint64_t>& ids,
      common::ObIArray<share::schema::ObColDesc>& descs) const;

  int fill_table_location(ObPhyTableLocation& loc, ObResultSet& res, const share::schema::ObTableSchema& table);
  int fill_locations(ObResultSet& res);

  // detect %scan_table_id_ && divide ranges.
  int generate_build_param();
  int store_build_param();
  int load_build_param();

  int split_ranges();
  int split_ranges(common::ObArray<common::ObArray<common::ObNewRange>>& ranges,
      const share::schema::ObTableSchema& sample_table, const int64_t row_cnt,
      const share::schema::ObTableSchema& split_table, const int64_t range_cnt);
  int concat_column_names(common::ObSqlString& str, const common::ObIArray<share::schema::ObColDesc>& columns,
      const bool need_name, const bool need_alias);

  int add_access_replica(const common::ObPartitionKey& pkey, const common::ObAddr& addr);
  int finish_add_access_replica(ObResultSet& res);

  const char* name_quote() const
  {
    return oracle_mode_ ? "\"" : "`";
  }

private:
  bool inited_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObCommonSqlProxy* user_sql_proxy_;
  // The builder is running in background thread, can not rely on the lib::is_oracle_mode()
  bool oracle_mode_;

  ObSqlCtx* sql_ctx_;
  BuildIndexJob job_;
  uint64_t execution_id_;
  uint64_t scan_table_id_;

  TaskRoutingPorxy task_routing_proxy_;

  common::ObArenaAllocator allocator_;

  common::ObArray<common::ObArray<common::ObNewRange>> scan_ranges_;
  common::ObArray<common::ObArray<common::ObNewRange>> index_ranges_;

  const share::schema::ObTableSchema* data_table_;
  const share::schema::ObTableSchema* scan_table_;
  const share::schema::ObTableSchema* index_table_;

  int64_t abs_timeout_;

  common::ObArray<std::pair<common::ObPartitionKey, common::ObAddr>> access_replicas_;
  ObLatch access_replica_latch_;  // only protect access_replicas_

  DISALLOW_COPY_AND_ASSIGN(ObIndexSSTableBuilder);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_OB_INDEX_SSTABLE_BUILDER_H_
