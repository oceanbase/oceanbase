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

#define USING_LOG_PREFIX SQL

#include "ob_index_sstable_builder.h"
#include "executor/ob_executor_rpc_impl.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_max_id_fetcher.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/stat/ob_stat_manager.h"
#include "share/stat/ob_table_stat.h"
#include "sql/engine/dml/ob_table_append_local_sort_data.h"
#include "sql/engine/dml/ob_table_append_sstable.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/sort/ob_sort.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_uk_row_transform.h"
#include "sql/executor/ob_determinate_task_transmit.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql.h"
#include "sql/plan_cache/ob_cache_object_factory.h"

namespace oceanbase {
namespace sql {

using namespace common;
using namespace share;
using namespace share::schema;

class ObIndexSSTableBuilder::BuildParam {
  OB_UNIS_VERSION_V(1);

public:
  BuildParam(
      uint64_t& data_table_id, RangeArrayArray& data_ranges, RangeArrayArray& index_ranges, common::ObIAllocator& alloc)
      : scan_table_id_(data_table_id), scan_ranges_(data_ranges), index_ranges_(index_ranges), alloc_(alloc)
  {}

  TO_STRING_KV(K(scan_table_id_), K(scan_ranges_), K(index_ranges_));

private:
  uint64_t& scan_table_id_;
  RangeArrayArray& scan_ranges_;
  RangeArrayArray& index_ranges_;
  ObIAllocator& alloc_;
};

class ObIndexSSTableBuilder::ObBuildExecutor : public sqlclient::ObIExecutor {
public:
  ObBuildExecutor(ObIndexSSTableBuilder& builder) : ObIExecutor(), builder_(builder)
  {}
  virtual ~ObBuildExecutor()
  {}

  virtual int64_t get_schema_version() const override
  {
    return builder_.job_.schema_version_;
  }

  virtual int execute(ObSql& engine, ObSqlCtx& ctx, ObResultSet& res) override
  {
    return builder_.build(engine, ctx, res);
  }
  virtual int process_result(ObResultSet& res) override
  {
    const ObNewRow* row = NULL;
    int ret = OB_SUCCESS;
    while (OB_SUCC(res.get_next_row(row))) {}
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get next row failed", K(ret));
    }

    int tmp_ret = builder_.finish_add_access_replica(res);
    if (OB_SUCCESS != tmp_ret) {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }

    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("build index sstable", ObIExecutor, "job", builder_.job_);

private:
  ObIndexSSTableBuilder& builder_;
};

class ObIndexSSTableBuilder::ObClearExecutor : public sqlclient::ObIExecutor {
public:
  ObClearExecutor(int64_t job_id) : ObIExecutor(), job_id_(job_id)
  {}
  virtual ~ObClearExecutor()
  {}

  virtual int execute(ObSql& engine, ObSqlCtx& ctx, ObResultSet& res) override;

  virtual int process_result(ObResultSet&) override
  {
    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("clear index sstable builder intermediate result", ObIExecutor, K(job_id_));

private:
  int64_t job_id_;
};

class ObIndexSSTableBuilder::BuildIndexGuard : public common::ObDLinkBase<ObIndexSSTableBuilder::BuildIndexGuard> {
public:
  typedef ObIndexSSTableBuilder::BuildIndexGuard self_t;
  BuildIndexGuard(const uint64_t index_table_id) : unique_(true), index_table_id_(index_table_id)
  {
    ObLatchWGuard g(get_lock(), ObLatchIds::DEFAULT_MUTEX);
    DLIST_FOREACH_X(it, get_list(), unique_)
    {
      if (it->index_table_id_ == index_table_id) {
        unique_ = false;
      }
    }
    if (unique_) {
      if (!get_list().add_last(this)) {
        LOG_WARN("list add node failed");
      }
    }
  }

  virtual ~BuildIndexGuard()
  {
    ObLatchWGuard g(get_lock(), ObLatchIds::DEFAULT_MUTEX);
    unlink();
  }

  bool is_unique() const
  {
    return unique_;
  }

private:
  static ObLatch& get_lock()
  {
    static ObLatch latch;
    return latch;
  }
  static common::ObDList<self_t>& get_list()
  {
    static common::ObDList<self_t> list;
    return list;
  }

  bool unique_;
  uint64_t index_table_id_;
};

int ObIndexSSTableBuilder::ObClearExecutor::execute(ObSql& sql_engine, ObSqlCtx& ctx, ObResultSet& res)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = ctx.session_info_;
  int64_t timeout = 0;
  if (job_id_ < 0 || OB_ISNULL(GCTX.executor_rpc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job id", K(ret), K(job_id_), K(GCTX.executor_rpc_));
  } else if (OB_FAIL(sql_engine.init_result_set(ctx, res))) {
    LOG_WARN("init result set failed", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL session", K(ret));
  } else if (OB_FAIL(session->get_query_timeout(timeout))) {
    LOG_WARN("get timeout failed", K(ret));
  } else {
    ObExecutorRpcCtx rpc_ctx(session->get_effective_tenant_id(),
        session->get_query_start_time() + timeout,
        res.get_exec_context().get_task_exec_ctx().get_min_cluster_version(),
        NULL /* retry_info */,
        session,
        false /* is_plain_select */);
    if (OB_FAIL(clear_interm_result(job_id_, *ctx.sql_proxy_, rpc_ctx, *GCTX.executor_rpc_))) {
      LOG_WARN("clear interm result failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObIndexSSTableBuilder::BuildParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, scan_table_id_);

  const RangeArrayArray* all_range[] = {&scan_ranges_, &index_ranges_};
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(all_range); i++) {
    auto& range_array_array = *all_range[i];
    int64_t range_array_cnt = range_array_array.count();
    LST_DO_CODE(OB_UNIS_ENCODE, range_array_cnt);
    FOREACH_X(rangearray, range_array_array, OB_SUCC(ret))
    {
      int64_t range_cnt = rangearray->count();
      LST_DO_CODE(OB_UNIS_ENCODE, range_cnt);
      FOREACH_X(range, *rangearray, OB_SUCC(ret))
      {
        LST_DO_CODE(OB_UNIS_ENCODE, *range);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObIndexSSTableBuilder::BuildParam)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN, scan_table_id_);

  const RangeArrayArray* all_range[] = {&scan_ranges_, &index_ranges_};
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(all_range); i++) {
    auto& range_array_array = *all_range[i];
    int64_t range_array_cnt = range_array_array.count();
    LST_DO_CODE(OB_UNIS_ADD_LEN, range_array_cnt);
    FOREACH_X(rangearray, range_array_array, OB_SUCC(ret))
    {
      int64_t range_cnt = rangearray->count();
      LST_DO_CODE(OB_UNIS_ADD_LEN, range_cnt);
      FOREACH_X(range, *rangearray, OB_SUCC(ret))
      {
        LST_DO_CODE(OB_UNIS_ADD_LEN, *range);
      }
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObIndexSSTableBuilder::BuildParam)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, scan_table_id_);

  ObObj objs[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  ObNewRange derange;

  RangeArrayArray* all_range[] = {&scan_ranges_, &index_ranges_};
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(all_range); i++) {
    auto& range_array_array = *all_range[i];
    int64_t range_array_cnt = 0;
    LST_DO_CODE(OB_UNIS_DECODE, range_array_cnt);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(range_array_array.prepare_allocate(range_array_cnt))) {
      LOG_WARN("array prepare allocate failed", K(ret), K(range_array_cnt));
    } else {
      FOREACH_X(rangearray, range_array_array, OB_SUCC(ret))
      {
        int64_t range_cnt = 0;
        LST_DO_CODE(OB_UNIS_DECODE, range_cnt);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rangearray->prepare_allocate(range_cnt))) {
          LOG_WARN("array prepare allocate failed", K(range_cnt));
        } else {
          FOREACH_X(range, *rangearray, OB_SUCC(ret))
          {
            derange.start_key_.assign(objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
            derange.end_key_.assign(objs + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
            LST_DO_CODE(OB_UNIS_DECODE, derange);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(deep_copy_range(alloc_, derange, *range))) {
              LOG_WARN("deep copy range failed", K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObIndexSSTableBuilder::ReplicaPicker::route(
    Policy policy, const ObTaskInfo& task, const ObIArray<common::ObAddr>& previous, ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (SAME_WITH_CHILD == policy) {
    if (task.get_child_task_results().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no child result", K(ret));
    } else {
      server = task.get_child_task_results().at(0).get_task_location().get_server();
    }
  } else {
    ObPartitionKey pkey;
    if (task.get_range_location().part_locs_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty partition location", K(ret));
    } else {
      pkey = task.get_range_location().part_locs_.at(0).partition_key_;

      if (DATA_REPLICA_PICKER == policy) {
        if (OB_FAIL(pick_data_replica(pkey, previous, server))) {
          LOG_WARN("pick data replica failed", K(ret), K(pkey), K(previous));
        }
      } else if (INDEX_REPLICA_PICKER == policy) {
        if (OB_FAIL(pick_index_replica(pkey, previous, server))) {
          LOG_WARN("pick data replica failed", K(ret), K(pkey), K(previous));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown policy", K(ret), K(policy));
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::TaskRoutingPorxy::route(
    Policy policy, const ObTaskInfo& task, const ObIArray<common::ObAddr>& previous, ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(routing_) || task.get_range_location().part_locs_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(routing_));
  } else if (OB_FAIL(routing_->route(policy, task, previous, server))) {
    LOG_WARN("route failed", K(ret), K(task), K(previous));
  } else if (OB_FAIL(builder_.add_access_replica(task.get_range_location().part_locs_.at(0).partition_key_, server))) {
    LOG_WARN("add access replica failed", K(ret), K(task));
  }
  return ret;
}

ObIndexSSTableBuilder::ObIndexSSTableBuilder()
    : inited_(false),
      sql_proxy_(NULL),
      user_sql_proxy_(NULL),
      oracle_mode_(false),
      sql_ctx_(NULL),
      execution_id_(OB_INVALID_ID),
      scan_table_id_(OB_INVALID_ID),
      task_routing_proxy_(*this),
      data_table_(NULL),
      scan_table_(NULL),
      index_table_(NULL),
      abs_timeout_(0)
{}

ObIndexSSTableBuilder::~ObIndexSSTableBuilder()
{}

int ObIndexSSTableBuilder::build()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(timeout_ctx.set_abs_timeout(abs_timeout_))) {
    LOG_WARN("set timeout failed", K(ret));
  } else {
    BuildIndexGuard build_index_guard(job_.index_table_id_);
    if (build_index_guard.is_unique()) {
      ObBuildExecutor executor(*this);
      if (OB_FAIL(user_sql_proxy_->execute(extract_tenant_id(job_.data_table_id_), executor))) {
        LOG_WARN("build index sstable failed", K(executor));
      }
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("another index sstable builder of this index table exist, need retry", K(ret), K(job_));
    }
  }
  LOG_INFO("build index sstable finish", K(ret), K(job_), K(execution_id_), K(abs_timeout_));
  return ret;
}

int ObIndexSSTableBuilder::init(common::ObMySQLProxy& sql_proxy, common::ObOracleSqlProxy& oracle_sql_proxy,
    const BuildIndexJob& job, ReplicaPicker& picker, int64_t const abs_timeout_us)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObSysVariableSchema* sys_variable_schema = NULL;
  oracle_mode_ = false;
  const uint64_t fetch_tenant_id =
      is_sys_table(job.data_table_id_) ? OB_SYS_TENANT_ID : extract_tenant_id(job.data_table_id_);
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(job));
  } else if (OB_FAIL(
                 ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(fetch_tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret), K(fetch_tenant_id));
  } else if (OB_FAIL(
                 schema_guard.get_sys_variable_schema(extract_tenant_id(job.data_table_id_), sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret), "tenant_id", extract_tenant_id(job.data_table_id_));
  } else if (NULL == sys_variable_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is NULL", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(oracle_mode_))) {
    LOG_WARN("get oracle mode failed", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    if (oracle_mode_) {
      user_sql_proxy_ = &oracle_sql_proxy;
    } else {
      user_sql_proxy_ = sql_proxy_;
    }
    job_ = job;
    task_routing_proxy_.set_routing(&picker);
    abs_timeout_ = abs_timeout_us;
    inited_ = true;
  }
  return ret;
}

int ObIndexSSTableBuilder::build(ObSql& sql_engine, ObSqlCtx& sql_ctx, ObResultSet& result)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan* phy_plan = NULL;
  ObPhyOperator* cur_op = NULL;
  sql_ctx_ = &sql_ctx;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(sql_engine.init_result_set(sql_ctx, result))) {
    LOG_WARN("init result set failed", K(ret));
  } else if (OB_ISNULL(result.get_exec_context().get_task_executor_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL task execute context", K(ret));
  } else if (OB_ISNULL(sql_ctx.schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL schema guard", K(ret));
  } else if (OB_FAIL(sql_ctx.schema_guard_->get_table_schema(job_.data_table_id_, data_table_))) {
    LOG_WARN("get table schema failed", K(ret), K(job_.data_table_id_));
  } else if (OB_ISNULL(data_table_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(job_.data_table_id_));
  } else if (OB_FAIL(sql_ctx.schema_guard_->get_table_schema(job_.index_table_id_, index_table_))) {
    LOG_WARN("get table schema failed", K(ret), "table_id", job_.index_table_id_);
  } else if (OB_ISNULL(index_table_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), "table_id", job_.index_table_id_);
  } else if (OB_FAIL(generate_build_param())) {
    LOG_WARN("generate build param failed", K(ret));
  } else if (OB_FAIL(sql_ctx.schema_guard_->get_table_schema(scan_table_id_, scan_table_))) {
    LOG_WARN("get table schema failed", K(ret), K(scan_table_id_));
  } else if (OB_ISNULL(scan_table_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(scan_table_id_));
  } else {
    result.get_session().set_read_snapshot_version(job_.snapshot_version_);
    result.get_exec_context().get_task_executor_ctx()->set_sys_job_id(job_.job_id_);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObCacheObjectFactory::alloc(
                 phy_plan, INDEX_BUILDER_HANDLE, result.get_session().get_effective_tenant_id()))) {
    LOG_WARN("alloc physical plan failed", K(ret));
  } else if (OB_FAIL(gen_data_scan(*phy_plan, cur_op))) {
    LOG_WARN("generate data scan failed", K(ret));
  } else if (OB_FAIL(gen_data_exchange(*phy_plan, cur_op))) {
    LOG_WARN("generate data exchange failed", K(ret));
  } else if (OB_FAIL(gen_build_macro(*phy_plan, cur_op))) {
    LOG_WARN("generate build macro plan failed", K(ret));
  } else if (OB_FAIL(gen_macro_exchange(*phy_plan, cur_op))) {
    LOG_WARN("generate macro exchange failed", K(ret));
  } else if (OB_FAIL(gen_build_sstable(*phy_plan, cur_op))) {
    LOG_WARN("generate build sstable plan failed", K(ret));
  } else if (OB_FAIL(gen_sstable_exchange(*phy_plan, cur_op))) {
    LOG_WARN("generate sstable exchange failed", K(ret));
  } else if (OB_FAIL(result.get_exec_context().init_physical_plan_ctx(*phy_plan))) {
    LOG_WARN("init physical plan ctx failed", K(ret));
  } else {
    phy_plan->set_main_query(cur_op);
  }

  int64_t sys_schema_version = OB_INVALID_ID;
  int64_t tenant_schema_version = OB_INVALID_ID;
  if (OB_SUCC(ret)) {
    int64_t tenant_id = result.get_session().get_effective_tenant_id();
    if (OB_FAIL(sql_ctx.schema_guard_->get_schema_version(tenant_id, tenant_schema_version))) {
      LOG_WARN("fail to get schema version", K(ret), K(tenant_id));
    } else if (OB_FAIL(sql_ctx.schema_guard_->get_schema_version(OB_SYS_TENANT_ID, sys_schema_version))) {
      LOG_WARN("fail to get schema version", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    phy_plan->set_location_type(OB_PHY_PLAN_UNCERTAIN);
    phy_plan->set_plan_type(OB_PHY_PLAN_DISTRIBUTED);
    phy_plan->set_tenant_schema_version(tenant_schema_version);
    phy_plan->set_sys_schema_version(sys_schema_version);
    phy_plan->set_stmt_type(stmt::T_BUILD_INDEX_SSTABLE);
    phy_plan->get_query_hint().parallel_ = job_.degree_of_parallelism_;

    ObSEArray<schema::ObSchemaObjVersion, 3> obj_vers;
    const schema::ObTableSchema* tables[] = {data_table_, scan_table_, index_table_};
    if (data_table_->get_table_id() == scan_table_->get_table_id()) {
      tables[1] = NULL;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tables); i++) {
      if (NULL == tables[i]) {
        continue;
      }
      schema::ObSchemaObjVersion obj_ver;
      obj_ver.object_id_ = tables[i]->get_table_id();
      obj_ver.object_type_ = schema::DEPENDENCY_TABLE;
      obj_ver.version_ = tables[i]->get_schema_version();
      if (OB_FAIL(obj_vers.push_back(obj_ver))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_plan->get_dependency_table().assign(obj_vers))) {
        LOG_WARN("array assign failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_locations(result))) {
      LOG_WARN("fill table locations failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    result.set_physical_plan(INDEX_BUILDER_HANDLE, phy_plan);
    result.set_returning(false);
    result.get_exec_context().set_execution_id(execution_id_);
    result.get_exec_context().set_reusable_interm_result(true);
  } else {
    if (NULL != phy_plan) {
      ObCacheObjectFactory::free(phy_plan, INDEX_BUILDER_HANDLE);
      phy_plan = NULL;
    }
  }

  return ret;
}

int ObIndexSSTableBuilder::column_ids2desc(
    const schema::ObTableSchema& table, const ObIArray<uint64_t>& ids, ObIArray<schema::ObColDesc>& descs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = descs.count(); OB_SUCC(ret) && i < ids.count(); i++) {
    const ObColumnSchemaV2* col = table.get_column_schema(ids.at(i));
    if (OB_ISNULL(col)) {
      ret = OB_SUCCESS;
      LOG_WARN("column not found", K(ret), K(ids.at(i)));
    } else {
      schema::ObColDesc desc;
      desc.col_id_ = col->get_column_id();
      desc.col_type_ = col->get_meta_type();
      if (OB_FAIL(descs.push_back(desc))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::gen_data_scan(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_ctx_) || OB_ISNULL(sql_ctx_->session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql context not set", K(ret));
  } else if (OB_ISNULL(scan_table_) || OB_ISNULL(index_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table or index_table schema is NULL", K(ret), KP(scan_table_), KP(index_table_));
  } else {
    ObArray<schema::ObColDesc> columns;         // columns of index table
    ObArray<uint64_t> column_ids;               // data table's column id of index table columns
    ObArray<schema::ObColDesc> access_columns;  // access columns of data table
    ObArray<uint64_t> access_column_ids;        // access column ids of data table
    int64_t uk_col_cnt = 0;

    if (OB_FAIL(index_table_->get_column_ids(columns))) {
      LOG_WARN("get column idx failed", K(ret));
    } else {
      FOREACH(col, columns)
      {
        if (col->col_id_ >= OB_MIN_SHADOW_COLUMN_ID) {
          break;
        }
        uk_col_cnt++;
      }
      uk_col_cnt = std::min(index_table_->get_rowkey_column_num(), uk_col_cnt);
    }

    FOREACH_X(col, columns, OB_SUCC(ret))
    {
      uint64_t cid = col->col_id_;
      if (cid >= OB_MIN_SHADOW_COLUMN_ID) {
        cid -= OB_MIN_SHADOW_COLUMN_ID;
      }
      if (OB_FAIL(add_var_to_array_no_dup(column_ids, cid))) {
        LOG_WARN("add to array failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(access_column_ids.assign(column_ids))) {
        LOG_WARN("array assign failed", K(ret));
      } else if (OB_FAIL(column_ids2desc(*scan_table_, access_column_ids, access_columns))) {
        LOG_WARN("add column id to desc failed", K(ret));
      }
    }

    // make generate column expressions
    ObSEArray<ObColumnExpression*, 4> generate_col_exprs;
    ObSEArray<uint64_t, 4> ref_cols;
    FOREACH_X(cid, column_ids, OB_SUCC(ret))
    {
      const ObColumnSchemaV2* col = scan_table_->get_column_schema(*cid);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column not found", K(ret), K(*cid));
      } else if (col->is_generated_column()) {
        ObISqlExpression* expr = NULL;
        const bool make_column_expr = true;
        ref_cols.reuse();
        if (OB_FAIL(col->get_cascaded_column_ids(ref_cols))) {
          LOG_WARN("get cascade column id of generate column failed", K(ret), K(*col));
        } else if (OB_FAIL(append_array_no_dup(access_column_ids, ref_cols))) {
          LOG_WARN("append array failed", K(ret));
        } else if (OB_FAIL(column_ids2desc(*scan_table_, access_column_ids, access_columns))) {
          LOG_WARN("add column id to desc failed", K(ret));
        } else if (OB_FAIL(ObSQLUtils::make_generated_expression_from_str(col->get_cur_default_value().get_string(),
                       *sql_ctx_->session_info_,
                       *scan_table_,
                       *col,
                       access_columns,
                       phy_plan.get_allocator(),
                       expr,
                       make_column_expr))) {
          LOG_WARN("make generated expression failed", K(ret), K(*col));
        } else if (NULL == expr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr generated", K(ret));
        } else {
          ObColumnExpression* col_expr = static_cast<ObColumnExpression*>(expr);
          col_expr->set_result_index(cid - column_ids.begin());
          if (OB_FAIL(generate_col_exprs.push_back(col_expr))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }

    ObTableScan* scan = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_TABLE_SCAN_WITH_CHECKSUM, scan))) {
      LOG_WARN("alloc physical table scan failed", K(ret));
    } else {
      scan->set_index_table_id(scan_table_->get_table_id());
      scan->set_ref_table_id(scan_table_->get_table_id());
      scan->set_schema_version(scan_table_->get_schema_version());
      scan->set_table_location_key(scan_table_->get_table_id());
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan->init_output_column(access_column_ids.count()))) {
        LOG_WARN("init output column failed", K(ret));
      } else {
        scan->set_column_count(access_column_ids.count());
        FOREACH_X(cid, column_ids, OB_SUCC(ret))
        {
          if (OB_FAIL(scan->add_output_column(*cid))) {
            LOG_WARN("add output column failed", K(ret));
          }
        }
        FOREACH_X(e, generate_col_exprs, OB_SUCC(ret))
        {
          if (OB_FAIL(scan->add_virtual_column_expr(*e))) {
            LOG_WARN("add virtual column failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && access_column_ids.count() > column_ids.count()) {
        // extra columns added for generated column, projector needed
        int64_t projector_size = column_ids.count();
        int32_t* projector = phy_plan.alloc_projector(projector_size);
        if (NULL == projector) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate projector failed", K(ret));
        } else {
          for (int64_t i = 0; i < projector_size; i++) {
            projector[i] = static_cast<int32_t>(i);
          }
          scan->set_projector(projector, projector_size);
        }
      }

      const bool index_back = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan->get_table_param().convert(*scan_table_, *scan_table_, access_column_ids, index_back))) {
        LOG_WARN("convert table scan param failed", K(ret));
      } else {
        scan->get_table_param().disable_padding();
        cur_op = scan;
      }
    }

    if (OB_SUCC(ret) && index_table_->is_unique_index()) {
      ObUKRowTransform* uk_row_tf = NULL;
      if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_UK_ROW_TRANSFORM, uk_row_tf))) {
        LOG_WARN("alloc unique index row transform operator failed", K(ret));
      } else if (OB_FAIL(uk_row_tf->init_column_array(columns.count()))) {
        LOG_WARN("init column array failed", K(ret));
      } else {
        uk_row_tf->set_column_count(columns.count());
        uk_row_tf->set_uk_col_cnt(uk_col_cnt);
        uk_row_tf->set_shadow_pk_cnt(index_table_->get_rowkey_column_num() - uk_col_cnt);
        FOREACH_X(col, columns, OB_SUCC(ret))
        {
          const int64_t cid =
              col->col_id_ >= OB_MIN_SHADOW_COLUMN_ID ? col->col_id_ - OB_MIN_SHADOW_COLUMN_ID : col->col_id_;
          const int64_t idx = std::find(column_ids.begin(), column_ids.end(), cid) - column_ids.begin();
          if (idx >= column_ids.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column not find", K(ret), K(cid), K(*col));
          } else if (OB_FAIL(uk_row_tf->get_columns().push_back(idx))) {
            LOG_WARN("add to array failed", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(uk_row_tf->set_child(0, *cur_op))) {
            LOG_WARN("set child failed", K(ret));
          } else {
            cur_op = uk_row_tf;
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::gen_data_exchange(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op)
{
  int ret = OB_SUCCESS;
  ObDeterminateTaskTransmit* transmit = NULL;
  if (NULL == scan_table_ || NULL == index_table_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table or index_table schema is NULL", K(ret), KP(scan_table_), KP(index_table_));
  } else if (OB_FAIL(alloc_determinate_transmit(phy_plan, transmit)) || OB_ISNULL(transmit)) {
    ret = OB_ISNULL(transmit) ? OB_ERR_UNEXPECTED : ret;
    LOG_WARN("alloc determine task transmit failed", K(ret));
  } else {
    transmit->set_background(true);  // run in background threads
    transmit->set_result_reusable(true);
    transmit->set_column_count(index_table_->get_column_count());
    transmit->set_task_routing_policy(ObDeterminateTaskTransmit::ITaskRouting::DATA_REPLICA_PICKER);
    bool check_dropped_schema = false;
    schema::ObTablePartitionKeyIter data_keys(*scan_table_, check_dropped_schema);
    int64_t task_cnt = 0;
    if (scan_ranges_.count() == 1) {
      task_cnt = data_keys.get_partition_num() * scan_ranges_.at(0).count();
    } else {
      if (data_keys.get_partition_num() != scan_ranges_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table partition count mismatch with range array count", K(ret));
      } else {
        FOREACH(a, scan_ranges_)
        {
          task_cnt += a->count();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(transmit->get_range_locations().init(task_cnt))) {
      LOG_WARN("init fixed array failed", K(ret));
    } else if (OB_FAIL(transmit->get_tasks().init(task_cnt))) {
      LOG_WARN("init fixed array failed", K(ret));
    } else {
      int64_t idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < data_keys.get_partition_num(); i++) {
        auto& ranges = scan_ranges_.at(std::min(i, scan_ranges_.count() - 1));
        ObPartitionKey pkey;
        if (OB_FAIL(data_keys.next_partition_key_v2(pkey))) {
          LOG_WARN("get partition keys failed", K(ret));
        }
        FOREACH_X(range, ranges, OB_SUCC(ret))
        {
          if (OB_FAIL(
                  transmit->get_range_locations().push_back(ObTaskInfo::ObRangeLocation(phy_plan.get_allocator())))) {
            LOG_WARN("array push back failed", K(ret));
          } else if (FALSE_IT(transmit->get_range_locations().at(
                      transmit->get_range_locations().count() - 1).part_locs_.set_allocator(
                          &phy_plan.get_allocator()))) {
          } else if (OB_FAIL(transmit->get_range_locations().at(idx).part_locs_.init(1))) {
            LOG_WARN("init fix array failed", K(ret));
          } else {
            ObTaskInfo::ObPartLoc part_loc;
            part_loc.partition_key_ = pkey;
            if (OB_FAIL(part_loc.scan_ranges_.push_back(*range))) {
              LOG_WARN("array push back failed", K(ret));
            } else if (OB_FAIL(transmit->get_range_locations().at(idx).part_locs_.push_back(part_loc))) {
              LOG_WARN("array push back failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ObDeterminateTaskTransmit::TaskIndex task_idx;
            task_idx.loc_idx_ = static_cast<int32_t>(idx);
            task_idx.part_loc_idx_ = 0;
            if (OB_FAIL(transmit->get_tasks().push_back(task_idx))) {
              LOG_WARN("array push back failed", K(ret));
            } else {
              idx++;
            }
          }
        }
      }

      if (OB_SUCC(ret) && transmit->get_tasks().count() != task_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task count mismatch", K(ret), K(task_cnt), "generated_tasks", transmit->get_tasks().count());
      }
    }

    // setup shuffle info
    if (OB_SUCC(ret)) {
      // Always set repartition table id, task executor need index table id to check
      // the index building is needed.
      transmit->set_repartition_table_id(index_table_->get_table_id());
      if (index_table_->is_partitioned_table()) {
        transmit->set_shuffle_by_part();
        if (index_ranges_.count() > 1 && index_ranges_.count() != index_table_->get_all_part_num()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index table partition count mismatch with range array count", K(ret));
        }
      }
      if (OB_SUCC(ret) && (index_ranges_.count() > 1 || index_ranges_.at(0).count() > 1)) {
        transmit->set_shuffle_by_range();
        auto& shuffle_ranges = transmit->get_shuffle_ranges();
        if (OB_FAIL(shuffle_ranges.init(index_ranges_.count()))) {
          LOG_WARN("init fixed array failed", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < index_ranges_.count(); i++) {
            if (OB_FAIL(shuffle_ranges.push_back(ObFixedArray<ObNewRange, ObIAllocator>(phy_plan.get_allocator())))) {
              LOG_WARN("array push back failed", K(ret));
            } else if (OB_FAIL(shuffle_ranges.at(i).assign(index_ranges_.at(i)))) {
              LOG_WARN("array assign failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && index_table_->get_all_part_num() > 1 &&
          (index_ranges_.count() > 1 || index_ranges_.at(0).count() > 1)) {
        const int64_t part_num = index_table_->get_all_part_num();
        if (OB_FAIL(transmit->get_start_slice_ids().init(part_num))) {
          LOG_WARN("init fixed array failed", K(ret));
        } else {
          int64_t pos = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
            if (OB_FAIL(transmit->get_start_slice_ids().push_back(pos))) {
              LOG_WARN("array push back failed", K(ret));
            } else {
              pos += index_ranges_.at(std::min(i, index_ranges_.count() - 1)).count();
            }
          }
        }
      }
    }

    // setup result mapping
    if (OB_SUCC(ret)) {
      int64_t receive_task_cnt = 0;
      if (index_ranges_.count() == 1) {
        receive_task_cnt = index_table_->get_all_part_num() * index_ranges_.at(0).count();
      } else {
        FOREACH(ranges, index_ranges_)
        {
          receive_task_cnt += ranges->count();
        }
      }
      transmit->set_split_task_count(receive_task_cnt);

      if (OB_FAIL(transmit->get_result_mapping().init(receive_task_cnt))) {
        LOG_WARN("init fixed array failed", K(ret));
      } else {
        ObDeterminateTaskTransmit::ResultRange result_range;
        result_range.task_range_.begin_ = 0;
        result_range.task_range_.end_ = static_cast<int32_t>(transmit->get_tasks().count());
        for (int64_t i = 0; OB_SUCC(ret) && i < receive_task_cnt; i++) {
          result_range.slice_range_.begin_ = static_cast<int32_t>(i);
          result_range.slice_range_.end_ = static_cast<int32_t>(i + 1);
          if (OB_FAIL(transmit->get_result_mapping().push_back(result_range))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(transmit->set_child(0, *cur_op))) {
        LOG_WARN("set child failed", K(ret));
      } else {
        cur_op = transmit;
      }
    }
  }

  ObTaskOrderReceive* receive = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_TASK_ORDER_RECEIVE, receive))) {
    LOG_WARN("alloc physical task order receive failed", K(ret));
  } else if (OB_FAIL(receive->set_child(0, *cur_op))) {
    LOG_WARN("set child failed", K(ret));
  } else {
    receive->set_column_count(index_table_->get_column_count());
    cur_op = receive;
  }
  return ret;
}

int ObIndexSSTableBuilder::gen_build_macro(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op)
{
  int ret = OB_SUCCESS;
  ObSort* sort = NULL;
  ObArray<schema::ObColDesc> columns;
  if (NULL == scan_table_ || NULL == index_table_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table or index_table schema is NULL", K(ret), KP(scan_table_), KP(index_table_));
  } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_SORT, sort))) {
    LOG_WARN("alloc physical sort operator failed", K(ret));
  } else if (OB_FAIL(index_table_->get_column_ids(columns))) {
    LOG_WARN("get column idx failed", K(ret));
  } else {
    sort->set_column_count(columns.count());
    if (OB_FAIL(sort->init_sort_columns(index_table_->get_rowkey_column_num()))) {
      LOG_WARN("init sort column failed",
          K(ret), "rowkey_cnt", index_table_->get_rowkey_column_num());
    } else if (OB_FAIL(sort->init_op_schema_obj(index_table_->get_rowkey_column_num()))) {
      LOG_WARN("fail to init op schema obj", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_table_->get_rowkey_column_num(); i++) {
        const bool ascending = true;
        if (OB_FAIL(sort->add_sort_column(i,
                columns.at(i).col_type_.get_collation_type(),
                ascending,
                ITEM_TO_OBJ_TYPE(columns.at(i).col_type_.get_type()),
                default_asc_direction()))) {
          LOG_WARN("add sort column failed", K(ret));
        } else {
          // add sort column type to op_schema_objs_
          ObOpSchemaObj op_schema_obj(ITEM_TO_OBJ_TYPE(columns.at(i).col_type_.get_type()));
          if (OB_FAIL(sort->get_op_schema_objs_for_update().push_back(op_schema_obj))) {
            LOG_WARN("failed to push back element", K(ret));
          } else {
            // do nothing
          }
        }
      }
    }
    // TODO : set mem limit
    // sort->set_mem_limit();
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sort->set_child(0, *cur_op))) {
        LOG_WARN("set child failed", K(ret));
      } else {
        cur_op = sort;
      }
    }
  }

  ObTableAppendLocalSortData* append_data = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_APPEND_LOCAL_SORT_DATA, append_data))) {
    LOG_WARN("alloc physical append data physical failed", K(ret));
  } else {
    append_data->set_column_count(1);
    append_data->set_table_id(index_table_->get_table_id());
    if (OB_FAIL(append_data->set_child(0, *cur_op))) {
      LOG_WARN("set child failed", K(ret));
    } else {
      cur_op = append_data;
    }
  }

  return ret;
}

int ObIndexSSTableBuilder::gen_macro_exchange(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op)
{
  int ret = OB_SUCCESS;
  ObDeterminateTaskTransmit* transmit = NULL;
  if (NULL == scan_table_ || NULL == index_table_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table or index_table schema is NULL", K(ret), KP(scan_table_), KP(index_table_));
  } else if (OB_FAIL(alloc_determinate_transmit(phy_plan, transmit)) || OB_ISNULL(transmit)) {
    ret = OB_ISNULL(transmit) ? OB_ERR_UNEXPECTED : ret;
    LOG_WARN("alloc determine task transmit failed", K(ret));
  } else {
    transmit->set_background(true);  // run in background threads
    transmit->set_result_reusable(true);
    transmit->set_column_count(1);
    transmit->set_task_routing_policy(ObDeterminateTaskTransmit::ITaskRouting::INDEX_REPLICA_PICKER);
    bool check_dropped_schema = false;
    schema::ObTablePartitionKeyIter index_keys(*index_table_, check_dropped_schema);
    auto& range_locations = transmit->get_range_locations();
    int64_t task_cnt = 0;
    if (index_ranges_.count() == 1) {
      task_cnt = index_keys.get_partition_num() * index_ranges_.at(0).count();
    } else {
      if (index_keys.get_partition_num() != index_ranges_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index table partition count mismatch with range array count", K(ret));
      } else {
        FOREACH(a, index_ranges_)
        {
          task_cnt += a->count();
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(range_locations.init(index_keys.get_partition_num()))) {
      LOG_WARN("fixed array init failed", K(ret));
    } else if (OB_FAIL(transmit->get_tasks().init(task_cnt))) {
      LOG_WARN("fixed array init failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_keys.get_partition_num(); i++) {
        if (OB_FAIL(range_locations.push_back(ObTaskInfo::ObRangeLocation(phy_plan.get_allocator())))) {
          LOG_WARN("array push back failed", K(ret));
        } else if (FALSE_IT(
                range_locations.at(range_locations.count() - 1).part_locs_.set_allocator(
                    &phy_plan.get_allocator()))) {
        } else {
          auto& ranges = index_ranges_.at(std::min(i, index_ranges_.count() - 1));
          ObPartitionKey pkey;
          if (OB_FAIL(index_keys.next_partition_key_v2(pkey))) {
            LOG_WARN("get partition keys failed", K(ret));
          } else if (OB_FAIL(range_locations.at(i).part_locs_.init(ranges.count()))) {
            LOG_WARN("fixed array init failed", K(ret));
          } else {
            FOREACH_X(range, ranges, OB_SUCC(ret))
            {
              ObTaskInfo::ObPartLoc part_loc;
              part_loc.partition_key_ = pkey;
              if (OB_FAIL(part_loc.scan_ranges_.push_back(*range))) {
                LOG_WARN("array push back failed", K(ret));
              } else if (range_locations.at(i).part_locs_.push_back(part_loc)) {
                LOG_WARN("array push back failed", K(ret));
              } else {
                ObDeterminateTaskTransmit::TaskIndex task_idx;
                task_idx.loc_idx_ = static_cast<int32_t>(i);
                task_idx.part_loc_idx_ = static_cast<int32_t>(range_locations.at(i).part_locs_.count() - 1);
                if (OB_FAIL(transmit->get_tasks().push_back(task_idx))) {
                  LOG_WARN("array push back failed", K(ret));
                }
              }
            }
          }
        }
      }

      if (OB_SUCC(ret) && transmit->get_tasks().count() != task_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task count mismatch", K(ret), K(task_cnt), "generated_tasks", transmit->get_tasks().count());
      }
    }

    // setup result mapping
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(transmit->get_result_mapping().init(range_locations.count()))) {
      LOG_WARN("array push back failed", K(ret));
    } else {
      transmit->set_split_task_count(range_locations.count());
      ObDeterminateTaskTransmit::ResultRange result_range;
      result_range.slice_range_.begin_ = 0;
      result_range.slice_range_.end_ = 1;
      int64_t start_pos = 0;
      FOREACH_CNT_X(loc, range_locations, OB_SUCC(ret))
      {
        result_range.task_range_.begin_ = static_cast<int32_t>(start_pos);
        start_pos += loc->part_locs_.count();
        result_range.task_range_.end_ = static_cast<int32_t>(start_pos);
        if (OB_FAIL(transmit->get_result_mapping().push_back(result_range))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(transmit->set_child(0, *cur_op))) {
        LOG_WARN("set child failed", K(ret));
      } else {
        cur_op = transmit;
      }
    }
  }

  ObTaskOrderReceive* receive = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_TASK_ORDER_RECEIVE, receive))) {
    LOG_WARN("alloc physical task order receive failed", K(ret));
  } else if (OB_FAIL(receive->set_child(0, *cur_op))) {
    LOG_WARN("set child failed", K(ret));
  } else {
    receive->set_column_count(1);
    cur_op = receive;
  }

  return ret;
}

int ObIndexSSTableBuilder::gen_build_sstable(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op)
{
  int ret = OB_SUCCESS;
  ObTableAppendSSTable* build_sstable = NULL;
  if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_APPEND_SSTABLE, build_sstable))) {
    LOG_WARN("alloc physical build sstable operator failed", K(ret));
  } else {
    build_sstable->set_table_id(index_table_->get_table_id());
    if (OB_FAIL(build_sstable->set_child(0, *cur_op))) {
      LOG_WARN("set child failed", K(ret));
    } else {
      cur_op = build_sstable;
    }
  }

  return ret;
}

int ObIndexSSTableBuilder::gen_sstable_exchange(ObPhysicalPlan& phy_plan, ObPhyOperator*& cur_op)
{
  int ret = OB_SUCCESS;
  ObDeterminateTaskTransmit* transmit = NULL;
  if (NULL == scan_table_ || NULL == index_table_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table or index_table schema is NULL", K(ret), KP(scan_table_), KP(index_table_));
  } else if (OB_FAIL(alloc_determinate_transmit(phy_plan, transmit)) || OB_ISNULL(transmit)) {
    ret = OB_ISNULL(transmit) ? OB_ERR_UNEXPECTED : ret;
    LOG_WARN("alloc determine task transmit failed", K(ret));
  } else {
    transmit->set_task_routing_policy(ObDeterminateTaskTransmit::ITaskRouting::SAME_WITH_CHILD);
    bool check_dropped_schema = false;
    schema::ObTablePartitionKeyIter index_keys(*index_table_, check_dropped_schema);
    auto& range_locations = transmit->get_range_locations();
    if (OB_FAIL(range_locations.init(index_keys.get_partition_num()))) {
      LOG_WARN("fixed array init failed", K(ret));
    } else if (OB_FAIL(transmit->get_tasks().init(index_keys.get_partition_num()))) {
      LOG_WARN("fixed array init failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < index_keys.get_partition_num(); i++) {
        if (OB_FAIL(range_locations.push_back(ObTaskInfo::ObRangeLocation(phy_plan.get_allocator())))) {
          LOG_WARN("array push back failed", K(ret));
        } else if (FALSE_IT(
                range_locations.at(range_locations.count() - 1).part_locs_.set_allocator(
                    &phy_plan.get_allocator()))) {
        } else {
          auto& range_location = range_locations.at(i);
          if (OB_FAIL(range_location.part_locs_.init(1))) {
            LOG_WARN("fixed array init failed", K(ret));
          } else {
            ObTaskInfo::ObPartLoc part_loc;
            if (OB_FAIL(index_keys.next_partition_key_v2(part_loc.partition_key_))) {
              LOG_WARN("get next partition key failed", K(ret));
            } else if (OB_FAIL(range_location.part_locs_.push_back(part_loc))) {
              LOG_WARN("array push back failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObDeterminateTaskTransmit::TaskIndex task_idx;
          task_idx.loc_idx_ = static_cast<int32_t>(i);
          task_idx.part_loc_idx_ = 0;
          if (OB_FAIL(transmit->get_tasks().push_back(task_idx))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }

    // setup result mapping
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(transmit->get_result_mapping().init(1))) {
      LOG_WARN("array push back failed", K(ret));
    } else {
      transmit->set_split_task_count(1);
      ObDeterminateTaskTransmit::ResultRange result_range;
      result_range.task_range_.begin_ = 0;
      result_range.task_range_.end_ = static_cast<int32_t>(range_locations.count());
      result_range.slice_range_.begin_ = 0;
      result_range.slice_range_.end_ = 1;
      if (OB_FAIL(transmit->get_result_mapping().push_back(result_range))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(transmit->set_child(0, *cur_op))) {
        LOG_WARN("set child failed", K(ret));
      } else {
        cur_op = transmit;
      }
    }
  }

  ObTaskOrderReceive* receive = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_TASK_ORDER_RECEIVE, receive))) {
    LOG_WARN("alloc physical task order receive failed", K(ret));
  } else if (OB_FAIL(receive->set_child(0, *cur_op))) {
    LOG_WARN("set child failed", K(ret));
  } else {
    receive->set_column_count(1);
    receive->set_in_root_job(true);
    cur_op = receive;
  }

  return ret;
}

int ObIndexSSTableBuilder::alloc_determinate_transmit(ObPhysicalPlan& phy_plan, ObDeterminateTaskTransmit*& transmit)
{
  int ret = OB_SUCCESS;
  transmit = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(phy_plan.alloc_operator_by_type(PHY_DETERMINATE_TASK_TRANSMIT, transmit))) {
    LOG_WARN("alloc determine task transmit failed", K(ret));
  } else {
    transmit->set_task_routing(&task_routing_proxy_);
    transmit->get_job_conf().set_task_split_type(ObTaskSpliter::DETERMINATE_TASK_SPLIT);
  }
  return ret;
}
int ObIndexSSTableBuilder::query_execution_id(
    uint64_t& execution_id, const int64_t job_id, const int64_t snapshot_version, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT execution_id FROM %s WHERE job_id = %ld AND snapshot_version = %ld LIMIT 1",
            OB_ALL_BUILD_INDEX_PARAM_TNAME,
            job_id,
            snapshot_version))) {
      LOG_WARN("string assign failed", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      LOG_WARN("NULL result", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      LOG_WARN("get execution id failed", K(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "execution_id", execution_id, uint64_t);
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::clear_interm_result(common::ObMySQLProxy& sql_proxy, const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObClearExecutor executor(job_id);
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(sql_proxy.execute(OB_SYS_TENANT_ID, executor))) {
    LOG_WARN("clear intermediate result failed", K(ret), K(job_id));
  }
  LOG_INFO("clear build index interm result", K(ret), K(job_id));
  return ret;
}

int ObIndexSSTableBuilder::clear_interm_result(
    const int64_t job_id, common::ObMySQLProxy& sql_proxy, ObExecutorRpcCtx& ctx, ObExecutorRpcImpl& rpc_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (job_id <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job id", K(job_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT execution_id, sql_job_id, task_id, svr_ip, svr_port, slice_count"
                                      " FROM %s WHERE job_id = %ld",
                   OB_ALL_SQL_EXECUTE_TASK_TNAME,
                   job_id))) {
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL result", K(ret));
    } else {
      char ip[OB_IP_STR_BUFF] = "";
      int32_t port = 0;
      ObAddr server;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next result failed", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
          break;
        }
        uint64_t execution_id = 0;
        uint64_t sql_job_id = 0;
        uint64_t task_id = 0;
        int64_t slice_count = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "execution_id", execution_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "sql_job_id", sql_job_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id, uint64_t);
        int64_t tmp = 0;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "svr_ip", ip, OB_IP_STR_BUFF, tmp);
        UNUSED(tmp);  // make compiler happy
        EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", port, int32_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "slice_count", slice_count, int64_t);

        ObJobID jid;
        jid.set_server(ObExecutionID::global_id_addr());
        jid.set_execution_id(execution_id);
        jid.set_job_id(sql_job_id);
        ObTaskID tid;
        tid.set_ob_job_id(jid);
        tid.set_task_id(task_id);
        ObSliceID slice_id;
        slice_id.set_ob_task_id(tid);
        if (OB_FAIL(ret)) {
        } else if (!server.set_ip_addr(ip, port)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set server failed", K(ret), K(ip), K(port));
        } else {
          // FIXME : one rpc for one task
          for (int64_t i = 0; i < slice_count; i++) {
            slice_id.set_slice_id(i);
            int tmp_ret = rpc_proxy.close_result(ctx, slice_id, server);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("clear interm result failed", K(ret), K(slice_id), K(server));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::store_build_param()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t fragment_size = 2 << 10;  // 2K
    ObSqlString buf;
    ObSqlString hex;
    const BuildParam build_param(scan_table_id_, scan_ranges_, index_ranges_, allocator_);
    const int64_t ser_len = build_param.get_serialize_size();
    int64_t pos = 0;
    if (OB_FAIL(buf.reserve(ser_len))) {
      LOG_WARN("sql string reserve failed", K(ret), K(ser_len));
    } else if (OB_FAIL(build_param.serialize(buf.ptr(), ser_len, pos))) {
      LOG_WARN("serialize failed", K(ret));
    } else if (OB_FAIL(buf.set_length(ser_len))) {
      LOG_WARN("sql string set length failed", K(ret), K(ser_len));
    } else {
      ObSqlString sql;
      ObSqlString val;
      ObSqlString hex;
      ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
      for (int64_t i = 0; OB_SUCC(ret) && i * fragment_size < ser_len; i++) {
        dml.reset();
        ObString fragment(std::min(fragment_size, ser_len - (i * fragment_size)), buf.ptr() + i * fragment_size);
        if ((OB_FAIL(hex.assign("HEX(")))) {
          LOG_WARN("string assign failed", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(fragment, hex))) {
          LOG_WARN("append hex escaped string failed", K(ret));
        } else if (OB_FAIL(hex.append(")"))) {
          LOG_WARN("string append failed", K(ret));
        } else if (OB_FAIL(dml.add_pk_column("job_id", job_.job_id_)) ||
                   OB_FAIL(dml.add_pk_column("snapshot_version", job_.snapshot_version_)) ||
                   OB_FAIL(dml.add_pk_column("execution_id", execution_id_)) ||
                   OB_FAIL(dml.add_pk_column("seq_no", i)) || OB_FAIL(dml.add_column("param", hex.ptr()))) {
          LOG_WARN("add column failed", K(ret));
        }
        if (OB_SUCC(ret) && 0 == i) {
          if (OB_FAIL(dml.splice_column_names(val))) {
            LOG_WARN("splice column name failed", K(ret));
          } else if (OB_FAIL(
                         sql.assign_fmt("INSERT INTO %s (%s) VALUES ", OB_ALL_BUILD_INDEX_PARAM_TNAME, val.ptr()))) {
            LOG_WARN("sql assign failed", K(ret));
          }
        }
        val.reset();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(dml.splice_values(val))) {
          LOG_WARN("splice insert values failed", K(ret));
        } else if (OB_FAIL(sql.append_fmt("%s (%s)", 0 == i ? "" : ", ", val.ptr()))) {
          LOG_WARN("append sql failed", K(ret));
        }
      }

      int64_t affected_rows = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(ret));
      } else if (affected_rows <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows", K(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::load_build_param()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    int64_t read_exec_id = 0;
    ObString fragment;
    ObSqlString param;
    BuildParam build_param(scan_table_id_, scan_ranges_, index_ranges_, allocator_);
    int64_t pos = 0;
    if (!is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("SELECT execution_id, UNHEX(param) AS param FROM %s "
                                      "WHERE job_id = %ld AND snapshot_version = %ld ORDER BY seq_no",
                   OB_ALL_BUILD_INDEX_PARAM_TNAME,
                   job_.job_id_,
                   job_.snapshot_version_))) {
      LOG_WARN("string assign failed", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      LOG_WARN("NULL result", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get result failed", K(ret));
          }
          break;
        } else if (OB_FAIL(result->get_int("execution_id", read_exec_id))) {
          LOG_WARN("get int failed", K(ret));
        } else if (OB_FAIL(result->get_varchar("param", fragment))) {
          LOG_WARN("get varchar failed", K(ret));
        } else if (OB_FAIL(param.append(fragment))) {
          LOG_WARN("string append failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == read_exec_id) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          execution_id_ = static_cast<uint64_t>(read_exec_id);
          if (OB_FAIL(build_param.deserialize(param.ptr(), param.length(), pos))) {
            LOG_WARN("deserialize build param failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("load build param", K(job_), K(execution_id_), K(build_param));
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::split_ranges()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool check_dropped_schema = false;
    schema::ObTablePartitionKeyIter data_keys(*data_table_, check_dropped_schema);
    int64_t row_cnt = 0;
    int64_t data_size = 0;
    while (OB_SUCC(ret)) {
      ObTableStat ts;
      ObPartitionKey pkey;
      if (OB_FAIL(data_keys.next_partition_key_v2(pkey))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get partition keys failed", K(ret));
        }
        break;
      } else if (OB_FAIL(ObStatManager::get_instance().get_table_stat(pkey, ts))) {
        LOG_WARN("get table stat failed", K(ret), K(pkey));
      } else {
        row_cnt += ts.get_row_count();
        data_size += ts.get_data_size();
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t dop_task_scale = 13;
      const int64_t min_scan_granule =
          data_table_->get_tablet_size() > 0 ? data_table_->get_tablet_size() : OB_DEFAULT_TABLET_SIZE;
      const int64_t max_scan_task_cnt = job_.degree_of_parallelism_ * dop_task_scale;
      const int64_t scan_task_cnt = std::min(max_scan_task_cnt, data_size / min_scan_granule);
      const int64_t data_range_cnt = std::max(1L, scan_task_cnt / data_table_->get_all_part_num());
      if (OB_FAIL(split_ranges(scan_ranges_, *data_table_, row_cnt, *data_table_, data_range_cnt))) {
        LOG_WARN("split ranges failed", K(ret), K(row_cnt), K(data_range_cnt));
      }

      // FIXME :
      // One index task may occupy 2MB memory in intermediate result storing,
      // we restrict to 100 for reasonable memory usage temporary.
      const int64_t index_task_cnt = std::min(scan_task_cnt, 100L);
      const int64_t index_range_cnt = std::max(1L, index_task_cnt / index_table_->get_all_part_num());
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(split_ranges(index_ranges_, *data_table_, row_cnt, *index_table_, index_range_cnt))) {
        LOG_WARN("split ranges failed", K(ret), K(row_cnt), K(index_range_cnt));
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::split_ranges(ObArray<ObArray<ObNewRange>>& ranges_array,
    const schema::ObTableSchema& sample_table, const int64_t row_cnt, const schema::ObTableSchema& split_table,
    const int64_t range_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<schema::ObColDesc> columns;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (row_cnt < 0 || range_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_cnt), K(range_cnt));
  } else if (OB_FAIL(split_table.get_rowkey_column_ids(columns))) {
    LOG_WARN("get rowkey column ids failed", K(ret));
  } else {
    // truncate shadow pk.
    int64_t idx = 0;
    for (; idx < columns.count(); idx++) {
      if (columns.at(idx).col_id_ >= OB_MIN_SHADOW_COLUMN_ID) {
        break;
      }
    }
    while (columns.count() > idx) {
      columns.pop_back();
    }

    // FIXME: : different ranges for different partition,
    // to support range or list partition.
    if (OB_FAIL(ranges_array.prepare_allocate(1))) {
      LOG_WARN("prepare allocate failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    auto& ranges = ranges_array.at(0);
    ObObj* start_row_key = NULL;
    ObObj* end_row_key = NULL;
    int64_t rowkey_count = split_table.get_rowkey_column_num();
    if (OB_ISNULL(start_row_key = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * rowkey_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for start_obj failed", K(ret));
    } else if (OB_ISNULL(end_row_key = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * rowkey_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for end_obj failed", K(ret));
    } else {
      for (int i = 0; i < rowkey_count; ++i) {
        start_row_key[i] = ObRowkey::MIN_OBJECT;
        end_row_key[i] = ObRowkey::MAX_OBJECT;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (range_cnt <= 1) {
      ObNewRange range;
      range.table_id_ = split_table.get_table_id();
      range.set_whole_range();

      range.start_key_.assign(start_row_key, rowkey_count);
      range.end_key_.assign(end_row_key, rowkey_count);

      if (OB_FAIL(ranges.push_back(range))) {
        LOG_WARN("array push back failed", K(ret));
      }
    } else {
      // 100 is enough, multiply by 10 to avoid sampling too few blocks.
      const int64_t sample_scale = 1000;
      // limit max sample rows to restrict memory usage.
      const int64_t max_sample_rows = 100000;
      int64_t sample_rows = std::min(sample_scale * range_cnt, max_sample_rows);
      const double max_sample_pct = 10;
      double sample_pct = static_cast<double>(sample_rows * 100L) / static_cast<double>(row_cnt);
      sample_pct = std::min(sample_pct, max_sample_pct);
      ObSqlString sql;
      ObSqlString col_name;
      ObSqlString col_alias;
      ObSqlString col_name_alias;
      const schema::ObDatabaseSchema* db = NULL;
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult* result = NULL;
        if (OB_FAIL(concat_column_names(col_name, columns, true /* need column name */, false /* need alias */))) {
          LOG_WARN("concat column name failed", K(ret));
        } else if (OB_FAIL(
                       concat_column_names(col_alias, columns, false /* need column name */, true /* need alias */))) {
          LOG_WARN("concat alias name failed", K(ret));
        } else if (OB_FAIL(concat_column_names(
                       col_name_alias, columns, true /* need column name */, true /* need alias */))) {
          LOG_WARN("concat column name and alias name failed", K(ret));
        } else if (OB_FAIL(sql_ctx_->schema_guard_->get_database_schema(sample_table.get_database_id(), db))) {
          LOG_WARN("get database schema failed", K(ret));
        } else if (OB_ISNULL(db)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL databases", K(ret));
        } else if (OB_FAIL(sql.assign_fmt(
                       "SELECT %.*s FROM "
                       "(SELECT %.*s, bucket, ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY %.*s) rn FROM "
                       "(SELECT %.*s, NTILE(%ld) OVER (ORDER BY %.*s) bucket FROM "
                       "(SELECT %.*s FROM %s%.*s%s.%s%.*s%s SAMPLE BLOCK(%g)) a) b) c WHERE rn = 1 "
                       "GROUP BY %.*s ORDER BY %.*s",
                       static_cast<int>(col_alias.length()),
                       col_alias.ptr(),

                       static_cast<int>(col_alias.length()),
                       col_alias.ptr(),
                       static_cast<int>(col_alias.length()),
                       col_alias.ptr(),

                       static_cast<int>(col_alias.length()),
                       col_alias.ptr(),
                       range_cnt,
                       static_cast<int>(col_alias.length()),
                       col_alias.ptr(),

                       static_cast<int>(col_name_alias.length()),
                       col_name_alias.ptr(),
                       name_quote(),
                       db->get_database_name_str().length(),
                       db->get_database_name_str().ptr(),
                       name_quote(),
                       name_quote(),
                       sample_table.get_table_name_str().length(),
                       sample_table.get_table_name_str().ptr(),
                       name_quote(),
                       sample_pct,

                       static_cast<int>(col_alias.length()),
                       col_alias.ptr(),
                       static_cast<int>(col_alias.length()),
                       col_alias.ptr()))) {
          LOG_WARN("string assign failed", K(ret));
        } else if (OB_FAIL(user_sql_proxy_->read(res, extract_tenant_id(job_.data_table_id_), sql.ptr()))) {
          LOG_WARN("execute sql failed", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL result", K(ret));
        } else {
          ObNewRange range;
          range.table_id_ = split_table.get_table_id();
          const ObNewRow* row = NULL;
          ObObj objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
          ObRowkey start(start_row_key, rowkey_count);

          bool first_row_ignored = false;
          while (OB_SUCC(ret)) {
            ObRowkey end(end_row_key, rowkey_count);
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("get next result failed", K(ret));
                break;
              }
            } else if (OB_ISNULL(row = result->get_row())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL row", K(ret));
            } else {
              if (!first_row_ignored) {
                first_row_ignored = true;
                continue;
              } else {
                ObRowkey rowkey(objs, row->get_count());
                for (int64_t i = 0; i < row->get_count(); i++) {
                  objs[i] = row->get_cell(i);
                }
                if (OB_FAIL(rowkey.deep_copy(end, allocator_))) {
                  LOG_WARN("rowkey deep copy failed", K(ret));
                }
              }
            }
            if (OB_SUCC(ret)) {
              range.start_key_ = start;
              range.end_key_ = end;
              if (!end.is_max_row()) {
                range.border_flag_.set_inclusive_end();
              }
              if (OB_FAIL(ranges.push_back(range))) {
                LOG_WARN("range push back failed", K(ret));
              } else {
                if (end.is_max_row()) {
                  break;
                } else {
                  start = end;
                }
              }
            }  // end while
          }
        }
      }
    }
  }
  LOG_INFO("split range", "table_id", split_table.get_table_id(), K(row_cnt), K(range_cnt), K(ranges_array));
  return ret;
}

int ObIndexSSTableBuilder::concat_column_names(
    ObSqlString& str, const ObIArray<schema::ObColDesc>& columns, const bool need_name, const bool need_alias)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
      if (i > 0) {
        if (OB_FAIL(str.append(", "))) {
          LOG_WARN("string append failed", K(ret));
        }
      }
      if (need_name) {
        auto col = data_table_->get_column_schema(columns.at(i).col_id_);
        if (OB_ISNULL(col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column schema failed", K(ret), "col", columns.at(i));
        } else if (OB_FAIL(str.append_fmt("%s%.*s%s",
                       name_quote(),
                       col->get_column_name_str().length(),
                       col->get_column_name_str().ptr(),
                       name_quote()))) {
          LOG_WARN("append string failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && need_alias) {
        if (OB_FAIL(str.append_fmt("%scol%ld", need_name ? " AS " : "", i))) {
          LOG_WARN("append string failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::generate_build_param()
{
  int ret = OB_SUCCESS;
  bool loaded = false;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(load_build_param())) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("load build param failed", K(ret));
    }
  } else {
    loaded = true;
  }

  if (OB_SUCC(ret) && !loaded) {
    // TODO : to be continue
    scan_table_id_ = job_.data_table_id_;

    if (OB_FAIL(split_ranges())) {
      LOG_WARN("split range failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && !loaded) {
    ObMaxIdFetcher id_generator(*sql_proxy_);
    if (OB_FAIL(id_generator.fetch_new_max_id(
            OB_SYS_TENANT_ID, OB_MAX_SQL_EXECUTION_ID_TYPE, execution_id_, ObSqlExecutionIDMap::OUTER_ID_BASE))) {
      LOG_WARN("generate execution id failed", K(ret));
    } else if (OB_FAIL(store_build_param())) {
      LOG_WARN("store build param failed", K(ret));
    }
  }

  return ret;
}

int ObIndexSSTableBuilder::fill_table_location(
    ObPhyTableLocation& loc, ObResultSet& res, const schema::ObTableSchema& table)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> part_ids;
  ;
  bool check_dropped_schema = false;
  schema::ObTablePartitionKeyIter keys(table, check_dropped_schema);
  auto loc_cache = res.get_exec_context().get_task_exec_ctx().get_partition_location_cache();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!table.is_valid() || NULL == loc_cache) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table), KP(loc_cache));
  } else {
    ObPhyTableLocationInfo info;
    info.set_table_location_key(table.get_table_id(), table.get_table_id());
    info.set_direction(UNORDERED);
    if (ObDuplicateScope::DUPLICATE_SCOPE_NONE != table.get_duplicate_scope()) {
      info.set_duplicate_type(ObDuplicateType::DUPLICATE);
    }
    while (OB_SUCC(ret)) {
      int64_t part_id = 0;
      if (OB_FAIL(keys.next_partition_id_v2(part_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("partition id iterate failed", K(ret));
        }
      } else if (OB_FAIL(part_ids.push_back(part_id))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTableLocation::set_partition_locations(res.get_exec_context(),
                   *loc_cache,
                   table.get_table_id(),
                   part_ids,
                   info.get_phy_part_loc_info_list_for_update()))) {
      LOG_WARN("set partition locations failed", K(ret));
    } else {
      // we pick replica by ReplicaPicker, set selected replica idx of partition location info
      // just make it valid.
      FOREACH_CNT_X(part_loc, info.get_phy_part_loc_info_list_for_update(), OB_SUCC(ret))
      {
        if (OB_FAIL(part_loc->set_selected_replica_idx(0))) {
          LOG_WARN("set select replica index failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(loc.assign_from_phy_table_loc_info(info))) {
        LOG_WARN("assign table location info to table location failed", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::fill_locations(ObResultSet& res)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObPhyTableLocation, 2> locs;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(res.get_exec_context().get_task_executor_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL task execute context", K(ret));
  } else if (OB_FAIL(locs.prepare_allocate(2))) {
    LOG_WARN("array prepare allocate failed", K(ret));
  } else {
    // TODO : data_table_ for local index
    const schema::ObTableSchema* table = scan_table_;
    if (OB_FAIL(fill_table_location(locs.at(0), res, *table)) ||
        OB_FAIL(fill_table_location(locs.at(1), res, *index_table_))) {
      LOG_WARN("fill table location failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(res.get_exec_context().get_task_executor_ctx()->set_table_locations(locs))) {
        LOG_WARN("set table locations failed", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexSSTableBuilder::add_access_replica(const ObPartitionKey& pkey, const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard g(access_replica_latch_, ObLatchIds::DEFAULT_MUTEX);
  if (OB_FAIL(add_var_to_array_no_dup(access_replicas_, std::make_pair(pkey, addr)))) {
    LOG_WARN("array push back failed");
  }
  return ret;
}

// Add accessed replicas to table location, used in end_stmt() to notify
// transaction module clear context.
int ObIndexSSTableBuilder::finish_add_access_replica(ObResultSet& res)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(res.get_exec_context().get_task_executor_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObLatchWGuard g(access_replica_latch_, ObLatchIds::DEFAULT_MUTEX);
    ObPhyTableLocationIArray& table_locations = res.get_exec_context().get_task_executor_ctx()->get_table_locations();
    FOREACH_CNT_X(r, access_replicas_, OB_SUCC(ret))
    {
      FOREACH_CNT_X(loc, table_locations, OB_SUCC(ret))
      {
        if (r->first.get_table_id() == loc->get_table_location_key()) {
          FOREACH_CNT_X(rl, loc->get_partition_location_list(), OB_SUCC(ret))
          {
            if (r->first.get_table_id() == rl->get_table_id() &&
                r->first.get_partition_id() == rl->get_partition_id() &&
                r->first.get_partition_cnt() == rl->get_partition_cnt()) {
              ObReplicaLocation new_location = rl->get_replica_location();
              new_location.server_ = r->second;
              rl->set_replica_location(new_location);
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
