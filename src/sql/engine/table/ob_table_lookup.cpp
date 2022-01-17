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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_lookup_task_builder.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/executor/ob_task.h"
#include "sql/executor/ob_mini_task_executor.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {

class ObTableLookup::ObTableLookupCtx : public ObPhyOperatorCtx {
  friend class ObTableLookup;

public:
  explicit ObTableLookupCtx(ObExecContext& ctx)
      : ObPhyOperatorCtx(ctx),
        allocator_(),
        result_(),
        result_ite_(),
        state_(INDEX_SCAN),
        end_(false),
        lookup_task_executor_(ctx.get_allocator()),
        task_builder_(ctx.get_allocator()),
        partitions_ranges_(),
        partition_cnt_(0),
        table_location_(nullptr),
        main_table_rowkey_(),
        schema_guard_(nullptr)
  {}
  virtual ~ObTableLookupCtx()
  {}
  virtual void destroy();
  int store_row(int64_t part_id, const common::ObNewRow* row, const bool table_has_hidden_pk, int64_t& part_row_cnt);
  int execute(ObExecContext& ctx);
  int get_next_row(const common::ObNewRow*& row);
  void set_end(bool end)
  {
    end_ = end;
  }
  bool end()
  {
    return end_;
  }
  // int init_partition_ranges(int64_t part_cnt);
  int clean_mem();
  void reset();
  int set_partition_cnt(int64_t partition_cnt);
  void set_table_location(const ObTableLocation* table_location)
  {
    table_location_ = table_location;
  };
  bool is_target_partition(int64_t pid);
  void set_mem_attr(const ObMemAttr& attr)
  {
    partitions_ranges_.set_mem_attr(attr);
  }

private:
  common::ObArenaAllocator allocator_;
  // for result
  ObMiniTaskResult result_;
  common::ObRowStore::Iterator result_ite_;
  LookupState state_;
  // iterate end
  bool end_;
  ObLookupMiniTaskExecutor lookup_task_executor_;
  ObLookupTaskBuilder task_builder_;
  // ranges
  ObMultiPartitionsRangesWarpper partitions_ranges_;
  // partition count
  int64_t partition_cnt_;
  const ObTableLocation* table_location_;
  ObNewRow main_table_rowkey_;
  share::schema::ObSchemaGetterGuard* schema_guard_;
};

int ObTableLookup::ObTableLookupCtx::set_partition_cnt(int64_t partition_cnt)
{
  int ret = OB_SUCCESS;
  if (partition_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition cnt is invalid", K(ret), K(partition_cnt));
  } else {
    partition_cnt_ = partition_cnt;
  }
  return ret;
}

void ObTableLookup::ObTableLookupCtx::destroy()
{
  result_.reset();
  result_ite_.reset();
  state_ = EXECUTION_FINISHED;
  task_builder_.reset();
  end_ = true;
  lookup_task_executor_.destroy();
  partitions_ranges_.release();
  allocator_.~ObArenaAllocator();
  ObPhyOperatorCtx::destroy_base();
}

void ObTableLookup::ObTableLookupCtx::reset()
{
  result_.reset();
  result_ite_.reset();
  state_ = INDEX_SCAN;
  task_builder_.reset();
  allocator_.reset();
  end_ = false;
}

int ObTableLookup::ObTableLookupCtx::store_row(
    int64_t part_id, const common::ObNewRow* row, const bool table_has_hidden_pk, int64_t& part_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partitions_ranges_.add_range(
          part_id, table_location_->get_ref_table_id(), row, table_has_hidden_pk, part_row_cnt))) {
    LOG_WARN("Failed to add range", K(ret));
  }
  return ret;
}

int ObTableLookup::ObTableLookupCtx::execute(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  ObMiniTaskRetryInfo retry_info;
  if (OB_ISNULL(table_location_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The table location is null", K(ret));
  } else if (OB_FAIL(task_builder_.build_lookup_tasks(
                 ctx, partitions_ranges_, table_location_->get_table_id(), table_location_->get_ref_table_id()))) {
    LOG_WARN("Failed to build lookup tasks", K(ret));
  } else if (OB_FAIL(lookup_task_executor_.execute(ctx,
                 task_builder_.get_lookup_task_list(),
                 task_builder_.get_lookup_taskinfo_list(),
                 retry_info,
                 result_))) {
    LOG_WARN("Failed to remote execute table scan", K(ret));
  }
  LOG_TRACE("Lookup get some result",
      K(result_.get_task_result().get_row_count()),
      K(retry_info.need_retry()),
      K(partitions_ranges_.count()));
  retry_info.do_retry_execution();
  while (OB_SUCC(ret) && retry_info.need_retry()) {
    retry_info.add_retry_times();
    LOG_TRACE("Table lookup retry remote partition", K(retry_info.get_retry_times()));
    if (OB_FAIL(ctx.check_status())) {
      LOG_WARN("Check physical plan status failed", K(ret));
    } else if (OB_FAIL(task_builder_.rebuild_overflow_task(retry_info))) {
      LOG_WARN("Failed to rebuild overflow task", K(ret));
    } else if (OB_FAIL(lookup_task_executor_.retry_overflow_task(ctx,
                   task_builder_.get_lookup_task_list(),
                   task_builder_.get_lookup_taskinfo_list(),
                   retry_info,
                   result_))) {
      LOG_WARN("Failed to retry overflow task", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(clean_mem())) {
      LOG_WARN("Failed to clean range mem", K(ret));
    }
  }
  return ret;
}

int ObTableLookup::ObTableLookupCtx::clean_mem()
{
  int ret = OB_SUCCESS;
  partitions_ranges_.release();
  task_builder_.reset();
  return ret;
}

int ObTableLookup::ObTableLookupCtx::get_next_row(const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  common::ObNewRow& cur_row = get_cur_row();
  if (OB_FAIL(result_ite_.get_next_row(cur_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      // We get a ob_iter_end, reuse the scanner.
      result_.get_task_result().reuse();
      result_ite_ = result_.get_task_result().begin();
    }
  } else {
    row = &cur_row;
  }
  return ret;
}

ObTableLookup::ObTableLookup(common::ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc), remote_table_scan_(NULL), lookup_info_(), table_location_(alloc)
{}

ObTableLookup::~ObTableLookup()
{}

int ObTableLookup::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  ObTableLookupCtx* lookup_ctx = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObExecutorRpcImpl* exec_rpc = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObSqlCtx* sql_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTableLookupCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create phy operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("init cur row failed", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx)) || OB_ISNULL(sql_ctx = ctx.get_sql_ctx()) ||
             OB_ISNULL(sql_ctx->schema_guard_)) {
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, exec_rpc))) {
    LOG_WARN("get task executor rpc failed", K(ret));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_ISNULL(task_ctx = ctx.get_task_executor_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task executor ctx is null", K(ret));
  } else if (OB_ISNULL(remote_table_scan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remote_table_scan is null", K(ret));
  } else {
    ObCurTraceId::TraceId execution_id;
    ObJobID ob_job_id;
    execution_id.init(ObCurTraceId::get_addr());
    ob_job_id.set_mini_task_type();
    ob_job_id.set_server(execution_id.get_addr());
    ob_job_id.set_execution_id(execution_id.get_seq());
    ob_job_id.set_job_id(remote_table_scan_->get_id());
    lookup_ctx = static_cast<ObTableLookupCtx*>(op_ctx);
    if (OB_FAIL(lookup_ctx->task_builder_.init(lookup_info_, get_phy_plan(), ob_job_id, &ctx, remote_table_scan_))) {
      LOG_WARN("init task builder failed", K(ret));
    } else if (OB_FAIL(lookup_ctx->lookup_task_executor_.init(*my_session, exec_rpc))) {
      LOG_WARN("init executor failed", K(ret));
    } else if (OB_FAIL(lookup_ctx->set_partition_cnt(lookup_info_.partition_num_))) {
      LOG_WARN("the partition cnt is invalid", K(ret));
    } else {
      ObMemAttr mem_attr;
      mem_attr.tenant_id_ = my_session->get_effective_tenant_id();
      mem_attr.label_ = ObModIds::OB_SQL_TABLE_LOOKUP;
      // the scanner will not transport through net, no net packet size limit (64M)
      lookup_ctx->result_.get_task_result().set_mem_size_limit(INT64_MAX);
      lookup_ctx->result_ite_ = lookup_ctx->result_.get_task_result().begin();
      lookup_ctx->set_table_location(&table_location_);
      lookup_ctx->allocator_.set_attr(mem_attr);
      lookup_ctx->set_mem_attr(mem_attr);
      lookup_ctx->result_.set_tenant_id(my_session->get_effective_tenant_id());
      lookup_ctx->schema_guard_ = sql_ctx->schema_guard_;
    }
  }
  return ret;
}

int ObTableLookup::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init op context failed", K(ret));
  }
  return ret;
}

int ObTableLookup::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTableLookupCtx* lookup_ctx = NULL;
  bool got_next_row = false;
  if (OB_ISNULL(lookup_ctx = GET_PHY_OPERATOR_CTX(ObTableLookupCtx, ctx, get_id()))) {
    LOG_WARN("get lookup ctx is null", K(ret));
  } else {
    const ObNewRow* next_row = NULL;
    do {
      switch (lookup_ctx->state_) {
        case INDEX_SCAN: {
          int64_t count = 0;
          int64_t part_row_cnt = 0;
          while (count < DEFAULT_BATCH_ROW_COUNT && part_row_cnt < DEFAULT_PARTITION_BATCH_ROW_COUNT && OB_SUCC(ret)) {
            ++count;
            if (OB_FAIL(child_op_->get_next_row(ctx, next_row))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("get next row from child failed", K(ret));
              }
            } else if (OB_FAIL(process_row(ctx, next_row, part_row_cnt))) {
              LOG_WARN("store the row failed", K(ret));
            }
          }
          if (OB_SUCC(ret) || OB_ITER_END == ret) {
            lookup_ctx->state_ = DISTRIBUTED_LOOKUP;
            lookup_ctx->set_end(OB_ITER_END == ret);
            ret = OB_SUCCESS;
          }
          break;
        }
        case DISTRIBUTED_LOOKUP: {
          if (OB_FAIL(lookup_ctx->execute(ctx))) {
            LOG_WARN("remote lookup failed", K(ret));
          } else {
            lookup_ctx->state_ = OUTPUT_ROWS;
          }
          break;
        }
        case OUTPUT_ROWS: {
          if (OB_FAIL(lookup_ctx->get_next_row(next_row))) {
            if (OB_ITER_END == ret) {
              if (!lookup_ctx->end()) {
                lookup_ctx->state_ = INDEX_SCAN;
                ret = OB_SUCCESS;
              } else {
                lookup_ctx->state_ = EXECUTION_FINISHED;
              }
            } else {
              LOG_WARN("look up get next row failed", K(ret));
            }
          } else {
            row = next_row;
            got_next_row = true;
            LOG_DEBUG("got next row from table lookup", KPC(row));
          }
          break;
        }
        case EXECUTION_FINISHED: {
          if (OB_SUCC(ret) || OB_ITER_END == ret) {
            ret = OB_ITER_END;
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected state", K(lookup_ctx->state_));
        }
      }
    } while (!(got_next_row || OB_FAIL(ret)));
  }
  return ret;
}

int ObTableLookup::process_row(ObExecContext& ctx, const ObNewRow* row, int64_t& part_row_cnt) const
{
  int ret = OB_SUCCESS;
  ObTableLookupCtx* lookup_ctx = NULL;
  ObMultiVersionSchemaService* schema_service = NULL;
  ObTaskExecutorCtx* task_ctx = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObSEArray<int64_t, 1> partition_ids;
  int64_t partition_id = -1;
  if (OB_ISNULL(lookup_ctx = GET_PHY_OPERATOR_CTX(ObTableLookupCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
  } else if (OB_FAIL(
                 table_location_.calculate_partition_id_by_row(ctx, lookup_ctx->schema_guard_, *row, partition_id))) {
    LOG_WARN("get partition range failed", K(ret));
  } else {
    // partition_id = partition_ids.at(0);
    if (OB_FAIL(lookup_ctx->store_row(partition_id, row, lookup_info_.is_old_no_pk_table_, part_row_cnt))) {
      LOG_WARN("the lookup ctx store row failed", K(ret));
    }
  }
  return ret;
}

int ObTableLookup::wait_all_task(ObTableLookupCtx& dml_ctx, ObPhysicalPlanCtx* plan_ctx) const
{
  int ret = OB_SUCCESS;
  /**
   * wait_all_task() will be called in close() of multi table dml operators, and close()
   * will be called if open() has been called, no matter open() return OB_SUCCESS or not.
   * so if we get a NULL multi_dml_ctx or plan_ctx here, just ignore it.
   */
  if (OB_NOT_NULL(plan_ctx) &&
      OB_FAIL(dml_ctx.lookup_task_executor_.wait_all_task(plan_ctx->get_timeout_timestamp()))) {
    LOG_WARN("wait all task failed", K(ret));
  }
  return ret;
}

int ObTableLookup::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableLookupCtx* lookup_ctx = NULL;
  if (OB_ISNULL(lookup_ctx = GET_PHY_OPERATOR_CTX(ObTableLookupCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    int wait_ret = wait_all_task(*lookup_ctx, ctx.get_physical_plan_ctx());
    if (OB_SUCCESS != wait_ret) {
      LOG_WARN("wait all task failed", K(wait_ret));
    }
    if (OB_FAIL(lookup_ctx->clean_mem())) {
      LOG_WARN("failed to clean ranges mem", K(ret));
    } else {
      ret = wait_ret;
      // the ObExecContext will invoke ObPhyOperatorCtx's destroy function
      // lookup_ctx->destroy();
    }
  }
  return ret;
}

int ObTableLookup::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTableLookupCtx* lookup_ctx = NULL;
  if (OB_ISNULL(lookup_ctx = GET_PHY_OPERATOR_CTX(ObTableLookupCtx, ctx, get_id()))) {
    LOG_WARN("get lookup ctx is null", K(ret));
  } else if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan operator failed", K(ret));
  } else if (OB_FAIL(lookup_ctx->clean_mem())) {
    LOG_WARN("failed to clean ranges mem", K(ret));
  } else {
    lookup_ctx->reset();
  }
  return ret;
}

int ObLookupInfo::assign(const _ObLookupInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  ref_table_id_ = other.ref_table_id_;
  partition_num_ = other.partition_num_;
  schema_version_ = other.schema_version_;
  partition_cnt_ = other.partition_cnt_;
  is_old_no_pk_table_ = other.is_old_no_pk_table_;
  return ret;
}

void ObLookupInfo::reset()
{
  // do nothing
}

OB_DEF_SERIALIZE(ObLookupInfo)
{
  int ret = OK_;
  UNF_UNUSED_SER;
  LST_DO_CODE(
      OB_UNIS_ENCODE, ref_table_id_, table_id_, partition_num_, partition_cnt_, schema_version_, is_old_no_pk_table_)
  return ret;
}

OB_DEF_DESERIALIZE(ObLookupInfo)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  LST_DO_CODE(
      OB_UNIS_DECODE, ref_table_id_, table_id_, partition_num_, partition_cnt_, schema_version_, is_old_no_pk_table_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLookupInfo)
{
  int64_t len = 0;
  LST_DO_CODE(
      OB_UNIS_ADD_LEN, ref_table_id_, table_id_, partition_num_, partition_cnt_, schema_version_, is_old_no_pk_table_);
  return len;
}

OB_SERIALIZE_MEMBER((ObTableLookup, ObSingleChildPhyOperator), lookup_info_, table_location_);

}  // namespace sql
}  // namespace oceanbase
