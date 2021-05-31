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
#include "sql/engine/dml/ob_table_conflict_row_fetcher.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_query_iterator_factory.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/profile/ob_perf_event.h"
namespace oceanbase {
using namespace storage;
using namespace share;
namespace sql {
OB_DEF_SERIALIZE(ObTCRFetcherInput)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(part_conflict_rows_.count());
  ARRAY_FOREACH(part_conflict_rows_, i)
  {
    if (OB_ISNULL(part_conflict_rows_.at(i).conflict_row_store_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row store is null");
    }
    OB_UNIS_ENCODE(part_conflict_rows_.at(i).part_key_);
    OB_UNIS_ENCODE(*part_conflict_rows_.at(i).conflict_row_store_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTCRFetcherInput)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(part_conflict_rows_.count());
  ARRAY_FOREACH_NORET(part_conflict_rows_, i)
  {
    if (part_conflict_rows_.at(i).conflict_row_store_ != NULL) {
      OB_UNIS_ADD_LEN(part_conflict_rows_.at(i).part_key_);
      OB_UNIS_ADD_LEN(*part_conflict_rows_.at(i).conflict_row_store_);
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObTCRFetcherInput)
{
  int ret = OB_SUCCESS;
  int64_t conflict_row_cnt = 0;
  OB_UNIS_DECODE(conflict_row_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < conflict_row_cnt; ++i) {
    void* ptr = NULL;
    if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObRowStore)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate row store failed", K(sizeof(ObRowStore)));
    } else {
      ObRowStore* row_store = new (ptr) ObRowStore(*allocator_, ObModIds::OB_SQL_ROW_STORE, OB_SERVER_TENANT_ID, false);
      ObPartConflictRowStore conflict_row_store;
      OB_UNIS_DECODE(conflict_row_store.part_key_);
      OB_UNIS_DECODE(*row_store);
      if (OB_SUCC(ret)) {
        conflict_row_store.conflict_row_store_ = row_store;
        if (OB_FAIL(part_conflict_rows_.push_back(conflict_row_store))) {
          LOG_WARN("store conflict row_store failed", K(ret));
        }
      }
      if (OB_FAIL(ret) && row_store != NULL) {
        row_store->~ObRowStore();
      }
    }
  }
  return ret;
}

void ObTCRFetcherInput::set_deserialize_allocator(ObIAllocator* allocator)
{
  allocator_ = allocator;
}

int ObTCRFetcherInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = OB_SUCCESS;
  allocator_ = &ctx.get_allocator();
  ObPartConflictRowStore conflict_row_store;
  ObIArray<ObTaskInfo::ObPartLoc>& part_locs = task_info.get_range_location().part_locs_;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_locs.count(); ++i) {
    uint64_t part_key_ref_id = part_locs.at(i).part_key_ref_id_;
    if (part_key_ref_id == op.get_id()) {
      conflict_row_store.part_key_ = part_locs.at(i).partition_key_;
      conflict_row_store.conflict_row_store_ = part_locs.at(i).row_store_;
      if (OB_FAIL(part_conflict_rows_.push_back(conflict_row_store))) {
        LOG_WARN("store part conflict row failed", K(ret));
      }
    }
  }
  LOG_DEBUG("build TCRFetcher Input", K(ret), K_(part_conflict_rows));
  return ret;
}

ObTableConflictRowFetcher::ObTableConflictRowFetcher(ObIAllocator& alloc)
    : ObNoChildrenPhyOperator(alloc),
      table_id_(OB_INVALID_ID),
      index_tid_(OB_INVALID_ID),
      conflict_column_ids_(alloc),
      access_column_ids_(alloc),
      only_data_table_(false)
{}

ObTableConflictRowFetcher::~ObTableConflictRowFetcher()
{}

OB_SERIALIZE_MEMBER((ObTableConflictRowFetcher, ObNoChildrenPhyOperator), table_id_, index_tid_, conflict_column_ids_,
    access_column_ids_, only_data_table_);

int ObTableConflictRowFetcher::create_operator_input(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObTCRFetcherInput* input = NULL;
  if (OB_FAIL(CREATE_PHY_OP_INPUT(ObTCRFetcherInput, ctx, get_id(), get_type(), input))) {
    LOG_WARN("fail to create table scan input", K(ret), "op_id", get_id(), "op_type", get_type());
  }
  UNUSED(input);
  return ret;
}

int ObTableConflictRowFetcher::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam dml_param;
  OZ(init_op_ctx(ctx));
  OZ(ObTableModify::init_dml_param(ctx, index_tid_, *this, only_data_table_, NULL, dml_param));
  OZ(fetch_conflict_rows(ctx, dml_param));
  return ret;
}

OB_INLINE int ObTableConflictRowFetcher::fetch_conflict_rows(ObExecContext& ctx, ObDMLBaseParam& dml_param) const
{
  int ret = OB_SUCCESS;
  ObTCRFetcherCtx* fetcher_ctx = NULL;
  ObTCRFetcherInput* fetcher_input = NULL;
  ObTaskExecutorCtx* executor_ctx = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  ObPartitionService* partition_service = NULL;
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("my_session is null");
  } else if (OB_ISNULL(executor_ctx = GET_TASK_EXECUTOR_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get task executor ctx", K(ret));
  } else if (OB_ISNULL(partition_service = executor_ctx->get_partition_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get partition service", K(ret));
  } else if (OB_ISNULL(fetcher_ctx = GET_PHY_OPERATOR_CTX(ObTCRFetcherCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(fetcher_input = GET_PHY_OP_INPUT(ObTCRFetcherInput, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator input failed", K(ret), K_(id));
  } else {
    // get rowkey info of different partitions
    ObIArray<ObPartConflictRowStore>& part_conflict_rows = fetcher_input->part_conflict_rows_;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_conflict_rows.count(); ++i) {
      const ObPartitionKey& pkey = part_conflict_rows.at(i).part_key_;
      ObConflictRowIterator conflict_row_iter(part_conflict_rows.at(i).conflict_row_store_->begin());
      ObIArray<ObNewRowIterator*>& iter_array = fetcher_ctx->duplicated_iter_array_;
      if (OB_FAIL(partition_service->fetch_conflict_rows(my_session->get_trans_desc(),
              dml_param,
              pkey,
              conflict_column_ids_,
              access_column_ids_,
              conflict_row_iter,
              iter_array))) {
        LOG_WARN("fetch conflict rows from partition service failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableConflictRowFetcher::inner_close(ObExecContext& ctx) const
{
  ObTCRFetcherCtx* fetcher_ctx = GET_PHY_OPERATOR_CTX(ObTCRFetcherCtx, ctx, get_id());
  if (fetcher_ctx != NULL) {
    for (int64_t i = 0; i < fetcher_ctx->duplicated_iter_array_.count(); ++i) {
      if (fetcher_ctx->duplicated_iter_array_.at(i) != NULL) {
        ObQueryIteratorFactory::free_insert_dup_iter(fetcher_ctx->duplicated_iter_array_.at(i));
        fetcher_ctx->duplicated_iter_array_.at(i) = NULL;
      }
    }
  }
  return ObNoChildrenPhyOperator::inner_close(ctx);
}

int64_t ObTableConflictRowFetcher::to_string_kv(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(table_id), K_(conflict_column_ids), K_(access_column_ids));
  return pos;
}

int ObTableConflictRowFetcher::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObTCRFetcherCtx* fetcher_ctx = NULL;
  ObTCRFetcherInput* fetcher_input = NULL;
  if (OB_ISNULL(fetcher_ctx = GET_PHY_OPERATOR_CTX(ObTCRFetcherCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (OB_ISNULL(fetcher_input = GET_PHY_OP_INPUT(ObTCRFetcherInput, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get physical operator input failed", K(ret), K_(id));
  } else {
    bool find_next_iter = false;
    do {
      // need to convert the iterator array from storage layer to a row iterator.
      if (find_next_iter) {
        ++fetcher_ctx->curr_row_index_;
        find_next_iter = false;
      }
      if (OB_UNLIKELY(fetcher_ctx->curr_row_index_ >= fetcher_ctx->duplicated_iter_array_.count())) {
        ret = OB_ITER_END;
        LOG_DEBUG("fetch conflict row iterator end");
      } else {
        // find current row iterator
        ObNewRowIterator* duplicated_iter = fetcher_ctx->duplicated_iter_array_.at(fetcher_ctx->curr_row_index_);
        ObNewRow* tmp_row = NULL;
        if (OB_ISNULL(duplicated_iter)) {
          // find next iter
          find_next_iter = true;
        } else if (OB_FAIL(duplicated_iter->get_next_row(tmp_row))) {
          if (OB_ITER_END == ret) {
            // find next iter
            ret = OB_SUCCESS;
            find_next_iter = true;
          } else {
            LOG_WARN("get next row from duplicated iter failed", K(ret));
          }
        } else {
          row = tmp_row;
        }
      }
    } while (OB_SUCC(ret) && find_next_iter);
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("table conflict row fetch next row", KPC(row), K(fetcher_ctx->curr_row_index_));
    if (OB_FAIL(copy_cur_row(*fetcher_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  return ret;
}

int ObTableConflictRowFetcher::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObTCRFetcherCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create table conflict row operator context failed", K(ret), K(get_type()));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator context is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, need_copy_row_for_compute()))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
