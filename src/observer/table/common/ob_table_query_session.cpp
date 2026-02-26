/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#define USING_LOG_PREFIX SERVER
#include "ob_table_query_session.h"

namespace oceanbase
{
namespace table
{

int ObITableQueryAsyncSession::init()
{
  int ret = OB_SUCCESS;
  lib::MemoryContext mem_context = nullptr;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "TbASess", ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::ALLOC_THREAD_SAFE);
  if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context, param))) {
    LOG_WARN("fail to create mem context", K(ret));
  } else if (OB_ISNULL(mem_context)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null mem context ", K(ret));
  } else if (OB_NOT_NULL(iterator_mementity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator_mementity_ should not be NULL", K(ret));
  } else {
    iterator_mementity_ = mem_context;
  }
  return ret;
}

int ObITableQueryAsyncSession::alloc_req_timeinfo()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_timeinfo_ = OB_NEWx(observer::ObReqTimeInfo, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate req time info", K(ret));
  }
  return ret;
}

int ObTableNewQueryAsyncSession::get_or_create_query_iter(ObTableEntityType entity_type, ObIAsyncQueryIter *&query_iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(query_iter_)) {
    if (entity_type == ObTableEntityType::ET_HKV) {
      if (OB_ISNULL(query_iter_ = OB_NEWx(ObHbaseAsyncQueryIter, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObHbaseAsyncQueryIter", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Get query iterator when entity type is not equals to ET_HKV is not supported", K(ret), K(entity_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Get query iterator when entity type is not equals to ET_HKV");
    }
  }

  if (OB_SUCC(ret)) {
    query_iter = query_iter_;
  }

  return ret;
}

int ObTableNewQueryAsyncSession::init_query_ctx(const uint64_t table_id,
                                                const ObTabletID tablet_id,
                                                const ObString &table_name,
                                                const bool is_tablegroup_req,
                                                table::ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;
  async_query_ctx_.credential_ = credential;
  int64_t tenant_id = credential.tenant_id_;
  if (OB_FAIL(TABLEAPI_OBJECT_POOL_MGR->get_sess_info(credential, async_query_ctx_.sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  } else if (OB_ISNULL(async_query_ctx_.sess_guard_.get_sess_node_val())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sesion info", K(ret), K(credential));
  } else if (OB_FAIL(ObHTableUtils::init_schema_info(table_name, table_id, credential, is_tablegroup_req,
                                                     async_query_ctx_.schema_guard_,
                                                     async_query_ctx_.table_schema_,
                                                     async_query_ctx_.schema_cache_guard_))) {
    LOG_WARN("fail to init schema info", K(ret), K(table_id), K(table_name));
  } else {
    async_query_ctx_.table_id_ = table_id;
    async_query_ctx_.tablet_id_ = tablet_id;
  }
  return ret;
}

int ObTableNewQueryAsyncSession::get_or_create_exec_ctx(ObTableExecCtx *&async_exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_exec_ctx_)) {
    table_exec_ctx_ = OB_NEWx(ObTableExecCtx, &allocator_, allocator_, async_query_ctx_.sess_guard_,
                              async_query_ctx_.schema_cache_guard_, nullptr, async_query_ctx_.credential_,
                              async_query_ctx_.schema_guard_, async_query_ctx_.trans_param_);
    if (OB_ISNULL(table_exec_ctx_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate table exec ctx", K(ret));
    } else {
      table_exec_ctx_->set_table_schema(async_query_ctx_.table_schema_);
    }
  }

  if (OB_SUCC(ret)) {
    async_exec_ctx = table_exec_ctx_;
  }

  return ret;
}

void ObTableNewQueryAsyncSession::set_timeout_ts()
{
  if (OB_NOT_NULL(query_iter_)) {
    int64_t timeout_ts = query_iter_->get_session_time_out_ts();
    if (timeout_ts != UINT64_MAX) {
      timeout_ts_ = timeout_ts;
    }
  }
}

uint64_t ObTableNewQueryAsyncSession::get_lease_timeout_period() const
{
  uint64_t lease_timeout_period = 0;
  if (OB_NOT_NULL(query_iter_)) {
    uint64_t timeout_priod = query_iter_->get_lease_timeout_period();
    if (timeout_priod != UINT64_MAX) {
      lease_timeout_period = timeout_priod;
    }
  }
  return lease_timeout_period;
}

void ObTableQueryAsyncSession::set_result_iterator(ObTableQueryResultIterator *query_result)
{
  result_iterator_ = query_result;
  if (OB_NOT_NULL(result_iterator_)) {
    result_iterator_->set_query(&query_);
    result_iterator_->set_query_async();
  }
}

void ObTableQueryAsyncSession::set_hbase_result_iterator(ObHbaseQueryResultIterator *hbase_result)
{
  hbase_result_iterator_ = hbase_result;
}

int ObTableQueryAsyncSession::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObITableQueryAsyncSession::init())) {
    LOG_WARN("fail to init ObITableQueryAsyncSession", K(ret));
  } else {
    lease_timeout_period_ = ObHTableUtils::get_hbase_scanner_timeout(MTL_ID()) * 1000;
  }
  return ret;
}

void ObTableQueryAsyncSession::set_timeout_ts()
{
    timeout_ts_ = ObTimeUtility::current_time() + lease_timeout_period_;
}

int ObTableQueryAsyncSession::deep_copy_select_columns(const common::ObIArray<common::ObString> &query_cols_names_,
                                                       const common::ObIArray<common::ObString> &tb_ctx_cols_names_)
{
  int ret = OB_SUCCESS;
  // use column names specified in the query if provided
  // otherwise default to column names from the table context
  const common::ObIArray<common::ObString> &source_cols = query_cols_names_.count() == 0 ? tb_ctx_cols_names_ : query_cols_names_;
  for (int64_t i = 0; OB_SUCC(ret) && i < source_cols.count(); i++) {
    common::ObString select_column;
    if (OB_FAIL(ob_write_string(allocator_, source_cols.at(i), select_column))) {
      LOG_WARN("fail to deep copy select column", K(ret), K(select_columns_.at(i)));
    } else if (OB_FAIL(select_columns_.push_back(select_column))) {
      LOG_WARN("fail to push back select column", K(ret), K(select_column));
    }
  }
  return ret;
}



} // end of namespace table
} // end of namespace oceanbase