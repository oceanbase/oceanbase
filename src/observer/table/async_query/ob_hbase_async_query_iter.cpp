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
#include "observer/table/async_query/ob_hbase_async_query_iter.h"
#include "observer/table/ob_table_audit.h"

using namespace oceanbase::sql::stmt;

namespace oceanbase
{
namespace table
{
  ObHbaseAsyncQueryIter::ObHbaseAsyncQueryIter()
    : ObIAsyncQueryIter(),
      allocator_(ObMemAttr(MTL_ID(), "HbaseAsyncQIter")),
      result_iter_(nullptr),
      cf_service_guard_(nullptr),
      query_(),
      hbase_query_(nullptr)
  {
    lease_timeout_period_ = ObHTableUtils::get_hbase_scanner_timeout(MTL_ID()) * 1000;
  }

  ObHbaseAsyncQueryIter::~ObHbaseAsyncQueryIter()
  {
    if (OB_NOT_NULL(result_iter_)) {
      result_iter_->close();
      result_iter_->~ObHbaseQueryResultIterator();
      result_iter_ = nullptr;
    }
    if (OB_NOT_NULL(cf_service_guard_)) {
      cf_service_guard_->~ObHbaseCfServiceGuard();
      cf_service_guard_ = nullptr;
    }
  }

  int ObHbaseAsyncQueryIter::start(const ObTableQueryAsyncRequest &req, ObTableExecCtx &exec_ctx, ObTableQueryAsyncResult &result)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(exec_ctx.get_audit_ctx())) {
      exec_ctx.get_audit_ctx()->partition_cnt_ = req.query_.get_tablet_ids().count();
    }
    OB_TABLE_START_AUDIT(exec_ctx.get_credential(),
                         exec_ctx.get_sess_guard(),
                         exec_ctx.get_table_name(),
                         exec_ctx.get_audit_ctx(),
                         req.query_);
    if (OB_NOT_NULL(exec_ctx.get_audit_ctx())) {
      exec_ctx.get_audit_ctx()->need_audit_ = false; // avoid record audit in tableapi
    }

    bool is_multi_cf_req = ObHTableUtils::is_tablegroup_req(req.table_name_, req.entity_type_);
    if (OB_FAIL(init_query_and_sel_cols(req, exec_ctx))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to init query and select columns", K(ret), K(req));
      }
    } else if (OB_ISNULL(cf_service_guard_ = (OB_NEWx(ObHbaseCfServiceGuard, &allocator_, allocator_, is_multi_cf_req)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cf service guard", K(ret), K(is_multi_cf_req));
    } else {
      ObHbaseColumnFamilyService *cf_service = nullptr;
      if (OB_FAIL(cf_service_guard_->get_cf_service(cf_service))) {
        LOG_WARN("failed to alloc column family service", K(ret));
      } else if (OB_ISNULL(hbase_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hbase query is null", K(ret));
      } else if (OB_FAIL(cf_service->query(*hbase_query_, exec_ctx, result_iter_))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get query result iter", K(ret), KPC_(hbase_query));
        }
      } else if (OB_FAIL(result.deep_copy_property_names(query_.get_select_columns()))) {
        LOG_WARN("fail to deep copy property names", K(ret), K(query_.get_select_columns()));
      } else if (OB_FAIL(result_iter_->get_next_result(result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else if (result_iter_->has_more_result()) {
        result.is_end_ = false;
      } else {
        // no more result
        result.is_end_ = true;
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      result.is_end_ = true;
    }

    if (OB_NOT_NULL(exec_ctx.get_audit_ctx())) {
      exec_ctx.get_audit_ctx()->need_audit_ = true;
    }
    // record ob rows
    exec_ctx.add_stat_row_count(result.get_row_count());
    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, exec_ctx.get_trans_param().tx_snapshot_,
                       stmt_type, StmtType::T_KV_QUERY,
                       return_rows, result.get_row_count(),
                       has_table_scan, true,
                       filter,
                       (OB_ISNULL(exec_ctx.get_audit_ctx())
                                ? nullptr : exec_ctx.get_audit_ctx()->filter_));
    return ret;
  }

  int ObHbaseAsyncQueryIter::init_query_and_sel_cols(const ObTableQueryAsyncRequest &req,
                                                     ObTableExecCtx &exec_ctx)
  {
    int ret = OB_SUCCESS;
    const ObTableQuery &query = req.query_;
    const uint64_t table_id = req.table_id_;
    if (OB_FAIL(query.deep_copy(allocator_, query_))) {
      LOG_WARN("fail to deep copy query", K(ret), K(query));
    } else {
      const ObIArray<ObString> &query_sel_cols = query_.get_select_columns();
      if (query_sel_cols.empty()) {
        ObKvSchemaCacheGuard &schema_cache_guard = exec_ctx.get_schema_cache_guard();
        const ObIArray<ObTableColumnInfo *> &column_infos = schema_cache_guard.get_column_info_array();
        for (int i = 0; OB_SUCC(ret) && i < column_infos.count(); i++) {
          ObTableColumnInfo *column_info = column_infos.at(i);
          if (OB_ISNULL(column_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column info is null", K(ret), K(i));
          } else {
            ObString select_col;
            if (OB_FAIL(ob_write_string(allocator_, column_info->column_name_, select_col))) {
              LOG_WARN("fail to deep copy select column", K(ret), K(column_info->column_name_));
            } else if (OB_FAIL(query_.add_select_column(select_col))) {
              LOG_WARN("fail to add select column", K(ret), K(select_col));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (query.get_tablet_ids().empty()) {
        ret = OB_ITER_END;
        LOG_DEBUG("tablet id is empty", K(ret));
      } else if (OB_ISNULL(hbase_query_ = OB_NEWx(ObHbaseQuery,
                                                  &allocator_,
                                                  table_id,
                                                  query.get_tablet_ids().at(0),
                                                  query_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObHbaseQuery", K(ret));
      } else {}
    }

    return ret;
  }

  int ObHbaseAsyncQueryIter::next(ObTableExecCtx &exec_ctx, ObTableQueryAsyncResult &result)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(exec_ctx.get_audit_ctx())) {
      exec_ctx.get_audit_ctx()->partition_cnt_ = query_.get_tablet_ids().count();
    }
    OB_TABLE_START_AUDIT(exec_ctx.get_credential(),
                         exec_ctx.get_sess_guard(),
                         exec_ctx.get_table_name(),
                         exec_ctx.get_audit_ctx(),
                         query_);
    if (OB_NOT_NULL(exec_ctx.get_audit_ctx())) {
      exec_ctx.get_audit_ctx()->need_audit_ = false; // avoid record audit in tableapi
    }

    if (OB_ISNULL(result_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null result iterator", K(ret));
    } else if (OB_FAIL(result.deep_copy_property_names(query_.get_select_columns()))) {
      LOG_WARN("fail to deep copy property names", K(ret));
    } else if (OB_FAIL(result_iter_->get_next_result(result))) {
      if (OB_ITER_END == ret) {
        result.is_end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next result", K(ret));
      }
    } else {
      result.is_end_ = !result_iter_->has_more_result();
    }

    if (OB_NOT_NULL(exec_ctx.get_audit_ctx())) {
      exec_ctx.get_audit_ctx()->need_audit_ = true;
    }
    // record ob rows
    exec_ctx.add_stat_row_count(result.get_row_count());
    OB_TABLE_END_AUDIT(ret_code, ret,
                       snapshot, exec_ctx.get_trans_param().tx_snapshot_,
                       stmt_type, StmtType::T_KV_QUERY,
                       return_rows, result.get_row_count(),
                       has_table_scan, true,
                       filter,
                       (OB_ISNULL(exec_ctx.get_audit_ctx())
                                ? nullptr : exec_ctx.get_audit_ctx()->filter_));
    return ret;
  }

  int ObHbaseAsyncQueryIter::renew(ObTableQueryAsyncResult &result)
  {
    int ret = OB_SUCCESS;
    result.is_end_ = false;
    return ret;
  }

  int ObHbaseAsyncQueryIter::end(ObTableQueryAsyncResult &result)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(result_iter_)) {
      result_iter_->close();
      result_iter_->~ObHbaseQueryResultIterator();
      result_iter_ = nullptr;
    }
    result.is_end_ = true;
    return ret;
  }

  uint64_t ObHbaseAsyncQueryIter::get_session_time_out_ts() const
  {
    return ObTimeUtility::current_time() + lease_timeout_period_;
  }

  uint64_t ObHbaseAsyncQueryIter::get_lease_timeout_period() const
  {
    return lease_timeout_period_;
  }

} // end of namespace table
} // end of namespace oceanbase
