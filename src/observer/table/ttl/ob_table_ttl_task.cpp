/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_table_ttl_task.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/table/ob_table_query_and_mutate_processor.h"
#include "lib/utility/utility.h"
#include "share/table/ob_table_util.h"
#include "observer/table/redis/ob_redis_iterator.h"

using namespace oceanbase::sql;
using namespace oceanbase::transaction;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::table;
using namespace oceanbase::rootserver;

/**
 * ---------------------------------------- ObTableTTLRowIterator ----------------------------------------
 */

int ObTableTTLRowIterator::init_common(const share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the table api ttl delete row iterator has been inited, ", KR(ret));
  } else {
    iter_end_ts_ = ObTimeUtility::current_time() + ONE_ITER_EXECUTE_MAX_TIME;
    ObSArray<uint64_t> full_column_ids;
    if (OB_FAIL(index_schema.get_column_ids(full_column_ids))) {
      LOG_WARN("fail to get full column ids", KR(ret));
    } else {
      bool is_index_table = index_schema.is_index_table();
      for (uint64_t i = 0; OB_SUCC(ret) && i < full_column_ids.count(); i++) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        uint64_t column_id = full_column_ids.at(i);
        if (OB_ISNULL(column_schema = index_schema.get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column schema", KR(ret), K(column_id));
        } else if (column_schema->is_shadow_column()) {
          // do nothing
        } else {
          ColumnPair pair(i, column_schema->get_column_name_str());
          pair.col_id_ = column_id;
          if ((is_index_table && column_schema->is_index_column()) ||
              (!is_index_table && column_schema->is_rowkey_column())) {
              pair.is_rowkey_column_ = true;
              rowkey_cnt_++;
              pair.rowkey_pos_idx_ = is_index_table ? column_schema->get_index_position()
                                                  : column_schema->get_rowkey_position();
          }
          if (OB_FAIL(column_pairs_.push_back(pair))) {
            LOG_WARN("fail to push back", KR(ret), K(i), "column_name", column_schema->get_column_name_str());
          }
        }
      }
    }
  }
  return ret;
}

int ObTableTTLRowIterator::close()
{
  int ret = OB_SUCCESS;
  column_pairs_.reset();
  return ObTableApiScanRowIterator::close();
}

/**
 * ---------------------------------------- ObTableTTLDeleteTask ----------------------------------------
 */
ObTableTTLDeleteTask::ObTableTTLDeleteTask():
    ObITask(ObITaskType::TASK_TYPE_TTL_DELETE),
    rowkey_allocator_(ObMemAttr(MTL_ID(), "TTLDelTaskRKey")),
    is_inited_(false),
    param_(),
    info_(),
    sess_guard_(),
    schema_guard_(),
    table_schema_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "TTLDelTaskCtx")),
    rowkey_(),
    ttl_tablet_scheduler_(NULL),
    hbase_cur_version_(0),
    scan_index_schema_(nullptr),
    scan_index_()
{
  MEMSET(scan_index_buf_, 0, sizeof(scan_index_buf_));
}

ObTableTTLDeleteTask::~ObTableTTLDeleteTask()
{
}

int ObTableTTLDeleteTask::init(ObTabletTTLScheduler *ttl_tablet_scheduler,
                               const ObTTLTaskParam &ttl_para,
                               ObTTLTaskInfo &ttl_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(ttl_tablet_scheduler) || ttl_info.table_id_ == OB_INVALID_ID ||
            !ttl_info.ls_id_.is_valid() || !ttl_info.is_valid() || ttl_para.tenant_id_ == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ttl_tablet_scheduler), K(ttl_info), K(ttl_para));
  } else {
    if (ttl_info.row_key_.empty()) {
      rowkey_.reset();
    } else {
      int64_t pos = 0;
      if (OB_FAIL(rowkey_.deserialize(rowkey_allocator_, ttl_info.row_key_.ptr(), ttl_info.row_key_.length(), pos))) {
        LOG_WARN("fail to deserialize rowkey",  KR(ret), K(ttl_info.row_key_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_credential(ttl_para))) {
      LOG_WARN("fail to init credential", KR(ret));
    } else if (OB_FAIL(init_sess_info())) {
      LOG_WARN("fail to init sess info guard", K(ret));
    } else if (OB_FAIL(init_schema_info(ttl_info.tenant_id_, ttl_info.table_id_))) {
      LOG_WARN("fail to init schema info", K(ttl_info), K(ttl_para));
    } else if (OB_FAIL(init_scan_index(ttl_info.scan_index_))) {
      LOG_WARN("fail to init scan index", K(ttl_info), K(ttl_para));
    } else {
      param_ = ttl_para;
      info_ = ttl_info;
      info_.scan_index_ = scan_index_; // record the scan index in current task to task info
      info_.reset_cnt();
      info_.err_code_ = OB_SUCCESS;
      ttl_tablet_scheduler_ = ttl_tablet_scheduler;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableTTLDeleteTask::init_scan_index(const ObString &scan_index)
{
  int ret = OB_SUCCESS;
  if (scan_index.length() < 0 || scan_index.length() > OB_MAX_OBJECT_NAME_LENGTH) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan index length is unexpected", KR(ret), K(scan_index.length()), K(scan_index));
  } else {
    MEMCPY(scan_index_buf_, scan_index.ptr(), scan_index.length());
    scan_index_.assign_ptr(scan_index_buf_, scan_index.length());
  }
  return ret;
}

int ObTableTTLDeleteTask::init_sess_info()
{
  int ret = OB_SUCCESS;
  // try get session from session pool
  if (OB_FAIL(TABLEAPI_OBJECT_POOL_MGR->get_sess_info(credential_, sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K(credential_));
  } else if (OB_ISNULL(sess_guard_.get_sess_node_val())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), K(credential_));
  }
  return ret;
}

int ObTableTTLDeleteTask::init_schema_info(int64_t tenant_id, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (schema_guard_.is_inited()) {
    // skip and do nothing
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id,
                                                    table_id,
                                                    table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema_) || table_schema_->get_table_id() == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get simple table schema", K(ret), K(table_id), KP(table_schema_));
  }
  return ret;
}

int ObTableTTLDeleteTask::init_kv_schema_guard(ObKvSchemaCacheGuard &schema_cache_guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_cache_guard.init(credential_.tenant_id_,
                                             table_schema_->get_table_id(),
                                             table_schema_->get_schema_version(),
                                             schema_guard_))) {
    LOG_WARN("fail to init schema cache guard", K(ret), K_(credential_.tenant_id));
  }
  return ret;
}

int ObTableTTLDeleteTask::init_credential(const ObTTLTaskParam &ttl_param)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = nullptr;
  int64_t tenant_id = ttl_param.tenant_id_;
  int64_t user_id = ttl_param.user_id_;
  int64_t database_id = ttl_param.database_id_;
  schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
    LOG_WARN("fail to get user id", KR(ret), K(tenant_id), K(user_id));
  } else if (OB_ISNULL(user_info)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("user not exist", KR(ret), K(tenant_id), K(user_id));
  } else {
    credential_.cluster_id_ = GCONF.cluster_id;
    credential_.tenant_id_ = tenant_id;
    credential_.user_id_ = user_id;
    credential_.database_id_ = database_id;
    credential_.expire_ts_ = 0;
    credential_.hash(credential_.hash_val_, user_info->get_passwd_str().hash());
  }

  return ret;
}

int ObTableTTLDeleteTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool need_stop = false;
    // pre-check to quick cancel/suspend current task in case of tenant ttl state changed
    // since it maybe wait in dag queue for too long
    if (OB_FAIL(ttl_tablet_scheduler_->report_task_status(info_, param_, need_stop, false))) {
      LOG_WARN("fail to report ttl task status", KR(ret));
    }
    // explicit cover retcode
    while(!need_stop) {
      lib::ContextParam param;
      param.set_mem_attr(MTL_ID(), "TTLDeleteMemCtx", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      CREATE_WITH_TEMP_CONTEXT(param) {
        if (OB_FAIL(process_one())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to process one", KR(ret));
          }
        }
        // explicit cover retcode
        if (OB_FAIL(ttl_tablet_scheduler_->report_task_status(info_, param_, need_stop))) {
          LOG_WARN("fail to report ttl task status", KR(ret));
        }
        allocator_.reuse();
      }
    }
  }
  ttl_tablet_scheduler_->dec_dag_ref();
  return ret;
}

int ObTableTTLDeleteTask::process_one()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  sql::TransState trans_state;
  ObTableTransParam trans_param;
  ObTableTTLOperationResult result;
  ObTableApiSpec *scan_spec = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObKvSchemaCacheGuard schema_cache_guard;
  ObTableApiCacheGuard cache_guard;
  ObTableTTLOperation ttl_operation(info_.tenant_id_,
                                    info_.table_id_,
                                    param_,
                                    PER_TASK_DEL_ROWS,
                                    rowkey_,
                                    hbase_cur_version_);

  SMART_VAR(ObTableCtx, scan_ctx, allocator_) {
    ObKVAttr attr;
    ObTableQuery query;
    if (OB_FAIL(init_kv_schema_guard(schema_cache_guard))) {
      LOG_WARN("fail to init kv schema cache guard", K(ret));
    } else if (OB_FAIL(schema_cache_guard.get_kv_attributes(attr))) {
      LOG_WARN("fail to get kv attributes", K(ret), K(attr));
    } else if (OB_FAIL(decide_and_check_scan_index(*table_schema_, attr))) {
      LOG_WARN("fail to decide and check scan index", K(ret), K(attr));
    } else if (OB_FAIL(init_ob_table_query(query, attr))) {
      LOG_WARN("fail to init query", K(ret), K(query), K(attr));
    } else if (OB_FAIL(init_scan_tb_ctx(schema_cache_guard, query, scan_ctx, cache_guard))) {
      LOG_WARN("fail to init tb ctx", KR(ret));
    } else if (OB_FAIL(trans_param.init(false,
                                        ObTableConsistencyLevel::STRONG,
                                        scan_ctx.get_ls_id(),
                                        get_timeout_ts(),
                                        scan_ctx.need_dist_das()))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::start_trans(trans_param))) {
      LOG_WARN("fail to start transaction", K(ret), K(scan_ctx));
    } else if (OB_FAIL(scan_ctx.init_trans(trans_param.trans_desc_, trans_param.tx_snapshot_))) {
      LOG_WARN("fail to init trans", KR(ret));
    } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&scan_ctx, scan_spec))) {
      LOG_WARN("fail to get scan spec from cache", KR(ret));
    } else {
      ObTableTTLRowIterator *row_iter = nullptr;
      if (attr.type_ == ObKVAttr::REDIS) {
        ObRedisRowIterator *redis_row_iter = nullptr;
        if (OB_ISNULL(redis_row_iter = OB_NEWx(ObRedisRowIterator, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc ObTableTTLDeleteRowIterator", K(ret));
        } else if (OB_FAIL(redis_row_iter->init_ttl(*table_schema_, ttl_operation.del_row_limit_))) {
          LOG_WARN("fail to init redis row iterator", KR(ret));
        } else {
          row_iter = redis_row_iter;
        }
      } else {
        ObTableTTLDeleteRowIterator *ttl_row_iter = nullptr;
        if (OB_ISNULL(ttl_row_iter = OB_NEWx(ObTableTTLDeleteRowIterator, &allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc ObTableTTLDeleteRowIterator", K(ret));
        } else if (OB_FAIL(ttl_row_iter->init(*scan_index_schema_, table_schema_->get_ttl_definition(), ttl_operation))) {
          LOG_WARN("fail to init ttl row iterator", KR(ret));
        } else {
          row_iter = ttl_row_iter;
        }
      }

      ObTableApiExecutor *executor = nullptr;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan_spec->create_executor(scan_ctx, executor))) {
        LOG_WARN("fail to generate executor", KR(ret), K(scan_ctx));
      } else if (OB_FAIL(row_iter->open(static_cast<ObTableApiScanExecutor*>(executor)))) {
        LOG_WARN("fail to open scan row iterator", KR(ret));
      } else if (OB_FAIL(execute_ttl_delete(schema_cache_guard,
                                            *row_iter,
                                            result,
                                            trans_param.trans_desc_,
                                            trans_param.tx_snapshot_))) {
        LOG_WARN("fail to execute ttl table", KR(ret));
      }

      if (OB_NOT_NULL(row_iter)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = row_iter->close())) {
          LOG_WARN("fail to close row iter", K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }

      if (OB_NOT_NULL(scan_spec)) {
        scan_spec->destroy_executor(executor);
        scan_ctx.set_expr_info(nullptr);
      }
    }
  }

  if (OB_NOT_NULL(trans_param.trans_state_ptr_)
      && trans_param.trans_state_ptr_->is_start_trans_executed()
      && trans_param.trans_state_ptr_->is_start_trans_success()) {
    int tmp_ret = ret;
    trans_param.is_rollback_ = (OB_SUCCESS != ret);
    trans_param.trace_info_ = &ObTableUtils::get_kv_ttl_trace_info();
    if (OB_FAIL(ObTableTransUtils::sync_end_trans(trans_param))) {
      LOG_WARN("fail to end trans", KR(ret), K(trans_param));
    }
    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  }

  info_.scan_cnt_ += result.get_scan_row();
  info_.err_code_ = ret;
  info_.row_key_ = result.get_end_rowkey();
  if (OB_SUCC(ret)) {
    info_.max_version_del_cnt_ += result.get_max_version_del_row();
    info_.ttl_del_cnt_ += result.get_ttl_del_row();
    if (result.get_del_row() < PER_TASK_DEL_ROWS
        && result.get_end_ts() > ObTimeUtility::current_time()) {
      ret = OB_ITER_END; // finsh task
      info_.err_code_ = ret;
      LOG_DEBUG("finish delete", KR(ret), K_(info));
    }
  }

  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_DEBUG("finish process one", KR(ret), K(cost), K_(info));
  return ret;
}

int ObTableTTLDeleteTask::decide_and_check_scan_index(const share::schema::ObTableSchema &table_schema,
                                                      ObKVAttr &attr)
{
  int ret = OB_SUCCESS;
  if (ObTTLUtil::is_default_scan_index(scan_index_)) {
    scan_index_schema_ = &table_schema;
  } else if (attr.type_ == ObKVAttr::REDIS) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "redis model do ttl task with scan index");
    LOG_WARN("redis model do ttl task with scan index is not supported", K(ret), K(attr));
  } else if (attr.type_ == ObKVAttr::HBASE) {
    ObHbaseModeType mode_type = ObHbaseModeType::OB_INVALID_MODE_TYPE;
    if (OB_FAIL(ObHTableUtils::get_mode_type(table_schema, mode_type))) {
      LOG_WARN("failed to get hbase mode type", K(ret), K(table_schema.get_table_id()));
    } else if (mode_type == ObHbaseModeType::OB_INVALID_MODE_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected hbase mode type", K(ret), K(mode_type));
    } else if (mode_type == ObHbaseModeType::OB_HBASE_SERIES_TYPE) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("timeseries hbase table do ttl task with scan index is not supported",
                  K(ret), K(attr));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "timeseries hbase table do ttl task with scan index");
    } else if (mode_type == ObHbaseModeType::OB_HBASE_NORMAL_TYPE) {
      const share::schema::ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(ObTTLUtil::check_index_exists(schema_guard_,
                                                table_schema.get_tenant_id(),
                                                table_schema.get_table_id(),
                                                scan_index_,
                                                index_schema))) {
        LOG_WARN("failed to check index exists", K(ret), K(scan_index_));
      } else {
        ObSEArray<ObString, 4> ttl_columns;
        if (OB_FAIL(ObTTLUtil::get_hbase_ttl_columns(table_schema, ttl_columns))) {
          LOG_WARN("failed to get hbase ttl columns", K(ret));
        } else if (OB_FAIL(ObTTLUtil::check_index_columns(*index_schema, ttl_columns, true))) {
          LOG_WARN("failed to check index columns", K(ret));
        } else {
          scan_index_schema_ = index_schema;
        }
      }
    }
  } else { // table
    ObSEArray<ObString, 4> ttl_columns;
    const share::schema::ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(ObTTLUtil::check_index_exists(schema_guard_,
                                              table_schema.get_tenant_id(),
                                              table_schema.get_table_id(),
                                              scan_index_,
                                              index_schema))) {
      LOG_WARN("failed to check index exists", K(ret), K(scan_index_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is NULL", K(ret));
    } else if (OB_FAIL(ObTTLUtil::get_ttl_columns(table_schema.get_ttl_definition(), ttl_columns))) {
      LOG_WARN("failed to get ttl columns", K(ret));
    } else if (OB_FAIL(ObTTLUtil::check_index_columns(*index_schema, ttl_columns))) {
      LOG_WARN("failed to check index columns", K(ret));
    } else {
      scan_index_schema_ = index_schema;
    }
  }

  LOG_DEBUG("decide and check scan index", K(ret), K(scan_index_), K(attr));
  return ret;
}

int ObTableTTLDeleteTask::init_ob_table_query(ObTableQuery &query, ObKVAttr &attr)
{
  int ret = OB_SUCCESS;
  ObIArray<ObNewRange> &ranges = query.get_scan_ranges();
  if (ObTTLUtil::is_default_scan_index(scan_index_)) { // use primary index scan in TTL task
    // do nothing
  } else if (OB_ISNULL(scan_index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is NULL", K(ret), K(scan_index_));
  } else {
    query.set_scan_index(scan_index_);
    for (int64_t i = 0; i < scan_index_schema_->get_column_count() && OB_SUCC(ret); i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      ObString col_name;
      if (OB_ISNULL(col_schema = scan_index_schema_->get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column schema", K(ret));
      } else if (col_schema->is_shadow_column()) {
        // do nothing
      } else if (OB_FAIL(query.add_select_column(col_schema->get_column_name_str()))) {
        LOG_WARN("fail to add select column", K(ret), K(query));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_scan_ranges(query.get_scan_ranges(), attr))) {
      LOG_WARN("fail to get scan ranges", K(ret), K(query), K(attr));
    }
  }
  return ret;
}

int ObTableTTLDeleteTask::init_scan_tb_ctx(ObKvSchemaCacheGuard &schema_cache_guard,
                                           ObTableQuery &query,
                                           ObTableCtx &tb_ctx,
                                           ObTableApiCacheGuard &cache_guard)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  tb_ctx.set_scan(true);
  tb_ctx.set_schema_cache_guard(&schema_cache_guard);
  tb_ctx.set_schema_guard(&schema_guard_);
  tb_ctx.set_simple_table_schema(table_schema_);
  tb_ctx.set_sess_guard(&sess_guard_);
  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_common(credential_, get_tablet_id(), get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", KR(ret), "table_id", get_table_id(),
      "tablet_id", get_tablet_id(), "timeout_ts", get_timeout_ts());
  } else if (OB_FAIL(tb_ctx.init_ttl_scan(query))) {
    LOG_WARN("fail to init ttl scan ctx", KR(ret), K(tb_ctx));
  } else if (OB_FAIL(cache_guard.init(&tb_ctx))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard.get_expr_info(&tb_ctx, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(tb_ctx, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", KR(ret), K(tb_ctx));
  } else {
    tb_ctx.set_init_flag(true);
    tb_ctx.set_expr_info(expr_frame_info);
  }

  return ret;
}

int ObTableTTLDeleteTask::process_ttl_delete(ObKvSchemaCacheGuard &schema_cache_guard,
                                             const ObITableEntity &new_entity,
                                             int64_t &affected_rows,
                                             transaction::ObTxDesc *trans_desc,
                                             transaction::ObTxReadSnapshot &snapshot,
                                             bool is_skip_scan)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ObTableOperationResult op_result;
  SMART_VAR(ObTableCtx, delete_ctx, allocator_) {
    delete_ctx.set_is_ttl_delete(true);
    if (OB_FAIL(init_tb_ctx(schema_cache_guard, new_entity, delete_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret), K(new_entity));
    } else if (FALSE_IT(delete_ctx.set_skip_scan(is_skip_scan))) {
    } else if (OB_FAIL(delete_ctx.init_trans(trans_desc, snapshot))) {
      LOG_WARN("fail to init trans", K(ret), K(delete_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_DELETE>(delete_ctx, op_result))) {
      LOG_WARN("fail to process delete op", K(ret));
    } else {
      affected_rows = op_result.get_affected_rows();
    }
  }
  return ret;
}

int ObTableTTLDeleteTask::init_tb_ctx(ObKvSchemaCacheGuard &schema_cache_guard,
                                      const ObITableEntity &entity,
                                      ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ctx.set_entity(&entity);
  ctx.set_entity_type(ObTableEntityType::ET_KV);
  ctx.set_operation_type(ObTableOperationType::DEL);
  ctx.set_batch_operation(NULL);
  ctx.set_schema_cache_guard(&schema_cache_guard);
  ctx.set_schema_guard(&schema_guard_);
  ctx.set_simple_table_schema(table_schema_);
  ctx.set_sess_guard(&sess_guard_);
  if (!ctx.is_init()) {
    if (OB_FAIL(ctx.init_common(credential_, get_tablet_id(), get_timeout_ts()))) {
      LOG_WARN("fail to init table ctx common part", K(ret), "table_id", get_table_id(),
        "tablet_id", get_tablet_id(), "timeout_ts", get_timeout_ts());
    } else if (OB_FAIL(ctx.init_delete())) {
      LOG_WARN("fail to init delete ctx", K(ret), K(ctx));
    } else if (OB_FAIL(ctx.init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
    } else {
      ctx.set_is_ttl_table(false);
      ctx.set_is_redis_ttl_table(false);
    }
  }

  return ret;
}

/**
 * ---------------------------------------- ObTableHRowKeyTTLDelTask ----------------------------------------
 */

ObTableHRowKeyTTLDelTask::ObTableHRowKeyTTLDelTask()
  : hrowkey_alloc_(ObMemAttr(MTL_ID(), "HRowkeyTaskAlc")),
    rowkeys_()
{
  rowkeys_.set_attr(ObMemAttr(MTL_ID(), "HRowkeyTaskRks"));
}

ObTableHRowKeyTTLDelTask::~ObTableHRowKeyTTLDelTask()
{}

int ObTableHRowKeyTTLDelTask::init(ObTabletTTLScheduler *ttl_tablet_scheduler,
                                   const table::ObTTLHRowkeyTaskParam &ttl_para,
                                   table::ObTTLTaskInfo &ttl_info)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &rowkeys = ttl_para.get_rowkeys();
  int64_t rowkey_size = rowkeys.count();
  if (OB_FAIL(rowkeys_.prepare_allocate(rowkey_size))) {
    LOG_WARN("fail to reserve rowkeys", K(ret), K(rowkey_size));
  }

  for (int i = 0; OB_SUCC(ret) && i < rowkey_size; i++) {
    if (OB_FAIL(ob_write_string(hrowkey_alloc_, rowkeys.at(i), rowkeys_.at(i)))) {
      LOG_WARN("fail to write hbase rowkey string", K(ret), K(rowkeys.at(i)));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ObTableTTLDeleteTask::init(ttl_tablet_scheduler, ttl_para, ttl_info))) {
    LOG_WARN("fail to init ObTableTTLDeleteTask", KR(ret), K(ttl_para), K(ttl_info));
  }
  return ret;
}

int ObTableHRowKeyTTLDelTask::get_scan_ranges(ObIArray<ObNewRange> &ranges, ObKVAttr &attr)
{
  int ret = OB_SUCCESS;
  UNUSED(attr);
  if (rowkeys_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkeys is empty", K(ret));
  } else {
    ObRowkey &start_rowkey = get_start_rowkey();
    uint64 rowkey_size = rowkeys_.count();
    ObString start_rowkey_str;
    if (!start_rowkey.is_valid()) {
      start_rowkey_str = rowkeys_.at(0);
    } else {
      ObObj *obj_ptr = const_cast<ObObj *>(start_rowkey.get_obj_ptr());
      start_rowkey_str = obj_ptr[ObHTableConstants::COL_IDX_K].get_string();
    }
    for (int i = 0; OB_SUCC(ret) && i < rowkeys_.count(); i++) {
      if (start_rowkey_str.compare(rowkeys_.at(i))) {
        // continue
      } else {
        ObString rowkey = rowkeys_[i];
        // make range [rowkey, min, min], [rowkey, max, max]
        // push range in ranges
        ObNewRange range;
        ObObj start_key[3];
        start_key[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey);
        start_key[ObHTableConstants::COL_IDX_Q].set_min_value();
        start_key[ObHTableConstants::COL_IDX_T].set_min_value();
        ObObj end_key[3];
        start_key[ObHTableConstants::COL_IDX_K].set_varbinary(rowkey);
        end_key[ObHTableConstants::COL_IDX_Q].set_max_value();
        end_key[ObHTableConstants::COL_IDX_T].set_max_value();
        range.start_key_.assign(start_key, 3);
        range.end_key_.assign(end_key, 3);
        if (OB_FAIL(ranges.push_back(range))) {
          LOG_WARN("fail to add scan range", K(ret));
        }
      }
    }
  }

  return ret;
}

/**
 * ---------------------------------------- ObTableTTLDag ----------------------------------------
 */
ObTableTTLDag::ObTableTTLDag()
  : ObIDag(ObDagType::DAG_TYPE_TTL),
    is_inited_(false), param_(), info_(),
    compat_mode_(lib::Worker::CompatMode::INVALID)
{
}

ObTableTTLDag::~ObTableTTLDag()
{
}

int ObTableTTLDag::init(const ObTTLTaskParam &param, ObTTLTaskInfo &info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableTTLDag has already been inited", KR(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task para", KR(ret), K(info));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task info", KR(ret), K(param));
  } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(info.tenant_id_, info.table_id_, compat_mode_))) {
    LOG_WARN("fail to get compat mode", KR(ret), K(info.tenant_id_), K(info.table_id_));
  } else {
    param_ = param;
    is_inited_ = true;
    info_ = info;
    consumer_group_id_ = info_.consumer_group_id_;
  }
  return ret;
}

bool ObTableTTLDag::operator==(const ObIDag& other) const
{
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObTableTTLDag &dag = static_cast<const ObTableTTLDag &>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !dag.param_.is_valid() ||
                    !dag.info_.is_valid() || !dag.info_.is_valid())) {
      LOG_ERROR_RET(OB_ERR_SYS, "invalid argument", K_(param), K_(info), K(dag.param_), K(dag.info_));
    } else {
      is_equal = ((info_ == dag.info_) && (param_ == dag.param_));
    }
  }
  return is_equal;
}

uint64_t ObTableTTLDag::hash() const
{
  uint64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid() || !info_.is_valid())) {
    LOG_ERROR_RET(OB_ERR_SYS, "invalid argument", K(is_inited_), K(param_));
  } else {
    hash_val = common::murmurhash(&info_.tenant_id_, sizeof(info_.tenant_id_), hash_val);
    hash_val = common::murmurhash(&info_.task_id_, sizeof(info_.task_id_), hash_val);
    hash_val = common::murmurhash(&info_.table_id_, sizeof(info_.table_id_), hash_val);
    hash_val += info_.tablet_id_.hash();
    hash_val = common::murmurhash(&param_.ttl_, sizeof(param_.ttl_), hash_val);
    hash_val = common::murmurhash(&param_.max_version_, sizeof(param_.max_version_), hash_val);
  }
  return hash_val;
}

int ObTableTTLDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableTTLDag has not been initialized", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ttl task: ls_id = %ld, tenant_id = %ld, table_id = %ld, "
                                     "tablet_id=%ld, max_version=%d, time_to_live=%d",
                                     info_.ls_id_.id(),
                                     info_.tenant_id_,
                                     info_.table_id_,
                                     info_.tablet_id_.id(),
                                     param_.max_version_,
                                     param_.ttl_))) {
    LOG_WARN("fail to fill comment", KR(ret), K(param_), K(info_));
  }
  return ret;
}

int ObTableTTLDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_NOT_INIT)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableTTLDag has not been initialized", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(info_.tenant_id_),
                                static_cast<int64_t>(info_.ls_id_.id()),
                                static_cast<int64_t>(info_.table_id_),
                                static_cast<int64_t>(info_.tablet_id_.id())))) {
    LOG_WARN("fail to fill info param", KR(ret), K_(info));
  }
  return ret;
}

ObTableTTLDeleteRowIterator::ObTableTTLDeleteRowIterator()
    : hbase_kq_allocator_(ObMemAttr(MTL_ID(), "TTLHbaseCQAlloc")),
      max_version_(0),
      time_to_live_ms_(0),
      limit_del_rows_(-1),
      cur_del_rows_(0),
      cur_rowkey_(),
      cur_qualifier_(),
      is_last_row_ttl_(true),
      is_hbase_table_(false),
      has_cell_ttl_(false),
      hbase_new_cq_(false)
{
}

int ObTableTTLDeleteRowIterator::init(const schema::ObTableSchema &index_schema,
                                      const ObString &ttl_definition,
                                      const ObTableTTLOperation &ttl_operation)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the table api ttl delete row iterator has been inited, ", KR(ret));
  } else if (OB_UNLIKELY(!ttl_operation.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl operation", KR(ret), K(ttl_operation));
  } else if (OB_FAIL(ttl_checker_.init(index_schema, ttl_definition, true))) {
    LOG_WARN("fail to init ttl checker", KR(ret));
  } else if (OB_FAIL(init_common(index_schema))) {
    LOG_WARN("fail to init ObTableTTLRowIterator", K(ret), K(index_schema));
  } else {
    time_to_live_ms_ = ttl_operation.time_to_live_ * 1000l;
    max_version_ = ttl_operation.max_version_;
    limit_del_rows_ = ttl_operation.del_row_limit_;
    is_hbase_table_ = ttl_operation.is_htable_;
    has_cell_ttl_ = ttl_operation.has_cell_ttl_;
    hbase_new_cq_ = is_hbase_table_ ? false : true;

    if (OB_SUCC(ret) && is_hbase_table_ && ttl_operation.start_rowkey_.is_valid()) {
      if (ttl_operation.start_rowkey_.get_obj_cnt() != 3 || OB_ISNULL(ttl_operation.start_rowkey_.get_obj_ptr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(ttl_operation.start_rowkey_));
      } else {
        ObObj *obj_ptr = const_cast<ObObj *>(ttl_operation.start_rowkey_.get_obj_ptr());
        cur_rowkey_ = obj_ptr[ObHTableConstants::COL_IDX_K].get_string();
        cur_qualifier_ = obj_ptr[ObHTableConstants::COL_IDX_Q].get_string();
        cur_version_ = ttl_operation.hbase_cur_version_;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObTableTTLDeleteRowIterator::covert_row_to_entity(const ObNewRow &row, ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  if (row.get_count() != column_pairs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count is not equal to column pairs count", KR(ret), K(row.get_count()), K(column_pairs_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); i++) {
      ObString column_name = column_pairs_.at(i).col_name_;
      ObObj value = row.get_cell(i);
      if (OB_FAIL(entity.set_property(column_name, value))) {
        LOG_WARN("fail to set property", KR(ret), K(column_name), K(value));
      }
    }
  }
  return ret;
}
// get next expired row to delete
int ObTableTTLDeleteRowIterator::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (cur_del_rows_ >= limit_del_rows_) {
    ret = OB_ITER_END;
    LOG_DEBUG("finish get next row", KR(ret), K(cur_del_rows_), K(limit_del_rows_));
  } else {
    bool is_expired = false;
    while(OB_SUCC(ret) && !is_expired) {
      int64_t cur_ts = ObTimeUtility::current_time();
      if (cur_ts > iter_end_ts_ && hbase_new_cq_) {
        ret = OB_ITER_END;
        LOG_DEBUG("iter_end_ts reached, stop current iterator", KR(ret), K(cur_ts), K_(iter_end_ts));
      } else if (OB_FAIL(ObTableApiScanRowIterator::get_next_row(row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        }
        last_row_ = nullptr;
      } else {
        last_row_ = row;
        // NOTE: For hbase table, the row expired if and only if
        // 1. The row's version exceed maxversion
        // 2. The row's expired time(cell_ts + ttl) exceed current time
        if (is_hbase_table_) {
          scan_cnt_++;
          ObTableEntity entity;
          if (OB_FAIL(covert_row_to_entity(*row, entity))) {
            LOG_WARN("fail to covert row to entity", KR(ret));
          } else {
            ObHTableCellEntity2 cell(&entity);
            ObString cell_rowkey = cell.get_rowkey();
            ObString cell_qualifier = cell.get_qualifier();
            int64_t cell_ts = -cell.get_timestamp(); // obhtable timestamp is nagative in ms
            if ((cell_rowkey != cur_rowkey_) || (cell_qualifier != cur_qualifier_)) {
              hbase_new_cq_ = true;
              cur_version_ = 1;
              hbase_kq_allocator_.reuse();
              if (OB_FAIL(ob_write_string(hbase_kq_allocator_, cell_rowkey, cur_rowkey_))) {
                LOG_WARN("fail to copy cell rowkey", KR(ret), K(cell_rowkey));
              } else if (OB_FAIL(ob_write_string(hbase_kq_allocator_, cell_qualifier, cur_qualifier_))) {
                LOG_WARN("fail to copy cell qualifier", KR(ret), K(cell_qualifier));
              }
            } else {
              cur_version_++;
            }
            // NOTE: after ttl_cnt_ or cur_del_rows_ is incremented, the row must be return to delete iterator
            // cuz we will check the affected_rows correctness after finish delete.
            if (max_version_ > 0 && cur_version_ > max_version_) {
              max_version_cnt_++;
              cur_del_rows_++;
              is_expired = true;
            } else if (time_to_live_ms_ > 0 && (cell_ts + time_to_live_ms_ < ObHTableUtils::current_time_millis())) {
              ttl_cnt_++;
              cur_del_rows_++;
              is_expired = true;
            } else if (has_cell_ttl_) {
              int64_t cell_ttl_time = INT64_MAX;
              if (OB_FAIL(cell.get_ttl(cell_ttl_time))) {
                LOG_WARN("fail to get ttl", KR(ret));
              } else if (cell_ttl_time < ObHTableUtils::current_time_millis() - cell_ts) {
                ttl_cnt_++;
                cur_del_rows_++;
                is_expired = true;
              }
            }
          }
        } else {
          // NOTE: For relation table, the row expired if and only if
          // 1. The row's expired time (the result of ttl definition) exceed current time
          scan_cnt_++;
          if (OB_FAIL(ttl_checker_.check_row_expired(*row, is_expired))) {
            LOG_WARN("fail to check row expired", KR(ret));
          } else if (is_expired) {
            ttl_cnt_++;
            cur_del_rows_++;
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next row", KR(ret));
    }
  }
  return ret;
}

int ObTableTTLDeleteRowIterator::close()
{
  int ret = OB_SUCCESS;
  hbase_kq_allocator_.reset();
  ttl_checker_.reset();
  return ObTableTTLRowIterator::close();
}

int ObTableTTLDeleteTask::construct_delete_entity(ObIArray<ObTableTTLRowIterator::ColumnPair> &column_pairs,
                                                  const ObNewRow &row,
                                                  ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  if (row.get_count() != column_pairs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count is not equal to column pairs count", KR(ret), K(row.get_count()), K(column_pairs.count()));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret));
  } else {
    bool is_index_scan = !ObTTLUtil::is_default_scan_index(scan_index_);
    // init rowkey array
    int64_t rowkey_cnt = table_schema_->get_rowkey_column_num();
    if (OB_FAIL(entity.init_rowkey(rowkey_cnt))) {
      LOG_WARN("fail to init rowkey", K(rowkey_cnt), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); i++) {
      ObTableTTLRowIterator::ColumnPair &pair = column_pairs.at(i);
      const ObColumnSchemaV2 *column_schema = nullptr;
      if (OB_ISNULL(column_schema = table_schema_->get_column_schema(pair.col_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get column schema", KR(ret), K(pair.col_id_));
      } else if (column_schema->is_rowkey_column()) {
        if (OB_FAIL(entity.set_rowkey_value(column_schema->get_rowkey_position() - 1, row.get_cell(i)))) {
          LOG_WARN("fail to add rowkey value", KR(ret), K(row.get_cell(i)), K(pair.col_name_));
        }
      } else if (!is_index_scan) {
        if (OB_FAIL(entity.set_property(pair.col_name_, row.get_cell(i)))) {
          LOG_WARN("fail to set property", KR(ret), K(pair.col_name_), K(row.get_cell(i)));
        }
      }
    }
  }
  return ret;
}

int ObTableTTLDeleteTask::execute_ttl_delete(ObKvSchemaCacheGuard &schema_cache_guard,
                                             ObTableTTLRowIterator &ttl_row_iter,
                                             ObTableTTLOperationResult &result,
                                             transaction::ObTxDesc *trans_desc,
                                             transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  bool is_index_scan = !ObTTLUtil::is_default_scan_index(scan_index_);
  while (OB_SUCC(ret)) {
    ObNewRow *row = nullptr;
    if (OB_FAIL(ttl_row_iter.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      int64_t tmp_affect_rows = 0;
      if (OB_FAIL(construct_delete_entity(ttl_row_iter.column_pairs_, *row, delete_entity_))) {
        LOG_WARN("fail to construct delete entity", KR(ret));
      } else if (OB_FAIL(process_ttl_delete(schema_cache_guard, delete_entity_, tmp_affect_rows, trans_desc, snapshot, !is_index_scan))) {
        LOG_WARN("fail to execute table delete", K(ret));
      } else {
        affected_rows += tmp_affect_rows;
      }
      LOG_DEBUG("[TTL Task] execute ttl delete", K(is_index_scan), KPC(row), K(delete_entity_));
    }
    delete_entity_.reset();
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    uint64_t iter_ttl_cnt = ttl_row_iter.ttl_cnt_;
    uint64_t iter_max_version_cnt = ttl_row_iter.max_version_cnt_;
    uint64_t iter_return_cnt = iter_ttl_cnt + iter_max_version_cnt;
    if (OB_UNLIKELY(affected_rows != iter_return_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected affect rows", K(ret), K(affected_rows), K(iter_return_cnt), K(iter_ttl_cnt));
    } else {
      result.ttl_del_rows_ = iter_ttl_cnt;
      result.max_version_del_rows_ = iter_max_version_cnt;
      result.scan_rows_ = ttl_row_iter.scan_cnt_;
      result.iter_end_ts_ = ttl_row_iter.iter_end_ts_;
    }
  }

  if (OB_SUCC(ret)) {
    int64_t rowkey_cnt = ttl_row_iter.rowkey_cnt_;
    if (OB_NOT_NULL(ttl_row_iter.last_row_)) {
      int64_t row_cell_cnt = ttl_row_iter.last_row_->get_count();
      if (row_cell_cnt < rowkey_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rowkey column count", KR(ret), K(row_cell_cnt), K(rowkey_cnt));
      } else {
        rowkey_.reset();
        rowkey_allocator_.reuse();
        ObObj *rowkey_buf = nullptr;
        if (OB_ISNULL(rowkey_buf = static_cast<ObObj*>(rowkey_allocator_.alloc(sizeof(ObObj) * rowkey_cnt)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc cells buffer", K(ret), K(rowkey_cnt));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < ttl_row_iter.column_pairs_.count(); i++) {
            ObTableTTLRowIterator::ColumnPair &pair = ttl_row_iter.column_pairs_.at(i);
            if (!pair.is_rowkey_column_) {
              // do nothing
            } else {
              int64_t cell_idx = pair.cell_idx_;
              int64_t rowkey_pos_idx = pair.rowkey_pos_idx_ - 1;
              if (cell_idx >= row_cell_cnt || cell_idx < 0 || rowkey_pos_idx >= rowkey_cnt || rowkey_pos_idx < 0) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected cell ids", KR(ret), K(cell_idx), K(row_cell_cnt), K(rowkey_pos_idx), K(rowkey_cnt));
              } else if (OB_FAIL(ob_write_obj(rowkey_allocator_, ttl_row_iter.last_row_->cells_[cell_idx], rowkey_buf[rowkey_pos_idx]))) {
                LOG_WARN("fail to copy obj", KR(ret), K(cell_idx), KPC(ttl_row_iter.last_row_->cells_));
              }
            }
          }

          if (OB_SUCC(ret)) {
            rowkey_.assign(rowkey_buf, rowkey_cnt);
            hbase_cur_version_ = ttl_row_iter.cur_version_;
          }
        }
      }
    }

    if (OB_SUCC(ret) && rowkey_.is_valid()) {
      // if ITER_END in ttl_row_iter, rowkey_ will not be assigned by last_row_ in this round
      ObRowkey saved_rowkey = rowkey_;
      if (param_.is_htable_) {
        // for hbase table, only k,q is saved, set t to min, cuz we do not remember version in sys table
        const int hbase_rowkey_size = 3;
        ObObj *hbase_rowkey_objs = nullptr;
        if (rowkey_.get_obj_cnt() < hbase_rowkey_size) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), K_(rowkey));
        } else if (OB_ISNULL(hbase_rowkey_objs =
            static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * hbase_rowkey_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc", K(ret), K(hbase_rowkey_size));
        } else {
          ObObj *raw_obj_ptr = const_cast<ObObj *>(rowkey_.get_obj_ptr());
          hbase_rowkey_objs[ObHTableConstants::COL_IDX_K] = raw_obj_ptr[ObHTableConstants::COL_IDX_K];
          hbase_rowkey_objs[ObHTableConstants::COL_IDX_Q] = raw_obj_ptr[ObHTableConstants::COL_IDX_Q];
          hbase_rowkey_objs[ObHTableConstants::COL_IDX_T].set_min_value();
          saved_rowkey.assign(hbase_rowkey_objs, hbase_rowkey_size);
        }
      }

      if (OB_SUCC(ret)) {
        uint64_t buf_len = saved_rowkey.get_serialize_size();
        char *buf = static_cast<char *>(allocator_.alloc(buf_len));
        int64_t pos = 0;
        if (OB_FAIL(saved_rowkey.serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize", K(ret), K(buf_len), K(pos), K_(rowkey));
        } else {
          result.end_rowkey_.assign_ptr(buf, buf_len);
        }
      }
      LOG_DEBUG("save end rowkey", KR(ret), K(saved_rowkey), KPC(ttl_row_iter.last_row_));
    }
  }
  LOG_DEBUG("execute ttl delete", K(ret), K(result));
  return ret;
}

int ObTableTTLDeleteTask::get_scan_ranges(ObIArray<ObNewRange> &ranges, const ObKVAttr &kv_attributes)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  range.end_key_.set_max_row();
  ObRowkey real_start_key;
  ObRowkey &start_rowkey = get_start_rowkey();
  if (!start_rowkey.is_valid()) {
    real_start_key.set_min_row();
  } else {
    real_start_key = start_rowkey;
    // redis ttl table should include last meta row
    if (kv_attributes.is_redis_ttl_ || !ObTTLUtil::is_default_scan_index(scan_index_)) {
      range.border_flag_.set_inclusive_start();
    }
  }
  range.start_key_ = real_start_key;

  if (OB_FAIL(ranges.push_back(range))) {
    LOG_WARN("fail to add scan range", K(ret), K(range));
  }
  LOG_DEBUG("[TTL Task] get scan ranges", K(ret), K(range));
  return ret;
}