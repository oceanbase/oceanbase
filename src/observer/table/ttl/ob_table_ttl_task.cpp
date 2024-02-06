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
#include "share/table/ob_table_ttl_common.h"
#include "share/table/ob_table.h"
#include "observer/table/ob_table_op_wrapper.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/table/ob_table_query_common.h"
#include "observer/table/ob_table_query_and_mutate_processor.h"
#include "lib/utility/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::transaction;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::table;
using namespace oceanbase::rootserver;

const ObString ObTableTTLDeleteTask::TTL_TRACE_INFO = ObString::make_string("TTL Delete");

/**
 * ---------------------------------------- ObTableTTLDeleteTask ----------------------------------------
 */
ObTableTTLDeleteTask::ObTableTTLDeleteTask():
    ObITask(ObITaskType::TASK_TYPE_TTL_DELETE),
    is_inited_(false),
    param_(),
    info_(),
    allocator_(ObMemAttr(MTL_ID(), "TTLDelTaskCtx")),
    rowkey_(),
    ttl_tablet_mgr_(NULL),
    rowkey_allocator_(ObMemAttr(MTL_ID(), "TTLDelTaskRKey"))
{
}

ObTableTTLDeleteTask::~ObTableTTLDeleteTask()
{
}

int ObTableTTLDeleteTask::init(ObTenantTabletTTLMgr *ttl_tablet_mgr,
                               const ObTTLTaskParam &ttl_para,
                               ObTTLTaskInfo &ttl_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(ttl_tablet_mgr) || ttl_info.table_id_ == OB_INVALID_ID || !ttl_info.ls_id_.is_valid() || !ttl_info.is_valid() || ttl_para.tenant_id_ == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ttl_tablet_mgr), K(ttl_info), K(ttl_para));
  } else {
    if (ttl_info.row_key_.empty()) {
      rowkey_.reset();
    } else {
      int64_t pos = 0;
      if (OB_FAIL(rowkey_.deserialize(allocator_, ttl_info.row_key_.ptr(), ttl_info.row_key_.length(), pos))) {
        LOG_WARN("fail to deserialize rowkey",  KR(ret), K(ttl_info.row_key_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_credential(ttl_para))) {
      LOG_WARN("fail to init credential", KR(ret));
    } else {
      param_ = ttl_para;
      info_ = ttl_info;
      info_.err_code_ = OB_SUCCESS;
      ttl_tablet_mgr_ = ttl_tablet_mgr;
      is_inited_ = true;
    }
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
  } else if(OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
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
    if (OB_FAIL(ttl_tablet_mgr_->report_task_status(info_, param_, need_stop, false))) {
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
        if (OB_FAIL(ttl_tablet_mgr_->report_task_status(info_, param_, need_stop))) {
          LOG_WARN("fail to report ttl task status", KR(ret));
        }
        allocator_.reuse();
      }
    }
  }
  ttl_tablet_mgr_->dec_dag_ref();
  return ret;
}

int ObTableTTLDeleteTask::process_one()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  ObTxDesc *trans_desc = nullptr;
  ObTxReadSnapshot tx_snapshot;
  TransState trans_state;
  ObTableTTLOperationResult result;
  ObTableApiSpec *scan_spec = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObTableTTLOperation ttl_operation(info_.tenant_id_,
                                    info_.table_id_,
                                    param_,
                                    PER_TASK_DEL_ROWS,
                                    rowkey_);
  SMART_VAR(ObTableCtx, scan_ctx, allocator_) {
    if (OB_FAIL(init_scan_tb_ctx(scan_ctx, cache_guard))) {
      LOG_WARN("fail to init tb ctx", KR(ret));
    } else if (OB_FAIL(ObTableApiProcessorBase::start_trans_(
                        false,
                        trans_desc,
                        tx_snapshot,
                        ObTableConsistencyLevel::STRONG,
                        &trans_state,
                        scan_ctx.get_table_id(),
                        scan_ctx.get_ls_id(),
                        get_timeout_ts()))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(scan_ctx.init_trans(trans_desc, tx_snapshot))) {
      LOG_WARN("fail to init trans", KR(ret));
    } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&scan_ctx, scan_spec))) {
      LOG_WARN("fail to get scan spec from cache", KR(ret));
    } else {
      ObTableTTLDeleteRowIterator row_iter;
      ObTableApiExecutor *executor = nullptr;
      if (OB_FAIL(scan_spec->create_executor(scan_ctx, executor))) {
        LOG_WARN("fail to generate executor", KR(ret), K(scan_ctx));
      } else if (OB_FAIL(row_iter.init(*scan_ctx.get_table_schema(), ttl_operation))){
        LOG_WARN("fail to init ttl row iterator", KR(ret));
      } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor*>(executor)))) {
        LOG_WARN("fail to open scan row iterator", KR(ret));
      } else if (OB_FAIL(execute_ttl_delete(row_iter, result, trans_desc, tx_snapshot))) {
        LOG_WARN("fail to execute ttl table", KR(ret));
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
        LOG_WARN("fail to close row iter", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }

      if (OB_NOT_NULL(scan_spec)) {
        scan_spec->destroy_executor(executor);
        scan_ctx.set_expr_info(nullptr);
      }
    }
  }

  if (trans_state.is_start_trans_executed() && trans_state.is_start_trans_success()) {
    int tmp_ret = ret;
    if (OB_FAIL(ObTableApiProcessorBase::sync_end_trans_(OB_SUCCESS != ret, trans_desc, get_timeout_ts(),
                                                         nullptr, &TTL_TRACE_INFO))) {
      LOG_WARN("fail to end trans", KR(ret));
    }
    ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  }

  info_.max_version_del_cnt_ += result.get_max_version_del_row();
  info_.ttl_del_cnt_ += result.get_ttl_del_row();
  info_.scan_cnt_ += result.get_scan_row();
  info_.err_code_ = ret;
  info_.row_key_ = result.get_end_rowkey();
  if (OB_SUCC(ret) && result.get_del_row() < PER_TASK_DEL_ROWS) {
    ret = OB_ITER_END; // finsh task
    info_.err_code_ = ret;
    LOG_DEBUG("finish delete", KR(ret), K_(info));
  }
  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_DEBUG("finish process one", KR(ret), K(cost));
  return ret;
}

int ObTableTTLDeleteTask::init_scan_tb_ctx(ObTableCtx &tb_ctx, ObTableApiCacheGuard &cache_guard)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  tb_ctx.set_scan(true);
  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_common(credential_,
                                        get_tablet_id(),
                                        get_table_id(),
                                        get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", KR(ret), "table_id", get_table_id(),
      "tablet_id", get_tablet_id(), "timeout_ts", get_timeout_ts());
  } else if (OB_FAIL(tb_ctx.init_ttl_delete(get_start_rowkey()))) {
    LOG_WARN("fail to init delete ctx", KR(ret), K(tb_ctx));
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

int ObTableTTLDeleteTask::process_ttl_delete(const ObITableEntity &new_entity,
                                             int64_t &affected_rows,
                                             transaction::ObTxDesc *trans_desc,
                                             transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ObTableOperationResult op_result;
  SMART_VAR(ObTableCtx, delete_ctx, allocator_) {
    if (OB_FAIL(init_tb_ctx(new_entity, delete_ctx))) {
      LOG_WARN("fail to init table ctx", K(ret), K(new_entity));
    } else if (FALSE_IT(delete_ctx.set_skip_scan(true))) {
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

int ObTableTTLDeleteTask::init_tb_ctx(const ObITableEntity &entity,
                                      ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ctx.set_entity(&entity);
  ctx.set_entity_type(ObTableEntityType::ET_KV);
  ctx.set_operation_type(ObTableOperationType::DEL);
  ctx.set_batch_operation(NULL);

  if (!ctx.is_init()) {
    if (OB_FAIL(ctx.init_common(credential_,
                                get_tablet_id(),
                                get_table_id(),
                                get_timeout_ts()))) {
      LOG_WARN("fail to init table ctx common part", K(ret), "table_id", get_table_id(),
        "tablet_id", get_tablet_id(), "timeout_ts", get_timeout_ts());
    } else if (OB_FAIL(ctx.init_delete())) {
      LOG_WARN("fail to init delete ctx", K(ret), K(ctx));
    } else if (OB_FAIL(ctx.init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
    } else {
      ctx.set_is_ttl_table(false);
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

int64_t ObTableTTLDag::hash() const
{
  int64_t hash_val = 0;
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
    : allocator_(ObMemAttr(MTL_ID(), "TTLDelRowIter")),
      is_inited_(false),
      max_version_(0),
      time_to_live_ms_(0),
      limit_del_rows_(-1),
      cur_del_rows_(0),
      cur_version_(0),
      cur_rowkey_(),
      cur_qualifier_(),
      max_version_cnt_(0),
      ttl_cnt_(0),
      scan_cnt_(0),
      is_last_row_ttl_(true),
      is_hbase_table_(false),
      last_row_(nullptr),
      rowkey_cnt_(0)
{
}

int ObTableTTLDeleteRowIterator::init(const schema::ObTableSchema &table_schema,
                                      const ObTableTTLOperation &ttl_operation)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the table api ttl delete row iterator has been inited, ", KR(ret));
  } else if (OB_UNLIKELY(!ttl_operation.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl operation", KR(ret), K(ttl_operation));
  } else if (OB_FAIL(ttl_checker_.init(table_schema, true))) {
    LOG_WARN("fail to init ttl checker", KR(ret));
  } else {
    time_to_live_ms_ = ttl_operation.time_to_live_ * 1000l;
    max_version_ = ttl_operation.max_version_;
    limit_del_rows_ = ttl_operation.del_row_limit_;
    is_hbase_table_ = ttl_operation.is_htable_;
    rowkey_cnt_ = table_schema.get_rowkey_column_num();
    ObSArray<uint64_t> rowkey_column_ids;
    ObSArray<uint64_t> full_column_ids;
    if (OB_FAIL(table_schema.get_rowkey_column_ids(rowkey_column_ids))) {
      LOG_WARN("fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(table_schema.get_column_ids(full_column_ids))) {
      LOG_WARN("fail to get full column ids", KR(ret));
    } else {
      for (int64_t i = 0, idx = -1; OB_SUCC(ret) && i < rowkey_column_ids.count(); i++) {
        if (has_exist_in_array(full_column_ids, rowkey_column_ids[i], &idx)) {
          if (OB_FAIL(rowkey_cell_ids_.push_back(idx))) {
            LOG_WARN("fail to add rowkey cell idx", K(ret), K(i), K(rowkey_column_ids));
          }
        }
      }

      for (uint64_t i = 0; OB_SUCC(ret) && i < full_column_ids.count(); i++) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        uint64_t column_id = full_column_ids.at(i);
        if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column schema", KR(ret), K(column_id));
        } else if (!column_schema->is_rowkey_column()) {
          PropertyPair pair(i, column_schema->get_column_name_str());
          if (OB_FAIL(properties_pairs_.push_back(pair))) {
            LOG_WARN("fail to push back", KR(ret), K(i), "column_name", column_schema->get_column_name_str());
          }
        }
      }
    }

    if (OB_SUCC(ret) && is_hbase_table_ && ttl_operation.start_rowkey_.is_valid()) {
      if (ttl_operation.start_rowkey_.get_obj_cnt() != 3 || OB_ISNULL(ttl_operation.start_rowkey_.get_obj_ptr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(ttl_operation.start_rowkey_));
      } else {
        ObObj *obj_ptr = const_cast<ObObj *>(ttl_operation.start_rowkey_.get_obj_ptr());
        cur_rowkey_ = obj_ptr[ObHTableConstants::COL_IDX_K].get_string();
        cur_qualifier_ = obj_ptr[ObHTableConstants::COL_IDX_Q].get_string();
        is_inited_ = true;
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
      if (OB_FAIL(ObTableApiScanRowIterator::get_next_row(row, false/*need_deep_copy*/))) {
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
          ObHTableCellEntity cell(row);
          ObString cell_rowkey = cell.get_rowkey();
          ObString cell_qualifier = cell.get_qualifier();
          int64_t cell_ts = -cell.get_timestamp(); // obhtable timestamp is nagative in ms
          if ((cell_rowkey != cur_rowkey_) || (cell_qualifier != cur_qualifier_)) {
            cur_version_ = 1;
            allocator_.reuse();
            if (OB_FAIL(ob_write_string(allocator_, cell_rowkey, cur_rowkey_))) {
              LOG_WARN("fail to copy cell rowkey", KR(ret), K(cell_rowkey));
            } else if (OB_FAIL(ob_write_string(allocator_, cell_qualifier, cur_qualifier_))) {
              LOG_WARN("fail to copy cell qualifier", KR(ret), K(cell_qualifier));
            }
          } else {
            cur_version_++;
          }
          if (max_version_ > 0 && cur_version_ > max_version_) {
            max_version_cnt_++;
            cur_del_rows_++;
            is_last_row_ttl_ = false;
            is_expired = true;
          } else if (time_to_live_ms_ > 0 && (cell_ts + time_to_live_ms_ < ObHTableUtils::current_time_millis())) {
            ttl_cnt_++;
            cur_del_rows_++;
            is_last_row_ttl_ = true;
            is_expired = true;
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
            is_last_row_ttl_ = true;
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

int ObTableTTLDeleteTask::execute_ttl_delete(ObTableTTLDeleteRowIterator &ttl_row_iter,
                                             ObTableTTLOperationResult &result,
                                             transaction::ObTxDesc *trans_desc,
                                             transaction::ObTxReadSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  while (OB_SUCC(ret)) {
    ObNewRow *row = nullptr;
    if (OB_FAIL(ttl_row_iter.get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      int64_t rowkey_cnt = ttl_row_iter.rowkey_cell_ids_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
        if (OB_FAIL(delete_entity_.add_rowkey_value(row->get_cell(ttl_row_iter.rowkey_cell_ids_[i])))) {
          LOG_WARN("fail to add rowkey value", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ttl_row_iter.properties_pairs_.count(); i++) {
        ObTableTTLDeleteRowIterator::PropertyPair &pair = ttl_row_iter.properties_pairs_.at(i);
        if (OB_FAIL(delete_entity_.set_property(pair.property_name_, row->get_cell(pair.cell_idx_)))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(i), K_(ttl_row_iter.properties_pairs));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t tmp_affect_rows = 0;
        if (OB_FAIL(process_ttl_delete(delete_entity_, tmp_affect_rows, trans_desc, snapshot))) {
          LOG_WARN("fail to execute table delete", K(ret));
        } else {
          affected_rows += tmp_affect_rows;
        }
      }
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
      LOG_ERROR("unexpected affect rows", K(ret), K(affected_rows), K(iter_return_cnt));
    } else {
      result.ttl_del_rows_ = iter_ttl_cnt;
      result.max_version_del_rows_ = iter_max_version_cnt;
      result.scan_rows_ = ttl_row_iter.scan_cnt_;
    }
  }

  if (OB_SUCC(ret)) {
    int64_t rowkey_cnt = ttl_row_iter.rowkey_cell_ids_.count();
    if (OB_NOT_NULL(ttl_row_iter.last_row_)) {
      int64_t row_cell_cnt = ttl_row_iter.last_row_->get_count();
      if (row_cell_cnt < rowkey_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rowkey column count", KR(ret), K(row_cell_cnt), K(rowkey_cnt));
      } else {
        rowkey_allocator_.reuse();
        ObObj *rowkey_buf = nullptr;
        common::ObIArray<uint64_t> &row_cell_ids = ttl_row_iter.rowkey_cell_ids_;
        if (OB_ISNULL(rowkey_buf = static_cast<ObObj*>(rowkey_allocator_.alloc(sizeof(ObObj) * rowkey_cnt)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc cells buffer", K(ret), K(rowkey_cnt));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
            int64_t cell_idx = row_cell_ids.at(i);
            if (cell_idx >= row_cell_cnt || cell_idx < 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected cell ids", KR(ret), K(cell_idx), K(row_cell_cnt));
            } else if (OB_FAIL(ob_write_obj(rowkey_allocator_, ttl_row_iter.last_row_->cells_[cell_idx], rowkey_buf[i]))) {
              LOG_WARN("fail to copy obj", KR(ret), K(cell_idx), KPC(ttl_row_iter.last_row_->cells_));
            }
          }

          if (OB_SUCC(ret)) {
            rowkey_.assign(rowkey_buf, rowkey_cnt);
          }
        }
      }
    }

    if (OB_SUCC(ret) && rowkey_.is_valid()) {
      // if ITER_END in ttl_row_iter, rowkey_ will not be assigned by last_row_ in this round
      uint64_t buf_len = rowkey_.get_serialize_size();
      char *buf = static_cast<char *>(allocator_.alloc(buf_len));
      int64_t pos = 0;
      if (OB_FAIL(rowkey_.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize", K(ret), K(buf_len), K(pos), K_(rowkey));
      } else {
        result.end_rowkey_.assign_ptr(buf, buf_len);
      }
    }
  }
  LOG_DEBUG("execute ttl delete", K(ret), K(result));
  return ret;
}
