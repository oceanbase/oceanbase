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

#define USING_LOG_PREFIX STORAGE
#include "ob_delete_lob_meta_row_task.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace compaction;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace observer;
using namespace name;

namespace storage
{

int ObDeleteLobMetaRowParam::init(const ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg.tenant_id_;
  const int64_t table_id = arg.source_table_id_;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDeleteLobMetaRowParam has been inited before", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(tenant_id, table_id, compat_mode_))) {
    LOG_WARN("failed to get compat mode", K(ret), K(arg));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    tenant_id_ = tenant_id;
    table_id_ = table_id;
    schema_id_ = arg.dest_schema_id_;
    schema_version_ = arg.schema_version_;
    tablet_id_ = arg.source_tablet_id_;
    dest_tablet_id_ = arg.dest_tablet_id_;
    ls_id_ = arg.ls_id_;
    task_id_ = arg.task_id_;
    execution_id_ = arg.execution_id_;
    tablet_task_id_ = arg.tablet_task_id_;
    data_format_version_ = arg.data_format_version_;
    snapshot_version_ =  arg.snapshot_version_;
    FLOG_INFO("succeed to init ObDeleteLobMetaRowParam", K(ret), KPC(this));
  }
  return ret;
}
ObDeleteLobMetaRowDag::ObDeleteLobMetaRowDag()
  : ObIDag(ObDagType::DAG_TYPE_DDL_DEL_LOB_META), is_inited_(false), param_()
{
}


ObDeleteLobMetaRowDag::~ObDeleteLobMetaRowDag()
{
}

int ObDeleteLobMetaRowDag::init(const ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDeleteLobMetaRowDag has already been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg));
  } else if (OB_FAIL(param_.init(arg))) {
    LOG_WARN("fail to init dag param", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", K(ret), K(param_));
  } else {
    consumer_group_id_ = arg.consumer_group_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObDeleteLobMetaRowDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObDeleteLobMetaRowTask *delete_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_task(delete_task))) {
    LOG_WARN("allocate task failed", K(ret));
  } else if (OB_ISNULL(delete_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr task", K(ret));
  } else if (OB_FAIL(delete_task->init(param_))) {
    LOG_WARN("init prepare task failed", K(ret));
  } else if (OB_FAIL(add_task(*delete_task))) {
    LOG_WARN("add task failed", K(ret));
  }
  return ret;
}

bool ObDeleteLobMetaRowDag::ignore_warning()
{
  return OB_EAGAIN == dag_ret_
    || OB_NEED_RETRY == dag_ret_
    || OB_TASK_EXPIRED == dag_ret_;
}

int64_t ObDeleteLobMetaRowDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    tmp_ret = OB_ERR_SYS;
    LOG_ERROR("table schema must not be NULL", K(tmp_ret), K(is_inited_), K(param_));
  } else {
    hash_val = param_.tenant_id_
             + param_.table_id_
             + param_.schema_id_
             + param_.ls_id_.hash()
             + param_.tablet_id_.hash()
             + param_.dest_tablet_id_.hash()
             + ObDagType::DAG_TYPE_DDL_DEL_LOB_META;
  }
  return hash_val;
}

bool ObDeleteLobMetaRowDag::operator==(const ObIDag &other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObDeleteLobMetaRowDag &dag = static_cast<const ObDeleteLobMetaRowDag &>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !dag.param_.is_valid())) {
      tmp_ret = OB_ERR_SYS;
      LOG_ERROR("invalid argument", K(tmp_ret), K(param_), K(dag.param_));
    } else {
      is_equal = (param_.tenant_id_ == dag.param_.tenant_id_) && (param_.tenant_id_ == dag.param_.tenant_id_) &&
                 (param_.table_id_ == dag.param_.table_id_) && (param_.schema_id_ == dag.param_.schema_id_) &&
                 (param_.ls_id_ == dag.param_.ls_id_) && (param_.tablet_id_ == dag.param_.tablet_id_) &&
                 (param_.dest_tablet_id_ == dag.param_.dest_tablet_id_) &&
                 (param_.delete_lob_meta_ret_ == dag.param_.delete_lob_meta_ret_);
    }
  }
  return is_equal;
}

int ObDeleteLobMetaRowDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeleteLobMetaRowDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param_));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                param_.ls_id_.id(),
                                static_cast<int64_t>(param_.table_id_),
                                static_cast<int64_t>(param_.tablet_id_.id()),
                                static_cast<int64_t>(param_.dest_tablet_id_.id()),
                                param_.schema_version_,
                                param_.snapshot_version_))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObDeleteLobMetaRowDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeleteLobMetaRowDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(param_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "logstream_id=%ld tablet_id=%ld lob_meta_tablet_id=%ld",
                              param_.ls_id_.id(), param_.tablet_id_.id(), param_.dest_tablet_id_.id()))) {
    LOG_WARN("fill dag key for ddl table merge dag failed", K(ret), K(param_));
  }
  return ret;
}

int ObDeleteLobMetaRowDag::report_replica_build_status()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObComplementDataDag has not been inited", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_DDL_REPORT_REPLICA_BUILD_STATUS_FAIL) OB_SUCCESS;
      LOG_INFO("report replica build status errsim", K(ret));
    }
#endif
    obrpc::ObDDLBuildSingleReplicaResponseArg arg;
    ObAddr rs_addr;
    arg.tenant_id_ = param_.tenant_id_;
    arg.dest_tenant_id_ = param_.tenant_id_;
    arg.ls_id_ = param_.ls_id_;
    arg.dest_ls_id_ = param_.ls_id_;
    arg.tablet_id_ = param_.tablet_id_;
    arg.source_table_id_ = param_.table_id_;
    arg.dest_schema_id_ = param_.schema_id_;
    arg.ret_code_ = param_.delete_lob_meta_ret_;
    arg.snapshot_version_ = param_.snapshot_version_;
    arg.schema_version_ = param_.schema_version_;
    arg.dest_schema_version_ = param_.schema_version_;
    arg.task_id_ = param_.task_id_;
    arg.execution_id_ = param_.execution_id_;
    arg.server_addr_ = GCTX.self_addr();
    FLOG_INFO("send replica build status response to RS", K(ret), K(arg));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), KP(GCTX.rs_mgr_));
    } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("fail to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).build_ddl_single_replica_response(arg))) {
      LOG_WARN("fail to send build ddl single replica response", K(ret), K(arg));
    }
  }
  return ret;
}

ObDeleteLobMetaRowTask::ObDeleteLobMetaRowTask()
  : ObITask(TASK_TYPE_DELETE_LOB_META_ROW), is_inited_(false), param_(nullptr)
{
}

ObDeleteLobMetaRowTask::~ObDeleteLobMetaRowTask()
{
}

int ObDeleteLobMetaRowTask::init(ObDeleteLobMetaRowParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDeleteLobMetaRowTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    param_ = &param;
    is_inited_ = true;
  }
  return ret;
}

int ObDeleteLobMetaRowTask::init_scan_param(ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeleteLobMetaRowTask has not been inited", K(ret));
  } else {
    const uint64_t tenant_id = param_->tenant_id_;
    const int64_t table_id = param_->table_id_;
    const int64_t schema_version = param_->schema_version_;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                                            tenant_id, schema_guard))) {
      LOG_WARN("get tenant schema failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(table_id), K(tenant_id));
    } else {
      scan_param.tablet_id_ = param_->tablet_id_;
      scan_param.schema_version_ = param_->schema_version_;
      scan_param.is_get_ = false;
      scan_param.ls_id_ = param_->ls_id_;
      ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                            false, // daily_merge
                            false, // optimize
                            false, // sys scan
                            true, // full_row
                            false, // index_back
                            false, // query_stat
                            ObQueryFlag::MysqlMode, // sql_mode
                            false // read_latest
                          );
      scan_param.scan_flag_.flag_ = query_flag.flag_;
      scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
      scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
      scan_param.index_id_ = 0;
      for (uint32_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); i++) {
        const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema_by_idx(i);
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(i), KPC(this));
        } else if (column_schema->get_data_type() != ObLongTextType) {
          // do nothing
        } else if (OB_FAIL(scan_param.column_ids_.push_back(column_schema->get_column_id()))) {
          LOG_WARN("push col id failed.", K(ret), K(i));
        } else {
          collation_type_ = column_schema->get_collation_type();
        }
      }
      if (OB_SUCC(ret)) {
        scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
        // table param
        scan_param.index_id_ = 0; // table id
        // set timeout
        scan_param.timeout_ =INT64_MAX;
        // scan_param.virtual_column_exprs_
        scan_param.limit_param_.limit_ = -1;
        scan_param.limit_param_.offset_ = 0;
        scan_param.sql_mode_ = SMO_DEFAULT;
        // common set
        scan_param.allocator_ = &(param_->allocator_);
        scan_param.for_update_ = false;
        scan_param.for_update_wait_timeout_ = scan_param.timeout_;
        scan_param.scan_allocator_ = &(param_->allocator_);
        scan_param.frozen_version_ = -1;
        scan_param.force_refresh_lc_ = false;
        scan_param.output_exprs_ = nullptr;
        scan_param.aggregate_exprs_ = nullptr;
        scan_param.op_ = nullptr;
        scan_param.row2exprs_projector_ = nullptr;
        scan_param.need_scn_ = false;
        scan_param.pd_storage_flag_ = false;
        ObTableParam *table_param = NULL;
        void *buf = nullptr;
        if (OB_ISNULL(buf = param_->allocator_.alloc(sizeof(ObTableParam)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Fail to allocate memory", K(ret));
        } else {
          table_param = new (buf) ObTableParam(param_->allocator_);
          table_param->get_enable_lob_locator_v2() = true;
          table_param->set_is_vec_index(true);
          if (OB_FAIL(table_param->convert(*table_schema, scan_param.column_ids_, sql::ObStoragePushdownFlag()))) {
            LOG_WARN("failed to convert table param.", K(ret));
          } else {
            scan_param.table_param_ = table_param;
          }
        }
      } // end of init snapshot

      // init scan range
      if (OB_SUCC(ret)) {
        ObNewRange scan_range;
        scan_range.table_id_ = table_id;
        scan_range.set_whole_range();
        if (OB_FAIL(scan_param.key_ranges_.push_back(scan_range))) {
          LOG_WARN("failed to push back scan range", K(ret));
        }
      } // end of set scan range
    }
  }
  return ret;
}

int ObDeleteLobMetaRowTask::process()
{
  int ret = OB_SUCCESS;
  int end_trans_ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
   if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeleteLobMetaRowTask has not been inited", K(ret));
  } else {
    ObTableScanParam scan_param;
    transaction::ObTxDesc *tx_desc = nullptr;
    transaction::ObTransService *txs = MTL(transaction::ObTransService*);
    ObNewRowIterator *scan_iter = nullptr;
    ObAccessService *tsc_service = MTL(ObAccessService *);
    blocksstable::ObDatumRow *datum_row = nullptr;
    ObTableScanIterator *table_scan_iter = nullptr;
    storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);
    ObIDag *tmp_dag = get_dag();
    const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    if (OB_ISNULL(txs) || OB_ISNULL(tsc_service) || OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should not be null", K(ret), KP(txs), KP(tsc_service), KP(lob_mngr));
    } else if (OB_FAIL(ObInsertLobColumnHelper::start_trans(param_->ls_id_, true/*is_for_read*/, timeout_us, tx_desc))) {
      LOG_WARN("fail to get tx_desc", K(ret));
    } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, param_->ls_id_, timeout_us, scan_param.snapshot_))) {
      LOG_WARN("fail to get snapshot", K(ret));
    } else if (OB_FAIL(init_scan_param(scan_param))) {
      LOG_WARN("fail to init scan_param", K(ret));
    } else if (OB_FAIL(tsc_service->table_scan(scan_param, scan_iter))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(scan_param), K(ret));
      }
    } else if (OB_FALSE_IT(table_scan_iter = static_cast<ObTableScanIterator *>(scan_iter))) {
    } else if (OB_ISNULL(table_scan_iter)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("scan iter is nullptr", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row from snapshot table.", K(ret));
          }
        } else if (datum_row->get_column_count() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
        } else if (OB_FAIL(ObInsertLobColumnHelper::delete_lob_column(param_->allocator_,
                                                                      param_->ls_id_,
                                                                      param_->tablet_id_,
                                                                      collation_type_,
                                                                      datum_row->storage_datums_[0],
                                                                      timeout_us,
                                                                      true))) {
          LOG_WARN("failed to delete lob column", K(ret));
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }

    if (nullptr != tx_desc) {
      if (OB_SUCCESS != (end_trans_ret = ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, INT64_MAX))) {
        LOG_WARN("fail to end read trans", K(ret));
        ret = end_trans_ret;
      }
    }

    // revert_scan_iter if it is not null, even though ret != OB_SUCCESS
    if (nullptr != scan_iter && nullptr != tsc_service) {
      if (OB_SUCCESS != tsc_service->revert_scan_iter(scan_iter)) {
        LOG_WARN("fail to revert scan iter", K(ret));
      }
    }

    if (OB_NOT_NULL(tmp_dag)) {
      ObDeleteLobMetaRowDag *dag = nullptr;
      if (OB_ISNULL(tmp_dag) || ObDagType::DAG_TYPE_DDL_DEL_LOB_META != tmp_dag->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag is invalid", K(ret), KP(tmp_dag));
      } else if (FALSE_IT(dag = static_cast<ObDeleteLobMetaRowDag *>(tmp_dag))) {
      } else if (OB_SUCCESS != (tmp_ret = dag->report_replica_build_status())) {
        // do not override ret if it has already failed.
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("fail to report replica build status", K(ret), K(tmp_ret));
      }
    }

    if (OB_FAIL(ret)) {
      param_->delete_lob_meta_ret_ = ret;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

} //end namespace stroage
} //end namespace oceanbase
