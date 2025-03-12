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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_coordinator_trans.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/table_load/ob_table_load_empty_insert_tablet_ctx_manager.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace lib;
using namespace table;
using namespace sql;
using namespace obrpc;
using namespace share;

ObTableLoadCoordinatorCtx::ObTableLoadCoordinatorCtx(ObTableLoadTableCtx *ctx)
  : ctx_(ctx),
    allocator_("TLD_CoordCtx"),
    task_scheduler_(nullptr),
    exec_ctx_(nullptr),
    error_row_handler_(nullptr),
    empty_insert_tablet_ctx_manager_(nullptr),
    sequence_schema_(&allocator_),
    last_trans_gid_(1024),
    next_session_id_(0),
    status_(ObTableLoadStatusType::NONE),
    error_code_(OB_SUCCESS),
    enable_heart_beat_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  idx_array_.set_tenant_id(MTL_ID());
  commited_trans_ctx_array_.set_tenant_id(MTL_ID());
}

ObTableLoadCoordinatorCtx::~ObTableLoadCoordinatorCtx()
{
  destroy();
}

int ObTableLoadCoordinatorCtx::init(const ObIArray<uint64_t> &column_ids,
                                    const ObIArray<ObTabletID> &tablet_ids,
                                    ObTableLoadExecCtx *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadCoordinatorCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(
               column_ids.count() != ctx_->param_.column_count_ || nullptr == exec_ctx ||
               !exec_ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ctx_->param_), K(column_ids), KPC(exec_ctx));
  } else {
    if (OB_FAIL(target_schema_.init(ctx_->param_.tenant_id_, ctx_->ddl_param_.dest_table_id_))) {
      LOG_WARN("fail to init table load schema", KR(ret), K(ctx_->param_.tenant_id_),
               K(ctx_->ddl_param_.dest_table_id_));
    }
    // init column idxs
    else if (OB_FAIL(init_column_idxs(column_ids))) {
      LOG_WARN("fail to init column idxs", KR(ret), K(column_ids));
    }
    // init partition_calc_
    else if (OB_FAIL(
               partition_calc_.init(ctx_->param_, ctx_->session_info_, tablet_ids))) {
      LOG_WARN("fail to init partition calc", KR(ret));
    }
    // init trans_allocator_
    else if (OB_FAIL(trans_allocator_.init("TLD_CTransPool", ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to init trans allocator", KR(ret));
    }
    // init trans_map_
    else if (OB_FAIL(
               trans_map_.create(1024, "TLD_TransMap", "TLD_TransMap", ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to create trans map", KR(ret));
    }
    // init trans_ctx_map_
    else if (OB_FAIL(trans_ctx_map_.create(1024, "TLD_TCtxMap", "TLD_TCtxMap",
                                           ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to create trans ctx map", KR(ret));
    }
    // init segment_trans_ctx_map_
    else if (OB_FAIL(segment_ctx_map_.init("TLD_SegCtxMap", ctx_->param_.tenant_id_))) {
      LOG_WARN("fail to init segment ctx map", KR(ret));
    }
    // init error_row_handler_
    else if (OB_ISNULL(error_row_handler_ =
                         OB_NEWx(ObTableLoadErrorRowHandler, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadErrorRowHandler", KR(ret));
    } else if (OB_FAIL(error_row_handler_->init(ctx_->param_, result_info_, ctx_->job_stat_))) {
      LOG_WARN("fail to init error row handler", KR(ret));
    }
    // init sequence_cache_ and sequence_schema_
    else if (ctx_->schema_.has_identity_column_ && OB_FAIL(init_sequence())) {
      LOG_WARN("fail to init sequence", KR(ret));
    }
    // init partition ids
    else if (OB_FAIL(init_partition_ids(tablet_ids))) {
      LOG_WARN("fail to init partition ids", KR(ret));
    }
    // init empty_insert_tablet_ctx_manager_
    else if (ObDirectLoadMethod::is_full(ctx_->param_.method_)
             && !empty_partition_ids_.empty()
             && OB_FAIL(init_empty_insert_tablet_ctx_manager())) {
      LOG_WARN("fail to init empty insert tablet ctx manager", KR(ret));
    }
    if (OB_SUCC(ret)) {
      exec_ctx_ = exec_ctx;
      is_inited_ = true;
    } else {
      destroy();
    }
  }
  return ret;
}

void ObTableLoadCoordinatorCtx::stop()
{
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
  }
  LOG_INFO("coordinator ctx stop succ");
}

void ObTableLoadCoordinatorCtx::destroy()
{
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  if (nullptr != error_row_handler_) {
    error_row_handler_->~ObTableLoadErrorRowHandler();
    allocator_.free(error_row_handler_);
    error_row_handler_ = nullptr;
  }
  for (TransMap::const_iterator iter = trans_map_.begin(); iter != trans_map_.end(); ++iter) {
    ObTableLoadCoordinatorTrans *trans = iter->second;
    abort_unless(0 == trans->get_ref_count());
    trans_allocator_.free(trans);
  }
  trans_map_.reuse();
  for (TransCtxMap::const_iterator iter = trans_ctx_map_.begin(); iter != trans_ctx_map_.end();
       ++iter) {
    ObTableLoadTransCtx *trans_ctx = iter->second;
    ctx_->free_trans_ctx(trans_ctx);
  }
  if (nullptr != session_ctx_array_) {
    for (int64_t i = 0; i < ctx_->param_.write_session_count_; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->~SessionContext();
    }
    allocator_.free(session_ctx_array_);
    session_ctx_array_ = nullptr;
  }
  trans_ctx_map_.reuse();
  segment_ctx_map_.reset();
  commited_trans_ctx_array_.reset();
  if (nullptr != empty_insert_tablet_ctx_manager_) {
    empty_insert_tablet_ctx_manager_->~ObTableLoadEmptyInsertTabletCtxManager();
    allocator_.free(empty_insert_tablet_ctx_manager_);
    empty_insert_tablet_ctx_manager_ = nullptr;
  }
}

int ObTableLoadCoordinatorCtx::advance_status(ObTableLoadStatusType status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTableLoadStatusType::NONE == status || ObTableLoadStatusType::ERROR == status ||
                  ObTableLoadStatusType::ABORT == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(status));
  } else {
    obsys::ObWLockGuard guard(status_lock_);
    if (OB_UNLIKELY(ObTableLoadStatusType::ERROR == status_)) {
      ret = error_code_;
      LOG_WARN("coordinator has error", KR(ret));
    } else if (OB_UNLIKELY(ObTableLoadStatusType::ABORT == status_)) {
      ret = OB_CANCELED;
      LOG_WARN("coordinator is abort", KR(ret));
    }
    // normally, the state is advanced step by step
    else if (OB_UNLIKELY(static_cast<int64_t>(status) != static_cast<int64_t>(status_) + 1)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("unexpected status", KR(ret), K(status), K(status_));
    }
    // advance status
    else {
      status_ = status;
      table_load_status_to_string(status_, ctx_->job_stat_->coordinator_.status_);
      add_to_all_server_event();
      LOG_INFO("LOAD DATA COORDINATOR advance status", K(status));
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::set_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS == error_code)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(error_code));
  } else {
    obsys::ObWLockGuard guard(status_lock_);
    if (static_cast<int64_t>(status_) >= static_cast<int64_t>(ObTableLoadStatusType::ERROR)) {
      // ignore
    } else {
      status_ = ObTableLoadStatusType::ERROR;
      error_code_ = error_code;
      table_load_status_to_string(status_, ctx_->job_stat_->coordinator_.status_);
      add_to_all_server_event();
      LOG_INFO("LOAD DATA COORDINATOR status error", KR(error_code));
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::set_status_abort()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(status_lock_);
  if (ObTableLoadStatusType::ABORT == status_) {
    LOG_INFO("LOAD DATA COORDINATOR already abort");
  } else {
    status_ = ObTableLoadStatusType::ABORT;
    error_code_ = (error_code_ == OB_SUCCESS ? OB_CANCELED : error_code_);
    table_load_status_to_string(status_, ctx_->job_stat_->coordinator_.status_);
    add_to_all_server_event();
    LOG_INFO("LOAD DATA COORDINATOR status abort");
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::check_status(ObTableLoadStatusType status) const
{
  int ret = OB_SUCCESS;
  {
    obsys::ObRLockGuard guard(status_lock_);
    if (OB_UNLIKELY(status != status_)) {
      if (ObTableLoadStatusType::ERROR == status_) {
        ret = error_code_;
      } else if (ObTableLoadStatusType::ABORT == status_) {
        ret = OB_CANCELED;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(exec_ctx_->check_status())) {
      LOG_WARN("fail to check status", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::alloc_trans_ctx(const ObTableLoadTransId &trans_id,
                                               ObTableLoadTransCtx *&trans_ctx)
{
  int ret = OB_SUCCESS;
  trans_ctx = nullptr;
  // 分配trans_ctx
  if (OB_ISNULL(trans_ctx = ctx_->alloc_trans_ctx(trans_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc trans ctx", KR(ret), K(trans_id));
  }
  // 把trans_ctx插入map
  else if (OB_FAIL(trans_ctx_map_.set_refactored(trans_ctx->trans_id_, trans_ctx))) {
    LOG_WARN("fail to set trans ctx", KR(ret), K(trans_ctx->trans_id_));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != trans_ctx) {
      ctx_->free_trans_ctx(trans_ctx);
      trans_ctx = nullptr;
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::alloc_trans(const ObTableLoadSegmentID &segment_id,
                                           ObTableLoadCoordinatorTrans *&trans)
{
  int ret = OB_SUCCESS;
  trans = nullptr;
  const uint64_t trans_gid = ATOMIC_AAF(&last_trans_gid_, 1);
  const int32_t default_session_id =
    (ATOMIC_FAA(&next_session_id_, 1) % ctx_->param_.write_session_count_) + 1;
  ObTableLoadTransId trans_id(segment_id, trans_gid);
  ObTableLoadTransCtx *trans_ctx = nullptr;
  // 分配trans_ctx
  if (OB_FAIL(alloc_trans_ctx(trans_id, trans_ctx))) {
    LOG_WARN("fail to alloc trans ctx", KR(ret), K(trans_id));
  }
  // 构造trans
  else if (OB_ISNULL(trans = trans_allocator_.alloc(trans_ctx, default_session_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableLoadCoordinatorTrans", KR(ret));
  } else if (OB_FAIL(trans->init())) {
    LOG_WARN("fail to init trans", KR(ret), K(trans_id));
  } else if (OB_FAIL(trans_map_.set_refactored(trans_id, trans))) {
    LOG_WARN("fail to set_refactored", KR(ret), K(trans_id));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != trans) {
      trans_allocator_.free(trans);
      trans = nullptr;
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::init_column_idxs(const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  idx_array_.reset();
  const ObIArray<ObColDesc> &column_descs = ctx_->schema_.column_descs_;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
    const ObColDesc &col_desc = column_descs.at(i);
    bool found_column = (ctx_->schema_.is_table_with_hidden_pk_column_ && i == 0); // skip hidden pk in heap table
    // todo@lanyi find the pk column using column id
    // 在源数据的列数组中找到对应的列
    for (int64_t j = 0; OB_SUCC(ret) && OB_LIKELY(!found_column) && j < column_ids.count(); ++j) {
      const uint64_t column_id = column_ids.at(j);
      if (col_desc.col_id_ == column_id) {
        found_column = true;
        if (OB_FAIL(idx_array_.push_back(j))) {
          LOG_WARN("fail to push back column idx", KR(ret), K(idx_array_), K(i), K(col_desc), K(j),
                   K(column_ids));
        }
      }
    }
    if (OB_SUCC(ret) && !found_column) {
      if (OB_UNLIKELY(ctx_->param_.px_mode_)) {
        ret = OB_SCHEMA_NOT_UPTODATE;
        LOG_WARN("column not found", KR(ret), K(idx_array_), K(column_descs), K(column_ids));
      } else if (OB_FAIL(idx_array_.push_back(-1))) {
        LOG_WARN("fail to push back column idx", KR(ret), K(idx_array_), K(i), K(col_desc));
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::generate_autoinc_params(AutoincParam &autoinc_param)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObTableLoadSchema::get_table_schema(ctx_->param_.tenant_id_,
                                                  ctx_->param_.table_id_,
                                                  schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(ctx_->param_.tenant_id_),
                                         K(ctx_->param_.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(ctx_->param_.tenant_id_), K(ctx_->param_.table_id_));
  } else {
    //ddl对于auto increment是最后进行自增值同步，对于autoinc_param参数初始化得使用原表table id的table schema
    ObColumnSchemaV2 *autoinc_column_schema = nullptr;
    uint64_t column_id = 0;
    for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
         OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", KR(ret), KP(column_schema));
      } else {
        column_id = column_schema->get_column_id();
        if (column_schema->is_autoincrement() && column_id != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
          autoinc_column_schema = column_schema;
          break;
        }
      }
    }//end for
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(autoinc_column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null autoinc column schema", KR(ret), KP(autoinc_column_schema));
      } else {
        autoinc_param.tenant_id_ = ctx_->param_.tenant_id_;
        autoinc_param.autoinc_table_id_ = ctx_->param_.table_id_;
        autoinc_param.autoinc_first_part_num_ = table_schema->get_first_part_num();
        autoinc_param.autoinc_table_part_num_ = table_schema->get_all_part_num();
        autoinc_param.autoinc_col_id_ = column_id;
        autoinc_param.auto_increment_cache_size_ = MAX_INCREMENT_CACHE_SIZE;
        autoinc_param.part_level_ = table_schema->get_part_level();
        autoinc_param.autoinc_col_type_ = autoinc_column_schema->get_data_type();
        autoinc_param.total_value_count_ = 1;
        autoinc_param.autoinc_desired_count_ = 0;
        autoinc_param.autoinc_mode_is_order_ = table_schema->is_order_auto_increment_mode();
        autoinc_param.autoinc_auto_increment_ = table_schema->get_auto_increment();
        autoinc_param.autoinc_increment_ = 1;
        autoinc_param.autoinc_offset_ = 1;
        autoinc_param.part_value_no_order_ = true;
        if (autoinc_column_schema->is_tbl_part_key_column()) {
          // don't keep intra-partition value asc order when partkey column is auto inc
          autoinc_param.part_value_no_order_ = true;
        }
        autoinc_param.autoinc_version_ = table_schema->get_truncate_version();
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::init_sequence()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_->param_.tenant_id_;
  const uint64_t table_id = ctx_->ddl_param_.dest_table_id_;
  share::schema::ObSchemaGetterGuard table_schema_guard;
  share::schema::ObSchemaGetterGuard sequence_schema_guard;
  const ObSequenceSchema *sequence_schema = nullptr;
  const ObTableSchema *target_table_schema = nullptr;
  uint64_t sequence_id = OB_INVALID_ID;
  if (OB_FAIL(ObTableLoadSchema::get_table_schema(tenant_id, table_id, table_schema_guard,
                                                  target_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else {
    //ddl对于identity是建表的时候进行自增值同步，对于sequence参数初始化得用隐藏表table id的table schema
    for (ObTableSchema::const_column_iterator iter = target_table_schema->column_begin();
          OB_SUCC(ret) && iter != target_table_schema->column_end(); ++iter) {
      ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", K(column_schema));
      } else {
        uint64_t column_id = column_schema->get_column_id();
        if (column_schema->is_identity_column() && column_id != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
          sequence_id = column_schema->get_sequence_id();
          break;
        }
      }
    }//end for
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                     tenant_id,
                     sequence_schema_guard))) {
    LOG_WARN("get schema guard failed", KR(ret));
  } else if (OB_FAIL(sequence_schema_guard.get_sequence_schema(
                     tenant_id,
                     sequence_id,
                     sequence_schema))) {
    LOG_WARN("fail get sequence schema", K(sequence_id), KR(ret));
  } else if (OB_ISNULL(sequence_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", KR(ret));
  } else if (OB_FAIL(sequence_schema_.assign(*sequence_schema))) {
    LOG_WARN("cache sequence_schema fail", K(tenant_id), K(sequence_id), KR(ret));
  }
  return ret;
}

void ObTableLoadCoordinatorCtx::add_to_all_server_event()
{
  SERVER_EVENT_ADD("direct_load", "load_data",
                   "tenant_id", MTL_ID(),
                   "table_name", ctx_->job_stat_->table_name_,
                   "table_id", ctx_->job_stat_->job_id_,
                   "status", ctx_->job_stat_->coordinator_.status_);
}

int ObTableLoadCoordinatorCtx::init_session_ctx_array()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  AutoincParam autoinc_param;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(SessionContext) * ctx_->param_.write_session_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else if (ctx_->schema_.has_autoinc_column_ && OB_FAIL(generate_autoinc_params(autoinc_param))) {
    LOG_WARN("fail to init auto increment param", KR(ret));
  } else {
    session_ctx_array_ = new (buf) SessionContext[ctx_->param_.write_session_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->param_.write_session_count_; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->autoinc_param_ = autoinc_param;
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::init_partition_ids(const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *origin_table_schema = nullptr;
  const ObTableSchema *target_table_schema = nullptr;
  ObArray<ObTabletID> all_origin_tablet_ids, all_target_tablet_ids;
  ObArray<ObObjectID> all_origin_part_ids, all_target_part_ids;
  ObTableLoadPartitionId origin_id, target_id;
  ObHashSet<ObTabletID> tablet_ids_set;
  if (OB_FAIL(ObTableLoadSchema::get_schema_guard(ctx_->param_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(schema_guard,
                                                         ctx_->param_.tenant_id_,
                                                         ctx_->param_.table_id_,
                                                         origin_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(ctx_->param_.tenant_id_), K(ctx_->param_.table_id_));
  } else if (OB_ISNULL(origin_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is nullptr", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(schema_guard,
                                                         ctx_->param_.tenant_id_,
                                                         ctx_->ddl_param_.dest_table_id_,
                                                         target_table_schema))) {
    LOG_WARN("fail to get target schema", KR(ret),
                                          K(ctx_->param_.tenant_id_),
                                          K(ctx_->ddl_param_.dest_table_id_));
  } else if (OB_ISNULL(target_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target schema is nullptr", KR(ret));
  } else if (OB_FAIL(origin_table_schema->get_all_tablet_and_object_ids(all_origin_tablet_ids,
                                                                        all_origin_part_ids))) {
    LOG_WARN("fail to get all origin tablet ids and part ids", KR(ret));
  } else if (OB_FAIL(target_table_schema->get_all_tablet_and_object_ids(all_target_tablet_ids,
                                                                        all_target_part_ids))) {
    LOG_WARN("fail to get all target tablet ids and part ids", KR(ret));
  } else if (tablet_ids.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_origin_tablet_ids.count(); ++i) {
      origin_id.partition_id_ = all_origin_part_ids.at(i);
      origin_id.tablet_id_ = all_origin_tablet_ids.at(i);
      target_id.partition_id_ = all_target_part_ids.at(i);
      target_id.tablet_id_ = all_target_tablet_ids.at(i);
      if (OB_FAIL(partition_ids_.push_back(origin_id))) {
        LOG_WARN("fail to push back origin id", KR(ret));
      } else if (OB_FAIL(target_partition_ids_.push_back(target_id))) {
        LOG_WARN("fail to push back target id", KR(ret));
      }
    }
  } else if (OB_FAIL(tablet_ids_set.create(tablet_ids.count(),
                                           ObMemAttr(MTL_ID(), "TLD_TABLETID")))) {
    LOG_WARN("fail to create tablet ids set", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      if (OB_FAIL(tablet_ids_set.set_refactored(tablet_ids.at(i)))) {
        LOG_WARN("fail to set refactored", KR(ret), K(tablet_ids.at(i)));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_origin_tablet_ids.count(); ++i) {
      origin_id.partition_id_ = all_origin_part_ids.at(i);
      origin_id.tablet_id_ = all_origin_tablet_ids.at(i);
      target_id.partition_id_ = all_target_part_ids.at(i);
      target_id.tablet_id_ = all_target_tablet_ids.at(i);
      ret = tablet_ids_set.exist_refactored(all_origin_tablet_ids.at(i));
      // non_empty partition
      if (OB_HASH_EXIST == ret) {
        if (OB_FAIL(partition_ids_.push_back(origin_id))) {
          LOG_WARN("fail to push origin id", KR(ret));
        } else if (OB_FAIL(target_partition_ids_.push_back(target_id))) {
          LOG_WARN("fail to push target id", KR(ret));
        }
      }
      // empty partition
      else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(empty_partition_ids_.push_back(origin_id))) {
          LOG_WARN("fail to push empty origin id", KR(ret));
        } else if (OB_FAIL(empty_target_partition_ids_.push_back(target_id))) {
          LOG_WARN("fail to push empty target id", KR(ret));
        }
      } else {
        LOG_WARN("fail to search tablet ids set", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(partition_ids_.count() != tablet_ids.count())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition ids count is not equal to tablet ids count, schema maybe change, need retry",
                KR(ret), K(partition_ids_), K(tablet_ids));
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::init_partition_location_and_store_infos()
{
  int ret = OB_SUCCESS;
  store_infos_.reset();
  ObTableLoadArray<ObAddr> all_addr_array;
  if (OB_FAIL(ObTableLoadPartitionLocation::init_partition_location(partition_ids_,
                                                                    target_partition_ids_,
                                                                    partition_location_,
                                                                    target_partition_location_))) {
    LOG_WARN("fail to init partition location", KR(ret));
  } else if (OB_FAIL(partition_location_.get_all_leader(all_addr_array))) {
    LOG_WARN("fail to get all leader", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_addr_array.count(); ++i) {
      StoreInfo store_info;
      store_info.addr_ = all_addr_array.at(i);
      store_info.enable_heart_beat_ = false;
      if (OB_FAIL(store_infos_.push_back(store_info))) {
        LOG_WARN("fail to push back store info", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::init_empty_insert_tablet_ctx_manager()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(empty_insert_tablet_ctx_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty_insert_tablet_ctx_manager is not nullptr", KR(ret));
  } else if (OB_ISNULL(empty_insert_tablet_ctx_manager_
                        = OB_NEWx(ObTableLoadEmptyInsertTabletCtxManager,
                                  (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new empty_insert_tablet_ctx_manager", KR(ret));
  } else if (OB_FAIL(empty_insert_tablet_ctx_manager_->init(empty_partition_ids_,
                                                            empty_target_partition_ids_))) {
    LOG_WARN("fail to init empty_insert_tablet_ctx_manager", KR(ret));
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::start_trans(const ObTableLoadSegmentID &segment_id,
                                           ObTableLoadCoordinatorTrans *&trans)
{
  int ret = OB_SUCCESS;
  trans = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else if (OB_FAIL(check_status(ObTableLoadStatusType::LOADING))) {
    LOG_WARN("fail to check status", KR(ret), K_(status));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      } else {
        if (OB_FAIL(segment_ctx_map_.create(segment_id, segment_ctx))) {
          LOG_WARN("fail to create", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(nullptr != segment_ctx->current_trans_ ||
                      nullptr != segment_ctx->committed_trans_ctx_)) {
        ret = OB_ENTRY_EXIST;
        LOG_WARN("trans already exist", KR(ret));
      } else {
        if (OB_FAIL(alloc_trans(segment_id, trans))) {
          LOG_WARN("fail to alloc trans", KR(ret));
        } else {
          segment_ctx->current_trans_ = trans;
          trans->inc_ref_count();
        }
      }
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::commit_trans(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    const ObTableLoadSegmentID &segment_id = trans->get_trans_id().segment_id_;
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected trans", KR(ret));
      }
    } else if (OB_UNLIKELY(segment_ctx->current_trans_ != trans)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans", KR(ret));
    } else if (OB_FAIL(trans->check_trans_status(ObTableLoadTransStatusType::COMMIT))) {
      LOG_WARN("fail to check trans status commit", KR(ret));
    } else if (OB_FAIL(commited_trans_ctx_array_.push_back(trans->get_trans_ctx()))) {
      LOG_WARN("fail to push back trans ctx", KR(ret));
    } else {
      segment_ctx->current_trans_ = nullptr;
      segment_ctx->committed_trans_ctx_ = trans->get_trans_ctx();
      trans->set_dirty();
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::abort_trans(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    obsys::ObWLockGuard guard(rwlock_);
    const ObTableLoadSegmentID &segment_id = trans->get_trans_id().segment_id_;
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected trans", KR(ret));
      }
    } else if (OB_UNLIKELY(segment_ctx->current_trans_ != trans)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected trans", KR(ret));
    } else if (OB_FAIL(trans->check_trans_status(ObTableLoadTransStatusType::ABORT))) {
      LOG_WARN("fail to check trans status abort", KR(ret));
    } else {
      segment_ctx->current_trans_ = nullptr;
      trans->set_dirty();
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

void ObTableLoadCoordinatorCtx::put_trans(ObTableLoadCoordinatorTrans *trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else if (OB_ISNULL(trans)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(trans));
  } else {
    ObTableLoadTransCtx *trans_ctx = trans->get_trans_ctx();
    if (0 == trans->dec_ref_count() && trans->is_dirty()) {
      ObTableLoadTransStatusType trans_status = trans_ctx->get_trans_status();
      OB_ASSERT(ObTableLoadTransStatusType::COMMIT == trans_status ||
                ObTableLoadTransStatusType::ABORT == trans_status);
      obsys::ObWLockGuard guard(rwlock_);
      if (OB_FAIL(trans_map_.erase_refactored(trans->get_trans_id()))) {
        LOG_WARN("fail to erase_refactored", KR(ret));
      } else {
        trans_allocator_.free(trans);
        trans = nullptr;
      }
    }
  }
  if (OB_FAIL(ret)) {
    set_status_error(ret);
  }
}

int ObTableLoadCoordinatorCtx::get_trans(const ObTableLoadTransId &trans_id,
                                         ObTableLoadCoordinatorTrans *&trans) const
{
  int ret = OB_SUCCESS;
  trans = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_map_.get_refactored(trans_id, trans))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get_refactored", KR(ret), K(trans_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      trans->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::get_trans_ctx(const ObTableLoadTransId &trans_id,
                                             ObTableLoadTransCtx *&trans_ctx) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_ctx_map_.get_refactored(trans_id, trans_ctx))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("fail to get trans ctx", KR(ret), K(trans_id));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::get_segment_trans_ctx(const ObTableLoadSegmentID &segment_id,
                                                     ObTableLoadTransCtx *&trans_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    SegmentCtx *segment_ctx = nullptr;
    if (OB_FAIL(segment_ctx_map_.get(segment_id, segment_ctx))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get segment ctx", KR(ret));
      }
    } else if (nullptr != segment_ctx->current_trans_) {
      trans_ctx = segment_ctx->current_trans_->get_trans_ctx();
    } else if (nullptr != segment_ctx->committed_trans_ctx_) {
      trans_ctx = segment_ctx->committed_trans_ctx_;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
    if (OB_NOT_NULL(segment_ctx)) {
      segment_ctx_map_.revert(segment_ctx);
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::get_active_trans_ids(
  ObIArray<ObTableLoadTransId> &trans_id_array) const
{
  int ret = OB_SUCCESS;
  trans_id_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    for (TransMap::const_iterator trans_iter = trans_map_.begin();
         OB_SUCC(ret) && trans_iter != trans_map_.end(); ++trans_iter) {
      if (OB_FAIL(trans_id_array.push_back(trans_iter->first))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::get_committed_trans_ids(
  ObTableLoadArray<ObTableLoadTransId> &trans_id_array, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  trans_id_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    if (OB_FAIL(trans_id_array.create(commited_trans_ctx_array_.count(), allocator))) {
      LOG_WARN("fail to create trans id array", KR(ret));
    } else {
      for (int64_t i = 0; i < commited_trans_ctx_array_.count(); ++i) {
        ObTableLoadTransCtx *trans_ctx = commited_trans_ctx_array_.at(i);
        trans_id_array[i] = trans_ctx->trans_id_;
      }
    }
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::check_exist_trans(bool &is_exist) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    is_exist = !trans_map_.empty();
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::check_exist_committed_trans(bool &is_exist) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorCtx not init", KR(ret));
  } else {
    obsys::ObRLockGuard guard(rwlock_);
    is_exist = !commited_trans_ctx_array_.empty();
  }
  return ret;
}

int ObTableLoadCoordinatorCtx::init_complete()
{
  int ret = OB_SUCCESS;
  // init task_scheduler_
  if (OB_ISNULL(task_scheduler_ = OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (&allocator_),
                                          ctx_->param_.write_session_count_,
                                          ctx_->param_.table_id_,
                                          "Coordinator",
                                          ctx_->session_info_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
  }
  // init session_ctx_array_
  else if (OB_FAIL(init_session_ctx_array())) {
    LOG_WARN("fail to init session ctx array", KR(ret));
  } else if (OB_FAIL(task_scheduler_->init())) {
    LOG_WARN("fail to init task scheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->start())) {
    LOG_WARN("fail to start task scheduler", KR(ret));
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
