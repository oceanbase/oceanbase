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
#include "ob_table_query_async_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "observer/ob_server.h"
#include "lib/string/ob_strings.h"
#include "lib/rc/ob_rc.h"
#include "storage/tx/ob_trans_service.h"
#include "ob_table_cg_service.h"
#include "ob_htable_filter_operator.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

/**
 * ---------------------------------------- ObTableQueryAsyncSession ----------------------------------------
 */
void ObTableQueryAsyncSession::set_result_iterator(ObTableQueryResultIterator *query_result)
{
  result_iterator_ = query_result;
  if (OB_NOT_NULL(result_iterator_)) {
    result_iterator_->set_query(&query_);
    result_iterator_->set_query_async();
  }
}

int ObTableQueryAsyncSession::init()
{
  int ret = OB_SUCCESS;
  lib::MemoryContext mem_context = nullptr;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), ObModIds::TABLE_PROC, ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::ALLOC_THREAD_SAFE);

  if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context, param))) {
    LOG_WARN("fail to create mem context", K(ret));
  } else if (OB_ISNULL(mem_context)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null mem context ", K(ret));
  } else if (OB_NOT_NULL(iterator_mementity_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator_mementity_ should be NULL", K(ret));
  } else {
    iterator_mementity_ = mem_context;
  }
  return ret;
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

/**
 * ----------------------------------- ObQueryAsyncSessionRecycle -------------------------------------
 */
void ObQueryAsyncSessionRecycle::runTimerTask()
{
  query_session_recycle();
}

void ObQueryAsyncSessionRecycle::query_session_recycle()
{
  ObQueryAsyncMgr::get_instance().clean_timeout_query_session();
}

/**
 * -----------------------------------Singleton ObQueryAsyncMgr -------------------------------------
 */
int64_t ObQueryAsyncMgr::once_ = 0;
ObQueryAsyncMgr *ObQueryAsyncMgr::instance_ = NULL;

ObQueryAsyncMgr::ObQueryAsyncMgr() : session_id_(0)
{}

ObQueryAsyncMgr &ObQueryAsyncMgr::get_instance()
{
  ObQueryAsyncMgr *instance = NULL;
  while (OB_UNLIKELY(once_ < 2)) {
    if (ATOMIC_BCAS(&once_, 0, 1)) {
      instance = OB_NEW(ObQueryAsyncMgr, ObModIds::TABLE_PROC);
      if (OB_LIKELY(OB_NOT_NULL(instance))) {
        if (common::OB_SUCCESS != instance->init()) {
          LOG_WARN_RET(OB_ERROR, "failed to init ObQueryAsyncMgr instance");
          OB_DELETE(ObQueryAsyncMgr, ObModIds::TABLE_PROC, instance);
          instance = NULL;
          ATOMIC_BCAS(&once_, 1, 0);
        } else {
          instance_ = instance;
          (void)ATOMIC_BCAS(&once_, 1, 2);
        }
      } else {
        (void)ATOMIC_BCAS(&once_, 1, 0);
      }
    }
  }
  return *(ObQueryAsyncMgr *)instance_;
}

int ObQueryAsyncMgr::init()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < DEFAULT_LOCK_ARR_SIZE; ++i) {
    locker_arr_[i].set_latch_id(ObLatchIds::TABLE_API_LOCK);
  }
  if (OB_FAIL(query_session_map_.create(QUERY_SESSION_MAX_SIZE, ObModIds::TABLE_PROC, ObModIds::TABLE_PROC))) {
    LOG_WARN("fail to create query session map", K(ret));
  } else if (FALSE_IT(timer_.set_run_wrapper(MTL_CTX()))) { // 设置当前租户上下文
  } else if (OB_FAIL(timer_.init())) {
    LOG_WARN("fail to init timer_", K(ret));
  } else if (OB_FAIL(timer_.schedule(query_session_recycle_, QUERY_SESSION_CLEAN_DELAY, true))) {
    LOG_WARN("fail to schedule query session clean task. ", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    LOG_WARN("fail to start query session clean task timer.", K(ret));
  }
  return ret;
}

uint64_t ObQueryAsyncMgr::generate_query_sessid()
{
  return ATOMIC_AAF(&session_id_, 1);
}

int ObQueryAsyncMgr::get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  get_locker(sessid).lock();
  if (OB_FAIL(query_session_map_.get_refactored(sessid, query_session))) {
    if (OB_HASH_NOT_EXIST != ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get session from query session map", K(ret));
    }
  } else if (OB_ISNULL(query_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null query session", K(ret), K(sessid));
  } else if (query_session->is_in_use()) { // one session cannot be held concurrently
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query session already in use", K(sessid));
  } else {
    query_session->set_in_use(true);
  }
  get_locker(sessid).unlock();
  return ret;
}

int ObQueryAsyncMgr::set_query_session(uint64_t sessid, ObTableQueryAsyncSession *query_session)
{
  int ret = OB_SUCCESS;
  bool force = false;
  if (OB_FAIL(query_session_map_.set_refactored(sessid, query_session, force))) {
    LOG_WARN("set query session failed", K(ret), K(sessid));
  }
  return ret;
}

void ObQueryAsyncMgr::clean_timeout_query_session()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<uint64_t, 128> session_id_array;
  ObGetAllSessionIdOp op(session_id_array);
  if (OB_FAIL(query_session_map_.foreach_refactored(op))) {
    LOG_WARN("fail to get all session id from query sesion map", K(ret));
  } else {
    for (int64_t i = 0; i < session_id_array.count(); i++) {
      uint64_t sess_id = session_id_array.at(i);
      ObTableQueryAsyncSession *query_session = nullptr;
      get_locker(sess_id).lock();
      if (OB_FAIL(query_session_map_.get_refactored(sess_id, query_session))) {
        LOG_DEBUG("query session already deleted by worker", K(ret), K(sess_id));
      } else if (OB_ISNULL(query_session)) {
        ret = OB_ERR_NULL_VALUE;
        (void)query_session_map_.erase_refactored(sess_id);
        LOG_WARN("unexpected null query sesion", K(ret));
      } else if (query_session->is_in_use()) {
      } else if (query_session->timeout_ts_ >= ObTimeUtility::current_time()) {
      } else {
        ObObjectID tenant_id = query_session->get_tenant_id();
        MTL_SWITCH(tenant_id) {
          ObAccessService *access_service = NULL;
          if (OB_FAIL(rollback_trans(*query_session))) {
            LOG_WARN("failed to rollback trans for query session", K(ret), K(sess_id));
          }
          (void)query_session_map_.erase_refactored(sess_id);
          OB_DELETE(ObTableQueryAsyncSession, ObModIds::TABLE_PROC, query_session);
          // connection loses or bug exists
          LOG_WARN("clean timeout query session success", K(ret), K(sess_id));
        } else {
          LOG_WARN("fail to switch tenant", K(ret), K(tenant_id));
        }
      }
      get_locker(sess_id).unlock();
    }
  }
}

int ObQueryAsyncMgr::rollback_trans(ObTableQueryAsyncSession &query_session)
{
  int ret = OB_SUCCESS;
  sql::TransState &trans_state = query_session.trans_state_;
  if (trans_state.is_start_trans_executed() && trans_state.is_start_trans_success()) {
    transaction::ObTxDesc *trans_desc = query_session.trans_desc_;
    if (OB_ISNULL(trans_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null trans_desc", K(ret));
    } else {
      transaction::ObTransService *txs = MTL(transaction::ObTransService*);
      // sync rollback tx
      NG_TRACE(T_end_trans_begin);
      if (OB_FAIL(txs->rollback_tx(*trans_desc))) {
        LOG_WARN("fail to rollback trans", KR(ret), KPC(trans_desc));
      } else {
        txs->release_tx(*trans_desc);
      }
      trans_state.clear_start_trans_executed();
      NG_TRACE(T_end_trans_end);
    }
  }
  LOG_DEBUG("ObQueryAsyncMgr::rollback_trans", KR(ret));
  query_session.trans_desc_ = NULL;
  trans_state.reset();
  return ret;
}

ObQueryAsyncMgr::ObQueryHashMap *ObQueryAsyncMgr::get_query_session_map()
{
  return &query_session_map_;
}

ObTableQueryAsyncSession *ObQueryAsyncMgr::alloc_query_session()
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncSession *query_session = OB_NEW(ObTableQueryAsyncSession, ObModIds::TABLE_PROC);
  if (OB_ISNULL(query_session)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ObTableQueryAsyncSession", K(ret));
  } else if (OB_FAIL(query_session->init())) {
    LOG_WARN("failed to init query session", K(ret));
    OB_DELETE(ObTableQueryAsyncSession, ObModIds::TABLE_PROC, query_session);
  }
  return query_session;
}

int ObQueryAsyncMgr::ObGetAllSessionIdOp::operator()(QuerySessionPair &entry) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(session_id_array_.push_back(entry.first))) {
    LOG_WARN("fail to push back query session id", K(ret));
  }
  return ret;
}

/**
 * -------------------------------------- ObTableQueryAsyncP ----------------------------------------
 */
ObTableQueryAsyncP::ObTableQueryAsyncP(const ObGlobalContext &gctx)
    : ObTableRpcProcessor(gctx),
      result_row_count_(0),
      query_session_id_(0),
      allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      query_session_(nullptr),
      is_full_table_scan_(false)
{}

int ObTableQueryAsyncP::deserialize()
{
  arg_.query_.set_deserialize_allocator(&allocator_);
  return ParentType::deserialize();
}

int ObTableQueryAsyncP::check_arg()
{
  int ret = OB_SUCCESS;
  if (arg_.query_type_ == ObQueryOperationType::QUERY_START && !arg_.query_.is_valid() &&
      arg_.query_.get_htable_filter().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table query request", K(ret), "query", arg_.query_);
  } else if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
                 arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
    LOG_WARN("some options not supported yet", K(ret), "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

void ObTableQueryAsyncP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObTableConsistencyLevel::STRONG == arg_.consistency_level_
                                         ? ObConsistencyLevel::STRONG
                                         : ObConsistencyLevel::WEAK;
  audit_record_.return_rows_ = result_.get_row_count();
  audit_record_.table_scan_ = is_full_table_scan_;
  audit_record_.affected_rows_ = 0;
  audit_record_.try_cnt_ = retry_count_ + 1;
}

uint64_t ObTableQueryAsyncP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  const uint64_t op_checksum = arg_.query_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  return checksum;
}

void ObTableQueryAsyncP::reset_ctx()
{
  result_row_count_ = 0;
  query_session_ = nullptr;
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQueryAsyncP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

int ObTableQueryAsyncP::get_session_id(uint64_t &real_sessid, uint64_t arg_sessid)
{
  int ret = OB_SUCCESS;
  real_sessid = arg_sessid;
  if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
    real_sessid = ObQueryAsyncMgr::get_instance().generate_query_sessid();
  }
  if (OB_UNLIKELY(real_sessid == ObQueryAsyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("session id is invalid", K(ret), K(real_sessid), K(arg_.query_type_));
  }
  return ret;
}

int ObTableQueryAsyncP::get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sessid == ObQueryAsyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("fail to get query session, session id is invalid", K(ret), K(sessid));
  } else if (ObQueryOperationType::QUERY_START == arg_.query_type_) { // query start
    query_session = ObQueryAsyncMgr::get_instance().alloc_query_session();
    if (OB_ISNULL(query_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate ObTableQueryAsyncSession", K(ret), K(sessid));
    } else if (OB_FAIL(ObQueryAsyncMgr::get_instance().set_query_session(sessid, query_session))) {
      LOG_WARN("fail to insert session to query map", K(ret), K(sessid));
      OB_DELETE(ObTableQueryAsyncSession, ObModIds::TABLE_PROC, query_session);
    } else {}
  } else if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_ || ObQueryOperationType::QUERY_END == arg_.query_type_) {
    if (OB_FAIL(ObQueryAsyncMgr::get_instance().get_query_session(sessid, query_session))) {
      LOG_WARN("fail to get query session from query sync mgr", K(ret), K(sessid));
    } else if (OB_ISNULL(query_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query session", K(ret), K(sessid));
    } else {
      // hook processor's trans_desc_
      trans_desc_ = query_session->get_trans_desc();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unkown query type", K(arg_.query_type_));
  }

  if (OB_SUCC(ret)) {
    trans_state_ptr_ = query_session->get_trans_state(); // hook processor's trans_state_
    query_session->set_timout_ts(get_timeout_ts());
  }

  return ret;
}

int ObTableQueryAsyncP::init_tb_ctx(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = *query_session_->get_allocator();
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObExprFrameInfo &expr_frame_info = query_ctx.expr_frame_info_;
  bool is_weak_read = arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL;
  ctx.set_scan(true);
  ctx.set_entity_type(arg_.entity_type_);

  if (ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(ctx));
  } else if (OB_FAIL(ctx.init_common(credential_,
                                     arg_.tablet_id_,
                                     arg_.table_name_,
                                     get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(ctx.init_scan(query_session_->get_query(), is_weak_read))) {
    LOG_WARN("fail to init table ctx scan part", K(ret), K(arg_.table_name_));
  } else if (arg_.table_id_ != ctx.get_ref_table_id()) { // todo: global_index need adapt
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("arg table id is not equal to schema table id", K(ret), K(arg_.table_id_), K(ctx.get_ref_table_id()));
  } else if (OB_FAIL(ObTableExprCgService::generate_exprs(ctx,
                                                          allocator,
                                                          expr_frame_info))) {
    LOG_WARN("fail to generate exprs", K(ret), K(ctx));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(ctx, expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(ctx.classify_scan_exprs())) {
    LOG_WARN("fail to classify scan exprs", K(ret));
  } else if (OB_FAIL(ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
  } else {
    ctx.set_init_flag(true);
    ctx.set_expr_info(&query_ctx.expr_frame_info_);
  }

  return ret;
}

int ObTableQueryAsyncP::execute_query()
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = query_session_->get_allocator();
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableQuery &query = query_session_->get_query();
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  ObTableCtx &tb_ctx = query_ctx.tb_ctx_;
  ObTableApiScanRowIterator &row_iter = query_ctx.row_iter_;
  ObTableQueryResultIterator *result_iter = nullptr;
  bool is_hkv = (ObTableEntityType::ET_HKV == arg_.entity_type_);

  // 1. create scan executor
  if (OB_FAIL(ObTableSpecCgService::generate<TABLE_API_EXEC_SCAN>(*allocator, tb_ctx, spec))) {
    LOG_WARN("fail to generate scan spec", K(ret), K(tb_ctx));
  } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
    LOG_WARN("fail to generate executor", K(ret), K(tb_ctx));
  } else {
    query_ctx.executor_ = static_cast<ObTableApiScanExecutor*>(executor);
    query_ctx.spec_ = spec;
  }

  // 2. create result iterator
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(*allocator,
                                                                  query,
                                                                  is_hkv,
                                                                  result_,
                                                                  tb_ctx,
                                                                  result_iter))) {
      LOG_WARN("fail to generate query result iterator", K(ret));
    } else if (OB_FAIL(row_iter.open(query_ctx.executor_))) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else {
      result_iter->set_scan_result(&row_iter);
      // hbase model, compress the result packet
      if (is_hkv) {
        ObCompressorType compressor_type = INVALID_COMPRESSOR;
        if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(
            GCONF.tableapi_transport_compress_func, compressor_type))) {
          compressor_type = INVALID_COMPRESSOR;
        } else if (NONE_COMPRESSOR == compressor_type) {
          compressor_type = INVALID_COMPRESSOR;
        }
        this->set_result_compress_type(compressor_type);
        ret = OB_SUCCESS; // reset ret
      }
    }
  }

  // 3. do scan and save result iter
  if (OB_SUCC(ret)) {
    ObTableQueryResult *one_result = nullptr;
    query_session_->set_result_iterator(result_iter);
    if (ObTimeUtility::current_time() > timeout_ts_) {
      ret = OB_TRANS_TIMEOUT;
      LOG_WARN("exceed operatiton timeout", K(ret));
    } else if (OB_FAIL(result_iter->get_next_result(one_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next result", K(ret));
      } else {
        ret = OB_SUCCESS;
        result_.is_end_ = true;
      }
    } else if (result_iter->has_more_result()) {
      result_.is_end_ = false;
      query_session_->set_trans_desc(trans_desc_); // save processor's trans_desc_ to query session
    } else {
      // no more result
      result_.is_end_ = true;
    }
  }

  return ret;
}

int ObTableQueryAsyncP::query_scan_with_init()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator *allocator = query_session_->get_allocator();
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableCtx &tb_ctx = query_ctx.tb_ctx_;
  ObTableQuery &query = query_session_->get_query();

  if (OB_FAIL(arg_.query_.deep_copy(*allocator, query))) { // 存储的key range是引用，所以这里需要深拷贝
    LOG_WARN("fail to deep copy query", K(ret), K(arg_.query_));
  } else if (OB_FAIL(init_tb_ctx(tb_ctx))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(query_session_->deep_copy_select_columns(query.get_select_columns(), tb_ctx.get_query_col_names()))) {
    LOG_WARN("fail to deep copy select columns from table ctx", K(ret));
  } else if (OB_FAIL(start_trans(true, /* is_readonly */
                                 sql::stmt::T_SELECT,
                                 arg_.consistency_level_,
                                 tb_ctx.get_ref_table_id(),
                                 tb_ctx.get_ls_id(),
                                 tb_ctx.get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret), K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
  } else if (OB_FAIL(execute_query())) {
    LOG_WARN("fail to execute query", K(ret));
  } else {
    audit_row_count_ = result_.get_row_count();
    result_.query_session_id_ = query_session_id_;
    is_full_table_scan_ = tb_ctx.is_full_table_scan();
  }

  return ret;
}

int ObTableQueryAsyncP::query_scan_without_init()
{
  int ret = OB_SUCCESS;
  ObTableQueryResultIterator *result_iter = query_session_->get_result_iterator();
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableCtx &tb_ctx = query_ctx.tb_ctx_;

  if (OB_ISNULL(result_iter)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null result iterator", K(ret));
  } else if (OB_FAIL(result_.deep_copy_property_names(query_session_->get_select_columns()))) {
    LOG_WARN("fail to deep copy property names to one result", K(ret), K(query_session_->get_query()));
  } else {
    ObTableQueryResult *query_result = nullptr;
    result_iter->set_one_result(&result_);  // set result_ as container
    if (ObTimeUtility::current_time() > timeout_ts_) {
      ret = OB_TRANS_TIMEOUT;
      LOG_WARN("exceed operatiton timeout", K(ret));
    } else if (OB_FAIL(result_iter->get_next_result(query_result))) {
      if (OB_ITER_END == ret) {
        result_.is_end_ = true;  // set scan end
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to scan result", K(ret));
      }
    } else {
      result_.is_end_ = !result_iter->has_more_result();
      result_.query_session_id_ = query_session_id_;
      audit_row_count_ = result_.get_row_count();
      is_full_table_scan_ = tb_ctx.is_full_table_scan();
    }
  }

  return ret;
}

int ObTableQueryAsyncP::process_query_start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_scan_with_init())) {
    LOG_WARN("failed to process query start scan with init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query start", K(ret), K(query_session_id_));
  }
  return ret;
}

int ObTableQueryAsyncP::process_query_next()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_scan_without_init())) {
    LOG_WARN("fail to query next scan without init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query next", K(ret), K(query_session_id_));
  }
  return ret;
}

int ObTableQueryAsyncP::process_query_end()
{
  int ret = OB_SUCCESS;
  result_.is_end_ = true;
  return ret;
}

int ObTableQueryAsyncP::try_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_query_type())) {
    LOG_WARN("query type is invalid", K(ret), K(arg_.query_type_));
  } else if (OB_FAIL(get_session_id(query_session_id_, arg_.query_session_id_))) {
    LOG_WARN("fail to get query session id", K(ret), K(arg_.query_session_id_));
  } else if (OB_FAIL(get_query_session(query_session_id_, query_session_))) {
    LOG_WARN("fail to get query session", K(ret), K(query_session_id_));
  } else if (FALSE_IT(timeout_ts_ = get_timeout_ts())) {
  } else if (FALSE_IT(table_id_ = arg_.table_id_)) {
  } else if (FALSE_IT(tablet_id_ = arg_.tablet_id_)) {
  } else {
    WITH_CONTEXT(query_session_->get_memory_ctx()) {
      if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
        ret = process_query_start();
      } else if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_) {
        ret = process_query_next();
      } else if (ObQueryOperationType::QUERY_END == arg_.query_type_) {
        ret = process_query_end();
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("query execution failed, need rollback", K(ret));
        int tmp_ret = ret;
        if (OB_FAIL(destory_query_session(true))) {
          LOG_WARN("faild to destory query session", K(ret));
        }
        ret = tmp_ret;
      } else if (result_.is_end_) {
        if (OB_FAIL(destory_query_session(false))) {
          LOG_WARN("fail to destory query session", K(ret), K(query_session_id_));
        }
      } else {
        query_session_->set_in_use(false);
      }
    }
  }

  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute query", K(ret), K_(arg), K(result_),
             K_(retry_count), K_(result_row_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query", K(ret), K_(arg), K_(timeout_ts), K_(retry_count), K(result_.is_end_),
              "receive_ts", get_receive_timestamp(), K_(result_row_count));
  #endif

  stat_event_type_ = ObTableProccessType::TABLE_API_TABLE_QUERY_ASYNC;  // table querysync
  return ret;
}

// session.in_use_ must be true
int ObTableQueryAsyncP::destory_query_session(bool need_rollback_trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, timeout_ts_))) {
    LOG_WARN("failed to end trans", K(ret), K(need_rollback_trans));
  }
  int tmp_ret = ret;

  ObQueryAsyncMgr::get_instance().get_locker(query_session_id_).lock();
  if (OB_ISNULL(query_session_)) {
    //overwrite ret
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null value", K(ret), KP_(query_session));
  } else if (OB_FAIL(ObQueryAsyncMgr::get_instance().get_query_session_map()->erase_refactored(query_session_id_))) {
    LOG_WARN("fail to erase query session from query sync mgr", K(ret));
  } else {
    ObTableQueryUtils::destroy_result_iterator(query_session_->get_result_iter());
    OB_DELETE(ObTableQueryAsyncSession, ObModIds::TABLE_PROC, query_session_);
    LOG_DEBUG("destory query session success", K(ret), K(query_session_id_));
  }
  ObQueryAsyncMgr::get_instance().get_locker(query_session_id_).unlock();

  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  return ret;
}

int ObTableQueryAsyncP::check_query_type()
{
  int ret = OB_SUCCESS;
  if (arg_.query_type_ < table::ObQueryOperationType::QUERY_START ||
            arg_.query_type_ >= table::ObQueryOperationType::QUERY_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query operation type", K(ret), K(arg_.query_type_));
  }
  return ret;
}