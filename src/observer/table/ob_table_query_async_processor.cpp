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
using namespace oceanbase::sql::stmt;

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
  param.set_mem_attr(MTL_ID(), "TbASess", ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::ALLOC_THREAD_SAFE);
  lease_timeout_period_ = ObHTableUtils::get_hbase_scanner_timeout(MTL_ID()) * 1000;
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

void ObTableQueryAsyncSession::set_timout_ts()
{
  if (session_type_ == ObTableEntityType::ET_HKV) {
    timeout_ts_ = ObTimeUtility::current_time() + lease_timeout_period_;
  }
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
 * ----------------------------------- ObTableHbaseRowKeyDefaultCompare -------------------------------------
 */

int ObTableHbaseRowKeyDefaultCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = 0;
    for (int i = 0; i < ObHTableConstants::COL_IDX_T && cmp_ret == 0; ++i) {
      cmp_ret = lhs.get_cell(i).get_string().compare(rhs.get_cell(i).get_string());
    }
  }
  return ret;
}

bool ObTableHbaseRowKeyDefaultCompare::operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs)
{
  int cmp_ret = 0;
  result_code_ = compare(lhs, rhs, cmp_ret);
  return cmp_ret < 0;
}


int ObTableHbaseRowKeyReverseCompare::compare(const common::ObNewRow &lhs, const common::ObNewRow &rhs, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lhs.is_valid() || !rhs.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(lhs), K(rhs), K(ret));
  } else {
    cmp_ret = lhs.get_cell(ObHTableConstants::COL_IDX_K).get_string().compare(rhs.get_cell(ObHTableConstants::COL_IDX_K).get_string());
    if (cmp_ret == 0) {
      cmp_ret = rhs.get_cell(ObHTableConstants::COL_IDX_Q).get_string().compare(lhs.get_cell(ObHTableConstants::COL_IDX_Q).get_string());
    }
  }
  return ret;
}

bool ObTableHbaseRowKeyReverseCompare::operator()(const common::ObNewRow &lhs, const common::ObNewRow &rhs)
{
  int cmp_ret = 0;
  result_code_ = compare(lhs, rhs, cmp_ret);
  return cmp_ret > 0;
}

/**
 * ----------------------------------- ObTableQueryASyncMgr -------------------------------------
 */
ObTableQueryASyncMgr::ObTableQueryASyncMgr()
  : allocator_(MTL_ID()),
    session_id_(0),
    is_inited_(false)
{
}

int ObTableQueryASyncMgr::mtl_init(ObTableQueryASyncMgr *&query_async_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_async_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_async_mgr is null", K(ret));
  } else if (OB_FAIL(query_async_mgr->init())) {
    LOG_WARN("failed to init table query async manager", K(ret));
  }
  return ret;
}

int ObTableQueryASyncMgr::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableQueryASyncMgr is not inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), *this,
                                  MIN_QUERY_SESSION_CLEAN_DELAY))) {
    LOG_WARN("failed to schedule QueryASyncMgr task", K(ret));
  }
  return ret;
}

void ObTableQueryASyncMgr::destroy()
{
  destroy_all_query_session();
  query_session_map_.destroy();
  is_inited_ = false;
}

void ObTableQueryASyncMgr::stop()
{
  TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), *this);
}

void ObTableQueryASyncMgr::wait()
{
  TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), *this);
}

void ObTableQueryASyncMgr::runTimerTask()
{
  clean_timeout_query_session();
}

int ObTableQueryASyncMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableQueryASyncMgr init twice", K(ret), KPC(this));
  } else {
    for (int64_t i = 0; i < DEFAULT_LOCK_ARR_SIZE; ++i) {
      locker_arr_[i].set_latch_id(ObLatchIds::TABLE_API_LOCK);
    }
    ObMemAttr attr(MTL_ID(), "TblAQueryAlloc");
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      LOG_WARN("fail to init allocator", K(ret));
    } else if (OB_FAIL(query_session_map_.create(QUERY_SESSION_MAX_SIZE, "TableAQueryBkt", "TableAQueryNode", MTL_ID()))) {
      LOG_WARN("fail to create query session map", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableQueryASyncMgr::generate_query_sessid(uint64_t &sess_id)
{
  int ret = OB_SUCCESS;
  sess_id = INVALID_SESSION_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableQueryASyncMgr is not inited", K(ret));
  } else {
    sess_id = ATOMIC_AAF(&session_id_, 1);
  }
  return ret;
}

int ObTableQueryASyncMgr::get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableQueryASyncMgr is not inited", K(ret));
  } else {
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
    } else if (query_session->get_session_type() == ObTableEntityType::ET_HKV &&
               query_session->timeout_ts_ < ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      LOG_WARN("session is timeout", K(ret), K(query_session));
    } else {
      query_session->set_in_use(true);
    }
    get_locker(sessid).unlock();
  }
  return ret;
}

int ObTableQueryASyncMgr::set_query_session(uint64_t sessid, ObTableQueryAsyncSession *query_session)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableQueryASyncMgr is not inited", K(ret));
  } else {
    bool force = false;
    if (OB_FAIL(query_session_map_.set_refactored(sessid, query_session, force))) {
      LOG_WARN("set query session failed", K(ret), K(sessid));
    }
  }
  return ret;
}

void ObTableQueryASyncMgr::destroy_all_query_session()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableQueryASyncMgr is not inited", K(ret));
  } else {
    common::ObSEArray<uint64_t, 128> session_id_array;
    ObGetAllSessionIdOp op(session_id_array);
    if (OB_FAIL(query_session_map_.foreach_refactored(op))) {
      LOG_WARN("fail to get all session id from query sesion map", K(ret));
    } else {
      for (int64_t i = 0; i < session_id_array.count(); i++) {
        uint64_t sess_id = session_id_array.at(i);
        ObTableQueryAsyncSession *query_session = nullptr;
        if (OB_FAIL(query_session_map_.get_refactored(sess_id, query_session))) {
          LOG_DEBUG("query session already deleted by worker", K(ret), K(sess_id));
        } else if (OB_ISNULL(query_session)) {
          ret = OB_ERR_NULL_VALUE;
          (void)query_session_map_.erase_refactored(sess_id);
          LOG_WARN("unexpected null query sesion", K(ret));
        } else {
          if (OB_FAIL(rollback_trans(*query_session))) {
            LOG_WARN("failed to rollback trans for query session", K(ret), K(sess_id));
          }
          (void)query_session_map_.erase_refactored(sess_id);
          ObTableQueryUtils::destroy_result_iterator(query_session->get_result_iter());
          free_query_session(query_session);
          LOG_WARN("clean timeout query session success", K(ret), K(sess_id));
        }
      }
    }
  }
}

void ObTableQueryASyncMgr::clean_timeout_query_session()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableQueryASyncMgr is not inited", K(ret));
  } else {
    common::ObSEArray<uint64_t, 128> session_id_array;
    ObGetAllSessionIdOp op(session_id_array);
    uint64_t now = ObTimeUtility::current_time();
    uint64_t min_timeout_period = ObHTableUtils::get_hbase_scanner_timeout(MTL_ID()) * 1000;
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
        } else if (query_session->timeout_ts_ >= now) {
          min_timeout_period = OB_MIN(query_session->timeout_ts_ - now, min_timeout_period);
        } else {
          if (OB_FAIL(rollback_trans(*query_session))) {
            LOG_WARN("failed to rollback trans for query session", K(ret), K(sess_id));
          }
          (void)query_session_map_.erase_refactored(sess_id);
          ObTableQueryUtils::destroy_result_iterator(query_session->get_result_iter());
          free_query_session(query_session);
          // connection loses or bug exists
          LOG_WARN("clean timeout query session success", K(ret), K(sess_id));
        }
        get_locker(sess_id).unlock();
      }
    }
    // Ignore the error code, adjust the timing, and start the next scheduled task.
    ret = OB_SUCCESS;
    uint64_t refresh_period_ms = OB_MAX(MIN_QUERY_SESSION_CLEAN_DELAY, min_timeout_period);
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), *this, refresh_period_ms))) {
      LOG_ERROR("fail to refresh schedule query session clean task. ", K(ret));
    } else {
      LOG_TRACE("schedule timer task for refresh next time", K(refresh_period_ms));
    }
  }
}

int ObTableQueryASyncMgr::rollback_trans(ObTableQueryAsyncSession &query_session)
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
  LOG_DEBUG("ObTableQueryASyncMgr::rollback_trans", KR(ret));
  query_session.trans_desc_ = NULL;
  trans_state.reset();
  return ret;
}

ObTableQueryASyncMgr::ObQueryHashMap *ObTableQueryASyncMgr::get_query_session_map()
{
  return &query_session_map_;
}

ObTableQueryAsyncSession *ObTableQueryASyncMgr::alloc_query_session()
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncSession *query_session = OB_NEWx(ObTableQueryAsyncSession, &allocator_);
  if (OB_ISNULL(query_session)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ObTableQueryAsyncSession", K(ret));
  } else if (OB_FAIL(query_session->init())) {
    LOG_WARN("failed to init query session", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(query_session)) {
    free_query_session(query_session);
    query_session = nullptr;
  }

  return query_session;
}

void ObTableQueryASyncMgr::free_query_session(ObTableQueryAsyncSession *query_session)
{
  if (OB_NOT_NULL(query_session)) {
    query_session->~ObTableQueryAsyncSession();
    allocator_.free(query_session);
  }
}

int ObTableQueryASyncMgr::ObGetAllSessionIdOp::operator()(QuerySessionPair &entry) {
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
      allocator_("TblQueryASyncP", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
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

int ObTableQueryAsyncP::get_session_id(uint64_t &real_sessid, uint64_t arg_sessid)
{
  int ret = OB_SUCCESS;
  real_sessid = arg_sessid;
  if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
    if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->generate_query_sessid(real_sessid))) {
      LOG_WARN("fail to get session id", K(ret), K(real_sessid));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(real_sessid == ObTableQueryASyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("session id is invalid", K(ret), K(real_sessid), K(arg_.query_type_));
  }
  return ret;
}

int ObTableQueryAsyncP::get_query_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sessid == ObTableQueryASyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("fail to get query session, session id is invalid", K(ret), K(sessid));
  } else if (ObQueryOperationType::QUERY_START == arg_.query_type_) { // query start
    query_session = MTL(ObTableQueryASyncMgr*)->alloc_query_session();
    if (OB_ISNULL(query_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate ObTableQueryAsyncSession", K(ret), K(sessid));
    } else if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->set_query_session(sessid, query_session))) {
      LOG_WARN("fail to insert session to query map", K(ret), K(sessid));
      MTL(ObTableQueryASyncMgr*)->free_query_session(query_session);
    } else {}
  } else if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_ ||
             ObQueryOperationType::QUERY_END == arg_.query_type_ ||
             ObQueryOperationType::QUERY_RENEW == arg_.query_type_) {
    if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_query_session(sessid, query_session))) {
      LOG_WARN("fail to get query session from query sync mgr", K(ret), K(sessid));
    } else if (OB_ISNULL(query_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query session", K(ret), K(sessid));
    } else {
      // hook processor's trans_desc_
      trans_param_.trans_desc_ = query_session->get_trans_desc();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unkown query type", K(arg_.query_type_));
  }

  if (OB_SUCC(ret)) {
    // use session trans_state_ which storage transaction state in query_next()
    trans_param_.trans_state_ptr_ = query_session->get_trans_state();
    query_session->set_session_type(arg_.entity_type_);
  }

  return ret;
}

int64_t ObTableQueryAsyncP::get_trans_timeout_ts()
{
  if (arg_.entity_type_ == ObTableEntityType::ET_HKV) {
    return INT_MAX64;
  }
  return get_timeout_ts();
}

int ObTableQueryAsyncP::init_tb_ctx(ObTableCtx& ctx, ObTableSingleQueryInfo& query_info) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_session_ is NULL", K(ret));
  } else {
    ObIAllocator &allocator = *query_session_->get_allocator();
    ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
    bool is_weak_read = arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL;
    ctx.set_scan(true);
    ctx.set_entity_type(arg_.entity_type_);
    ctx.set_schema_cache_guard(&query_info.schema_cache_guard_);
    ctx.set_schema_guard(&schema_guard_);
    ctx.set_simple_table_schema(query_info.simple_schema_);
    ctx.set_sess_guard(&query_ctx.sess_guard_);
    ctx.set_is_tablegroup_req(is_tablegroup_req_);

    ObObjectID tmp_object_id = OB_INVALID_ID;
    ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
    ObTabletID real_tablet_id;
    if (query_ctx.part_idx_ == OB_INVALID_INDEX && query_ctx.subpart_idx_ == OB_INVALID_INDEX) { // 非分区表
      real_tablet_id = query_info.simple_schema_->get_tablet_id();
    } else if (OB_FAIL(query_info.simple_schema_->get_part_id_and_tablet_id_by_idx(query_ctx.part_idx_,
                                                                                    query_ctx.subpart_idx_,
                                                                                    tmp_object_id,
                                                                                    tmp_first_level_part_id,
                                                                                    real_tablet_id))) {
      LOG_WARN("fail to get_part_id_and_tablet_id_by_idx", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (ctx.is_init()) {
      ret = OB_INIT_TWICE;
      LOG_INFO("tb ctx has been inited", K(ctx));
    } else if (OB_FAIL(ctx.init_common(credential_, real_tablet_id, get_timeout_ts()))) {
      LOG_WARN("fail to init table ctx common part", K(ret), K(query_info.simple_schema_->get_table_name()), K(ret));
    } else if (OB_FAIL(ctx.init_scan(query_info.query_, is_weak_read, query_info.table_id_))) {
      LOG_WARN("fail to init table ctx scan part", K(ret), K(query_info.simple_schema_->get_table_name()), K(query_info.table_id_));
    } else if (OB_FAIL(ctx.cons_column_items_for_cg())) {
      LOG_WARN("fail to construct column items", K(ret));
    } else if (OB_FAIL(ctx.generate_table_schema_for_cg())) {
      LOG_WARN("fail to generate table schema", K(ret));
    } else if (OB_FAIL(ObTableExprCgService::generate_exprs(ctx,
                                                            allocator,
                                                            query_info.expr_frame_info_))) {
      LOG_WARN("fail to generate exprs", K(ret), K(ctx));
    } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(ctx, query_info.expr_frame_info_))) {
      LOG_WARN("fail to alloc expr memory", K(ret));
    } else if (OB_FAIL(ctx.classify_scan_exprs())) {
      LOG_WARN("fail to classify scan exprs", K(ret));
    } else if (OB_FAIL(ctx.init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
    } else {
      ctx.set_init_flag(true);
      ctx.set_expr_info(&query_info.expr_frame_info_);
    }
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
  ctx.set_schema_cache_guard(&schema_cache_guard_);
  ctx.set_schema_guard(&schema_guard_);
  ctx.set_simple_table_schema(simple_table_schema_);
  ctx.set_sess_guard(&query_ctx.sess_guard_);
  ctx.set_is_tablegroup_req(is_tablegroup_req_);

  if (ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(ctx));
  } else if (OB_FAIL(ctx.init_common(credential_, arg_.tablet_id_, get_trans_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(ctx.init_scan(query_session_->get_query(), is_weak_read, arg_.table_id_))) {
    LOG_WARN("fail to init table ctx scan part", K(ret), K(arg_.table_name_), K(arg_.table_id_));
  } else if (!ctx.is_global_index_scan() && arg_.table_id_ != ctx.get_ref_table_id()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("arg table id is not equal to schema table id", K(ret), K(arg_.table_id_), K(ctx.get_ref_table_id()));
  } else if (OB_FAIL(ctx.cons_column_items_for_cg())) {
    LOG_WARN("fail to construct column items", K(ret));
  } else if (OB_FAIL(ctx.generate_table_schema_for_cg())) {
    LOG_WARN("fail to generate table schema", K(ret));
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

int ObTableQueryAsyncP::generate_merge_result_iterator()
{
  int ret = OB_SUCCESS;
  // Merge Iterator: Holds multiple underlying iterators, stored in its own heap
  ResultMergeIterator *merge_result_iter = nullptr;
  ObTableMergeFilterCompare *compare = nullptr;
  ObQueryFlag::ScanOrder scan_order = query_session_->get_query().get_scan_order();
  if (OB_ISNULL(merge_result_iter = OB_NEWx(ResultMergeIterator,
                                              query_session_->get_allocator(),
                                              query_session_->get_query(),
                                              result_ /* Response serlize row*/))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create merge_result_iter", K(ret));
  } else if (OB_FAIL(generate_multi_result_iterator(merge_result_iter))) {
    LOG_WARN("fail to generate multi result inner iterator", K(ret));
  } else if (scan_order == ObQueryFlag::Reverse && OB_ISNULL(compare = OB_NEWx(ObTableHbaseRowKeyReverseCompare, &merge_result_iter->get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (scan_order == ObQueryFlag::Forward && OB_ISNULL(compare = OB_NEWx(ObTableHbaseRowKeyDefaultCompare, &merge_result_iter->get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (OB_FAIL(merge_result_iter->init(compare))) {
    LOG_WARN("fail to build merge_result_iter", K(ret));
  } else {
    query_session_->set_result_iterator(merge_result_iter);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(merge_result_iter)) {
    merge_result_iter->~ResultMergeIterator();
    query_session_->get_allocator()->free(merge_result_iter);
    merge_result_iter = nullptr;
  }
  return ret;
}

int ObTableQueryAsyncP::process_columns(const ObIArray<ObString>& columns,
                                        ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                        ObArray<ObString>& real_columns) {
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObString column = columns.at(i);
    ObString real_column = column.after('.');
    ObString family = column.split_on('.');

    if (OB_FAIL(real_columns.push_back(real_column))) {
      LOG_WARN("fail to push column name", K(ret));
    } else if (OB_FAIL(family_addfamily_flag_pairs.push_back(std::make_pair(family, real_column.empty())))) {
      LOG_WARN("fail to push family name and addfamily flag", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAsyncP::update_table_info_columns(ObTableSingleQueryInfo* table_info,
                                            const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                            const ObArray<ObString>& real_columns,
                                            const std::pair<ObString, bool>& family_addfamily_flag) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table info is NULL", K(ret));
  } else {
    ObString family_name = family_addfamily_flag.first;
    table_info->query_.htable_filter().clear_columns();
    for (int i = 0; OB_SUCC(ret) && i < family_addfamily_flag_pairs.count(); ++i) {
      ObString curr_family = family_addfamily_flag_pairs.at(i).first;
      if (curr_family == family_name) {
        ObString real_column = real_columns.at(i);
        if (OB_FAIL(table_info->query_.htable_filter().add_column(real_column))) {
          LOG_WARN("fail to add column to htable_filter", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObTableQueryAsyncP::found_family(const ObString& table_name,
                                      const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                      std::pair<ObString, bool>& flag)
{
  bool is_found = false;
  ObSqlString tmp_name;
  for (int i = 0; !is_found && i < family_addfamily_flag_pairs.count(); ++i) {
    std::pair<ObString, bool> family_addfamily_flag = family_addfamily_flag_pairs.at(i);
    tmp_name.append(arg_.table_name_);
    tmp_name.append("$");
    tmp_name.append(family_addfamily_flag.first);
    if (table_name == tmp_name.string()) {
      flag = family_addfamily_flag;
      is_found = true;
    }
    tmp_name.reuse();
  }
  return is_found;
}

int ObTableQueryAsyncP::process_table_info(ObTableSingleQueryInfo* table_info,
                                            const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                            const ObArray<ObString>& real_columns,
                                            const std::pair<ObString, bool>& family_addfamily_flag) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table info is NULL", K(ret));
  } else if (family_addfamily_flag_pairs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("family with flag array is empty", K(ret));
  } else {
    if (family_addfamily_flag.second) {
      table_info->query_.htable_filter().clear_columns();
    } else if (OB_FAIL(update_table_info_columns(table_info, family_addfamily_flag_pairs, real_columns, family_addfamily_flag))) {
      LOG_WARN("fail to update table info columns", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAsyncP::generate_multi_result_iterator(ResultMergeIterator *merge_result_iter) {
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = query_session_->get_allocator();
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableQuery &query = query_session_->get_query();
  ObTableApiExecutor *executor = nullptr;
  ObTableApiSpec *spec = nullptr;
  ObIArray<ObTableQueryResultIterator*>& inner_result_iterator_list = merge_result_iter->get_inner_result_iterators();

  const ObIArray<ObString>& columns = query.htable_filter().get_columns();
  ObArray<std::pair<ObString, bool>> family_addfamily_flag_pairs;
  ObArray<ObString> real_columns;
  family_addfamily_flag_pairs.prepare_allocate_and_keep_count(8);
  real_columns.prepare_allocate_and_keep_count(8);
  if (OB_FAIL(process_columns(columns, family_addfamily_flag_pairs, real_columns))) {
    LOG_WARN("fail to process columns", K(ret));
  }

  for (int i = 0; OB_SUCC(ret) && i < query_ctx.multi_cf_infos_.count(); ++i) {
    ObTableSingleQueryInfo* table_info = query_ctx.multi_cf_infos_.at(i);
    bool is_found = true;
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table info is NULL", K(ret));
    } else if (!family_addfamily_flag_pairs.empty()) { // Only when there is a qualifier parameter
      std::pair<ObString, bool> is_add_family;
      is_found = found_family(table_info->schema_cache_guard_.get_table_name_str(),
                              family_addfamily_flag_pairs,
                              is_add_family);
      if (is_found && OB_FAIL(process_table_info(table_info,
                                                family_addfamily_flag_pairs,
                                                real_columns,
                                                is_add_family))) {
        LOG_WARN("fail to process table info", K(ret));
      }
    }

    ObTableCtx &tb_ctx = table_info->tb_ctx_;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!is_found) {
      // ignore this table
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableSpecCgService::generate<TABLE_API_EXEC_SCAN>(*allocator, tb_ctx, spec))) {
      LOG_WARN("fail to generate scan spec", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to generate executor", K(ret));
    } else {
      table_info->executor_ = static_cast<ObTableApiScanExecutor*>(executor);
      table_info->spec_ = spec;
      ObTableQueryResultIterator *result_iter = nullptr;
      if (OB_FAIL(ObTableQueryUtils::generate_htable_result_iterator(merge_result_iter->get_allocator(),
                                                                    table_info->query_,
                                                                    table_info->result_,
                                                                    table_info->tb_ctx_,
                                                                    result_iter))) {
        LOG_WARN("fail to generate query result iterator", K(ret));
      } else if (OB_ISNULL(result_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to create result iterable iter");
      } else if (OB_FAIL(table_info->iter_open())) {
        LOG_WARN("fail to open scan row iterator", K(ret));
      } else {
        result_iter->set_scan_result(&table_info->row_iter_);
        if (OB_FAIL(inner_result_iterator_list.push_back(result_iter))) {
          LOG_WARN(" fail to push back result_iter to array_result", K(ret));
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(result_iter)) {
        result_iter->~ObTableQueryResultIterator();
        merge_result_iter->get_allocator().free(result_iter);
        result_iter = nullptr;
      }
    }
  }
  return ret;
}

int ObTableQueryAsyncP::execute_multi_cf_query() {
  int ret = OB_SUCCESS;
  ObCompressorType compressor_type = INVALID_COMPRESSOR;
  ObTableQueryResult *one_result = nullptr;
  if (ObTimeUtility::fast_current_time() > timeout_ts_) {
    ret = OB_TRANS_TIMEOUT;
    LOG_WARN("exceed operatiton timeout", K(ret));
  } else if (OB_FAIL(result_.deep_copy_property_names(query_session_->get_select_columns()))) {
    LOG_WARN("fail to deep copy property names to one result", K(ret), K(query_session_->get_query()));
  } else if (OB_FAIL(query_session_->get_result_iterator()->get_next_result(one_result))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next result", K(ret));
    } else {
      ret = OB_SUCCESS;
      result_.is_end_ = true;
    }
  } else if (query_session_->get_result_iterator()->has_more_result()) {
    result_.is_end_ = false;
    // save processor's trans_desc_ to query session
    query_session_->set_trans_desc(trans_param_.trans_desc_);
  } else {
    // no more result
    result_.is_end_ = true;
  }

  // check if need compress the result
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCC(ret) &&
      OB_NOT_NULL(one_result) &&
      OB_TMP_FAIL(ObKVConfigUtil::get_compress_type(MTL_ID(),
                  one_result->get_result_size(),
                  compressor_type))) {
    LOG_WARN("fail to check compress config", K(tmp_ret), K(compressor_type));
  }
  this->set_result_compress_type(compressor_type);

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
  ObCompressorType compressor_type = INVALID_COMPRESSOR;

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
    }
  }

  // 3. do scan and save result iter
  if (OB_SUCC(ret)) {
    ObTableQueryResult *one_result = nullptr;
    query_session_->set_result_iterator(result_iter);
    if (ObTimeUtility::fast_current_time() > timeout_ts_) {
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
      query_session_->set_trans_desc(trans_param_.trans_desc_); // save processor's trans_desc_ to query session
    } else {
      // no more result
      result_.is_end_ = true;
    }

    // check if need compress the result
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCC(ret) &&
        OB_NOT_NULL(one_result) &&
        OB_TMP_FAIL(ObKVConfigUtil::get_compress_type(MTL_ID(), one_result->get_result_size(), compressor_type))) {
      LOG_WARN("fail to check compress config", K(tmp_ret), K(compressor_type));
    }
    this->set_result_compress_type(compressor_type);
  }
  return ret;
}

int ObTableQueryAsyncP::start_trans(bool is_readonly,
                                    const ObTableConsistencyLevel consistency_level,
                                    const ObLSID &ls_id,
                                    int64_t timeout_ts,
                                    bool need_global_snapshot,
                                    TransState *trans_state)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_state is null", K(ret));
  } else if (FALSE_IT(trans_param_.trans_state_ptr_ = trans_state)) {
  } else if (OB_FAIL(trans_param_.init(is_readonly,
                                       consistency_level,
                                       ls_id,
                                       timeout_ts,
                                       need_global_snapshot))) {
    LOG_WARN("fail to init trans param", K(ret));
  } else if (OB_FAIL(ObTableTransUtils::start_trans(trans_param_))) {
    LOG_WARN("fail to start trans", K(ret), K_(trans_param));
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
  } else if (OB_FAIL(start_trans(true,
                                 arg_.consistency_level_,
                                 tb_ctx.get_ls_id(),
                                 tb_ctx.get_timeout_ts(),
                                 tb_ctx.need_dist_das(),
                                 query_session_->get_trans_state()))) { // use session trans_state_
    LOG_WARN("fail to start readonly transaction", K(ret), K(tb_ctx));
  } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
  } else if (OB_FAIL(execute_query())) {
    LOG_WARN("fail to execute query", K(ret));
  } else {
    stat_row_count_ = result_.get_row_count();
    result_.query_session_id_ = query_session_id_;
    is_full_table_scan_ = tb_ctx.is_full_table_scan();
  }
  // reset the tb_ctx schema_guard and simple_table_schema after the query start finish whether success or not
  // if has query next the schema guard and simple table schema is no need to be got
  tb_ctx.set_schema_guard(nullptr);
  tb_ctx.set_simple_table_schema(nullptr);

  return ret;
}


int ObTableQueryAsyncP::query_scan_without_init(ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  ObTableQueryResultIterator *result_iter = query_session_->get_result_iterator();
  ObCompressorType compressor_type = INVALID_COMPRESSOR;
  OB_TABLE_START_AUDIT(credential_,
                       sess_guard_,
                       arg_.table_name_,
                       &audit_ctx_,
                       query_session_->get_query());

  if (OB_ISNULL(result_iter)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null result iterator", K(ret));
  } else if (OB_FAIL(result_.deep_copy_property_names(query_session_->get_select_columns()))) {
    LOG_WARN("fail to deep copy property names to one result", K(ret), K(query_session_->get_query()));
  } else {
    ObTableQueryResult *query_result = nullptr;
    result_iter->set_one_result(&result_);  // set result_ as container
    if (ObTimeUtility::fast_current_time() > timeout_ts_) {
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
      is_full_table_scan_ = tb_ctx.is_full_table_scan();
    }

    // check if need compress the result
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCC(ret) &&
        OB_NOT_NULL(query_result) &&
        OB_TMP_FAIL(ObKVConfigUtil::get_compress_type(MTL_ID(), query_result->get_result_size(), compressor_type))) {
      LOG_WARN("fail to check compress config", K(tmp_ret), K(compressor_type));
    }
    this->set_result_compress_type(compressor_type);
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, result_.get_row_count(),
                     has_table_scan, true,
                     filter, (OB_ISNULL(result_iter) ? nullptr : result_iter->get_filter()));
  return ret;
}

int ObTableQueryAsyncP::init_multi_cf_query_ctx(const ObString &arg_tablegroup_name) {
  int ret = OB_SUCCESS;
  ObTableQueryAsyncCtx *query_ctx = nullptr;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> sort_table_schemas;
  uint64_t tablegroup_id = OB_INVALID_ID;
  if (schema_cache_guard_.is_inited()) {
    // skip
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_ISNULL(query_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query session is NULL", K(ret));
  } else if (FALSE_IT(query_ctx = &(query_session_->get_query_ctx()))) {
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else if (OB_FAIL(schema_guard_.get_tablegroup_id(credential_.tenant_id_, arg_tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get table schema from table group name", K(ret), K(credential_.tenant_id_),
          K(credential_.database_id_), K(arg_tablegroup_name));
  } else if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(credential_.tenant_id_, tablegroup_id, sort_table_schemas))) {
    LOG_WARN("fail to get table schema from table group", K(ret), K(credential_.tenant_id_),
          K(credential_.database_id_), K(arg_tablegroup_name), K(tablegroup_id));
  } else {
    if (OB_SUCC(ret)) {
      if (query_session_->get_query().get_scan_order() == ObQueryFlag::Reverse) {
        lib::ob_sort(sort_table_schemas.begin(), sort_table_schemas.end(), [](const schema::ObSimpleTableSchemaV2* lhs,
                                                                              const schema::ObSimpleTableSchemaV2* rhs) {
          return lhs->get_table_name() > rhs->get_table_name();
        });
      } else {
        lib::ob_sort(sort_table_schemas.begin(), sort_table_schemas.end(), [](const schema::ObSimpleTableSchemaV2* lhs,
                                                                              const schema::ObSimpleTableSchemaV2* rhs) {
          return lhs->get_table_name() < rhs->get_table_name();
        });
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < sort_table_schemas.count(); ++i) {
      const schema::ObSimpleTableSchemaV2* table_schema = sort_table_schemas.at(i);
      ObTableSingleQueryInfo* query_info = nullptr;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is NULL", K(ret));
      } else if (OB_ISNULL(query_info = OB_NEWx(ObTableSingleQueryInfo, query_session_->get_allocator(), *query_session_->get_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate query_info", K(ret));
      } else {
        query_info->simple_schema_ = table_schema;
        if (OB_FAIL(query_info->schema_cache_guard_.init(credential_.tenant_id_,
                                            table_schema->get_table_id(),
                                            table_schema->get_schema_version(),
                                            schema_guard_))) {
          query_info->~ObTableSingleQueryInfo();
          LOG_WARN("fail to init schema_cache_guard_", K(ret));
        } else {
          query_info->table_id_ = table_schema->get_table_id();
          query_info->schema_version_ = table_schema->get_schema_version();
          int64_t part_idx = OB_INVALID_INDEX;
          int64_t subpart_idx = OB_INVALID_INDEX;
          if (arg_.table_id_ == table_schema->get_table_id()
              && arg_.tablet_id_.is_valid() // Get partidx only when it's a partitioned table
              && OB_FAIL(table_schema->get_part_idx_by_tablet(arg_.tablet_id_, part_idx, subpart_idx))) {
            LOG_WARN("fail to get_part_idx_by_tablet", K(ret));
          } else if (OB_FAIL(arg_.query_.deep_copy(*query_session_->get_allocator(), query_info->query_))) {
            LOG_WARN("fail to copy query to query_info", K(ret));
          } else {
            if (part_idx != OB_INVALID_INDEX) {
              query_ctx->part_idx_ = part_idx;
              query_ctx->subpart_idx_ = subpart_idx;
            }
            ObIArray<common::ObNewRange>& ranges = query_info->query_.get_scan_ranges();
            for (int i = 0; i < ranges.count(); ++i) {
              ObNewRange& range = ranges.at(i);
              range.table_id_ = query_info->table_id_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(query_ctx->multi_cf_infos_.push_back(query_info))) {
            LOG_WARN("fail to push query info", K(ret));
          }
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(query_info)) {
          query_info->~ObTableSingleQueryInfo();
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential_, query_ctx->sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K(credential_));
  } else if (OB_ISNULL(query_ctx->sess_guard_.get_sess_node_val())) { // sess_info is both use in start and next
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), K(credential_));
  }

  return ret;
}

int ObTableQueryAsyncP::init_query_ctx(const ObString &arg_table_name) {
  int ret = OB_SUCCESS;
  ObTableQueryAsyncCtx *query_ctx = nullptr;
  if (schema_cache_guard_.is_inited()) {
    // skip and do nothing
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_ISNULL(query_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query session is NULL", K(ret));
  } else if (FALSE_IT(query_ctx = &(query_session_->get_query_ctx()))) {
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else if (is_tablegroup_req_ && OB_FAIL(init_tablegroup_schema(arg_table_name))) {
    LOG_WARN("fail to get table schema from table group name", K(ret), K(credential_.tenant_id_),
              K(credential_.database_id_), K(arg_table_name));
  } else if (!is_tablegroup_req_
             && OB_FAIL(schema_guard_.get_simple_table_schema(credential_.tenant_id_,
                                                              credential_.database_id_,
                                                              arg_table_name,
                                                              false, /* is_index */
                                                              simple_table_schema_))) {
    LOG_WARN("fail to get simple table schema", K(ret), K(credential_.tenant_id_),
              K(credential_.database_id_), K(arg_table_name));
  } else if (OB_ISNULL(simple_table_schema_) || simple_table_schema_->get_table_id() == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get simple table schema", K(ret), K(arg_table_name), KP(simple_table_schema_));
  } else if (simple_table_schema_->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_USER_ERROR(OB_ERR_OPERATION_ON_RECYCLE_OBJECT);
    LOG_WARN("table is in recycle bin, not allow to do operation", K(ret), K(credential_.tenant_id_),
                K(credential_.database_id_), K(arg_table_name));
  } else if (is_tablegroup_req_ && arg_.table_id_ != simple_table_schema_->get_table_id()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table id not correct in table group", K(ret));
  } else if (OB_FAIL(schema_cache_guard_.init(credential_.tenant_id_,
                                              simple_table_schema_->get_table_id(),
                                              simple_table_schema_->get_schema_version(),
                                              schema_guard_))) {
    LOG_WARN("fail to init schema cache guard", K(ret));
  } else if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential_, query_ctx->sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K(credential_));
  } else if (OB_ISNULL(query_ctx->sess_guard_.get_sess_node_val())) { // sess_info is both use in start and next
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), K(credential_));
  } else {
    // use for query_next to get the kv_schema_cache
    query_ctx->table_id_ = simple_table_schema_->get_table_id();
    query_ctx->schema_version_= simple_table_schema_->get_schema_version();
  }
  return ret;
}

int ObTableQueryAsyncP::query_scan_multi_cf_with_init() {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(query_session_)) {
  } else {
    ObIAllocator *allocator = query_session_->get_allocator();
    ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
    common::ObArray<ObTableSingleQueryInfo*>& query_infos = query_ctx.multi_cf_infos_;
    ObTableQuery &query = query_session_->get_query();
    if (OB_ISNULL(allocator)) {
    } else if (OB_FAIL(arg_.query_.deep_copy(*allocator, query))) { // 存储的key range是引用，所以这里需要深拷贝
      LOG_WARN("fail to deep copy query", K(ret), K(arg_.query_));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < query_infos.count(); ++i) {
        ObTableSingleQueryInfo* info = query_ctx.multi_cf_infos_.at(i);
        if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is NULL", K(ret));
        } else if (OB_FAIL(init_tb_ctx(info->tb_ctx_, *info))) {
          LOG_WARN("fail to init_tb_ctx", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(query_session_->deep_copy_select_columns(query.get_select_columns(), query_infos.at(0)->tb_ctx_.get_query_col_names()))) {
      LOG_WARN("fail to deep copy select columns from table ctx", K(ret));
    } else if (OB_FAIL(start_trans(true,
                                    arg_.consistency_level_,
                                    query_infos.at(0)->tb_ctx_.get_ls_id(),
                                    query_infos.at(0)->tb_ctx_.get_timeout_ts(),
                                    query_infos.at(0)->tb_ctx_.need_dist_das(),
                                    query_session_->get_trans_state()))) {
      LOG_WARN("fail to start readonly transaction", K(ret), K(query_infos.at(0)->tb_ctx_));
    } else if (OB_FAIL(generate_merge_result_iterator())) {
      LOG_WARN("fail to generate_merge_result_iterator", K(ret));
    } else if (OB_FAIL(execute_multi_cf_query())) {
      LOG_WARN("fail to execute query", K(ret));
    } else {
      stat_row_count_ = result_.get_row_count();
      result_.query_session_id_ = query_session_id_;
      is_full_table_scan_ = query_infos.at(0)->tb_ctx_.is_full_table_scan();
    }
  }
  return ret;
}

int ObTableQueryAsyncP::process_multi_cf_query_start() {
  int ret = OB_SUCCESS;
  ObString tablegroup_name = arg_.table_name_;
  OB_TABLE_START_AUDIT(credential_,
                        sess_guard_,
                        tablegroup_name,
                        &audit_ctx_,
                        arg_.query_);

  if (OB_FAIL(init_multi_cf_query_ctx(tablegroup_name))) {
    LOG_WARN("fail to init multi cf schema guard", K(ret), K(tablegroup_name));
  } else if (OB_FAIL(query_scan_multi_cf_with_init())) {
    LOG_WARN("fail to query_scan_multi_cf_with_init", K(ret));
  } else {
    LOG_DEBUG("finish multi cf query start", K(ret), K(query_session_id_));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, result_.get_row_count(),
                     has_table_scan, true,
                     filter, (OB_ISNULL(query_session_->get_result_iterator()) ? nullptr : query_session_->get_result_iterator()->get_filter()));
  return ret;
}


int ObTableQueryAsyncP::process_multi_cf_query_next() {
  int ret = OB_SUCCESS;
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableSingleQueryInfo *info = nullptr;
  if (query_ctx.multi_cf_infos_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi_cf_infos is empty", K(ret));
  } else if (OB_ISNULL(info = query_ctx.multi_cf_infos_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi cf info is NULL", K(ret));
  } else if (OB_FAIL(query_scan_without_init(info->tb_ctx_))) {
    LOG_WARN("fail to query next scan without init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query next", K(ret), K(query_session_id_));
  }
  return ret;
}


int ObTableQueryAsyncP::process_query_start()
{
  int ret = OB_SUCCESS;
  OB_TABLE_START_AUDIT(credential_,
                       sess_guard_,
                       arg_.table_name_,
                       &audit_ctx_,
                       arg_.query_);

  if (OB_FAIL(init_query_ctx(arg_.table_name_))) {
    LOG_WARN("fail to init schema guard", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(query_scan_with_init())) {
    LOG_WARN("failed to process query start scan with init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query start", K(ret), K(query_session_id_));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, result_.get_row_count(),
                     has_table_scan, true,
                     filter, (OB_ISNULL(query_session_->get_result_iterator()) ? nullptr : query_session_->get_result_iterator()->get_filter()));
  return ret;
}

/*
  use for query_next:
    no need to get the table schema in query_next and use kv schema cache instead
*/
int ObTableQueryAsyncP::init_schema_cache_guard()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query session is NULL", K(ret));
  } else {
    ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
    if (schema_cache_guard_.is_inited()) {
      // do nothing
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(query_session_->get_tenant_id(), schema_guard_))) {
      LOG_WARN("fail to get schema guard", K(ret), K(query_session_->get_tenant_id()));
    } else if (OB_FAIL(schema_cache_guard_.init(query_session_->get_tenant_id(),
                                                query_ctx.table_id_,
                                                query_ctx.schema_version_,
                                                schema_guard_))) {
      LOG_WARN("fail to init schema cache guard", K(ret));
    } else {
      query_ctx.tb_ctx_.set_schema_cache_guard(&schema_cache_guard_);
    }
  }
  return ret;
}

int ObTableQueryAsyncP::process_query_next()
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableCtx &tb_ctx = query_ctx.tb_ctx_;

  if (OB_FAIL(init_schema_cache_guard())) {
    LOG_WARN("fail to init schema_cache guard", K(ret));
  } else if (OB_FAIL(query_scan_without_init(tb_ctx))) {
    LOG_WARN("fail to query next scan without init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query next", K(ret), K(query_session_id_));
  }
  return ret;
}

int ObTableQueryAsyncP::before_process()
{
  is_tablegroup_req_ = ObHTableUtils::is_tablegroup_req(arg_.table_name_, arg_.entity_type_);
  return ParentType::before_process();
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
  table_id_ = arg_.table_id_; // init move response need
  tablet_id_ = arg_.tablet_id_;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  if (OB_FAIL(check_query_type())) {
    LOG_WARN("query type is invalid", K(ret), K(arg_.query_type_));
  } else if (OB_FAIL(get_session_id(query_session_id_, arg_.query_session_id_))) {
    LOG_WARN("fail to get query session id", K(ret), K(arg_.query_session_id_));
  } else if (OB_FAIL(get_query_session(query_session_id_, query_session_))) {
    LOG_WARN("fail to get query session", K(ret), K(query_session_id_));
  } else if (FALSE_IT(timeout_ts_ = get_trans_timeout_ts())) {
  } else {
    WITH_CONTEXT(query_session_->get_memory_ctx()) {
      if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
        set_timeout(query_session_->get_lease_timeout_period());
        if (is_tablegroup_req_) {
          ret = process_multi_cf_query_start();
        } else {
          ret = process_query_start();
        }
      } else if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_) {
        if (is_tablegroup_req_) {
          ret = process_multi_cf_query_next();
        } else {
          ret = process_query_next();
        }
      } else if (ObQueryOperationType::QUERY_RENEW == arg_.query_type_) {
        result_.query_session_id_ = query_session_id_;
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
        query_session_->set_timout_ts();
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
  bool is_hkv = (ObTableEntityType::ET_HKV == arg_.entity_type_);
  if (is_hkv) {
    stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_QUERY_ASYNC;
  } else {
    stat_event_type_ = ObTableProccessType::TABLE_API_TABLE_QUERY_ASYNC;
  }
  return ret;
}

// session.in_use_ must be true
int ObTableQueryAsyncP::destory_query_session(bool need_rollback_trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, nullptr/* ObTableCreateCbFunctor */))) {
    LOG_WARN("failed to end trans", K(ret), K(need_rollback_trans));
  }
  int tmp_ret = ret;

  MTL(ObTableQueryASyncMgr*)->get_locker(query_session_id_).lock();
  if (OB_ISNULL(query_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null value", K(ret), KP_(query_session));
  } else if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_query_session_map()->erase_refactored(query_session_id_))) {
    LOG_WARN("fail to erase query session from query sync mgr", K(ret));
  } else {
    ObTableQueryUtils::destroy_result_iterator(query_session_->get_result_iter());
    MTL(ObTableQueryASyncMgr*)->free_query_session(query_session_);
    LOG_DEBUG("destory query session success", K(ret), K(query_session_id_));
  }
  MTL(ObTableQueryASyncMgr*)->get_locker(query_session_id_).unlock();

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