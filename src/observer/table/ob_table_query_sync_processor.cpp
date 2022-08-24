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
#include "ob_table_query_sync_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "storage/ob_partition_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "observer/ob_server.h"
#include "lib/string/ob_strings.h"
#include "lib/rc/ob_rc.h"
#include "observer/table/ob_htable_filter_operator.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

/**
 * ---------------------------------------- ObTableQuerySyncSession ----------------------------------------
 */
int ObTableQuerySyncSession::deep_copy_select_columns(const ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  const int64_t N = select_columns.count();
  ObString tmp_str;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
    if (OB_FAIL(ob_write_string(*get_allocator(), select_columns.at(i), tmp_str))) {
      LOG_WARN("failed to copy column name", K(ret));
      break;
    } else if (OB_FAIL(query_.add_select_column(tmp_str))) {
      LOG_WARN("failed to add column name", K(ret));
    }
  }  // end for
  return ret;
}

void ObTableQuerySyncSession::set_result_iterator(ObNormalTableQueryResultIterator *query_result)
{
  result_iterator_ = query_result;
  if (OB_NOT_NULL(result_iterator_)) {
    result_iterator_->set_query(&query_);
    result_iterator_->set_query_sync();
  }
}

void ObTableQuerySyncSession::set_htable_result_iterator(table::ObHTableFilterOperator *query_result)
{
  htable_result_iterator_ = query_result;
  if (OB_NOT_NULL(htable_result_iterator_)) {
    htable_result_iterator_->set_query(&query_);
    htable_result_iterator_->set_query_sync();
  }
}

int ObTableQuerySyncSession::init()
{
  int ret = OB_SUCCESS;
  lib::MemoryContext mem_context = nullptr;
  lib::ContextParam param;
  param.set_mem_attr(lib::current_tenant_id(), ObModIds::TABLE_PROC, ObCtxIds::DEFAULT_CTX_ID)
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
    table_service_ctx_.scan_param_.iterator_mementity_ = mem_context;
    table_service_ctx_.scan_param_.allocator_ = &mem_context->get_arena_allocator();
    iterator_mementity_ = mem_context;
  }
  return ret;
}

ObTableQuerySyncSession::~ObTableQuerySyncSession()
{
  if (OB_NOT_NULL(iterator_mementity_)) {
    DESTROY_CONTEXT(iterator_mementity_);
  }
}

/**
 * ----------------------------------- ObQuerySyncSessionRecycle -------------------------------------
 */
void ObQuerySyncSessionRecycle::runTimerTask()
{
  query_session_recycle();
}

void ObQuerySyncSessionRecycle::query_session_recycle()
{
  ObQuerySyncMgr::get_instance().clean_timeout_query_session();
}

/**
 * -----------------------------------Singleton ObQuerySyncMgr -------------------------------------
 */
int64_t ObQuerySyncMgr::once_ = 0;
ObQuerySyncMgr *ObQuerySyncMgr::instance_ = NULL;

ObQuerySyncMgr::ObQuerySyncMgr() : session_id_(0)
{}

ObQuerySyncMgr &ObQuerySyncMgr::get_instance()
{
  int ret = OB_SUCCESS;
  ObQuerySyncMgr *instance = NULL;
  while (OB_UNLIKELY(once_ < 2)) {
    if (ATOMIC_BCAS(&once_, 0, 1)) {
      instance = OB_NEW(ObQuerySyncMgr, ObModIds::TABLE_PROC);
      if (OB_NOT_NULL(instance)) {
        if (OB_FAIL(instance->init())) {
          LOG_WARN("failed to init ObQuerySyncMgr instance");
          OB_DELETE(ObQuerySyncMgr, ObModIds::TABLE_PROC, instance);
          instance = NULL;
          if (OB_UNLIKELY(!ATOMIC_BCAS(&once_, 1, 0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected error, once_ should be 1", K(ret));
          }
        } else {
          instance_ = instance;
          if (OB_UNLIKELY(!ATOMIC_BCAS(&once_, 1, 2))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected error, once_ should be 1", K(ret));
          }
        }
      } else {
        if(OB_UNLIKELY(!ATOMIC_BCAS(&once_, 1, 0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected error, once_ should be 1", K(ret));
        }
      }
    }
  }
  return *(ObQuerySyncMgr *)instance_;
}

int ObQuerySyncMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_session_map_.create(QUERY_SESSION_MAX_SIZE, ObModIds::TABLE_PROC, ObModIds::TABLE_PROC))) {
    LOG_WARN("fail to create query session map", K(ret));
  } else if (OB_FAIL(timer_.init())) {
    LOG_WARN("fail to init timer_", K(ret));
  } else if (OB_FAIL(timer_.schedule(query_session_recycle_, QUERY_SESSION_CLEAN_DELAY, true))) {
    LOG_WARN("fail to schedule query session clean task. ", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    LOG_WARN("fail to start query session clean task timer.", K(ret));
  }
  return ret;
}

uint64_t ObQuerySyncMgr::generate_query_sessid()
{
  return ATOMIC_AAF(&session_id_, 1);
}

int ObQuerySyncMgr::get_query_session(uint64_t sessid, ObTableQuerySyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  get_locker(sessid).lock();
  if (OB_FAIL(query_session_map_.get_refactored(sessid, query_session))) {
    if (OB_HASH_NOT_EXIST != ret) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_ISNULL(query_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null query session", K(ret), K(sessid));
  } else if (query_session->is_in_use()) {  // one session cannot be held concurrently
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query session already in use", K(sessid));
  } else {
    query_session->set_in_use(true);
  }
  get_locker(sessid).unlock();
  return ret;
}

int ObQuerySyncMgr::set_query_session(uint64_t sessid, ObTableQuerySyncSession *query_session)
{
  int ret = OB_SUCCESS;
  bool force = false;
  if (OB_FAIL(query_session_map_.set_refactored(sessid, query_session, false))) {
    LOG_WARN("set query session failed", K(ret), K(sessid));
  }
  return ret;
}

void ObQuerySyncMgr::clean_timeout_query_session()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<uint64_t, 128> session_id_array;
  ObGetAllSessionIdOp op(session_id_array);
  if (OB_FAIL(query_session_map_.foreach_refactored(op))) {
    LOG_WARN("fail to get all session id from query sesion map", K(ret));
  } else {
    for (int64_t i = 0; i < session_id_array.count(); i++) {
      uint64_t sess_id = session_id_array.at(i);
      ObTableQuerySyncSession *query_session = nullptr;
      get_locker(sess_id).lock();
      if (OB_FAIL(query_session_map_.get_refactored(sess_id, query_session))) {
        LOG_DEBUG("query session already deleted by worker", K(ret), K(sess_id));
      } else if (OB_ISNULL(query_session)) {
        ret = OB_ERR_NULL_VALUE;
        (void)query_session_map_.erase_refactored(sess_id);
        LOG_WARN("unexpected null query sesion", K(ret));
      } else if (query_session->is_in_use()) {
      } else if (QUERY_SESSION_TIMEOUT + query_session->get_timestamp() > ObTimeUtility::current_time()) {
      } else {
        const ObGlobalContext &gctx = ObServer::get_instance().get_gctx();  // get gctx
        storage::ObPartitionService *part_service = gctx.par_ser_;          // get part_service
        if (OB_ISNULL(part_service)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("free query session fail, part service null", K(ret));
        } else {
          ObTableServiceQueryCtx *table_service_ctx = query_session->get_table_service_ctx();
          if (OB_ISNULL(table_service_ctx)) {
            ret = OB_ERR_NULL_VALUE;
            LOG_WARN("free query session fail, table service context null", K(ret));
          } else {
            table_service_ctx->destroy_result_iterator(part_service);
          }
        }
        (void)query_session_map_.erase_refactored(sess_id);
        OB_DELETE(ObTableQuerySyncSession, ObModIds::TABLE_PROC, query_session);
        // connection loses or bug exists
        LOG_WARN("clean timeout query session success", K(ret), K(sess_id));
      }
      get_locker(sess_id).unlock();
    }
  }
}

ObQuerySyncMgr::ObQueryHashMap *ObQuerySyncMgr::get_query_session_map()
{
  return &query_session_map_;
}

ObTableQuerySyncSession *ObQuerySyncMgr::alloc_query_session()
{
  int ret = OB_SUCCESS;
  ObTableQuerySyncSession *query_session = OB_NEW(ObTableQuerySyncSession, ObModIds::TABLE_PROC);
  if (OB_ISNULL(query_session)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ObTableQuerySyncSession", K(ret));
  } else if (OB_FAIL(query_session->init())) {
    LOG_WARN("failed to init query session", K(ret));
    OB_DELETE(ObTableQuerySyncSession, ObModIds::TABLE_PROC, query_session);
  }
  return query_session;
}

int ObQuerySyncMgr::ObGetAllSessionIdOp::operator()(QuerySessionPair &entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session_id_array_.push_back(entry.first))) {
    LOG_WARN("fail to push back query session id", K(ret));
  }
  return ret;
}

/**
 * -------------------------------------- ObTableQuerySyncP ----------------------------------------
 */
ObTableQuerySyncP::ObTableQuerySyncP(const ObGlobalContext &gctx)
    : ObTableRpcProcessor(gctx),
      table_service_ctx_(nullptr),
      result_row_count_(0),
      query_session_id_(0),
      allocator_(ObModIds::TABLE_PROC),
      query_session_(nullptr),
      timeout_ts_(0)
{}

int ObTableQuerySyncP::deserialize()
{
  arg_.query_.set_deserialize_allocator(&allocator_);
  return ParentType::deserialize();
}

int ObTableQuerySyncP::check_arg()
{
  int ret = OB_SUCCESS;
  if (arg_.query_type_ == ObQueryOperationType::QUERY_START && !arg_.query_.is_valid() &&
      arg_.query_.get_htable_filter().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table query request", K(ret), "query", arg_.query_);
  } else if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
                 arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("some options not supported yet", K(ret), "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

void ObTableQuerySyncP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObTableConsistencyLevel::STRONG == arg_.consistency_level_
                                         ? ObConsistencyLevel::STRONG
                                         : ObConsistencyLevel::WEAK;
  audit_record_.return_rows_ = result_.get_row_count();
  audit_record_.table_scan_ = true;  // todo: exact judgement
  audit_record_.affected_rows_ = result_.get_row_count();
  audit_record_.try_cnt_ = retry_count_ + 1;
}

uint64_t ObTableQuerySyncP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  const uint64_t op_checksum = arg_.query_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  return checksum;
}

void ObTableQuerySyncP::reset_ctx()
{
  result_row_count_ = 0;
  query_session_ = nullptr;
  table_service_ctx_ = nullptr;
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQuerySyncP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

int ObTableQuerySyncP::get_partition_ids(uint64_t table_id, ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  uint64_t partition_id = arg_.partition_id_;
  if (OB_INVALID_ID == partition_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("partitioned table not supported", K(ret), K(table_id));
  } else if (OB_FAIL(part_ids.push_back(partition_id))) {
    LOG_WARN("failed to push back of partition id", K(ret));
  }
  return ret;
}

int ObTableQuerySyncP::get_session_id(uint64_t &real_sessid, uint64_t arg_sessid)
{
  int ret = OB_SUCCESS;
  real_sessid = arg_sessid;
  if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
    real_sessid = ObQuerySyncMgr::get_instance().generate_query_sessid();
  }
  if (OB_UNLIKELY(real_sessid == ObQuerySyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("session id is invalid", K(ret), K(real_sessid), K(arg_.query_type_));
  }
  return ret;
}

void ObTableQuerySyncP::set_htable_compressor()
{
  int ret = OB_SUCCESS;
  // hbase model, compress the result packet
  ObCompressorType compressor_type = INVALID_COMPRESSOR;
  if (OB_FAIL(ObCompressorPool::get_instance().get_compressor_type(
                  GCONF.tableapi_transport_compress_func, compressor_type))) {
    compressor_type = INVALID_COMPRESSOR;
  } else if (NONE_COMPRESSOR == compressor_type) {
    compressor_type = INVALID_COMPRESSOR;
  }
  this->set_result_compress_type(compressor_type);
}


int ObTableQuerySyncP::get_query_session(uint64_t sessid, ObTableQuerySyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sessid == ObQuerySyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("fail to get query session, session id is invalid", K(ret), K(sessid));
  } else if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
    query_session = ObQuerySyncMgr::get_instance().alloc_query_session();
    if (OB_ISNULL(query_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate ObTableQuerySyncSession", K(ret), K(sessid));
    } else if (OB_FAIL(ObQuerySyncMgr::get_instance().set_query_session(sessid, query_session))) {
      LOG_WARN("fail to insert session to query map", K(ret), K(sessid));
      OB_DELETE(ObTableQuerySyncSession, ObModIds::TABLE_PROC, query_session);
    }
  } else if (OB_FAIL(ObQuerySyncMgr::get_instance().get_query_session(sessid, query_session))) {
    LOG_WARN("fail to get query session from query sync mgr", K(ret), K(sessid));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(query_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query session", K(ret), K(sessid));
    } else {
      // set trans from session
      trans_desc_ptr_ = query_session->get_trans_desc();
      part_epoch_list_ptr_ = query_session->get_part_epoch_list();
      participants_ptr_ = query_session->get_part_leader_list();
      trans_state_ptr_ = query_session->get_trans_state();
    }
  }
  return ret;
}

int ObTableQuerySyncP::query_scan_with_old_context(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObTableQueryResultIterator *result_iterator;
  if (arg_.query_.get_htable_filter().is_valid()) {
    result_iterator = query_session_->get_htable_result_iterator();
    set_htable_compressor();
  } else {
    result_iterator = query_session_->get_result_iterator();
  }
  if (OB_ISNULL(result_iterator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query result iterator null", K(ret));
  } else {
    ObTableQueryResult *query_result = nullptr;
    result_iterator->set_one_result(&result_);  // set result_ as container
    if (ObTimeUtility::current_time() > timeout) {
      ret = OB_TRANS_TIMEOUT;
      LOG_WARN("exceed operatiton timeout", K(ret));
    } else if (OB_FAIL(result_iterator->get_next_result(query_result))) {
      if (OB_ITER_END == ret) {
        result_.is_end_ = true;  // set scan end
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to scan result", K(ret));
      }
    } else {
      result_.is_end_ = !result_iterator->has_more_result();
    }
  }
  return ret;
}

int ObTableQuerySyncP::query_scan_with_new_context(
    ObTableQuerySyncSession *query_session, table::ObTableQueryResultIterator *result_iterator, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (arg_.query_.get_htable_filter().is_valid()) {
    set_htable_compressor();
  }
  ObTableQueryResult *query_result = nullptr;
  if (ObTimeUtility::current_time() > timeout) {
    ret = OB_TRANS_TIMEOUT;
    LOG_WARN("exceed operatiton timeout", K(ret), K(rpc_pkt_)->get_timeout());
  } else if (OB_ISNULL(result_iterator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null result iterator", K(ret));
  } else if (OB_FAIL(result_iterator->get_next_result(query_result))) {
    if (OB_ITER_END == ret) {  // scan to end
      ret = OB_SUCCESS;
      result_.is_end_ = true;
    }
  } else if (result_iterator->has_more_result()) {
    result_.is_end_ = false;
    query_session->deep_copy_select_columns(arg_.query_);
    if (arg_.query_.get_htable_filter().is_valid()) {
      query_session->set_htable_result_iterator(dynamic_cast<table::ObHTableFilterOperator *>(result_iterator));
    } else {
      query_session->set_result_iterator(dynamic_cast<ObNormalTableQueryResultIterator *>(result_iterator));
    }
    
  } else {
    result_.is_end_ = true;
  }
  return ret;
}

int ObTableQuerySyncP::query_scan_with_init()
{
  int ret = OB_SUCCESS;
  table_service_ctx_ = query_session_->get_table_service_ctx();
  table_service_ctx_->scan_param_.is_thread_scope_ = false;
  uint64_t &table_id = table_service_ctx_->param_table_id();
  table_service_ctx_->init_param(timeout_ts_,
      this->get_trans_desc(),
      query_session_->get_allocator(),
      false /*ignored*/,
      arg_.entity_type_,
      table::ObBinlogRowImageType::MINIMAL /*ignored*/);
  ObSEArray<int64_t, 1> part_ids;
  table::ObTableQueryResultIterator *result_iterator = nullptr;
  const bool is_readonly = true;
  const ObTableConsistencyLevel consistency_level = arg_.consistency_level_;

  if (OB_FAIL(get_table_id(arg_.table_name_, arg_.table_id_, table_id))) {
    LOG_WARN("failed to get table id", K(ret));
  } else if (OB_FAIL(get_partition_ids(table_id, part_ids))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (1 != part_ids.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("should have one partition", K(ret), K(part_ids));
  } else if (FALSE_IT(table_service_ctx_->param_partition_id() = part_ids.at(0))) {
  } else if (OB_FAIL(
                 start_trans(is_readonly, sql::stmt::T_SELECT, consistency_level, table_id, part_ids, timeout_ts_))) {
    LOG_WARN("failed to start readonly transaction", K(ret));
  } else if (OB_FAIL(table_service_->execute_query(*table_service_ctx_, arg_.query_, result_, result_iterator))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("failed to execute query", K(ret), K(table_id));
    }
  } else if (OB_FAIL(query_scan_with_new_context(query_session_, result_iterator, timeout_ts_))) {
    LOG_WARN("fail to query, need rollback", K(ret));
  } else {
    audit_row_count_ = result_.get_row_count();
    result_.query_session_id_ = query_session_id_;
  }

  return ret;
}

int ObTableQuerySyncP::query_scan_without_init()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_session_->get_table_service_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null result iterator or table service context", K(ret));
  } else if (OB_FAIL(query_scan_with_old_context(timeout_ts_))) {
    LOG_WARN("fail to query scan with old context", K(ret));
  } else {
    audit_row_count_ = result_.get_row_count();
    result_.query_session_id_ = query_session_id_;
  }
  return ret;
}

int ObTableQuerySyncP::process_query_start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_scan_with_init())) {
    LOG_WARN("failed to process query start scan with init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query start", K(ret), K(query_session_id_));
  }
  return ret;
}

int ObTableQuerySyncP::process_query_next()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_scan_without_init())) {
    LOG_WARN("fail to query next scan without init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query next", K(ret), K(query_session_id_));
  }
  return ret;
}

int ObTableQuerySyncP::try_process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_query_type())) {
    LOG_WARN("query type is invalid", K(ret), K(arg_.query_type_));
  } else if (OB_FAIL(get_session_id(query_session_id_, arg_.query_session_id_))) {
    LOG_WARN("fail to get query session id", K(ret), K(arg_.query_session_id_));
  } else if (OB_FAIL(get_query_session(query_session_id_, query_session_))) {
    LOG_WARN("fail to get query session", K(ret), K(query_session_id_));
  } else if (FALSE_IT(table_service_ctx_ = query_session_->get_table_service_ctx())) {
  } else if (FALSE_IT(timeout_ts_ = get_timeout_ts())) {
  } else {
    if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
      ret = process_query_start();
    } else if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_) {
      ret = process_query_next();
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
      query_session_->set_timestamp(ObTimeUtility::current_time());
      query_session_->set_in_use(false);
    }
  }

  stat_event_type_ = ObTableProccessType::TABLE_API_TABLE_QUERY_SYNC;  // table querysync
  return ret;
}

// session.in_use_ must be true
int ObTableQuerySyncP::destory_query_session(bool need_rollback_trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, timeout_ts_))) {
    LOG_WARN("failed to end trans", K(ret), K(need_rollback_trans));
  }
  int tmp_ret = ret;
  ObQuerySyncMgr::get_instance().get_locker(query_session_id_).lock();
  if (OB_ISNULL(query_session_) || OB_ISNULL(table_service_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null value", K(ret), KP_(query_session), KP_(table_service_ctx));
  } else if (OB_FAIL(ObQuerySyncMgr::get_instance().get_query_session_map()->erase_refactored(query_session_id_))) {
    LOG_WARN("fail to erase query session from query sync mgr", K(ret));
  } else {
    table_service_ctx_->destroy_result_iterator(part_service_);
    OB_DELETE(ObTableQuerySyncSession, ObModIds::TABLE_PROC, query_session_);
    LOG_DEBUG("destory query session success", K(ret), K(query_session_id_), K(need_rollback_trans));
  }
  ObQuerySyncMgr::get_instance().get_locker(query_session_id_).unlock();

  ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  return ret;
}

int ObTableQuerySyncP::check_query_type()
{
  int ret = OB_SUCCESS;
  if (arg_.query_type_ != table::ObQueryOperationType::QUERY_START &&
      arg_.query_type_ != table::ObQueryOperationType::QUERY_NEXT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query operation type", K(ret), K(arg_.query_type_));
  }
  return ret;
}
