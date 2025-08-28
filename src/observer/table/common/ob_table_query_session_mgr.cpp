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
#include "ob_table_query_session_mgr.h"

using namespace oceanbase::table;

namespace oceanbase
{
namespace observer
{

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

int ObTableQueryASyncMgr::get_query_session(uint64_t sessid, ObITableQueryAsyncSession *&query_session)
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
               query_session->get_timeout_ts() < ObTimeUtility::current_time()) {
      ret = OB_TIMEOUT;
      LOG_WARN("session is timeout", K(ret), K(query_session));
    } else {
      query_session->set_in_use(true);
    }
    get_locker(sessid).unlock();
  }
  return ret;
}

int ObTableQueryASyncMgr::set_query_session(uint64_t sessid, ObITableQueryAsyncSession *query_session)
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
        ObITableQueryAsyncSession *query_session = nullptr;
        if (OB_FAIL(query_session_map_.get_refactored(sess_id, query_session))) {
          LOG_DEBUG("query session already deleted by worker", K(ret), K(sess_id));
        } else if (OB_ISNULL(query_session)) {
          ret = OB_ERR_NULL_VALUE;
          (void)query_session_map_.erase_refactored(sess_id);
          LOG_WARN("unexpected null query sesion", K(ret));
        } else {
          transaction::ObTxDesc* tx_desc = query_session->get_trans_desc();
          ObTableTransUtils::release_read_trans(tx_desc);
          (void)query_session_map_.erase_refactored(sess_id);
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
        ObITableQueryAsyncSession *query_session = nullptr;
        get_locker(sess_id).lock();
        if (OB_FAIL(query_session_map_.get_refactored(sess_id, query_session))) {
          LOG_DEBUG("query session already deleted by worker", K(ret), K(sess_id));
        } else if (OB_ISNULL(query_session)) {
          ret = OB_ERR_NULL_VALUE;
          (void)query_session_map_.erase_refactored(sess_id);
          LOG_WARN("unexpected null query sesion", K(ret));
        } else if (query_session->is_in_use()) {
        } else if (query_session->get_timeout_ts() >= now) {
          min_timeout_period = OB_MIN(query_session->get_timeout_ts() - now, min_timeout_period);
        } else {
          transaction::ObTxDesc* tx_desc = query_session->get_trans_desc();
          ObTableTransUtils::release_read_trans(tx_desc);
          (void)query_session_map_.erase_refactored(sess_id);
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

ObTableQueryASyncMgr::ObQueryHashMap *ObTableQueryASyncMgr::get_query_session_map()
{
  return &query_session_map_;
}

void ObTableQueryASyncMgr::free_query_session(ObITableQueryAsyncSession *query_session)
{
  if (OB_NOT_NULL(query_session)) {
    query_session->~ObITableQueryAsyncSession(); 
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

// session.in_use_ must be true
int ObTableQueryASyncMgr::destory_query_session(ObITableQueryAsyncSession *query_session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query session", K(ret));
  } else {
    transaction::ObTxDesc* tx_desc = query_session->get_trans_desc();
    ObTableTransUtils::release_read_trans(tx_desc);
    int64_t query_session_id = query_session->get_session_id();
    MTL(ObTableQueryASyncMgr*)->get_locker(query_session_id).lock();
    if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_query_session_map()->erase_refactored(query_session_id))) {
      LOG_WARN("fail to erase query session from query sync mgr", K(ret));
    } else {
      MTL(ObTableQueryASyncMgr*)->free_query_session(query_session);
      LOG_DEBUG("destory query session success", K(ret), K(query_session_id));
    }
    MTL(ObTableQueryASyncMgr*)->get_locker(query_session_id).unlock();
  }
  return ret;
}

template<typename T>
int ObTableQueryASyncMgr::get_query_session(uint64_t sessid,
                                            ObQueryOperationType query_type,
                                            ObITableQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sessid == ObTableQueryASyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("fail to get query session, session id is invalid", K(ret), K(sessid));
  } else if (ObQueryOperationType::QUERY_START == query_type) { // query start
    query_session = alloc_query_session<T>(sessid);
    if (OB_ISNULL(query_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate query session", K(ret), K(sessid));
    } else if (OB_FAIL(set_query_session(sessid, query_session))) {
      LOG_WARN("fail to insert session to query map", K(ret), K(sessid));
     free_query_session(query_session);
    } else {}
  } else if (ObQueryOperationType::QUERY_NEXT == query_type || 
             ObQueryOperationType::QUERY_END == query_type || 
             ObQueryOperationType::QUERY_RENEW == query_type) {
    if (OB_FAIL(get_query_session(sessid, query_session))) {
      LOG_WARN("fail to get query session from query sync mgr", K(ret), K(sessid));
    } else if (OB_ISNULL(query_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null query session", K(ret), K(sessid));
    } else {
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unkown query type", K(query_type));
  }
  return ret;
}

template int ObTableQueryASyncMgr::get_query_session<ObTableQueryAsyncSession>(
  uint64_t, ObQueryOperationType, ObITableQueryAsyncSession *&);
template int ObTableQueryASyncMgr::get_query_session<ObTableNewQueryAsyncSession>(
  uint64_t, ObQueryOperationType, ObITableQueryAsyncSession *&);

int ObTableQueryASyncMgr::get_session_id(uint64_t &real_sessid,
                                         uint64_t arg_sessid,
                                         const ObQueryOperationType query_type)
{
  int ret = OB_SUCCESS;
  real_sessid = arg_sessid;
  if (ObQueryOperationType::QUERY_START == query_type) {
    if (OB_FAIL(generate_query_sessid(real_sessid))) {
      LOG_WARN("fail to get session id", K(ret), K(real_sessid));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(real_sessid == ObTableQueryASyncMgr::INVALID_SESSION_ID)) {
    ret = OB_ERR_UNKNOWN_SESSION_ID;
    LOG_WARN("session id is invalid", K(ret), K(real_sessid), K(query_type));
  }
  return ret;
}

int ObTableQueryASyncMgr::check_query_type(const ObQueryOperationType query_type)
{
  int ret = OB_SUCCESS;
  if (query_type < table::ObQueryOperationType::QUERY_START ||
            query_type >= table::ObQueryOperationType::QUERY_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query operation type", K(ret), K(query_type));
  }
  return ret;
}


} // end of namespace table
} // end of namespace oceanbase