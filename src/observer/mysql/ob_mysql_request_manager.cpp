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

#include "ob_mysql_request_manager.h"
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/alloc/alloc_func.h"
#include "lib/thread/thread_mgr.h"
#include "lib/rc/ob_rc.h"
#include "common/ob_clock_generator.h"
#include "share/rc/ob_context.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/ob_server.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_callback.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/session/ob_basic_session_info.h"
#include "observer/mysql/ob_query_response_time.h"

namespace oceanbase
{
using namespace oceanbase::share::schema;
namespace obmysql
{

ObMySQLRequestRecord::~ObMySQLRequestRecord()
{

}

const int64_t ObMySQLRequestManager::EVICT_INTERVAL;

ObMySQLRequestManager::ObMySQLRequestManager()
  : inited_(false), destroyed_(false), request_id_(0), mem_limit_(0),
    allocator_(), queue_(), task_(),
    tenant_id_(OB_INVALID_TENANT_ID), tg_id_(-1), stop_flag_(true)
{
}

ObMySQLRequestManager::~ObMySQLRequestManager()
{
  if (inited_) {
    destroy();
  }
}

int ObMySQLRequestManager::init(uint64_t tenant_id,
                                const int64_t max_mem_size,
                                const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(queue_.init(ObModIds::OB_MYSQL_REQUEST_RECORD, queue_size, tenant_id))) {
    SERVER_LOG(WARN, "Failed to init ObMySQLRequestQueue", K(ret));
  } else if (OB_FAIL(allocator_.init(SQL_AUDIT_PAGE_SIZE,
                                     ObModIds::OB_MYSQL_REQUEST_RECORD,
                                     tenant_id,
                                     INT64_MAX))) {
    SERVER_LOG(WARN, "failed to init allocator", K(ret));
  } else {
    //check FIFO mem used and sql audit records every 1 seconds
    if (OB_FAIL(task_.init(this))) {
      SERVER_LOG(WARN, "fail to init sql audit time tast", K(ret));
    } else {
      mem_limit_ = max_mem_size;
      tenant_id_ = tenant_id;
      inited_ = true;
      destroyed_ = false;
    }
  }
  if ((OB_FAIL(ret)) && (!inited_)) {
    destroy();
  }
  return ret;
}

int ObMySQLRequestManager::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObMySQLRequestManager is not inited", K(tenant_id_));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::ReqMemEvict, tg_id_))) {
    SERVER_LOG(WARN, "create failed", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    SERVER_LOG(WARN, "init timer fail", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, task_, EVICT_INTERVAL, true))) {
    SERVER_LOG(WARN, "start eliminate task failed", K(ret));
  } else {
    stop_flag_ = false;
  }
  return ret;
}

void ObMySQLRequestManager::stop()
{
  if (inited_ && !stop_flag_) {
    TG_STOP(tg_id_);
  }
}

void ObMySQLRequestManager::wait()
{
  if (inited_ && !stop_flag_) {
    TG_WAIT(tg_id_);
    stop_flag_ = true;
  }
}

void ObMySQLRequestManager::destroy()
{
  if (!destroyed_) {
    TG_DESTROY(tg_id_);
    clear_queue();
    queue_.destroy();
    allocator_.destroy();
    inited_ = false;
    destroyed_ = true;
    stop_flag_ = true;
  }
}

/*
 * record infomation
 * 1.server addr           addr
 * 2.server port           int
 * 3.client addr           addr
 * 4.client port           int
 * 5.user_name             varchar
 * 6.request_id            int
 * 7.sql_id                int
 * 8.sql                   varchar
 * 9.request_time          int
 *10.elipsed_time          int
 *11.tenant_name           varchar
 */

int ObMySQLRequestManager::record_request(const ObAuditRecordData &audit_record,
                                          const bool enable_query_response_time_stats,
                                          bool is_sensitive)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObMySQLRequestRecord *record = NULL;
    char *buf = NULL;
    //alloc mem from allocator
    int64_t pos = sizeof(ObMySQLRequestRecord);
    int64_t total_size = sizeof(ObMySQLRequestRecord)
                     + audit_record.sql_len_
                     + audit_record.tenant_name_len_
                     + audit_record.user_name_len_
                     + audit_record.db_name_len_
                     + audit_record.params_value_len_
                     + audit_record.rule_name_len_
                     + audit_record.proxy_user_name_len_;
    if (NULL == (buf = (char*)alloc(total_size))) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        SERVER_LOG(WARN, "record concurrent fifoallocator alloc mem failed",
            K(total_size), K(tenant_id_), K(mem_limit_), K(request_id_), K(ret));
      }
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      record = new(buf)ObMySQLRequestRecord();
      record->allocator_ = &allocator_;
      record->data_ = audit_record;
      //deep copy sql
      if ((audit_record.sql_len_ > 0) && (NULL != audit_record.sql_)) {
        int64_t stmt_len = min(audit_record.sql_len_, OB_MAX_SQL_LENGTH);
        MEMCPY(buf + pos, audit_record.sql_, stmt_len);
        record->data_.sql_ = buf + pos;
        pos += stmt_len;
      }
      //deep copy params value
      if ((audit_record.params_value_len_ > 0) && (NULL != audit_record.params_value_)) {
        MEMCPY(buf + pos, audit_record.params_value_, audit_record.params_value_len_);
        record->data_.params_value_ = buf + pos;
        pos += audit_record.params_value_len_;
      }
      //deep copy rule name
      if ((audit_record.rule_name_len_ > 0) && (NULL != audit_record.rule_name_)) {
        MEMCPY(buf + pos, audit_record.rule_name_, audit_record.rule_name_len_);
        record->data_.rule_name_ = buf + pos;
        pos += audit_record.rule_name_len_;
      }
      //deep copy tenant_name
      if ((audit_record.tenant_name_len_ > 0) && (NULL != audit_record.tenant_name_)) {
        int64_t tenant_len = min(audit_record.tenant_name_len_, OB_MAX_TENANT_NAME_LENGTH);
        MEMCPY(buf + pos, audit_record.tenant_name_, tenant_len);
        record->data_.tenant_name_ = buf + pos;
        pos += tenant_len;
      }
      //deep copy user_name
      if ((audit_record.user_name_len_ > 0) && (NULL != audit_record.user_name_)) {
        int64_t user_len = min(audit_record.user_name_len_, OB_MAX_USER_NAME_LENGTH);
        MEMCPY(buf + pos, audit_record.user_name_, user_len);
        record->data_.user_name_ = buf + pos;
        pos += user_len;
      }
      //deep copy db_name
      if ((audit_record.db_name_len_ > 0) && (NULL != audit_record.db_name_)) {
        int64_t db_len = min(audit_record.db_name_len_, OB_MAX_DATABASE_NAME_LENGTH);
        MEMCPY(buf + pos, audit_record.db_name_, db_len);
        record->data_.db_name_ = buf + pos;
        pos += db_len;
      }
      //deep copy proxy_user_name
      if ((audit_record.proxy_user_name_len_ > 0) && (NULL != audit_record.proxy_user_name_)) {
        int64_t user_len = min(audit_record.proxy_user_name_len_, OB_MAX_USER_NAME_LENGTH);
        MEMCPY(buf + pos, audit_record.proxy_user_name_, user_len);
        record->data_.proxy_user_name_ = buf + pos;
        pos += user_len;
      }
      //for find bug
      // only print this log if enable_perf_event is enable,
      // for `receive_ts_` might be invalid if `enable_perf_event` is false
      if (lib::is_diagnose_info_enabled()
          && OB_UNLIKELY(ObClockGenerator::getClock() - audit_record.exec_timestamp_.receive_ts_ > US_PER_HOUR)) {
        SERVER_LOG(WARN, "record: query too slow ",
                   "elapsed", ObClockGenerator::getClock() - audit_record.exec_timestamp_.receive_ts_,
                   "receive_ts", audit_record.exec_timestamp_.receive_ts_);
      }

      // query response time
      if (enable_query_response_time_stats) {
        observer::ObRSTCollector::get_instance().collect_query_response_time(audit_record.tenant_id_,audit_record.get_elapsed_time());
      }

      //push into queue
      if (OB_SUCC(ret)) {
        if (is_sensitive) {
          free(record);
          record = NULL;
        } else if (OB_FAIL(queue_.push_with_imme_seq(record, record->data_.request_id_))) {
          //sql audit槽位已满时会push失败, 依赖后台线程进行淘汰获得可用槽位
          if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
            SERVER_LOG(WARN, "push into queue failed", K(ret));
          }
          free(record);
          record = NULL;
        }
      }
    }
  } // end
  return ret;
}

int ObMySQLRequestManager::get_mem_limit(uint64_t tenant_id,
                                         int64_t &mem_limit)
{
  int ret = OB_SUCCESS;
  int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id);
  // default mem limit
  mem_limit = static_cast<int64_t>(static_cast<double>(tenant_mem_limit) * SQL_AUDIT_MEM_FACTOR);

  // get mem_percentage from session info
  ObArenaAllocator alloc;
  ObObj obj_val;
  int64_t mem_pct = 0;
  if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(tenant_id,
                                                          alloc,
                                                          ObDataTypeCastParams(),
                                                          ObString(OB_SV_SQL_AUDIT_PERCENTAGE),
                                                          obj_val))) {
    LOG_WARN("failed to get global sys variable", K(ret), K(tenant_id), K(OB_SV_SQL_AUDIT_PERCENTAGE), K(obj_val));
  } else if (OB_FAIL(obj_val.get_int(mem_pct))) {
    LOG_WARN("failed to get int", K(ret), K(obj_val));
  } else if (mem_pct < 0 || mem_pct > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid value of sql audit mem percentage", K(ret), K(mem_pct));
  } else {
    mem_limit = static_cast<int64_t>(static_cast<double>(tenant_mem_limit * mem_pct) / 100);
    LOG_DEBUG("tenant sql audit memory limit",
             K(tenant_id), K(tenant_mem_limit), K(mem_pct), K(mem_limit));
  }
  return ret;
}

int ObMySQLRequestManager::mtl_new(ObMySQLRequestManager* &req_mgr)
{
  int ret = OB_SUCCESS;
  req_mgr = OB_NEW(ObMySQLRequestManager, ObMemAttr(MTL_ID(), ObModIds::OB_MYSQL_REQUEST_RECORD));
  if (nullptr == req_mgr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for ObMySQLRequestManager", K(ret));
  }
  return ret;
}

int ObMySQLRequestManager::mtl_init(ObMySQLRequestManager* &req_mgr)
{
  int ret = OB_SUCCESS;
  if (nullptr == req_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObMySQLRequestManager not alloc yet", K(ret));
  } else {
    uint64_t tenant_id = lib::current_resource_owner_id();
    int64_t mem_limit = lib::get_tenant_memory_limit(tenant_id);
    mem_limit = static_cast<int64_t>(static_cast<double>(mem_limit) * SQL_AUDIT_MEM_FACTOR);
    bool use_mini_queue = lib::is_mini_mode() || MTL_IS_MINI_MODE() || is_meta_tenant(tenant_id);
    int64_t queue_size = use_mini_queue ? MINI_MODE_MAX_QUEUE_SIZE : MAX_QUEUE_SIZE;
    if (OB_FAIL(req_mgr->init(tenant_id, mem_limit, queue_size))) {
      LOG_WARN("failed to init request manager", K(ret));
    } else {
      // do nothing
    }
    LOG_INFO("mtl init finish", K(tenant_id), K(mem_limit), K(queue_size), K(ret));
  }
  if (OB_FAIL(ret) && req_mgr != nullptr) {
    // cleanup
    common::ob_delete(req_mgr);
    req_mgr = nullptr;
  }
  return ret;
}

void ObMySQLRequestManager::mtl_destroy(ObMySQLRequestManager* &req_mgr)
{
  common::ob_delete(req_mgr);
  req_mgr = nullptr;
}

} // end of namespace obmysql
} // end of namespace oceanbase
