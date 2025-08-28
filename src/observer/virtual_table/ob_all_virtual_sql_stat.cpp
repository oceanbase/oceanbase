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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_all_virtual_sql_stat.h"
#include "lib/allocator/ob_mod_define.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "share/ash/ob_di_util.h"
#include "observer/ob_server_struct.h"
#include "sql/plan_cache/ob_plan_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share;

int ObGetAllSqlStatCacheIdOp::operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(key_array_) || OB_ISNULL(entry.second)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret));
  } else if (entry.second->get_ns() == ObLibCacheNameSpace::NS_SQLSTAT ||
             entry.second->get_ns() == ObLibCacheNameSpace::NS_CRSR) {
    if (!entry.second->added_lc()) {
      // do nothing
    } else if (OB_FAIL(key_array_->push_back(entry.first))) {
      LOG_WARN("fail to push back plan_id to key array", K(ret));
    }
  }
  return ret;
}

void ObAllVirtualSqlStat::reset()
{
  omt::ObMultiTenantOperator::reset();
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc is null", K(ret));
  } else if (OB_ISNULL(last_sql_stat_record_)) {
    // is nullptr , do nothing
  } else {
    ipstr_.reset();
    last_sql_stat_record_->~ObExecutedSqlStatRecord();
    allocator_->free(last_sql_stat_record_);
    tmp_sql_stat_map_.destroy();
    sql_stat_cache_id_array_.reset();
    sql_stat_cache_id_array_idx_ = 0;
    first_enter_ = true;
  }
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSqlStat::get_server_ip_and_port()
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  const common::ObAddr &addr = GCTX.self_addr();
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      LOG_WARN("failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}

int ObAllVirtualSqlStat::fill_row(
  const uint64_t tenant_id,
  const ObExecutedSqlStatRecord *sql_stat_record,
  common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;

  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
      case SVR_IP: {
        cells[cell_idx].set_varchar(ipstr_);
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[cell_idx].set_int(port_);
        break;
      }
      case TENANT_ID: {
        cells[cell_idx].set_int(tenant_id);
        break;
      }
      case SQL_ID: {
        cells[cell_idx].set_varchar(ObString::make_string(sql_stat_record->get_key().sql_id_));
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case PLAN_ID: {
        cells[cell_idx].set_int(sql_stat_record->get_sql_stat_info().get_plan_id());
        break;
      }
      case PLAN_HASH: {
        cells[cell_idx].set_uint64(sql_stat_record->get_key().plan_hash_);
        break;
      }
      case PLAN_TYPE: {
        cells[cell_idx].set_int(sql_stat_record->get_sql_stat_info().get_plan_type());
        break;
      }
      case QUERY_SQL: {
        ObCollationType src_cs_type = ObCharset::is_valid_collation(sql_stat_record->get_sql_stat_info().get_sql_cs_type()) ?
                sql_stat_record->get_sql_stat_info().get_sql_cs_type() : ObCharset::get_system_collation();
        ObString src_string(static_cast<int32_t>(STRLEN(sql_stat_record->get_sql_stat_info().get_query_sql())), sql_stat_record->get_sql_stat_info().get_query_sql());
        ObString dst_string;
        if (OB_FAIL(ObCharset::charset_convert(row_calc_buf_,
                                                      src_string,
                                                      src_cs_type,
                                                      ObCharset::get_system_collation(),
                                                      dst_string,
                                                      ObCharset::REPLACE_UNKNOWN_CHARACTER))) {
          SERVER_LOG(WARN, "fail to convert sql string", K(ret));
        } else {
          cells[cell_idx].set_lob_value(ObLongTextType, dst_string.ptr(),
                                        min(dst_string.length(), 1024));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        }
        break;
      }
      case SQL_TYPE: {
        cells[cell_idx].set_int(sql_stat_record->get_sql_stat_info().get_sql_type());
        break;
      }
      case MODULE: {
        cells[cell_idx].set_null(); // impl. later
        break;
      }
      case ACTION: {
        cells[cell_idx].set_null(); // impl. later
        break;
      }
      case PARSING_DB_ID: {
        cells[cell_idx].set_int(sql_stat_record->get_sql_stat_info().get_parsing_db_id());
        break;
      }
      case PARSING_DB_NAME: {
        cells[cell_idx].set_varchar(sql_stat_record->get_sql_stat_info().get_parsing_db_name(), 
               static_cast<ObString::obstr_size_t>(STRLEN(sql_stat_record->get_sql_stat_info().get_parsing_db_name())));
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case PARSING_USER_ID: {
        cells[cell_idx].set_int(sql_stat_record->get_sql_stat_info().get_parsing_user_id());
        break;
      }
      case EXECUTIONS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_executions_total());
        break;
      }
      case EXECUTIONS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_executions_delta());
        break;
      }
      case DISK_READS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_disk_reads_total());
        break;
      }
      case DISK_READS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_disk_reads_delta());
        break;
      }
      case BUFFER_GETS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_buffer_gets_total());
        break;
      }
      case BUFFER_GETS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_buffer_gets_delta());
        break;
      }
      case ELAPSED_TIME_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_elapsed_time_total());
        break;
      }
      case ELAPSED_TIME_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_elapsed_time_delta());
        break;
      }
      case CPU_TIME_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_cpu_time_total());
        break;
      }
      case CPU_TIME_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_cpu_time_delta());
        break;
      }
      case CCWAIT_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_ccwait_total());
        break;
      }
      case CCWAIT_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_ccwait_delta());
        break;
      }
      case USERIO_WAIT_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_userio_wait_total());
        break;
      }
      case USERIO_WAIT_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_userio_wait_delta());
        break;
      }
      case APWAIT_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_apwait_total());
        break;
      }
      case APWAIT_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_apwait_delta());
        break;
      }
      case PHYSICAL_READ_REQUESTS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_physical_read_requests_total());
        break;
      }
      case PHYSICAL_READ_REQUESTS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_physical_read_requests_delta());
        break;
      }
      case PHYSICAL_READ_BYTES_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_physical_read_bytes_total());
        break;
      }
      case PHYSICAL_READ_BYTES_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_physical_read_bytes_delta());
        break;
      }
      case WRITE_THROTTLE_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_write_throttle_total());
        break;
      }
      case WRITE_THROTTLE_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_write_throttle_delta());
        break;
      }
      case ROWS_PROCESSED_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_rows_processed_total());
        break;
      }
      case ROWS_PROCESSED_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_rows_processed_delta());
        break;
      }
      case MEMSTORE_READ_ROWS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_memstore_read_rows_total());
        break;
      }
      case MEMSTORE_READ_ROWS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_memstore_read_rows_delta());
        break;
      }
      case MINOR_SSSTORE_READ_ROWS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_minor_ssstore_read_rows_total());
        break;
      }
      case MINOR_SSSTORE_READ_ROWS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_minor_ssstore_read_rows_delta());
        break;
      }
      case MAJOR_SSSTORE_READ_ROWS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_major_ssstore_read_rows_total());
        break;
      }
      case MAJOR_SSSTORE_READ_ROWS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_major_ssstore_read_rows_delta());
        break;
      }
      case RPC_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_rpc_total());
        break;
      }
      case RPC_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_rpc_delta());
        break;
      }
      case FETCHES_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_fetches_total());
        break;
      }
      case FETCHES_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_fetches_delta());
        break;
      }
      case RETRY_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_retry_total());
        break;
      }
      case RETRY_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_retry_delta());
        break;
      }
      case PARTITION_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_partition_total());
        break;
      }
      case PARTITION_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_partition_delta());
        break;
      }
      case NESTED_SQL_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_nested_sql_total());
        break;
      }
      case NESTED_SQL_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_nested_sql_delta());
        break;
      }
      case SOURCE_IP: {
        if (sql_stat_record->get_key().source_addr_.is_valid()) {
          char ipbuf[common::OB_IP_STR_BUFF];
          ObString source_ip_str;
          const common::ObAddr &addr = sql_stat_record->get_key().source_addr_;
          if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
            SERVER_LOG(ERROR, "ip to string failed");
            ret = OB_ERR_UNEXPECTED;
          } else {
            source_ip_str = ObString::make_string(ipbuf);
            if (OB_FAIL(ob_write_string(*allocator_, source_ip_str, source_ip_str))) {
              LOG_WARN("failed to write string", K(ret));
            } else {
              cells[cell_idx].set_varchar(source_ip_str);
              cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          }
        } else {
          cells[cell_idx].set_varchar(ipstr_);
          cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        
        break;
      }
      case SOURCE_PORT: {
        if (sql_stat_record->get_key().source_addr_.is_valid()) {
          cells[cell_idx].set_int(sql_stat_record->get_key().source_addr_.get_port());
        } else {
          cells[cell_idx].set_int(port_);
        }
        break;
      }
      case ROUTE_MISS_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_route_miss_total());
        break;
      }
      case ROUTE_MISS_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_route_miss_delta());
        break;
      }
      case FIRST_LOAD_TIME: {
        int64_t first_load_timestamp = sql_stat_record->get_sql_stat_info().get_first_load_time();
        if (first_load_timestamp == 0) {
          cells[cell_idx].set_null();
        } else {
          cells[cell_idx].set_timestamp(first_load_timestamp);
        }
        break;
      }
      case PLAN_CACHE_HIT_TOTAL: {
        cells[cell_idx].set_int(sql_stat_record->get_plan_cache_hit_total());
        break;
      }
      case PLAN_CACHE_HIT_DELTA: {
        cells[cell_idx].set_int(sql_stat_record->get_plan_cache_hit_delta());
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column id", K(col_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualSqlStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}
void ObAllVirtualSqlStat::release_last_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc is null", K(ret));
  } else if (OB_ISNULL(last_sql_stat_record_)) {
    // is nullptr , do nothing
  } else {
    ipstr_.reset();
    last_sql_stat_record_->~ObExecutedSqlStatRecord();
    allocator_->free(last_sql_stat_record_);
    tmp_sql_stat_map_.destroy();
    sql_stat_cache_id_array_.reset();
    sql_stat_cache_id_array_idx_ = 0;
    first_enter_ = true;
  }
}


int ObAllVirtualSqlStat::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (first_enter_) {
    const int64_t default_bucket_num  = 64;
    if (OB_FAIL(get_server_ip_and_port())) {
        LOG_WARN("failed to get server ip and port", K(ret));
    } else if (OB_FAIL(tmp_sql_stat_map_.create(default_bucket_num, ObMemAttr(MTL_ID(), "TmpSqlStatMgr")))) {
      LOG_WARN("fail to create tmp sql stat map", K(ret));
    } else {
      if (OB_FAIL(load_next_batch_sql_stat())) {
        LOG_WARN("failed to get next batch sql stat", K(ret));
      } else {
        first_enter_ = false;
      }
    }
  }

  // clear last sql stat record
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc is null", K(ret));
    } else if (OB_ISNULL(last_sql_stat_record_)) {
      // is nullptr , do nothing
    } else {
      last_sql_stat_record_->~ObExecutedSqlStatRecord();
      allocator_->free(last_sql_stat_record_);
    }
  }

  if (OB_SUCC(ret)) {
    ObExecutedSqlStatRecord *sql_stat_record = nullptr;
    void *buf = nullptr;
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc is null", K(ret));
    } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObExecutedSqlStatRecord), ObMemAttr(MTL_ID(), "TmpSqlStat")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else if (FALSE_IT(sql_stat_record = new(buf) ObExecutedSqlStatRecord())) {
    } else {
      while (OB_SUCC(ret) && (!sql_stat_record->get_key().is_valid())) {
        if (OB_FAIL(get_next_sql_stat(*sql_stat_record))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next sql_stat_record", K(ret));
          }
        }
      } // end while
      

      if (OB_SUCC(ret)) {
        if (sql_stat_record->get_key().is_valid()) {
          if (OB_FAIL(fill_row(MTL_ID(), sql_stat_record, row))) {
            LOG_WARN("failed to get row from sql_stat_record", K(ret));
          } else {
            last_sql_stat_record_ = sql_stat_record;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get row from sql_stat_record", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObAllVirtualSqlStat::load_next_batch_sql_stat()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tmp_sql_stat_map_.clear())) {
    LOG_WARN("failed to clear tmp sql stat map", K(ret));
  } else if (FALSE_IT(sql_stat_cache_id_array_.reuse())) {
  } else {
    ObReqTimeGuard req_timeinfo_guard;
    ObPlanCache* plan_cache = MTL(ObPlanCache*);
    if (OB_NOT_NULL(plan_cache)) {
      ObGetAllSqlStatCacheIdOp op(&sql_stat_cache_id_array_);
      if (OB_FAIL(plan_cache->foreach_cache_obj(op))) {
        LOG_WARN("fail to get all sql stat cache id", K(ret));
      }
    } else {
      LOG_WARN("failed to get library cache", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(GCTX.session_mgr_)) {
        GCTX.session_mgr_->for_each_session(*this);
      }
    }
  }
  return ret;
}

bool ObAllVirtualSqlStat::operator()(sql::ObSQLSessionMgr::Key key, ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sess", K(ret));
  } else if (false == sess_info->is_valid()) {
    // do nothing
  } else if (ObSQLSessionState::QUERY_ACTIVE != sess_info->get_session_state()) {
    // do nothing
  } else if (sess_info->get_effective_tenant_id() == MTL_ID()) {
    // WARNNIGN!!!
    // Access to things like cur_sql_ctx_ and cur_plan is forbidden, 
    // these pointers are not guaranteed to be thread-safe and risk CORE!
    ObSqlStatRecordKey key;
    sess_info->get_cur_sql_id(key.sql_id_, sizeof(key.sql_id_));
    key.set_plan_hash(sess_info->get_current_plan_hash()); 

    ObExecutingSqlStatRecord &executing_sql_stat_record = sess_info->get_executing_sql_stat_record();
    ObExecutedSqlStatRecord *value = nullptr;
    SMART_VAR(common::ObDISessionCollect, session_collect) {
      if (!key.is_valid()) {
        // do nothing
      } else if (OB_FAIL(share::ObDiagnosticInfoUtil::get_the_diag_info(sess_info->get_server_sid(), session_collect))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("Fail to get tenant latch stat", KR(ret));
        }
      } else if (OB_FAIL(executing_sql_stat_record.record_sqlstat_end_value(&(session_collect.base_value_)))) {
        LOG_WARN("failed to record sqlstat end value in query virtual table", K(ret));
      } else if (OB_SUCC(tmp_sql_stat_map_.get_refactored(key, value))) {
        if (OB_FAIL(value->sum_stat_value(executing_sql_stat_record))) {
          LOG_WARN("sql_stat_value sum value failed", KR(ret));
        }
      } else if (OB_HASH_NOT_EXIST == ret) {
        void *buf = nullptr;
        ObString sql = ObString::make_empty_string();
        if (obmysql::COM_QUERY == sess_info->get_mysql_cmd() ||
            obmysql::COM_STMT_EXECUTE == sess_info->get_mysql_cmd() ||
            obmysql::COM_STMT_PREPARE == sess_info->get_mysql_cmd() ||
            obmysql::COM_STMT_PREXECUTE == sess_info->get_mysql_cmd()) {
          sql = sess_info->get_current_query_string();
        }

        if (OB_ISNULL(allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc is null", K(ret));
        } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObExecutedSqlStatRecord), ObMemAttr(MTL_ID(), "TmpSqlStatMgr")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc failed", K(ret));
        } else if (FALSE_IT(value = new(buf) ObExecutedSqlStatRecord())) {
        } else if (OB_FAIL(value->get_sql_stat_info().init(key, *sess_info, sql, nullptr /*phy_plan*/))) {
          LOG_WARN("failed to init sql stat info", K(ret));
        } else if (OB_FAIL(value->sum_stat_value(executing_sql_stat_record))) {
          LOG_WARN("sql_stat_value sum value failed", KR(ret));
        } else if (OB_FAIL(tmp_sql_stat_map_.set_refactored(key, value))) {
          LOG_WARN("tmp_sql_stat_map_ set refactored failed", KR(ret));
        }
      } else {
        LOG_WARN("get_refactored fail", KR(ret), K(key));
      }
    }
  }
  return true;
}

int ObAllVirtualSqlStat::get_next_sql_stat (sql::ObExecutedSqlStatRecord &sql_stat_value)
{
  int ret = OB_SUCCESS;
  if (sql_stat_cache_id_array_idx_ >= sql_stat_cache_id_array_.count()) {
    ret = OB_ITER_END;
  } else if (sql_stat_cache_id_array_idx_ < sql_stat_cache_id_array_.count()) {
    ObReqTimeGuard req_timeinfo_guard;
    ObPlanCache* plan_cache = MTL(ObPlanCache*);
    if (OB_NOT_NULL(plan_cache)) {
      uint64_t cur_sql_stat_cache_id = sql_stat_cache_id_array_.at(sql_stat_cache_id_array_idx_);
      sql_stat_cache_id_array_idx_++;
      ObCacheObjGuard guard(VT_SQL_STAT_HANDLE);
      int tmp_ret = plan_cache->ref_cache_obj(cur_sql_stat_cache_id, guard); //plan引用计数加VT_SQL_STAT_HANDLE
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        //do nothing;
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
      } else if (OB_ISNULL(guard.get_cache_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache object is NULL", K(ret));
      } else {
        bool is_succ_get_stat_value = false;
        const ObExecutedSqlStatRecord *tmp_sql_stat_value = nullptr;
        if (ObLibCacheNameSpace::NS_SQLSTAT == guard.get_cache_obj()->get_ns()) {
          ObSqlStatRecordObj *cache_obj = static_cast<ObSqlStatRecordObj *>(guard.get_cache_obj());
          if (OB_NOT_NULL(cache_obj) && OB_NOT_NULL(cache_obj->get_record_value()) && 
              cache_obj->get_record_value()->get_key().is_valid()) {
            tmp_sql_stat_value = cache_obj->get_record_value();
            is_succ_get_stat_value = true;
          } 
        } else if (ObLibCacheNameSpace::NS_CRSR == guard.get_cache_obj()->get_ns()) {
          ObPhysicalPlan *plan = static_cast<ObPhysicalPlan *>(guard.get_cache_obj());
          if (OB_NOT_NULL(plan) && plan->sql_stat_record_value_.get_key().is_valid()) {
            tmp_sql_stat_value = &(plan->sql_stat_record_value_);
            is_succ_get_stat_value = true;
          } 
        } 
        if (OB_SUCC(ret) && is_succ_get_stat_value) {
          if (OB_ISNULL(tmp_sql_stat_value)) {
            // continue, do nothing
          } else {
            ObExecutedSqlStatRecord *value = nullptr;
            if (OB_FAIL(sql_stat_value.assign(*(tmp_sql_stat_value)))) {
              LOG_WARN("failed to assign executed sql stat record", K(ret));
            } else {
              int tmp_ret = tmp_sql_stat_map_.get_refactored(sql_stat_value.get_key(), value);
              if (OB_HASH_NOT_EXIST == tmp_ret) {
                // do nothing
              } else if (OB_SUCCESS == tmp_ret) {
                if (OB_FAIL(sql_stat_value.sum_stat_value(*value))) {
                  LOG_WARN("sql_stat_value sum value failed", KR(ret));
                } else if (OB_FAIL(tmp_sql_stat_map_.erase_refactored(sql_stat_value.get_key()))) {
                  LOG_WARN("sql_stat_value earse value failed", KR(ret));
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to get from refactored", KR(ret), KR(tmp_ret));
              }
            }
          }
        }

      }
    }
  } else if (!tmp_sql_stat_map_.empty()) {
    if (OB_ISNULL(tmp_sql_stat_map_.begin()->second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql stat from map", K(ret));
    } else {
      const ObExecutedSqlStatRecord *tmp_sql_stat_value = tmp_sql_stat_map_.begin()->second;
      if (!tmp_sql_stat_value->get_key().is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql stat from map", K(ret));
      } else if (OB_FAIL(sql_stat_value.assign(*(tmp_sql_stat_value)))) {
        LOG_WARN("failed to assign executed sql stat record", K(ret));
      } else if (OB_FAIL(tmp_sql_stat_map_.erase_refactored(sql_stat_value.get_key()))) {
        LOG_WARN("sql_stat_value earse value failed", KR(ret));
      }
    }
  }
  return ret;
}

