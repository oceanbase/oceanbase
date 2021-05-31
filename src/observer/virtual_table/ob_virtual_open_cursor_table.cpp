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

#include "ob_virtual_open_cursor_table.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/oblog/ob_log_module.h"
#include "common/object/ob_object.h"
#include "observer/ob_req_time_service.h"
#include "observer/ob_server.h"
#include "lib/allocator/ob_mod_define.h"

#include "sql/plan_cache/ob_plan_cache_manager.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_callback.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace oceanbase {
namespace observer {
void ObVirtualOpenCursorTable::ObEachSessionId::reset()
{
  is_success_ = false;
  sids_map_.destroy();
}

int ObVirtualOpenCursorTable::ObEachSessionId::init(int64_t item_cnt, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  OZ(sids_map_.create(item_cnt * 4 / 3, ObModIds::OB_HASH_BUCKET));
  OX(allocator_ = allocator);
  return ret;
}

bool ObVirtualOpenCursorTable::ObEachSessionId::operator()(
    sql::ObSQLSessionMgr::Key key, sql::ObSQLSessionInfo* sess_info)
{
  UNUSED(key);
  SessionInfo vsp;
  is_success_ = true;
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(sess_info));
  CK(OB_NOT_NULL(allocator_));
  if (OB_SUCC(ret)) {
    sql::ObSQLSessionInfo::LockGuard lock_guard(sess_info->get_thread_data_lock());
    if (OB_NOT_NULL(sess_info->get_cur_phy_plan())) {
      vsp.version = sess_info->get_version();
      vsp.id = sess_info->get_sessid();
      vsp.addr = reinterpret_cast<uint64_t>(sess_info);
      OZ(ob_write_string(*allocator_, sess_info->get_user_name(), vsp.user_name));
      OZ(ob_write_string(*allocator_, sess_info->get_cur_phy_plan()->stat_.sql_id_, vsp.sql_id));
      if (vsp.is_valid()) {
        SessionInfoArray vsp_arr;
        OZ(sids_map_.get_refactored(vsp.sql_id, vsp_arr));

        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          OZ(vsp_arr.push_back(vsp));
          OZ(sids_map_.set_refactored(vsp.sql_id, vsp_arr));
        } else if (OB_SUCCESS != ret) {
          // do nothing
        } else {
          OZ(vsp_arr.push_back(vsp));
          OZ(sids_map_.set_refactored(vsp.sql_id, vsp_arr, 1));
        }
      }
    }
  }
  return is_success_ && (OB_SUCCESS == ret);
}

ObVirtualOpenCursorTable::ObVirtualOpenCursorTable()
    : ObVirtualTableScannerIterator(),
      sess_mgr_(NULL),
      pcm_(NULL),
      plan_id_array_idx_(0),
      plan_cache_(NULL),
      sess_arr_(NULL),
      sess_id_array_idx_(0),
      oesid_(allocator_),
      tenant_id_(OB_INVALID_ID),
      port_(0),
      is_travs_sess_(false),
      cache_obj_(NULL)
{}

ObVirtualOpenCursorTable::~ObVirtualOpenCursorTable()
{
  reset();
}

int ObVirtualOpenCursorTable::set_addr(const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString ipstr = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}

void ObVirtualOpenCursorTable::reset()
{
  sess_mgr_ = NULL;
  pcm_ = NULL;
  plan_id_array_idx_ = 0;
  plan_cache_ = NULL;
  sess_id_array_idx_ = 0;
  sess_arr_ = NULL;
  oesid_.reset();
  tenant_id_ = OB_INVALID_ID;
  ipstr_.reset();
  port_ = 0;
  is_travs_sess_ = false;
  ObVirtualTableScannerIterator::reset();
}

int ObVirtualOpenCursorTable::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t sess_cnt = 0;
  CK(OB_NOT_NULL(allocator_));
  OZ(sess_mgr_->get_session_count(sess_cnt));
  OZ(oesid_.init(sess_cnt, allocator_));
  OZ(sess_mgr_->for_each_session(oesid_));
  if (OB_SUCC(ret)) {
    // !!! Must have ObReqTimeGuard before any plan cache references
    ObReqTimeGuard req_timeinfo_guard;
    if (OB_UNLIKELY(NULL == pcm_)) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "pcm_ is NULL", K(ret));
    } else if (OB_UNLIKELY(NULL != plan_cache_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "before get_plan_cache, the point of plan_cache must be NULL", K(ret));
    } else if (OB_UNLIKELY(NULL == (plan_cache_ = pcm_->get_plan_cache(tenant_id_)))) {
      SERVER_LOG(WARN, "plan cache is null", K(ret));
    } else {
      ObPlanCache::PlanStatMap& plan_stats = plan_cache_->get_plan_stat_map();
      ObGetAllPlanIdOp plan_id_op(&plan_id_array_);
      if (OB_FAIL(plan_stats.foreach_refactored(plan_id_op))) {
        SERVER_LOG(WARN, "fail to traverse id2stat_map");
      } else {
        plan_id_array_idx_ = 0;
      }
    }
  }
  return ret;
}

int ObVirtualOpenCursorTable::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  if (OB_ISNULL(sess_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "session mgr is null", K(ret));
  } else {
    if (NULL == plan_cache_) {
      ret = OB_ITER_END;
      is_end = true;
    } else if (OB_SUCC(ret)) {
      if (is_travs_sess_) {
        bool is_filled = false;
        if (OB_FAIL(fill_cells(row, is_filled))) {
          SERVER_LOG(ERROR, "failed to fill cells", K(ret));
        }
      } else {
        bool is_filled = false;
        while (OB_SUCC(ret) && false == is_filled && false == is_end) {
          if (plan_id_array_idx_ < 0) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid plan_stat_array index", K(plan_id_array_idx_));
          } else if (plan_id_array_idx_ >= plan_id_array_.count()) {
            is_end = true;
            plan_id_array_idx_ = OB_INVALID_ID;
            plan_id_array_.reset();
            ret = OB_ITER_END;
            if (OB_UNLIKELY(NULL == plan_cache_)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "plan cache is null", K(ret));
            } else {
              plan_cache_->dec_ref_count();
              plan_cache_ = NULL;
            }
          } else {
            is_end = false;
            uint64_t plan_id = plan_id_array_.at(plan_id_array_idx_);
            ObCacheObject* cache_obj = NULL;
            ++plan_id_array_idx_;
            int tmp_ret = plan_cache_->ref_cache_obj(plan_id, GV_SQL_HANDLE, cache_obj_);  // increment plan ref cnt
            // if the plan has been evicted, continue to the next one
            if (OB_HASH_NOT_EXIST == tmp_ret) {
              // do nothing;
            } else if (OB_SUCCESS != tmp_ret) {
              ret = tmp_ret;
            } else if (!cache_obj_->is_sql_crsr()) {
              // do nothing;
            } else if (OB_FAIL(fill_cells(row, is_filled))) {  // plan exist
              SERVER_LOG(WARN, "fail to fill cells", K(cache_obj_), K(tenant_id_));
            } else {
            }
          }
        }  // while end
      }
    }
  }
  return ret;
}

int ObVirtualOpenCursorTable::fill_cells(ObNewRow*& row, bool& is_filled)
{
  int ret = OB_SUCCESS;
  const ObPhysicalPlan* plan = NULL;
  is_filled = false;
  if (!cache_obj_->is_sql_crsr()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "not a sql", K(ret));
  } else if (OB_ISNULL(plan = dynamic_cast<const ObPhysicalPlan*>(cache_obj_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null plan", K(ret), K(plan));
  } else if (OB_ISNULL(sess_arr_ = oesid_.get_sid_maps().get(plan->get_sql_id()))) {
    is_filled = false;
  } else {
    SERVER_LOG(WARN, "sid array count", K(sess_arr_->count()));
    if (sess_id_array_idx_ < sess_arr_->count()) {
      OZ(fill_cells_impl(sess_arr_->at(sess_id_array_idx_), plan));
      OX(sess_id_array_idx_++);
      OX(row = &cur_row_);
      is_filled = true;
    }

#define RESET_TRAVS_STATUS() \
  do {                       \
    is_travs_sess_ = false;  \
    sess_id_array_idx_ = 0;  \
  } while (0)

    // we failed, but we should release the reference to the plan cache immediately
    if (OB_FAIL(ret)) {
      RESET_TRAVS_STATUS();
    } else {
      if (sess_id_array_idx_ == sess_arr_->count()) {
        RESET_TRAVS_STATUS();
      } else {
        is_travs_sess_ = true;
      }
    }
    // for(int i = 0; OB_SUCC(ret) && i < sess_arr_->count(); ++i) {
    //   OZ (fill_cells_impl(sess_arr_->at(i), plan, plan_cache));
    // }
  }
  return ret;
}

int ObVirtualOpenCursorTable::fill_cells_impl(const SessionInfo& sess_info, const ObPhysicalPlan* plan)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  const int64_t col_count = output_column_ids_.count();
  ObCharsetType default_charset = ObCharset::get_default_charset();
  ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
  if (OB_UNLIKELY(NULL == cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID: {
          cells[i].set_int(tenant_id_);
          break;
        }
        case SVR_IP: {
          cells[i].set_varchar(ipstr_);
          cells[i].set_collation_type(default_collation);
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(port_);
          break;
        }
        case SADDR: {
          ObSqlString addr;
          ObString tmp_saddr;
          addr.append_fmt("%lx", sess_info.addr);
          OZ(ob_write_string(*allocator_, addr.string(), tmp_saddr));
          // get last 8 char, for oracle compatiable
          int64_t offset = tmp_saddr.length() > 8 ? tmp_saddr.length() - 8 : 0;
          ObString saddr(8, tmp_saddr.ptr() + offset);
          cells[i].set_varchar(saddr);
          cells[i].set_collation_type(default_collation);
          break;
        }
        case SID: {
          cells[i].set_int(sess_info.id);
          break;
        }
        case USER_NAME: {
          cells[i].set_varchar(sess_info.user_name);
          cells[i].set_collation_type(default_collation);
          break;
        }
        case ADDRESS: {  // memory address of the sql in plan cache
          // ObSqlString saddr;
          // saddr.append_fmt("%lx", reinterpret_cast<uint64_t>(plan));
          // ObString addr;
          // OZ (ob_write_string(*allocator_, saddr.string(), addr));
          // cells[i].set_raw(addr);
          cells[i].set_null();
          break;
        }
        case HASH_VALUE: {
          // cells[i].set_int(sess_info.id);
          cells[i].set_null();
          break;
        }
        case SQL_ID: {
          cells[i].set_varchar(sess_info.sql_id);
          cells[i].set_collation_type(default_collation);
          break;
        }
        case SQL_TEXT: {
          cells[i].set_varchar(ObString(60, plan->stat_.raw_sql_.ptr()));
          cells[i].set_collation_type(default_collation);
          break;
        }
        case LAST_SQL_ACTIVE_TIME: {
          cells[i].set_datetime(plan->stat_.last_active_time_);
          break;
        }
        case SQL_EXEC_ID: {
          cells[i].set_null();
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
