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

#include "observer/virtual_table/ob_all_plan_cache_stat.h"


#include "common/object/ob_object.h"

#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_callback.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace observer
{
ObAllPlanCacheBase::ObAllPlanCacheBase()
    : ObVirtualTableIterator(),
      tenant_id_array_(),
      tenant_id_array_idx_(0)
{

}

ObAllPlanCacheBase::~ObAllPlanCacheBase()
{
  reset();
}

void ObAllPlanCacheBase::reset()
{
  tenant_id_array_.reset();
  tenant_id_array_idx_ = 0;
}

int ObAllPlanCacheBase::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllPlanCacheStat::fill_cells(ObPlanCache &plan_cache)
{
#define SET_REF_HANDLE_COL(handle)                      \
  int64_t ref_idx = handle;                             \
  int64_t ref_cnt = ref_handle_mgr.get_ref_cnt(handle); \
  cells[i].set_int(ref_cnt);

  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  const ObPlanCacheStat &pc_stat = plan_cache.get_plan_cache_stat();
  ObString ipstr;
  const ObCacheRefHandleMgr &ref_handle_mgr = plan_cache.get_ref_handle_mgr();
  for (int64_t i =  0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
      //tenant id
    case TENANT_ID: {
      cells[i].set_int(plan_cache.get_tenant_id());
      break;
    }
      //svr_ip
    case SVR_IP: {
      ipstr.reset();
      if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
        SERVER_LOG(ERROR, "get server ip failed", K(ret));
      } else {
        cells[i].set_varchar(ipstr);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    }
      // svr_port
    case SVR_PORT: {
      cells[i].set_int(GCTX.self_addr().get_port());
      break;
    }
      //sql_num
    case SQL_NUM: {
      cells[i].set_int(plan_cache.get_cache_obj_size());
      break;
    }
      //mem_used
    case MEM_USED: {
      cells[i].set_int(plan_cache.get_mem_used());
      break;
    }
    case MEM_HOLD: {
      cells[i].set_int(plan_cache.get_mem_hold());
      break;
    }
    case ACCESS_COUNT: {
      cells[i].set_int(pc_stat.access_count_);
      break;
    }
    case HIT_COUNT: {
      cells[i].set_int(pc_stat.hit_count_);
      break;
    }
    //hit_rate
    case HIT_RATE: {
      if (pc_stat.access_count_ !=0) {
        cells[i].set_int(pc_stat.hit_count_*100/pc_stat.access_count_);
        SERVER_LOG(DEBUG, "rate:", "hit_count", pc_stat.hit_count_, "access_count", pc_stat.access_count_);
      } else {
        cells[i].set_int(0);
      }
      break;
    }
    //plan_num
    case PLAN_NUM: {//id->plan_stat map size;
      cells[i].set_int(plan_cache.get_cache_obj_size());
      break;
    }
      //mem_limit
    case MEM_LIMIT: {
      cells[i].set_int(plan_cache.get_mem_limit());
      break;
    }
      //hash_bucket
    case HASH_BUCKET: {
      cells[i].set_int(plan_cache.get_bucket_num());
      break;
    }
      //stmtkey_num, not used
    case STMTKEY_NUM: {
      cells[i].set_int(0);
      break;
    }
    case PC_REF_PLAN_LOCAL: {
      SET_REF_HANDLE_COL(PC_REF_PLAN_LOCAL_HANDLE);
      break;
    }
    case PC_REF_PLAN_REMOTE: {
      SET_REF_HANDLE_COL(PC_REF_PLAN_REMOTE_HANDLE);
      break;
    }
    case PC_REF_PLAN_DIST: {
      SET_REF_HANDLE_COL(PC_REF_PLAN_DIST_HANDLE);
      break;
    }
    case PC_REF_PLAN_ARR: {
      SET_REF_HANDLE_COL(PC_REF_PLAN_ARR_HANDLE);
      break;
    }
    case PC_REF_PLAN_STAT: {
      SET_REF_HANDLE_COL(PC_REF_PLAN_STAT_HANDLE);
      break;
    }
    case PC_REF_PL: {
      SET_REF_HANDLE_COL(PC_REF_PL_HANDLE);
      break;
    }
    case PC_REF_PL_STAT: {
      SET_REF_HANDLE_COL(PC_REF_PL_STAT_HANDLE);
      break;
    }
    case PLAN_GEN: {
      SET_REF_HANDLE_COL(PLAN_GEN_HANDLE);
      break;
    }
    case CLI_QUERY: {
      SET_REF_HANDLE_COL(CLI_QUERY_HANDLE);
      break;
    }
    case OUTLINE_EXEC: {
      SET_REF_HANDLE_COL(OUTLINE_EXEC_HANDLE);
      break;
    }
    case PLAN_EXPLAIN: {
      SET_REF_HANDLE_COL(PLAN_EXPLAIN_HANDLE);
      break;
    }
    case ASYN_BASELINE: {
      SET_REF_HANDLE_COL(CHECK_EVOLUTION_PLAN_HANDLE);
      break;
    }
    case LOAD_BASELINE: {
      SET_REF_HANDLE_COL(LOAD_BASELINE_HANDLE);
      break;
    }
    case PS_EXEC: {
      SET_REF_HANDLE_COL(PS_EXEC_HANDLE);
      break;
    }
    case GV_SQL: {
      SET_REF_HANDLE_COL(GV_SQL_HANDLE);
      break;
    }
    case PL_ANON: {
      SET_REF_HANDLE_COL(PL_ANON_HANDLE);
      break;
    }
    case PL_ROUTINE: {
      SET_REF_HANDLE_COL(PL_ROUTINE_HANDLE);
      break;
    }
    case PACKAGE_VAR: {
      SET_REF_HANDLE_COL(PACKAGE_VAR_HANDLE);
      break;
    }
    case PACKAGE_TYPE: {
      SET_REF_HANDLE_COL(PACKAGE_TYPE_HANDLE);
      break;
    }
    case PACKAGE_SPEC: {
      SET_REF_HANDLE_COL(PACKAGE_SPEC_HANDLE);
      break;
    }
    case PACKAGE_BODY: {
      SET_REF_HANDLE_COL(PACKAGE_BODY_HANDLE);
      break;
    }
    case PACKAGE_RESV: {
      SET_REF_HANDLE_COL(PACKAGE_RESV_HANDLE);
      break;
    }
    case GET_PKG: {
      SET_REF_HANDLE_COL(GET_PKG_HANDLE);
      break;
    }
    case INDEX_BUILDER: {
      SET_REF_HANDLE_COL(INDEX_BUILDER_HANDLE);
      break;
    }
    case PCV_SET: {
      SET_REF_HANDLE_COL(PCV_SET_HANDLE);
      break;
    }
    case PCV_RD: {
      SET_REF_HANDLE_COL(PCV_RD_HANDLE);
      break;
    }
    case PCV_WR: {
      SET_REF_HANDLE_COL(PCV_WR_HANDLE);
      break;
    }
    case PCV_GET_PLAN_KEY: {
      SET_REF_HANDLE_COL(PCV_GET_PLAN_KEY_HANDLE);
      break;
    }
    case PCV_GET_PL_KEY: {
      SET_REF_HANDLE_COL(PCV_GET_PL_KEY_HANDLE);
      break;
    }
    case PCV_EXPIRE_BY_USED: {
      SET_REF_HANDLE_COL(PCV_EXPIRE_BY_USED_HANDLE);
      break;
    }
    case PCV_EXPIRE_BY_MEM: {
      SET_REF_HANDLE_COL(PCV_EXPIRE_BY_MEM_HANDLE);
      break;
    }
    case LC_REF_CACHE_NODE: {
      SET_REF_HANDLE_COL(LC_REF_CACHE_NODE_HANDLE);
      break;
    }
    case LC_NODE: {
      SET_REF_HANDLE_COL(LC_NODE_HANDLE);
      break;
    }
    case LC_NODE_RD: {
      SET_REF_HANDLE_COL(LC_NODE_RD_HANDLE);
      break;
    }
    case LC_NODE_WR: {
      SET_REF_HANDLE_COL(LC_NODE_WR_HANDLE);
      break;
    }
    case LC_REF_CACHE_OBJ_STAT: {
      SET_REF_HANDLE_COL(LC_REF_CACHE_OBJ_STAT_HANDLE);
      break;
    }
    case PLAN_BASELINE: {
       SET_REF_HANDLE_COL(PLAN_BASELINE_HANDLE);
       break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN,
                 "invalid column id",
                 K(ret),
                 K(i),
                 K(output_column_ids_),
                 K(col_id));
      break;
    }
    }
  }
  return ret;
#undef SET_REF_HANDLE_COL
}

int ObAllPlanCacheStatI1::get_all_tenant_ids(ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_tenant_ids(key_ranges_))) {
    LOG_WARN("set tenant ids failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids_.count(); i++) {
    // to keep the save interface
    if (common::OB_INVALID_TENANT_ID == tenant_ids_.at(i)
        || is_virtual_tenant_id(tenant_ids_.at(i))
        || (!is_sys_tenant(effective_tenant_id_) && tenant_ids_.at(i) != effective_tenant_id_)) {
      // skip
    } else {
      ret = tenant_ids.push_back(tenant_ids_.at(i));
    }
  }
  return ret;
}

int ObAllPlanCacheStat::get_all_tenant_ids(ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  // sys tenant show all tenant plan cache stat
  if (common::OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid effective_tenant_id_", KR(ret), K(effective_tenant_id_));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
      SERVER_LOG(WARN, "failed to add tenant id", K(ret));
    }
  } else {
    tenant_ids.reset();
    // user tenant show self tenant stat
    if (OB_FAIL(tenant_ids.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "fail to push back effective_tenant_id_", KR(ret), K(effective_tenant_id_),
          K(tenant_ids));
    }
  }
  return ret;
}

int ObAllPlanCacheStat::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_all_tenant_ids(tenant_id_array_))) {
    SERVER_LOG(WARN, "fail get all tenant ids", K(ret));
  }
  return ret;
}
int ObAllPlanCacheStat::get_row_from_tenants()
{
  int ret = OB_SUCCESS;
  if (tenant_id_array_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid tenant_id_array index", K(ret), K(tenant_id_array_idx_));
  } else if (tenant_id_array_idx_ >= tenant_id_array_.count()) {
    ret = OB_ITER_END;
  } else {
    uint64_t tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
    MTL_SWITCH(tenant_id) {
      ObPlanCache *plan_cache = MTL(ObPlanCache*);
      if (OB_FAIL(fill_cells(*plan_cache))) {
        SERVER_LOG(WARN, "fail to fill cells", K(ret), K(cur_row_));
      } else {
        SERVER_LOG(DEBUG, "add plan cache", K(tenant_id));
      }
      ++tenant_id_array_idx_;
    }
  }
  return ret;
}

int ObAllPlanCacheStatI1::set_tenant_ids(const common::ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    int64_t tenant_id = -1;
    start_key.reset();
    end_key.reset();
    start_key = ranges.at(i).start_key_;
    end_key = ranges.at(i).end_key_;
    if (!(start_key.get_obj_cnt() > 0)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "assert start_key.get_obj_cnt() > 0", K(ret));
    } else if (!(start_key.get_obj_cnt() == end_key.get_obj_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "assert start_key.get_obj_cnt() == end_key.get_obj_cnt()", K(ret));
    }
    const ObObj *start_key_obj_ptr = NULL;
    const ObObj *end_key_obj_ptr = NULL;
    if (OB_SUCC(ret)) {
      start_key_obj_ptr = start_key.get_obj_ptr();
      end_key_obj_ptr = end_key.get_obj_ptr();
      if (OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(WARN, "invalid args", KP(start_key_obj_ptr), KP(end_key_obj_ptr));
      } else if ((!start_key_obj_ptr[0].is_min_value() || !end_key_obj_ptr[0].is_max_value())
          && start_key_obj_ptr[0] != end_key_obj_ptr[0]) {
        ret = OB_NOT_IMPLEMENT;
        SERVER_LOG(WARN, "tenant id exact value", K(ret));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]) {
        if (!(ObIntType == start_key_obj_ptr[0].get_type())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "assert ObIntType == start_key_obj_ptr[0].get_type()", K(ret));
        } else if (!(start_key_obj_ptr[0].get_type() == end_key_obj_ptr[0].get_type())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN,
                     "assert start_key_obj_ptr[0].get_type() == end_key_obj_ptr[0].get_type()",
                     K(ret));
        } else {
          tenant_id = start_key_obj_ptr[0].get_int();
          if (!(tenant_id >= 0)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "assert tenant_id >= 0", K(ret));
          } else if (OB_FAIL(add_var_to_array_no_dup(tenant_ids_,
                                                     static_cast<uint64_t>(tenant_id)))) {
            SERVER_LOG(WARN, "Failed to add tenant_id to array no duplicate", K(ret));
          } else { }//do nothing
        }
      }
    }
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
