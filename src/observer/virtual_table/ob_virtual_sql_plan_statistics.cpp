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

#include "observer/virtual_table/ob_virtual_sql_plan_statistics.h"
#include "common/object/ob_object.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_req_time_service.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "sql/plan_cache/ob_ps_cache.h"

using namespace oceanbase;
using namespace sql;
using namespace observer;
using namespace common;
namespace oceanbase
{
namespace observer
{
struct ObGetAllOperatorStatOp
{
  explicit ObGetAllOperatorStatOp(common::ObIArray<ObOperatorStat> *key_array)
    : key_array_(key_array)
  {
  }
  ObGetAllOperatorStatOp()
    : key_array_(NULL)
  {
  }
  void reset() { key_array_ = NULL; }
  int set_key_array(common::ObIArray<ObOperatorStat> *key_array)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == key_array) {
      ret = common::OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid argument", K(ret));
    } else {
      key_array_ = key_array;
    }
    return ret;
  }
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == key_array_) {
      ret = common::OB_NOT_INIT;
      SERVER_LOG(WARN, "invalid argument", K(ret));
    } else {
      ObOperatorStat stat;
      ObPhysicalPlan *plan = NULL;
      if (ObLibCacheNameSpace::NS_CRSR == entry.second->get_ns()) {
        if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan *>(entry.second))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected null plan", K(ret), K(plan));
        }
        for (int64_t i = 0; i < plan->op_stats_.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(plan->op_stats_.get_op_stat_accumulation(plan,
                                                               i, stat))) {
            SERVER_LOG(WARN, "fail to get op stat accumulation", K(ret), K(i));
          } else if (OB_FAIL(key_array_->push_back(stat))) {
            SERVER_LOG(WARN, "fail to push back plan_id", K(ret));
          }
        } // for end
      }
    }
    return ret;
  }

  common::ObIArray<ObOperatorStat> *key_array_;
};

ObVirtualSqlPlanStatistics::ObVirtualSqlPlanStatistics() :
    tenant_id_array_(),
    operator_stat_array_(),
    tenant_id_(0),
    tenant_id_array_idx_(0),
    operator_stat_array_idx_(OB_INVALID_ID)
{
}

ObVirtualSqlPlanStatistics::~ObVirtualSqlPlanStatistics()
{
  reset();
}

void ObVirtualSqlPlanStatistics::reset()
{
  operator_stat_array_.reset();
  tenant_id_array_.reset();
}

int ObVirtualSqlPlanStatistics::inner_open()
{
  int ret = OB_SUCCESS;
  uint64_t start_tenant_id = 0;
  uint64_t end_tenant_id = 0;
  if (key_ranges_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid key range", K(ret), K(key_ranges_.count()));
  } else {
    ObNewRange &range = key_ranges_.at(0);
    if (OB_UNLIKELY(range.get_start_key().get_obj_cnt() != 5
                    || range.get_end_key().get_obj_cnt() != 5)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected  # of rowkey columns",
                 K(ret),
                 "size of start key", range.get_start_key().get_obj_cnt(),
                 "size of end key", range.get_end_key().get_obj_cnt());
    } else {
      ObObj tenant_id_low = range.get_start_key().get_obj_ptr()[0];
      ObObj tenant_id_high = range.get_end_key().get_obj_ptr()[0];
      if (tenant_id_low.is_min_value()
          && tenant_id_high.is_max_value()) {
        start_tenant_id = OB_SYS_TENANT_ID;
        end_tenant_id = OB_SYS_TENANT_ID;
      } else if (tenant_id_low.get_type() != ObIntType
          || tenant_id_high.get_type() != ObIntType) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id_low), K(tenant_id_high));
      } else {
        start_tenant_id = tenant_id_low.get_int();
        end_tenant_id = tenant_id_high.get_int();
        if (start_tenant_id != end_tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid tenant id range, can only search one tenant",
                     K(ret), K(start_tenant_id), K(end_tenant_id));
        } else if (OB_SYS_TENANT_ID == start_tenant_id) {
          //查询租户为系统租户，可以查询所有的plan cache
          if (OB_FAIL(get_all_tenant_id())) {
            SERVER_LOG(WARN, "fail to get all tenant id", K(ret));
          }
        } else if (OB_FAIL(tenant_id_array_.push_back(start_tenant_id))) {
          SERVER_LOG(WARN, "fail to push back tenent id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVirtualSqlPlanStatistics::get_all_tenant_id()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
    SERVER_LOG(WARN, "failed to add tenant id", K(ret));
  }
  return ret;
}

int ObVirtualSqlPlanStatistics::get_row_from_specified_tenant(uint64_t tenant_id, bool &is_end)
{
  int ret = OB_SUCCESS;
  // !!! 引用plan cache资源之前必须加ObReqTimeGuard
  ObReqTimeGuard req_timeinfo_guard;
  is_end = false;
  sql::ObPlanCache *plan_cache = NULL;
  if (OB_INVALID_ID == static_cast<uint64_t>(operator_stat_array_idx_)) {
    plan_cache = MTL(ObPlanCache*);
    ObGetAllOperatorStatOp operator_stat_op(&operator_stat_array_);
    if (OB_FAIL(plan_cache->foreach_cache_obj(operator_stat_op))) {
      SERVER_LOG(WARN, "fail to traverse id2stat_map");
    } else {
      operator_stat_array_idx_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    if (operator_stat_array_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid operator_stat_array index", K(operator_stat_array_idx_));
    } else if (operator_stat_array_idx_ >= operator_stat_array_.count()) {
      is_end = true;
      operator_stat_array_idx_ = OB_INVALID_ID;
      operator_stat_array_.reset();
    } else {
      is_end = false;
      ObOperatorStat &opstat = operator_stat_array_.at(operator_stat_array_idx_);
      ++operator_stat_array_idx_;
      if (OB_FAIL(fill_cells(opstat))) {
        SERVER_LOG(WARN, "fail to fill cells", K(opstat), K(tenant_id));
      }
    }
  }
  SERVER_LOG(DEBUG,
             "add plan from a tenant",
             K(ret),
             K(tenant_id));
  return ret;
}

int ObVirtualSqlPlanStatistics::fill_cells(const ObOperatorStat &pstat)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObString ipstr;
  for (int64_t i =  0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
      //tenant id
      case TENANT_ID: {
        cells[i].set_int(tenant_id_array_.at(tenant_id_array_idx_));
        break;
      }
      //ip
      case SVR_IP: {
        // ip
        ipstr.reset();
        if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
          SERVER_LOG(ERROR, "get server ip failed", K(ret));
        } else {
          cells[i].set_varchar(ipstr);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      //port
      case SVR_PORT: {
        // svr_port
        cells[i].set_int(GCTX.self_addr().get_port());
        break;
      }
      //plan id
      case PLAN_ID: {
        cells[i].set_int(pstat.plan_id_);
        break;
      }
      case OPERATION_ID: {
        cells[i].set_int(pstat.operation_id_);
        break;
      }
      case EXECUTIONS: {
        cells[i].set_int(pstat.execute_times_);
        break;
      }
      case OUTPUT_ROWS: {
        cells[i].set_int(pstat.output_rows_);
        break;
      }

      case INPUT_ROWS: {
        cells[i].set_int(pstat.input_rows_);
        break;
      }

      case RESCAN_TIMES: {
        cells[i].set_int(pstat.rescan_times_);
        break;
      }
      case BUFFER_GETS: {
        cells[i].set_int(0);
        break;
      }
      case DISK_READS: {
        cells[i].set_int(0);
        break;
      }
      case DISK_WRITES: {
        cells[i].set_int(0);
        break;
      }
      case ELAPSED_TIME: {
        cells[i].set_int(0);
        break;
      }
      case EXTEND_INFO1: {
        cells[i].set_null();
        break;
      }
      case EXTEND_INFO2: {
        cells[i].set_null();
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
  } // end for
  return ret;
}

int ObVirtualSqlPlanStatistics::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_sub_end = false;
  do {
    is_sub_end = false;
    if (tenant_id_array_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid tenant_id_array idx", K(ret), K(tenant_id_array_idx_));
    } else if (tenant_id_array_idx_ >= tenant_id_array_.count()) {
      ret = OB_ITER_END;
      tenant_id_array_idx_ = 0;
    } else {
      uint64_t tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
      MTL_SWITCH(tenant_id) {
        if (OB_FAIL(get_row_from_specified_tenant(tenant_id,
                                                  is_sub_end))) {
          SERVER_LOG(WARN,
                     "fail to insert plan by tenant id",
                     K(ret),
                     "tenant id",
                     tenant_id_array_.at(tenant_id_array_idx_),
                     K(tenant_id_array_idx_));
        } else {
          if (is_sub_end) {
            ++tenant_id_array_idx_;
          }
        }
      }
    }
  } while(is_sub_end && OB_SUCCESS == ret);
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
} //end namespace observer
} //end namespace oceanbase

