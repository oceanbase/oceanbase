/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "sql/plan_cache/ob_cache_object_factory.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/monitor/ob_plan_info_manager.h"
#include "observer/ob_req_time_service.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_utils.h"
#include "ob_all_virtual_sql_plan.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "sql/ob_sql.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
namespace oceanbase {
namespace observer {

ObAllVirtualSqlPlan::PlanInfo::PlanInfo()
{
  reset();
}

ObAllVirtualSqlPlan::PlanInfo::~PlanInfo()
{

}

void ObAllVirtualSqlPlan::PlanInfo::reset()
{
  plan_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_ID;
}

ObAllVirtualSqlPlan::DumpAllPlan::DumpAllPlan()
{
  reset();
}

ObAllVirtualSqlPlan::DumpAllPlan::~DumpAllPlan()
{

}

void ObAllVirtualSqlPlan::DumpAllPlan::reset()
{
  plan_ids_ = NULL;
  tenant_id_ = OB_INVALID_ID;
}

int ObAllVirtualSqlPlan::DumpAllPlan::operator()(
    common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
{
  int ret = common::OB_SUCCESS;
  ObPhysicalPlan *plan = NULL;
  if (OB_ISNULL(entry.second) || OB_ISNULL(plan_ids_)) {
    ret = common::OB_NOT_INIT;
    SERVER_LOG(WARN, "invalid argument", K(ret));
  } else if (ObLibCacheNameSpace::NS_CRSR != entry.second->get_ns()) {
    // not sql plan
    // do nothing
  } else if (OB_ISNULL(plan = dynamic_cast<ObPhysicalPlan *>(entry.second))) {
    //do nothing
  } else if (NULL != plan->get_logical_plan().logical_plan_) {
    PlanInfo info;
    info.tenant_id_ = tenant_id_;
    info.plan_id_ = plan->get_plan_id();
    if (OB_FAIL(plan_ids_->push_back(info))) {
      SERVER_LOG(WARN, "failed to push back plan id", K(ret));
    }
  }
  return ret;
}

ObAllVirtualSqlPlan::ObAllVirtualSqlPlan() :
    ObVirtualTableScannerIterator(),
    plan_ids_(),
    plan_idx_(0),
    plan_items_(),
    plan_item_idx_(0),
    sql_id_(),
    db_id_(0),
    plan_hash_(0),
    gmt_create_(0),
    tenant_id_(0),
    plan_id_(0)
{
}

ObAllVirtualSqlPlan::~ObAllVirtualSqlPlan() {
  reset();
}

void ObAllVirtualSqlPlan::reset()
{
  ObVirtualTableScannerIterator::reset();
  plan_idx_ = 0;
  plan_items_.reuse();
  plan_item_idx_ = 0;
}

int ObAllVirtualSqlPlan::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(extract_tenant_and_plan_id(key_ranges_))) {
    SERVER_LOG(WARN, "set tenant id and plan id failed", K(ret));
  }
  return ret;
}

int ObAllVirtualSqlPlan::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && plan_item_idx_ >= plan_items_.count()) {
    if (plan_idx_ >= plan_ids_.count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(prepare_next_plan())) {
      SERVER_LOG(WARN, "failed to prepare next plan", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fill_cells(plan_items_.at(plan_item_idx_++)))) {
    SERVER_LOG(WARN, "failed to fill cell", K(ret));
  } else {
    //finish fetch one row
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualSqlPlan::fill_cells(ObSqlPlanItem *plan_item)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObString ipstr;
  common::ObAddr addr;
  #define REFINE_LENGTH(len) ((len) > MAX_LENGTH ? MAX_LENGTH : (len))
  if (OB_ISNULL(cells) || OB_ISNULL(plan_item)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(cells));
  }
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
    case TENANT_ID: {
      cells[cell_idx].set_int(tenant_id_);
      break;
    }
    case PLAN_ID: {
      cells[cell_idx].set_int(plan_id_);
      break;
    }
    case SVR_IP: {
      ipstr.reset();
      if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
        SERVER_LOG(ERROR, "get server ip failed", K(ret));
      } else {
        cells[cell_idx].set_varchar(ipstr);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
      }
      break;
    }
    case SVR_PORT: {
      addr.reset();
      addr = GCTX.self_addr();
      cells[cell_idx].set_int(addr.get_port());
      break;
    }
    case SQL_ID: {
      cells[cell_idx].set_varchar(sql_id_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case DB_ID: {
      cells[cell_idx].set_int(db_id_);
      break;
    }
    case PLAN_HASH: {
      cells[cell_idx].set_uint64(plan_hash_);
      break;
    }
    case GMT_CREATE: {
      cells[cell_idx].set_timestamp(gmt_create_);
      break;
    }
    case OPERATOR: {
      cells[cell_idx].set_varchar(plan_item->operation_, plan_item->operation_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OPTIONS: {
      cells[cell_idx].set_varchar(plan_item->options_, plan_item->options_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_NODE: {
      cells[cell_idx].set_varchar(plan_item->object_node_, plan_item->object_node_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_ID: {
      cells[cell_idx].set_int(plan_item->object_id_);
      break;
    }
    case OBJECT_OWNER: {
      cells[cell_idx].set_varchar(plan_item->object_owner_, plan_item->object_owner_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_NAME: {
      cells[cell_idx].set_varchar(plan_item->object_name_, plan_item->object_name_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_ALIAS: {
      cells[cell_idx].set_varchar(plan_item->object_alias_, plan_item->object_alias_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OBJECT_TYPE: {
      cells[cell_idx].set_varchar(plan_item->object_type_, plan_item->object_type_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OPTIMIZER: {
      cells[cell_idx].set_varchar(plan_item->optimizer_, REFINE_LENGTH(plan_item->optimizer_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case ID: {
      cells[cell_idx].set_int(plan_item->id_);
      break;
    }
    case PARENT_ID: {
      cells[cell_idx].set_int(plan_item->parent_id_);
      break;
    }
    case DEPTH: {
      cells[cell_idx].set_int(plan_item->depth_);
      break;
    }
    case POSITION: {
      cells[cell_idx].set_int(plan_item->position_);
      break;
    }
    case SEARCH_COLUMNS: {
      cells[cell_idx].set_int(plan_item->search_columns_);
      break;
    }
    case IS_LAST_CHILD: {
      cells[cell_idx].set_int((int)plan_item->is_last_child_);
      break;
    }
    case COST: {
      cells[cell_idx].set_int(plan_item->cost_);
      break;
    }
    case REAL_COST: {
      cells[cell_idx].set_int(plan_item->real_cost_);
      break;
    }
    case CARDINALITY: {
      cells[cell_idx].set_int(plan_item->cardinality_);
      break;
    }
    case REAL_CARDINALITY: {
      cells[cell_idx].set_int(plan_item->real_cardinality_);
      break;
    }
    case BYTES: {
      cells[cell_idx].set_int(plan_item->bytes_);
      break;
    }
    case ROWSET: {
      cells[cell_idx].set_int(plan_item->rowset_);
      break;
    }
    case OTHER_TAG: {
      cells[cell_idx].set_varchar(plan_item->other_tag_, REFINE_LENGTH(plan_item->other_tag_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PARTITION_START: {
      cells[cell_idx].set_varchar(plan_item->partition_start_, REFINE_LENGTH(plan_item->partition_start_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PARTITION_STOP: {
      cells[cell_idx].set_varchar(plan_item->partition_stop_, REFINE_LENGTH(plan_item->partition_stop_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PARTITION_ID: {
      cells[cell_idx].set_int(plan_item->partition_id_);
      break;
    }
    case OTHER: {
      cells[cell_idx].set_varchar(plan_item->other_, REFINE_LENGTH(plan_item->other_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case DISTRIBUTION: {
      cells[cell_idx].set_varchar(plan_item->distribution_, plan_item->distribution_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case CPU_COST: {
      cells[cell_idx].set_int(plan_item->cpu_cost_);
      break;
    }
    case IO_COST: {
      cells[cell_idx].set_int(plan_item->io_cost_);
      break;
    }
    case TEMP_SPACE: {
      cells[cell_idx].set_int(plan_item->temp_space_);
      break;
    }
    case ACCESS_PREDICATES: {
      cells[cell_idx].set_varchar(plan_item->access_predicates_, REFINE_LENGTH(plan_item->access_predicates_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case FILTER_PREDICATES: {
      cells[cell_idx].set_varchar(plan_item->filter_predicates_, REFINE_LENGTH(plan_item->filter_predicates_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case STARTUP_PREDICATES: {
      cells[cell_idx].set_varchar(plan_item->startup_predicates_, REFINE_LENGTH(plan_item->startup_predicates_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PROJECTION: {
      cells[cell_idx].set_varchar(plan_item->projection_, REFINE_LENGTH(plan_item->projection_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case SPECIAL_PREDICATES: {
      cells[cell_idx].set_varchar(plan_item->special_predicates_, REFINE_LENGTH(plan_item->special_predicates_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case TIME: {
      cells[cell_idx].set_int(plan_item->time_);
      break;
    }
    case QBLOCK_NAME: {
      cells[cell_idx].set_varchar(plan_item->qblock_name_, plan_item->qblock_name_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case REMARKS: {
      cells[cell_idx].set_varchar(plan_item->remarks_, REFINE_LENGTH(plan_item->remarks_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case OTHER_XML: {
      cells[cell_idx].set_varchar(plan_item->other_xml_, REFINE_LENGTH(plan_item->other_xml_len_));
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
      break;
    }
    }
  }
  return ret;
}

int ObAllVirtualSqlPlan::extract_tenant_and_plan_id(const common::ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key, end_key;
  int64_t N = ranges.count();
  bool is_always_true = false;
  bool is_always_false = false;
  plan_ids_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && !is_always_true && !is_always_false && i < N; i++) {
    start_key.reset();
    end_key.reset();
    start_key = ranges.at(i).start_key_;
    end_key = ranges.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();

    if (start_key.get_obj_cnt() != end_key.get_obj_cnt() ||
        start_key.get_obj_cnt() != ROWKEY_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid range", K(ret));
    } else if (OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid arguments", K(ret));
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX].is_min_value() &&
               end_key_obj_ptr[KEY_TENANT_ID_IDX].is_max_value()) {
      is_always_true = true;
      if (OB_FAIL(dump_all_tenant_plans())) {
        SERVER_LOG(WARN, "failed to dump all tenant plans", K(ret));
      }
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX].is_max_value() &&
               end_key_obj_ptr[KEY_TENANT_ID_IDX].is_min_value()) {
      is_always_false = true;
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX].is_min_value() ||
               end_key_obj_ptr[KEY_TENANT_ID_IDX].is_max_value() ||
               start_key_obj_ptr[KEY_TENANT_ID_IDX] != end_key_obj_ptr[KEY_TENANT_ID_IDX]) {
      ret = OB_NOT_IMPLEMENT;
      SERVER_LOG(WARN, "tenant id only supports exact value", K(ret));
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX] == end_key_obj_ptr[KEY_TENANT_ID_IDX]) {
      if (ObIntType != start_key_obj_ptr[KEY_TENANT_ID_IDX].get_type() ||
          (start_key_obj_ptr[KEY_TENANT_ID_IDX].get_type() != end_key_obj_ptr[KEY_TENANT_ID_IDX].get_type())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "expect tenant id type to be int",
                    K(start_key_obj_ptr[KEY_TENANT_ID_IDX].get_type()),
                    K(end_key_obj_ptr[KEY_TENANT_ID_IDX].get_type()));
      } else {
        int64_t tenant_id = start_key_obj_ptr[KEY_TENANT_ID_IDX].get_int();
        if (tenant_id != effective_tenant_id_ &&
            !is_sys_tenant(effective_tenant_id_)) {
          //do nothing
        } else if (start_key_obj_ptr[KEY_PLAN_ID_IDX].is_min_value() &&
                   end_key_obj_ptr[KEY_PLAN_ID_IDX].is_max_value()) {
          if (OB_FAIL(dump_tenant_plans(tenant_id))) {
            SERVER_LOG(WARN, "failed to dump tenant plans", K(ret));
          }
        } else if (start_key_obj_ptr[KEY_PLAN_ID_IDX].is_max_value() &&
                  end_key_obj_ptr[KEY_PLAN_ID_IDX].is_min_value()) {
          //do nothing
        } else if (start_key_obj_ptr[KEY_PLAN_ID_IDX].is_min_value() ||
                  end_key_obj_ptr[KEY_PLAN_ID_IDX].is_max_value() ||
                  start_key_obj_ptr[KEY_PLAN_ID_IDX] != end_key_obj_ptr[KEY_PLAN_ID_IDX]) {
          ret = OB_NOT_IMPLEMENT;
          SERVER_LOG(WARN, "tenant id only supports exact value", K(ret));
        } else if (start_key_obj_ptr[KEY_PLAN_ID_IDX] == end_key_obj_ptr[KEY_PLAN_ID_IDX]) {
          if (ObIntType != start_key_obj_ptr[KEY_PLAN_ID_IDX].get_type() ||
              (start_key_obj_ptr[KEY_PLAN_ID_IDX].get_type() != end_key_obj_ptr[KEY_PLAN_ID_IDX].get_type())) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "expect plan id type to be int",
                        K(start_key_obj_ptr[KEY_PLAN_ID_IDX].get_type()),
                        K(end_key_obj_ptr[KEY_PLAN_ID_IDX].get_type()));
          } else {
            int64_t plan_id = start_key_obj_ptr[KEY_PLAN_ID_IDX].get_int();
            PlanInfo info;
            info.tenant_id_ = tenant_id;
            info.plan_id_ = plan_id;
            if (OB_FAIL(plan_ids_.push_back(info))) {
              SERVER_LOG(WARN, "failed to push back plan info", K(ret));
            }
          }
        }
      }
    }
  } // for end
  return ret;
}

int ObAllVirtualSqlPlan::dump_all_tenant_plans()
{
  int ret = OB_SUCCESS;
  // get all tenant ids
  ObSEArray<uint64_t, 4> all_tenant_ids;
  if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(all_tenant_ids))) {
    SERVER_LOG(WARN, "failed to get all tenant ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant_ids.count(); i++) {
    if (all_tenant_ids.at(i) != effective_tenant_id_ &&
        !is_sys_tenant(effective_tenant_id_)) {
      //do nothing
    } else if (OB_FAIL(dump_tenant_plans(all_tenant_ids.at(i)))) {
      SERVER_LOG(WARN, "failed to dump tenant` plan", K(ret), K(i));
    }
  }
  return ret;
}

int ObAllVirtualSqlPlan::dump_tenant_plans(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_virtual_tenant_id(tenant_id)) {
    DumpAllPlan dump_plan;
    dump_plan.tenant_id_ = tenant_id;
    dump_plan.plan_ids_ = &plan_ids_;
    // !!!引用plan cache资源之前必须加ObReqTimeGuard
    ObReqTimeGuard req_timeinfo_guard;
    ObPlanCache *plan_cache = NULL;
    MTL_SWITCH(tenant_id) {
      plan_cache = MTL(ObPlanCache*);
      if (OB_ISNULL(plan_cache)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpect null plan cache", K(ret));
      } else if (OB_FAIL(plan_cache->foreach_alloc_cache_obj(dump_plan))) {
        SERVER_LOG(WARN, "failed to dump plan", K(ret));
      }
    } // mtl switch ends
    if (OB_OP_NOT_ALLOW == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObAllVirtualSqlPlan::prepare_next_plan()
{
  int ret = OB_SUCCESS;

  if (plan_idx_ >= plan_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "no more plan", K(ret));
  } else if (OB_INVALID_INDEX == plan_ids_.at(plan_idx_).tenant_id_ ||
             OB_INVALID_INDEX == plan_ids_.at(plan_idx_).plan_id_) {
    SERVER_LOG(DEBUG, "invalid tenant_id or plan_id");
  } else {
    tenant_id_ = plan_ids_.at(plan_idx_).tenant_id_;
    plan_id_ = plan_ids_.at(plan_idx_).plan_id_;
    //next plan
    ++plan_idx_;
    //init plan item array
    plan_items_.reuse();
    plan_item_idx_ = 0;
    ObPhysicalPlan *plan = NULL;
    // !!!引用plan cache资源之前必须加ObReqTimeGuard
    ObReqTimeGuard req_timeinfo_guard;
    ObPlanCache *plan_cache = NULL;
    ObCacheObjGuard guard(SQL_PLAN_HANDLE);
    int tmp_ret = OB_SUCCESS;
    MTL_SWITCH(tenant_id_) {
      plan_cache = MTL(ObPlanCache*);
      if (OB_SUCCESS != (tmp_ret = plan_cache->ref_plan(plan_id_, guard))) {
        // should not panic
      } else if (FALSE_IT(plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj()))) {
        // do nothing
      } else if (OB_ISNULL(plan)) {
        // maybe pl object, do nothing
      } else {
        //decompress logical plan
        ObLogicalPlanRawData &raw_plan = plan->get_logical_plan();
        if (OB_ISNULL(allocator_)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpect null allocator", K(ret));
        } else if (OB_FAIL(raw_plan.uncompress_logical_plan(*allocator_, plan_items_))) {
          SERVER_LOG(WARN, "failed to uncompress logical plan", K(ret));
        } else {
          db_id_ = plan->stat_.db_id_;
          plan_hash_ = plan->get_plan_hash_value();
          gmt_create_ = plan->stat_.gen_time_;
          if (plan->stat_.sql_id_.length() <= OB_MAX_SQL_ID_LENGTH) {
            MEMCPY(sql_id_, plan->stat_.sql_id_.ptr(), plan->stat_.sql_id_.length());
          } else {
            MEMSET(sql_id_, 0, OB_MAX_SQL_ID_LENGTH);
          }
        }
      }
    } // mtl switch ends
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
