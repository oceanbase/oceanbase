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

#include "ob_plan_cache_plan_explain.h"
#include "observer/ob_server_utils.h"
#include "sql/ob_sql.h"
#include "sql/engine/table/ob_table_scan_op.h"
using namespace oceanbase;
using namespace oceanbase::observer;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{
template<class Op>
int ObExpVisitor::add_row(const Op &cur_op)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else {
    ObString ipstr;
    common::ObAddr addr;
    ObQueryFlag scan_flag;
    for (int64_t i =  0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
      case ObPlanCachePlanExplain::TENANT_ID_COL : {
        cells[i].set_int(tenant_id_);
        break;
      }
      case ObPlanCachePlanExplain::IP_COL: {
        ipstr.reset();
        if (OB_FAIL(ObServerUtils::get_server_ip(&allocator_, ipstr))) {
          SERVER_LOG(ERROR, "get server ip failed", K(ret));
        } else {
          cells[i].set_varchar(ipstr);
          cells[i].set_collation_type(ObCharset::get_default_collation(
                                        ObCharset::get_default_charset()));
        }
        break;
      }
      case ObPlanCachePlanExplain::PORT_COL: {
        addr.reset();
        addr = GCTX.self_addr();
        cells[i].set_int(addr.get_port());
        break;
      }
      case ObPlanCachePlanExplain::PLAN_ID_COL: {
        cells[i].set_int(plan_id_);
        break;
      }
      case ObPlanCachePlanExplain::OP_NAME_COL: {
        char *buf = NULL;
        int64_t buf_len = cur_op.get_plan_depth() + strlen(cur_op.get_name()) + 1;
        int64_t pos = 0;
        if (OB_ISNULL(buf = static_cast<char *> (allocator_.alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          int64_t j = cur_op.get_plan_depth();
          while (j > 0 && pos < buf_len) {
            BUF_PRINTF(" ");
            --j;
          }
          if (OB_UNLIKELY(0 > snprintf(buf + pos, buf_len - pos, "%s", cur_op.get_name()))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to gen operator name");
          } else {
            cells[i].set_varchar(buf);
            cells[i].set_collation_type(ObCharset::get_default_collation(
                                        ObCharset::get_default_charset()));
          }
        }
        break;
      }
      case ObPlanCachePlanExplain::TBL_NAME_COL: {
        ObString tbl_name;
        ret = get_table_name(cur_op, tbl_name);
        if (OB_SUCC(ret)) {
          cells[i].set_varchar(tbl_name);
          cells[i].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
        }
        break;
      }
      case ObPlanCachePlanExplain::ROWS_COL: {
        cells[i].set_int(cur_op.get_rows());
        break;
      }
      case ObPlanCachePlanExplain::COST_COL: {
        cells[i].set_int(cur_op.get_cost());
        break;
      }
      case ObPlanCachePlanExplain::PROPERTY_COL: {
        ObString property;
        ret = get_property(cur_op, property);
        if (OB_SUCC(ret)) {
          cells[i].set_varchar(property);
          cells[i].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
        }
        break;
      }
      case ObPlanCachePlanExplain::PLAN_DEPTH_COL: {
        cells[i].set_int(cur_op.get_plan_depth());
        break;
      }
      case ObPlanCachePlanExplain::PLAN_LINE_ID_COL: {
        cells[i].set_int(cur_op.get_id());
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
    if (OB_SUCC(ret)) {
      // deep copy row
      if (OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      } else {
        // free memory
        allocator_.reuse();
      }
    }
  }
  return ret;
}

int ObOpSpecExpVisitor::add_row(const sql::ObOpSpec &op)
{
  return ObExpVisitor::add_row(op);
}

template<>
int ObExpVisitor::get_table_name<ObOpSpec>(const ObOpSpec &cur_op, ObString &table_name)
{
  int ret = OB_SUCCESS;
  char *buffer = NULL;
  ObString index_name;
  ObString tmp_table_name;
  if (OB_ISNULL(buffer = static_cast<char *>(allocator_.alloc(OB_MAX_PLAN_EXPLAIN_NAME_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory", K(ret));
  } else if (PHY_TABLE_SCAN == cur_op.get_type()) {
    const ObTableScanSpec &tsc_spec = static_cast<const ObTableScanSpec &>(cur_op);
    ObQueryFlag scan_flag;
    scan_flag.flag_ = tsc_spec.flags_;
    tmp_table_name = tsc_spec.table_name_;
    index_name = tsc_spec.index_name_;
    if (OB_FAIL(get_table_access_desc(tsc_spec.should_scan_index(), scan_flag,
                                      tmp_table_name, index_name, table_name))) {
        SERVER_LOG(WARN, "failed to get table name", K(ret));
    }
  } else {
    table_name = ObString::make_string("NULL");
  }
  return ret;
}

int ObExpVisitor::get_table_access_desc(bool is_idx_access, const ObQueryFlag &scan_flag, ObString &tab_name,
                                        const ObString &index_name, ObString &ret_name)
{
  int ret = OB_SUCCESS;
  char *buffer = NULL;
  int64_t tmp_pos = 0;

  if (OB_ISNULL(buffer = static_cast<char *>(allocator_.alloc(OB_MAX_PLAN_EXPLAIN_NAME_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    tmp_pos = tab_name.to_string(buffer, OB_MAX_PLAN_EXPLAIN_NAME_LENGTH);
    if (is_idx_access) {
        IGNORE_RETURN snprintf(buffer+tmp_pos,
                              OB_MAX_PLAN_EXPLAIN_NAME_LENGTH-tmp_pos,
                              LEFT_BRACKET);
        tmp_pos += strlen(LEFT_BRACKET);
        tmp_pos += index_name.to_string(buffer+tmp_pos,
                                        OB_MAX_PLAN_EXPLAIN_NAME_LENGTH - tmp_pos);
        if (scan_flag.is_reverse_scan()) {
          IGNORE_RETURN snprintf(buffer + tmp_pos,
                                 OB_MAX_PLAN_EXPLAIN_NAME_LENGTH - tmp_pos,
                                 COMMA_REVERSE);
          tmp_pos += strlen(COMMA_REVERSE);
        } else {
          // do nothing
        }
        IGNORE_RETURN snprintf(buffer + tmp_pos,
                               OB_MAX_PLAN_EXPLAIN_NAME_LENGTH - tmp_pos,
                               RIGHT_BRACKET);
        tmp_pos += strlen(RIGHT_BRACKET);
    } else {
      if (scan_flag.is_reverse_scan()) {
        IGNORE_RETURN snprintf(buffer + tmp_pos,
                               OB_MAX_PLAN_EXPLAIN_NAME_LENGTH - tmp_pos,
                               BRACKET_REVERSE);
        tmp_pos += strlen(BRACKET_REVERSE);
      } else { /* Do nothing */ }
    }

   if (OB_SUCC(ret)) {
      ret_name.assign_ptr(buffer, tmp_pos);
    }
  }
  return ret;
}

template<>
int ObExpVisitor::get_property<ObOpSpec>(const ObOpSpec &cur_op,
                                         common::ObString &property)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *> (allocator_.alloc(OB_MAX_OPERATOR_PROPERTY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    switch (cur_op.get_type()) {
    case PHY_TABLE_SCAN: {
      if (OB_FAIL(static_cast<const ObTableScanSpec &>(cur_op).explain_index_selection_info(
                  buf, OB_MAX_OPERATOR_PROPERTY_LENGTH, pos))) {
        if (ret == OB_SIZE_OVERFLOW) {
          ret = OB_SUCCESS;
          SERVER_LOG(INFO,
                     "The properties of ObTableScanSpec exceed "
                     "OB_MAX_OPERATOR_PROPERTY_LENGTH and have been truncated.",
                     K(ret), K(OB_MAX_OPERATOR_PROPERTY_LENGTH));
        } else {
          SERVER_LOG(WARN, "fail to gen property", K(ret));
        }
      } else {
        property.assign_ptr(buf, pos);
      }
      break;
    }
    default: {
      property = ObString::make_string("NULL");
      break;
    }
    }
  }
  return ret;
}

int ObCacheObjIterator::operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "HashMapPair second element is NULL", K(ret));
  } else if (ObLibCacheNameSpace::NS_CRSR != entry.second->get_ns()) {
  } else if (OB_FAIL(plan_id_array_.push_back(entry.first))){
    SERVER_LOG(WARN, "fail to push plan id into plan_id_array_", K(ret), K(entry.first));
  }
  return ret;
}

int ObCacheObjIterator::next(int64_t &tenant_id, ObCacheObjGuard &guard)
{
  int ret = OB_SUCCESS;
  bool find = false;
  do {
    if (plan_id_array_.count() == 0) {
      if (tenant_id_array_idx_ < tenant_id_array_.count() - 1) {
        ++tenant_id_array_idx_;
        tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
        MTL_SWITCH(tenant_id) {
          ObPlanCache* plan_cache = MTL(ObPlanCache*);
          if (OB_ISNULL(plan_cache)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "plan_cache is NULL", K(ret));
          } else if (!plan_cache->is_inited()) {
            // not inited
            SERVER_LOG(INFO, "plan cache is not inited, ", K(ret), K(tenant_id));
          } else if (OB_FAIL(plan_cache->foreach_cache_obj(*this))) {
            SERVER_LOG(WARN, "fail to traverse plan cache obj", K(ret));
          }
        } else {
          ret = OB_SUCCESS;
          SERVER_LOG(INFO, "fail to switch tenant, may be deleted", K(ret), K(tenant_id));
        }
      } else {
        ret = OB_ITER_END;
      }
    }
    if (OB_SUCC(ret)) {
      if (plan_id_array_.count() == 0) {
      } else {
        uint64_t plan_id = 0;
        tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
        MTL_SWITCH(tenant_id) {
          ObPlanCache* plan_cache = MTL(ObPlanCache*);
          if (OB_ISNULL(plan_cache)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "plan_cache is NULL", K(ret));
          } else if (OB_FAIL(plan_id_array_.pop_back(plan_id))) {
            SERVER_LOG(WARN, "failed to pop back plan id", K(ret));
          } else if (OB_ISNULL(plan_cache)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "plan_cache is NULL", K(ret));
          } else if (OB_FAIL(plan_cache->ref_cache_obj(plan_id, guard))) {
            if (ret == OB_HASH_NOT_EXIST) {
              ret = OB_SUCCESS;
            } else {
              SERVER_LOG(WARN, "fail to ref physical plan", K(ret));
            }
          } else {
            find = true;
          }
        } else {
          // tenant has been deleted, clear all plan id of the tenant
          plan_id_array_.reuse();
          ret = OB_SUCCESS;
          SERVER_LOG(INFO, "fail to switch tenant, may be deleted", K(ret), K(tenant_id));
        }
      }
    }
  } while (OB_SUCC(ret) && !find);
  return ret;
}

int ObPlanCachePlanExplain::set_tenant_plan_id(const common::ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  // display only one plan
  if (ranges.count() == 1 && ranges.at(0).is_single_rowkey()) {
    ObRowkey start_key = ranges.at(0).start_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    scan_all_plan_ = false;
    if (OB_ISNULL(start_key_obj_ptr)
        || start_key.get_obj_cnt() != 4)  /* (tenant_id, svr_ip, svr_port, plan_id) */ {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN,
                 "fail to init plan visitor",
                 K(ret),
                 K(start_key_obj_ptr),
                 "count", start_key.get_obj_cnt());
    } else {
      tenant_id_ = start_key_obj_ptr[0].get_int();
      plan_id_ = start_key_obj_ptr[3].get_int();
    }
  } else {
    scan_all_plan_ = true;
  }
  return ret;
}

int ObPlanCachePlanExplain::inner_open()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan *plan = NULL;
  static_engine_exp_visitor_.set_effective_tenant_id(effective_tenant_id_);
  if (OB_FAIL(set_tenant_plan_id(key_ranges_))) {
    LOG_WARN("set tenant id and plan id failed", K(ret));
  } else if (!scan_all_plan_) {
    ObPlanCache *plan_cache = NULL;
    // !!!引用plan cache资源之前必须加ObReqTimeGuard
    ObReqTimeGuard req_timeinfo_guard;
    ObCacheObjGuard guard(PLAN_EXPLAIN_HANDLE);
    int tmp_ret = OB_SUCCESS;
    if (!is_sys_tenant(effective_tenant_id_) && tenant_id_ != effective_tenant_id_) {
      // user tenant can only show self tenant infos
      // return nothing
      LOG_INFO("non-sys tenant can only show self tenant plan cache plan explain infos",
        K(effective_tenant_id_), K(tenant_id_), K(plan_id_));
    } else {
      MTL_SWITCH(tenant_id_) {
        plan_cache = MTL(ObPlanCache*);
        if (OB_SUCCESS != (tmp_ret = plan_cache->ref_plan(plan_id_, guard))) {
          // should not panic
        } else if (FALSE_IT(plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj()))) {
          // do nothing
        } else if (OB_ISNULL(plan)) {
          // maybe pl object, do nothing
        } else if (OB_NOT_NULL(plan->get_root_op_spec())) {
          if (OB_FAIL(static_engine_exp_visitor_.init(tenant_id_, plan_id_))) {
            SERVER_LOG(WARN, "failed to init visitor", K(ret));
          } else if (OB_FAIL(plan->get_root_op_spec()->accept(static_engine_exp_visitor_))) {
            SERVER_LOG(WARN, "fail to traverse physical plan", K(ret));
          }
        } else {
          // done
        }
      } else {
        // failed to switch tenant
        ret = OB_SUCCESS;
        SERVER_LOG(INFO, "fail to switch tenant, may be deleted", K(ret), K(tenant_id_));
      }
    }
  } else {
    // scan all plan
    if (is_sys_tenant(effective_tenant_id_)) {
      if (OB_ISNULL(GCTX.omt_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "GCTX.omt_ is NULL", K(ret));
      } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
        SERVER_LOG(WARN, "failed to get all tenant id", K(ret));
      }
    } else {
      // user tenant show self tenant info
      if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
        SERVER_LOG(WARN, "failed to push back tenant id", K(ret),
                   K(effective_tenant_id_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
  }

  return ret;
}

int ObPlanCachePlanExplain::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    do {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        } else {
          if (scan_all_plan_) {
            ret = OB_SUCCESS;
            ObReqTimeGuard req_timeinfo_guard;
            ObCacheObjGuard guard(PLAN_EXPLAIN_HANDLE);
            if (OB_FAIL(cache_obj_iterator_.next(tenant_id_, guard))) {
              if (OB_ITER_END == ret) {
                iter_end_ = true;
              } else {
                SERVER_LOG(WARN, "fail to get next physical plan", K(ret));
              }
            } else {
              ObPhysicalPlan *plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj());
              if (OB_FAIL(static_engine_exp_visitor_.init(tenant_id_, plan->get_plan_id()))) {
                SERVER_LOG(WARN, "failed to init visitor", K(ret));
              } else if (OB_FAIL(plan->get_root_op_spec()->accept(static_engine_exp_visitor_))) {
                SERVER_LOG(WARN, "fail to traverse physical plan", K(ret));
              }
            }
          } else {
            iter_end_ = true;
          }
        }
      } else {
        row = &cur_row_;
        break;
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

ObPlanCachePlanExplain::~ObPlanCachePlanExplain()
{
}

} // end namespace observr
} // end namespace oceanbase