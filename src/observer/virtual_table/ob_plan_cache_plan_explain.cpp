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
#include "observer/ob_server_struct.h"
#include "observer/ob_req_time_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_range.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/ob_sql.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
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
  } else if (OB_UNLIKELY(col_count < 1)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
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
        if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
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
        if (OB_ISNULL(buf = static_cast<char *> (allocator_->alloc(buf_len)))) {
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
      if (OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
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
  if (OB_ISNULL(buffer = static_cast<char *>(allocator_->alloc(OB_MAX_PLAN_EXPLAIN_NAME_LENGTH)))) {
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

  if (OB_ISNULL(buffer = static_cast<char *>(allocator_->alloc(OB_MAX_PLAN_EXPLAIN_NAME_LENGTH)))) {
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
  if (OB_ISNULL(buf = static_cast<char *> (allocator_->alloc(OB_MAX_OPERATOR_PROPERTY_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    switch (cur_op.get_type()) {
    case PHY_TABLE_SCAN: {
      if (OB_FAIL(static_cast<const ObTableScanSpec &>(cur_op).explain_index_selection_info(
                  buf, OB_MAX_OPERATOR_PROPERTY_LENGTH, pos))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to gen property", K(ret));
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

int ObPlanCachePlanExplain::set_tenant_plan_id(const common::ObIArray<common::ObNewRange> &ranges)
{
  int ret = OB_SUCCESS;
  // display only one plan
  if (ranges.count() == 1 && ranges.at(0).is_single_rowkey()) {
    ObRowkey start_key = ranges.at(0).start_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
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
  }
  return ret;
}

int ObPlanCachePlanExplain::inner_open()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlan *plan = NULL;
  // !!!引用plan cache资源之前必须加ObReqTimeGuard
  ObReqTimeGuard req_timeinfo_guard;
  if (OB_FAIL(set_tenant_plan_id(key_ranges_))) {
    LOG_WARN("set tenant id and plan id failed", K(ret));
  } else if (!is_sys_tenant(effective_tenant_id_) && tenant_id_ != effective_tenant_id_) {
    // user tenant can only show self tenant infos
    // return nothing
    LOG_INFO("non-sys tenant can only show self tenant plan cache plan explain infos",
        K(effective_tenant_id_), K(tenant_id_), K(plan_id_));
  } else if (OB_INVALID_INDEX != tenant_id_ && OB_INVALID_INDEX != plan_id_) {
    ObPlanCache *plan_cache = NULL;
    ObCacheObjGuard guard(PLAN_EXPLAIN_HANDLE);
    int tmp_ret = OB_SUCCESS;

    MTL_SWITCH(tenant_id_) {
      plan_cache = MTL(ObPlanCache*);
      if (OB_SUCCESS != (tmp_ret = plan_cache->ref_plan(plan_id_, guard))) {
        // should not panic
      } else if (FALSE_IT(plan = static_cast<ObPhysicalPlan*>(guard.get_cache_obj()))) {
        // do nothing
      } else if (OB_ISNULL(plan)) {
        // maybe pl object, do nothing
      } else if (OB_NOT_NULL(plan->get_root_op_spec())) {
        if (OB_FAIL(static_engine_exp_visitor_.init(tenant_id_, plan_id_, allocator_))) {
          SERVER_LOG(WARN, "failed to init visitor", K(ret));
        } else if (OB_FAIL(plan->get_root_op_spec()->accept(static_engine_exp_visitor_))) {
          SERVER_LOG(WARN, "fail to traverse physical plan", K(ret));
        }
      } else {
        // done
      }
    } // mtl switch ends
  } else {
    SERVER_LOG(DEBUG, "invalid tenant_id or plan_id", K_(tenant_id), K_(plan_id));
  }

  // data is ready
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
  }

  return ret;
}

int ObPlanCachePlanExplain::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret));
    }
  } else {
    row = &cur_row_;
  }

  return ret;
}

ObPlanCachePlanExplain::~ObPlanCachePlanExplain()
{
}

} // end namespace observr
} // end namespace oceanbase
