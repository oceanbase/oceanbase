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

#include "observer/virtual_table/ob_all_virtual_ps_item_info.h"

#include "sql/plan_cache/ob_ps_cache.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"

#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema.h"

using namespace oceanbase;
using namespace sql;
using namespace observer;
using namespace common;

int ObAllVirtualPsItemInfo::inner_get_next_row()
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
        if (OB_FAIL(get_next_row_from_specified_tenant(tenant_id, is_sub_end))) {
          SERVER_LOG(WARN, "get_next_row_from_specified_tenant failed", K(ret), K(tenant_id));
        } else {
          if (is_sub_end) {
            ++tenant_id_array_idx_;
          }
        }
      }
    }
  } while (is_sub_end && OB_SUCC(ret));
  return ret;
}

int ObAllVirtualPsItemInfo::inner_open()
{
  int ret = OB_SUCCESS;
  // sys tenant show all tenant infos
  if (is_sys_tenant(effective_tenant_id_)) {
    if(OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
      SERVER_LOG(WARN, "failed to add tenant id", K(ret));
    }
  } else {
    // user tenant show self tenant info
    if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "tenant id array fail to push back", KR(ret), K(effective_tenant_id_));
    }
  }
  return ret;
}

void ObAllVirtualPsItemInfo::reset()
{
  ObAllPlanCacheBase::reset();
  stmt_id_array_.reset();
  stmt_id_array_idx_ = OB_INVALID_ID;
  ps_cache_ = NULL;
}

int ObAllVirtualPsItemInfo::fill_cells(uint64_t tenant_id,
                                       ObPsStmtId stmt_id,
                                       ObPsStmtItem *stmt_item,
                                       ObPsStmtInfo *stmt_info)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  int64_t col_count = output_column_ids_.count();
  if (OB_ISNULL(stmt_info)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "stmt_info is NULL", K(ret));
  } else if (OB_INVALID_ID == stmt_id) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid stmt_id", K(ret), K(stmt_id));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ is NULL", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::TENANT_ID: {
        cells[i].set_int(tenant_id);
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::SVR_IP: {
        ObString ipstr;
        if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
          SERVER_LOG(WARN, "get server ip failed", K(ret));
        } else {
          cells[i].set_varchar(ipstr);
          cells[i].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
        }
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::SVR_PORT: {
        cells[i].set_int(GCTX.self_addr().get_port());
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::STMT_ID: {
        cells[i].set_int(stmt_id);
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::DB_ID: {
        cells[i].set_int(stmt_info->get_sql_key().db_id_);
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::PS_SQL: {
        ObString ps_sql;
        if (OB_FAIL(ob_write_string(*allocator_, stmt_info->get_ps_sql(), ps_sql))) {
          SERVER_LOG(WARN, "copy ps_sql failed", K(ret), K(stmt_info->get_ps_sql()));
        } else {
          cells[i].set_lob_value(ObLongTextType, ps_sql.ptr(),
                                 static_cast<int32_t>(ps_sql.length()));
          cells[i].set_collation_type(ObCharset::get_default_collation(
                                      ObCharset::get_default_charset()));
        }
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::PARAM_COUNT: {
        cells[i].set_int(stmt_info->get_num_of_param());
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::STMT_ITEM_REF_COUNT: {
        if (OB_ISNULL(stmt_item)) {
          cells[i].set_int(0);
        } else {
          cells[i].set_int(stmt_item->get_ref_count());
        }
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::STMT_INFO_REF_COUNT: {
        cells[i].set_int(stmt_info->get_ref_count());
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::MEM_HOLD: {
        cells[i].set_int(stmt_info->get_item_and_info_size());
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::STMT_TYPE: {
        cells[i].set_int(stmt_info->get_stmt_type());
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::CHECKSUM: {
        cells[i].set_int(stmt_info->get_ps_stmt_checksum());
        break;
      }
      case share::ALL_VIRTUAL_PS_ITEM_INFO_CDE::EXPIRED: {
        cells[i].set_bool(stmt_info->is_expired());
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(i), K(output_column_ids_), K(col_id));
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualPsItemInfo::get_next_row_from_specified_tenant(uint64_t tenant_id, bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  if (OB_INVALID_ID == stmt_id_array_idx_) {
    ps_cache_ = MTL(ObPsCache*);
    if (false == ps_cache_->is_inited()) {
      is_end = true;
      SERVER_LOG(DEBUG, "ps cache is not ready, ignore this", K(ret), K(ps_cache_->is_inited()));
    } else if (OB_FAIL(ps_cache_->get_all_stmt_id(&stmt_id_array_))) {
      SERVER_LOG(WARN, "get_all_stmt_id failed", K(ret));
    } else {
      stmt_id_array_idx_ = 0;
    }
  }
  if (OB_SUCCESS == ret && false == is_end) {
    bool is_filled = false;
    while (OB_SUCC(ret) && false == is_filled && false == is_end) {
      if (stmt_id_array_idx_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid plan_stat_array index", K(stmt_id_array_idx_));
      } else if (stmt_id_array_idx_ >= stmt_id_array_.count()) {
        is_end = true;
        stmt_id_array_idx_ = OB_INVALID_ID;
        stmt_id_array_.reset();
        if (OB_UNLIKELY(OB_ISNULL(ps_cache_))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "plan cache is null", K(ret));
        } else {
          ps_cache_ = NULL;
        }
      } else {
        is_end = false;
        uint64_t stmt_id= stmt_id_array_.at(stmt_id_array_idx_);
        ++stmt_id_array_idx_;
        ObPsStmtInfo *stmt_info = NULL;
        ObPsStmtItem *stmt_item = NULL;
        int tmp_ret = ps_cache_->ref_stmt_info(stmt_id, stmt_info);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          SERVER_LOG(DEBUG, "cannot get stmt_info, may be deleted", K(ret), K(tmp_ret), K(stmt_id));
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          SERVER_LOG(WARN, "ref_stmt_info failed", K(ret), K(stmt_id));
        } else if (OB_ISNULL(stmt_info)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "stmt_info is NULL", K(ret));
        } else if (FALSE_IT(tmp_ret = ps_cache_->ref_stmt_item(stmt_info->get_sql_key(),
                                                               stmt_item))) {
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
          SERVER_LOG(DEBUG, "cannot get stmt_item, may be deleted",
                     K(ret), K(tmp_ret), K(stmt_info->get_ps_sql()));
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          SERVER_LOG(WARN, "ref_stmt_item failed", K(ret), K(stmt_info->get_ps_sql()));
        } else if (OB_ISNULL(stmt_item)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "stmt_item is NULL", K(ret));
        } else {
          // done
        }
        SERVER_LOG(DEBUG, "all setup", K(ret), K(tmp_ret), KP(stmt_info), KP(stmt_item));

        if (OB_NOT_NULL(stmt_info)) {
          if (OB_SUCC(ret)) {
            if (OB_FAIL(fill_cells(tenant_id, stmt_id, stmt_item, stmt_info))) {
              SERVER_LOG(WARN, "fail to fill cells", K(ret), K(stmt_id), K(tenant_id));
            } else {
              is_filled = true;
            }
          }
        }

        // still need to deref_stmt_item even though ret is not OB_SUCCESS
        if (OB_NOT_NULL(stmt_item)) {
          stmt_item->dec_ref_count_check_erase();
          stmt_item = NULL;
        }

        // still need to deref_stmt_info even though ret is not OB_SUCCESS
        if (OB_NOT_NULL(stmt_info)) {
          if (OB_SUCCESS != (tmp_ret = ps_cache_->deref_stmt_info(stmt_id))) {
            if (OB_SUCCESS == ret) {
              // 如果ret不是OB_SUCCESS，则忽略了tmp_ret
              ret = tmp_ret;
            }
            SERVER_LOG(WARN, "deref_stmt_info failed", K(tmp_ret), K(stmt_id));
          } else {
            stmt_info = NULL;
          }
        }
      }
    } //while end
  }
  return ret;
}
