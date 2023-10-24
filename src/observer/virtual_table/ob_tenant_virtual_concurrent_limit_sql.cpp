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

#include "observer/virtual_table/ob_tenant_virtual_concurrent_limit_sql.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/row/ob_row.h"
#include "lib/utility/utility.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
namespace observer
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
void ObTenantVirtualConcurrentLimitSql::reset()
{
  ObTenantVirtualOutlineBase::reset();
}

int ObTenantVirtualConcurrentLimitSql::is_need_output(const ObOutlineInfo *outline_info, bool &is_output)
{
  int ret = OB_SUCCESS;
  is_output = false;
  bool is_recycle = false;
  bool has_limit = false;
  if (OB_ISNULL(outline_info) || OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", K(ret), K(outline_info), K(schema_guard_));
  } else if (OB_FAIL(outline_info->has_concurrent_limit_param(has_limit))) {
    LOG_WARN("fail to judge whether outline_info has limit param", K(ret));
  } else if (false == has_limit) {
    is_output = false;
  } else if (OB_FAIL(is_database_recycle(outline_info->get_database_id(), is_recycle))) {
    LOG_WARN("fail to judge database recycle", K(ret), KPC(outline_info));
  } else {
    is_output = !is_recycle;
  }

  return ret;
}

int ObTenantVirtualConcurrentLimitSql::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTenantVirtualOutlineBase::inner_open())) {
    LOG_WARN("fail to inner open", K(ret));
  } else {
    param_idx_ = 0;
  }
  return ret;
}


int ObTenantVirtualConcurrentLimitSql::get_next_concurrent_limit_row(const ObOutlineInfo *outline_info,
                                                                     bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  const ObMaxConcurrentParam *param = NULL;
  if (OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K(ret), K(outline_info));
  } else {
    const ObOutlineParamsWrapper &params_wrapper = outline_info->get_outline_params_wrapper();
    if (param_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param idx", K(ret), K(param_idx_));
    } else if (OB_UNLIKELY(param_idx_ >= params_wrapper.get_param_count())){
      is_iter_end = true;
    } else if (OB_ISNULL(param = params_wrapper.get_outline_param(param_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get outline param", K(ret), K(param_idx_));
    } else if (param->is_concurrent_limit_param()) {
      if (OB_FAIL(fill_cells(outline_info, param))) {
        LOG_WARN("fail to fill cells", K(ret), K(param_idx_), K(outline_info_idx_));
      } else {
        ++param_idx_;
      }
    } else {
      ++param_idx_;
      if (OB_FAIL(SMART_CALL(get_next_concurrent_limit_row(outline_info, is_iter_end)))) {
        LOG_WARN("fail to get next concurrent_limit_row", K(ret), K(param_idx_), K(outline_info_idx_));
      }
    }
  }
  return ret;
}
int ObTenantVirtualConcurrentLimitSql::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObOutlineInfo *outline_info = NULL;
  bool is_output = false;
  bool is_iter_end = false;
  if (outline_info_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array idx", K(ret), K(outline_info_idx_));
  } else if (outline_info_idx_ >= outline_infos_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(outline_info = outline_infos_.at(outline_info_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outline info is NULL", K(ret), K(outline_info_idx_));
  } else if (OB_FAIL(is_need_output(outline_info, is_output))) {
    LOG_WARN("fail to judge output", K(ret), KPC(outline_info));
  } else if (is_output) {
    if (OB_FAIL(get_next_concurrent_limit_row(outline_info, is_iter_end))) {
      LOG_WARN("fail to fill cells", K(ret), K(outline_info), K(outline_info_idx_));
    } else if (is_iter_end) {
      ++outline_info_idx_;
      param_idx_ = 0;
      if (OB_FAIL(SMART_CALL(inner_get_next_row(row)))) {
        LOG_WARN("fail to get_next_row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  } else {
    ++outline_info_idx_;
    if (OB_FAIL(SMART_CALL(inner_get_next_row(row)))) {
      LOG_WARN("fail to get_next_row", K(ret));
    }
  }
  return ret;
}

int ObTenantVirtualConcurrentLimitSql::fill_cells(const ObOutlineInfo *outline_info,
                                                  const ObMaxConcurrentParam *param)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(cells = cur_row_.cells_)
      || OB_ISNULL(allocator_)
      || OB_ISNULL(session_)
      || OB_ISNULL(outline_info)
      || OB_ISNULL(param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("some data member is NULL", K(ret), K(cells), K(allocator_), K(session_), K(outline_info), K(param));
  } else if (OB_UNLIKELY(reserved_column_cnt_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong column count", K(ret), K(reserved_column_cnt_), K(output_column_ids_.count()));
  } else {
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
      const uint64_t col_id = output_column_ids_.at(cell_idx);
      switch (col_id) {
        case TENANT_ID : {
          cells[cell_idx].set_int(static_cast<int64_t>(tenant_id_));
          break;
        }
        case DATABASE_ID : {
          cells[cell_idx].set_int(static_cast<int64_t>(outline_info->get_database_id()));
          break;
        }
        case OUTLINE_ID : {
          cells[cell_idx].set_int(static_cast<int64_t>(outline_info->get_outline_id()));
          break;
        }
        case DATABASE_NAME : {
          DBInfo db_info;
          if (OB_FAIL(database_infos_.get_refactored(outline_info->get_database_id(), db_info))) {
            LOG_WARN("fail to ge value from database_infos", K(ret), K(outline_info->get_database_id()));
          } else {
            cells[cell_idx].set_varchar(db_info.db_name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OUTLINE_NAME : {
          ObString outline_name;
          if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_name_str(), outline_name))) {
            LOG_WARN("fail to deep copy obstring", K(ret), K(outline_info->get_name_str()), K(outline_name));
          } else {
            cells[cell_idx].set_varchar(outline_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case  OUTLINE_CONTENT : {
          ObString outline_content;
          if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_outline_content_str(), outline_content))) {
            LOG_WARN("fail to deep copy obstring", K(ret), K(outline_info->get_outline_content_str()), K(outline_content));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, outline_content.ptr(),
                                          static_cast<int32_t>(outline_content.length()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case VISIBLE_SIGNATURE : {
          ObString local_str;
          ObString visible_sigature;
          if (OB_FAIL(outline_info->get_visible_signature(local_str))) {
            LOG_WARN("fail to get visibale signature", K(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, local_str, visible_sigature))) {
            LOG_WARN("fail to deep copy ObString", K(ret), K(local_str), K(visible_sigature));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, visible_sigature.ptr(),
                                          static_cast<int32_t>(visible_sigature.length()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SQL_TEXT : {
          ObString sql_text;
          if (OB_FAIL(ob_write_string(*allocator_, param->get_sql_text(), sql_text))) {
            LOG_WARN("fail to deep copy obstring", K(ret), K(param->get_sql_text()), K(sql_text));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, sql_text.ptr(),
                                          static_cast<int32_t>(sql_text.length()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case CONCURRENT_NUM : {
          cells[cell_idx].set_int(param->get_concurrent_num());
          break;
        }
        case LIMIT_TARGET : {
          ObString visible_signature;
          ObString limit_sql;
          if (outline_info->get_sql_text_str().empty()) {
            cells[cell_idx].set_lob_value(ObLongTextType, "", 0);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else if (OB_FAIL(outline_info->get_visible_signature(visible_signature))) {
            LOG_WARN("fail to get visibale signature", K(ret));
          } else if (OB_FAIL(ObOutlineInfo::gen_limit_sql(visible_signature, param, *session_, *allocator_, limit_sql))) {
            LOG_WARN("fail to gen limit sql", K(ret), K(visible_signature), KPC(param), K(limit_sql));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, limit_sql.ptr(),
                                          static_cast<int32_t>(limit_sql.length()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected column id", K(col_id), K(cell_idx), K(ret));
            break;
        }
      }
    }
  }
  return ret;
}

}//observer
}//oceanbase
