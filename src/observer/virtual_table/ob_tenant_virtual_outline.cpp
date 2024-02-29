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

#include "observer/virtual_table/ob_tenant_virtual_outline.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/row/ob_row.h"
#include "lib/utility/utility.h"
namespace oceanbase
{
namespace observer
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

void ObTenantVirtualOutlineBase::reset()
{
  ObVirtualTableIterator::reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  outline_info_idx_ = OB_INVALID_INDEX;
  outline_infos_.reset();
  database_infos_.reuse();
}

int ObTenantVirtualOutlineBase::inner_open()
{
  int ret = OB_SUCCESS;
  const uint64_t BUCKET_NUM = 100;
  if (OB_UNLIKELY(NULL == schema_guard_
                  || OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is not init", K(ret), K(schema_guard_), K(tenant_id_));
  } else if (OB_FAIL(schema_guard_->get_outline_infos_in_tenant(tenant_id_, outline_infos_))) {
    LOG_WARN("fail to get outline infos", K(ret), K(tenant_id_), K(outline_infos_));
  } else if (OB_FAIL(database_infos_.create(BUCKET_NUM, ObModIds::OMT_VIRTUAL_TABLE, ObModIds::OMT_VIRTUAL_TABLE))) {
    LOG_WARN("fail to create hash map", K(ret), K(BUCKET_NUM));
  } else {
    outline_info_idx_ = 0;
  }
  return ret;
}

int ObTenantVirtualOutlineBase::set_database_infos_and_get_value(uint64_t database_id, bool &is_recycle)
{
  int ret = OB_SUCCESS;
  is_recycle = false;
  DBInfo db_info;
  ObString db_name;
  const ObDatabaseSchema *db_schema = NULL;
  if (OB_ISNULL(schema_guard_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", K(ret), K(schema_guard_), K(allocator_));
  } else if (database_id == OB_MOCK_DEFAULT_DATABASE_ID) {
    // virtual outline database
    if (OB_FAIL(ob_write_string(*allocator_, OB_MOCK_DEFAULT_DATABASE_NAME, db_name))) {
      LOG_WARN("fail to write string", K(ret), K(db_schema->get_database_name_str()));
    } else if (FALSE_IT(db_info.db_name_ = db_name)) {
    } else if (FALSE_IT(db_info.is_recycle_ = false)) {
    } else if (OB_FAIL(database_infos_.set_refactored(database_id, db_info))) {
      LOG_WARN("fail to set hash_map", K(ret), K(database_id));
    } else {
      is_recycle = false;
    }
  } else if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, database_id, db_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K_(tenant_id), K(database_id));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("db_schema is NULL", K(ret), K(database_id));
  } else if (OB_FAIL(ob_write_string(*allocator_, db_schema->get_database_name_str(), db_name))) {
    LOG_WARN("fail to write string", K(ret), K(db_schema->get_database_name_str()));
  } else if (FALSE_IT(db_info.db_name_ = db_name)) {
  } else if (FALSE_IT(db_info.is_recycle_ = db_schema->is_in_recyclebin())) {
  } else if (OB_FAIL(database_infos_.set_refactored(database_id, db_info))) {
    LOG_WARN("fail to set hash_map", K(ret), K(database_id));
  } else {
    is_recycle = db_schema->is_in_recyclebin();
  }
  return ret;

}

int ObTenantVirtualOutlineBase::is_database_recycle(uint64_t database_id, bool &is_recycle)
{
  int ret = OB_SUCCESS;
  DBInfo db_info;
  is_recycle = false;
  if (false == database_infos_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hash map is not created", K(ret));
  } else {
    ret = database_infos_.get_refactored(database_id, db_info);
    if (OB_SUCC(ret)) {
      is_recycle = db_info.is_recycle_;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(set_database_infos_and_get_value(database_id, is_recycle))) {
        LOG_WARN("fail to set database recyle", K(ret), K(database_id));
      }
    } else {
      LOG_WARN("fail to get hash value", K(ret));
    }
  }
  return ret;
}

void ObTenantVirtualOutline::reset()
{
  ObTenantVirtualOutlineBase::reset();
}

int ObTenantVirtualOutline::fill_cells(const ObOutlineInfo *outline_info)
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(cells = cur_row_.cells_)
      || OB_ISNULL(allocator_)
      || OB_ISNULL(session_)
      || OB_ISNULL(outline_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("some data member is NULL", K(ret), K(cells), K(allocator_), K(session_),
              K(outline_info));
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
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OUTLINE_NAME : {
          ObString outline_name;
          if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_name_str(),
                                      outline_name))) {
            LOG_WARN("fail to deep copy obstring", K(ret),
                      K(outline_info->get_name_str()), K(outline_name));
          } else {
            cells[cell_idx].set_varchar(outline_name);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case DATABASE_NAME : {
          DBInfo db_info;
          if (is_outline_database_id(outline_info->get_database_id())) {
            cells[cell_idx].set_varchar(OB_MOCK_DEFAULT_DATABASE_NAME);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else if (OB_FAIL(database_infos_.get_refactored(outline_info->get_database_id(),
                              db_info))) {
            LOG_WARN("fail to ge value from database_infos", K(ret),
                      K(outline_info->get_database_id()));
          } else {
            cells[cell_idx].set_varchar(db_info.db_name_);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SQL_TEXT : {
          ObString sql_text;
          if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_sql_text_str(), sql_text))) {
            LOG_WARN("fail to deep copy obstring", K(ret),
                      K(outline_info->get_sql_text_str()), K(sql_text));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, sql_text.ptr(),
                                          static_cast<int32_t>(sql_text.length()));
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OUTLINE_TARGET : {
          ObString outline_target;
          if (outline_info->get_sql_text_str().empty()) {
            cells[cell_idx].set_lob_value(ObLongTextType, "", 0);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_outline_target_str(),
                                              outline_target))) {
            LOG_WARN("fail to deep copy obstring", K(ret),
                      K(outline_info->get_outline_target_str()), K(outline_target));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, outline_target.ptr(),
                                          static_cast<int32_t>(outline_target.length()));
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OUTLINE_SQL : {
          ObString outline_sql;
          if (outline_info->get_sql_text_str().empty()) {
            cells[cell_idx].set_lob_value(ObLongTextType, "", 0);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else if (OB_FAIL(outline_info->get_outline_sql(*allocator_, *session_, outline_sql))) {
            LOG_WARN("fail to get outline_sql", K(ret));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, outline_sql.ptr(),
                                          static_cast<int32_t>(outline_sql.length()));
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SQL_ID : {
          ObString sql_id;
          if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_sql_id_str(), sql_id))) {
            LOG_WARN("fail to deep copy obstring", K(ret),
                      K(outline_info->get_sql_id_str()), K(sql_id));
          } else {
            cells[cell_idx].set_varchar(sql_id);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OUTLINE_CONTENT : {
          ObString outline_content;
          if (OB_FAIL(ob_write_string(*allocator_, outline_info->get_outline_content_str(),
                                      outline_content))) {
            LOG_WARN("fail to deep copy obstring", K(ret),
                      K(outline_info->get_outline_content_str()), K(outline_content));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, outline_content.ptr(),
                                          static_cast<int32_t>(outline_content.length()));
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case FORMAT_SQL_TEXT : {
          ObString str("");
          cells[cell_idx].set_lob_value(ObLongTextType, str.ptr(), 0);
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case FORMAT_SQL_ID : {
          cells[cell_idx].set_varchar("");
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case FORMAT_OUTLINE : {
          cells[cell_idx].set_int(static_cast<int64_t>(false));
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

int ObTenantVirtualOutline::is_output_outline(const ObOutlineInfo *outline_info, bool &is_output)
{
  int ret = OB_SUCCESS;
  is_output = false;
  bool is_recycle = false;
  if (OB_ISNULL(outline_info) || OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is NULL", K(ret), K(outline_info), K(schema_guard_));
  } else if (outline_info->get_outline_content_str().empty()) {
    is_output = false;
  } else if (is_outline_database_id(outline_info->get_database_id())) {
    // __outline_default_db is a logical table and has no physical schema.
    // Therefore, always output this.
    is_output = true;
  } else if (OB_FAIL(is_database_recycle(outline_info->get_database_id(), is_recycle))) {
    LOG_WARN("fail to judge database recycle", K(ret), KPC(outline_info));
  } else {
    is_output = !is_recycle;
  }
  return ret;
}
int ObTenantVirtualOutline::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObOutlineInfo *outline_info = NULL;
  bool is_output = false;
  if (outline_info_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid array idx", K(ret), K(outline_info_idx_));
  } else if (outline_info_idx_ >= outline_infos_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(outline_info = outline_infos_.at(outline_info_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outline info is NULL", K(ret), K(outline_info_idx_));
  } else if (OB_FAIL(is_output_outline(outline_info, is_output))) {
    LOG_WARN("fail to judge output", K(ret), KPC(outline_info));
  } else if (is_output) {
    if (OB_FAIL(fill_cells(outline_info))) {
      LOG_WARN("fail to fill cells", K(ret), K(outline_info), K(outline_info_idx_));
    } else {
      ++outline_info_idx_;
      row = &cur_row_;
    }
  } else {
    ++outline_info_idx_;
    if (OB_FAIL(inner_get_next_row(row))) {
      LOG_WARN("fail to get_next_row", K(ret));
    }
  }

  return ret;
}
}
}
