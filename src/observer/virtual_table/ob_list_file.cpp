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
#include "observer/virtual_table/ob_list_file.h"
#include "share/schema/ob_schema_printer.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"


using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObListFile::ObListFile()
    : ObVirtualTableScannerIterator()
{
}

ObListFile::~ObListFile()
{
}

void ObListFile::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObListFile::inner_open()
{
  int ret = OB_SUCCESS;
  const ObLocationSchema *location_schema = NULL;
  uint64_t location_id = OB_INVALID_ID;
  ObString sub_path;
  ObString pattern;
  if (OB_FAIL(resolve_param(location_id, sub_path, pattern))) {
    LOG_WARN("fail to calc show location id", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == location_id)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "list file error");
  } else if (OB_UNLIKELY(NULL == schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard_ is null", K(ret), K(location_id));
  } else if (OB_FAIL(schema_guard_->get_location_schema_by_id(effective_tenant_id_,
                                                              location_id, location_schema))) {
    LOG_WARN("failed to get location_schema", K(ret), K_(effective_tenant_id), K(location_id));
  } else if (OB_UNLIKELY(NULL == location_schema)) {
    ret = OB_LOCATION_OBJ_NOT_EXIST;
    LOG_WARN("location_schema is null", K(ret), K(location_id));
  } else {
    ObArray<share::ObExternalTableBasicFileInfo> basic_file_infos;
    ObSqlString tmp;
    ObExprRegexpSessionVariables regexp_vars;
    if (ObSQLUtils::is_external_files_on_local_disk(location_schema->get_location_url())) {
      OZ (ObSQLUtils::check_location_access_priv(location_schema->get_location_url(), session_));
    }
    OZ (session_->get_regexp_session_vars(regexp_vars));
    ObSqlString full_path;
    OZ (full_path.append(location_schema->get_location_url_str()));
    if (OB_SUCC(ret) && full_path.length() > 0
        && *(full_path.ptr() + full_path.length() - 1) != '/'
        && !sub_path.empty()
        && sub_path[0] != '/') {
      OZ (full_path.append("/"));
    }
    if (OB_SUCC(ret) && sub_path != "/") {
      OZ (full_path.append(sub_path));
    }
    OZ (ObExternalTableUtils::collect_external_file_list(
      nullptr,
      location_schema->get_tenant_id(), -1 /*table id(UNUSED)*/,
      full_path.string(),
      location_schema->get_location_access_info(),
      pattern,
      "",
      false,
      regexp_vars,
      *allocator_,
      tmp,
      basic_file_infos));
    for (int64_t i = 0; OB_SUCC(ret) && i < basic_file_infos.count(); i++) {
      const ObString &file_url = basic_file_infos.at(i).url_;
      int64_t file_size = basic_file_infos.at(i).size_;
      if (OB_FAIL(fill_row_cells(location_id, sub_path, pattern, file_url, file_size))) {
        LOG_WARN("fail to fill row cells", K(ret), K(file_url));
      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
        LOG_WARN("fail to add row", K(ret), K(cur_row_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}

int ObListFile::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get_next_row failed", KR(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObListFile::resolve_param(uint64_t &location_id, ObString &sub_path, ObString &pattern)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0;
      OB_SUCCESS == ret && OB_INVALID_ID == location_id && i < key_ranges_.count(); ++i) {
    ObRowkey start_key = key_ranges_.at(i).start_key_;
    ObRowkey end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() >= 3 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else{
        if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
            && ObIntType == start_key_obj_ptr[0].get_type()) {
          location_id = start_key_obj_ptr[0].get_int();
        }
        if (start_key_obj_ptr[1] == end_key_obj_ptr[1]
            && ObVarcharType == start_key_obj_ptr[1].get_type()) {
          start_key_obj_ptr[1].get_varchar(sub_path);
        }
        if (start_key_obj_ptr[2] == end_key_obj_ptr[2]
            && ObVarcharType == start_key_obj_ptr[2].get_type()) {
          start_key_obj_ptr[2].get_varchar(pattern);
        }
      }
    }
  }
  return ret;
}

int ObListFile::fill_row_cells(uint64_t location_id,
                               const ObString &sub_path,
                               const ObString &pattern,
                               const ObString &file_url,
                               int64_t file_size)
{
  int ret = OB_SUCCESS;
  bool strict_mode = false;
  bool sql_quote_show_create = true;
  bool ansi_quotes = false;
  if (OB_ISNULL(cur_row_.cells_)
      || OB_ISNULL(schema_guard_)
      || OB_ISNULL(allocator_)
      || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("class isn't inited", K(cur_row_.cells_), K(schema_guard_), K(allocator_), K(session_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN,
              "cur row cell count is less than output coumn",
              K(ret),
              K(cur_row_.count_),
              K(output_column_ids_.count()));
  } else if (OB_FAIL(session_->get_show_ddl_in_compat_mode(strict_mode))) {
    SERVER_LOG(WARN, "failed to get _show_ddl_in_compat_mode", K(ret));
  } else if (OB_FAIL(session_->get_sql_quote_show_create(sql_quote_show_create))) {
    SERVER_LOG(WARN, "failed to get sql_quote_show_create", K(ret));
  } else if (OB_FALSE_IT(IS_ANSI_QUOTES(session_->get_sql_mode(), ansi_quotes))) {
    // do nothing
  } else {
    uint64_t cell_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case OB_APP_MIN_COLUMN_ID : {
          // location id
          cur_row_.cells_[cell_idx].set_int(location_id);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1 : {
          // sub path
          cur_row_.cells_[cell_idx].set_varchar(sub_path);
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2 : {
          // pattern
          cur_row_.cells_[cell_idx].set_varchar(pattern);
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 3 : {
          // file name
          cur_row_.cells_[cell_idx].set_varchar(file_url);
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 4 : {
          // file size
          cur_row_.cells_[cell_idx].set_int(file_size);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(cell_idx),
                    K(i), K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
  }
  return ret;
}
}/* ns observer*/
}/* ns oceanbase */
