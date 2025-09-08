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
#include "observer/virtual_table/ob_tenant_show_catalog_databases.h"

#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"
#include "share/catalog/ob_cached_catalog_meta_getter.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{

namespace observer
{
void ObTenantShowCatalogDatabases::reset()
{
  catalog_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableIterator::reset();
}

int ObTenantShowCatalogDatabases::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(schema_guard_) || OB_ISNULL(session_) || !is_valid_id(tenant_id_) || OB_ISNULL(allocator_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member doesn't init", K(ret), K(schema_guard_), K(session_), K(tenant_id_), K(allocator_));
  } else if (key_ranges_.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support yet", K(ret));
  } else {
    // get catalog id
    ObRowkey start_key;
    ObRowkey end_key;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid_id(catalog_id_) && i < key_ranges_.count(); ++i) {
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
      const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
      if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
        if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0] && ObIntType == start_key_obj_ptr[0].get_type()) {
          catalog_id_ = start_key_obj_ptr[0].get_int();
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_external_catalog_id(catalog_id_))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "show tables from non external catalog");
    }
  }

  if (OB_SUCC(ret)) {
    // check catalog is existed first
    // then check privilege
    const ObCatalogSchema *catalog_schema = NULL;
    ObSessionPrivInfo priv_info;
    if (OB_FAIL(session_->get_session_priv_info(priv_info))) {
      LOG_WARN("fail to get session priv info", K(ret));
    } else if (OB_FAIL(schema_guard_->get_catalog_schema_by_id(priv_info.tenant_id_,
                                                               catalog_id_,
                                                               catalog_schema))) {
      LOG_WARN("fail to get catalog schema", K(ret));
    } else if (OB_ISNULL(catalog_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "current session's catalog is not existed");
    } else if (OB_FAIL(schema_guard_->check_catalog_access(priv_info,
                                                           session_->get_enable_role_array(),
                                                           catalog_id_))) {
      // todo only to check catalog access now
      LOG_WARN("check catalog priv failed", K(ret));
    }
  }
  return ret;
}

int ObTenantShowCatalogDatabases::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      LOG_WARN("fail to fill scanner", K(ret));
    } else {
      start_to_read_ = true;
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObTenantShowCatalogDatabases::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = nullptr;
  ObArray<ObString> db_names;
  if (output_column_ids_.count() > 0 && OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  } else {
    share::ObCachedCatalogMetaGetter ob_catalog_meta_getter{*schema_guard_, *allocator_};
    if (OB_FAIL(ob_catalog_meta_getter.list_namespace_names(tenant_id_, catalog_id_, db_names))) {
      LOG_WARN("list_namespace_names failed", K(ret), K(tenant_id_), K(catalog_id_));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < db_names.size(); i++) {
    uint64_t cell_idx = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
      const int64_t col_id = output_column_ids_.at(j);
      switch (col_id) {
        case static_cast<int64_t>(ALL_COLUMNS::CATALOG_ID):
          cells[cell_idx].set_int(catalog_id_);
          break;
        case static_cast<int64_t>(ALL_COLUMNS::DATABASE_NAME):
          cells[cell_idx].set_varchar(db_names.at(i));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }

    if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
      LOG_WARN("fail to add row", K(ret), K(cur_row_));
    }
  }

  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}

} // namespace observer

} // namespace oceanbase