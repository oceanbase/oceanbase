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
#include "observer/virtual_table/ob_all_virtual_compatibility_control.h"
#include "share/ob_compatibility_control.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{
using namespace oceanbase::common;
using namespace oceanbase::share;

ObVirtualCompatibilityConflictControl::ObVirtualCompatibilityConflictControl()
  : ObVirtualTableScannerIterator()
{
}

ObVirtualCompatibilityConflictControl::~ObVirtualCompatibilityConflictControl()
{
  reset();
}

void ObVirtualCompatibilityConflictControl::reset()
{
}

int ObVirtualCompatibilityConflictControl::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      SERVER_LOG(WARN, "fail to fill scanner", K(ret));
    } else {
      start_to_read_ = true;
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObVirtualCompatibilityConflictControl::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const ObICompatInfo **infos = NULL;
  int64_t info_len = 0;
  ObCompatControl::get_compat_feature_infos(infos, info_len);
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  uint64_t compat_version = 0;
  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (output_column_ids_.count() > 0 && OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  } else if (OB_ISNULL(infos) || OB_UNLIKELY(ObCompatFeatureType::COMPAT_FEATURE_END > info_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid compat feature info array", K(ret), KP(infos), K(info_len));
  } else if (OB_FAIL(session_->get_compatibility_version(compat_version))) {
    LOG_WARN("failed to get compat version", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ObCompatFeatureType::COMPAT_FEATURE_END; ++i) {
    int cell_idx = 0;
    const ObICompatInfo *info = infos[i];
    if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
      int64_t col_id = output_column_ids_.at(j);
      switch(col_id) {
        case TENAND_ID: {
          cells[cell_idx].set_int(MTL_ID());
          break;
        }
        case NAME: {
          cells[cell_idx].set_varchar(info->name_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case DESCRIPTION: {
          cells[cell_idx].set_varchar(info->description_);
          cells[cell_idx].set_collation_type(coll_type);
          break;
        }
        case IS_ENABLE: {
          cells[cell_idx].set_bool(info->is_valid_version(compat_version));
          break;
        }
        case ENABLE_VERSIONS: {
          ObString range_str;
          if (OB_FAIL(info->print_version_range(range_str, *allocator_))) {
            LOG_WARN("failed to print version range", K(ret));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, range_str.ptr(),
                                          static_cast<int32_t>(range_str.length()));
            cells[cell_idx].set_collation_type(coll_type);
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(cell_idx), K(j), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
      LOG_WARN("failed to add row", K(ret), K(cur_row_));
    }
  }
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}

}
}
