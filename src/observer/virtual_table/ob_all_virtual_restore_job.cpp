/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_all_virtual_restore_job.h"
#include "share/ob_i_tablet_scan.h"
#include "share/restore/ob_physical_restore_info.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace observer
{

int ObAllVirtualRestoreJob::construct_columns(
    const uint64_t exec_tenant_id,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIteratePrivateVirtualTable::construct_columns(exec_tenant_id, sql))) {
    LOG_WARN("construct columns failed", KR(ret));
  } else if (OB_ALL_RESTORE_JOB_TID == base_table_id_ && OB_FAIL(sql.append(", `name`"))) {
    // Fetch the discriminator even when the caller only projects VALUE.
    LOG_WARN("append restore field name failed", KR(ret));
  }
  return ret;
}

int ObAllVirtualRestoreJob::try_convert_row(
    const ObNewRow *input_row,
    ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const bool restore_job = OB_ALL_RESTORE_JOB_TID == base_table_id_;
  const bool recover_table = OB_ALL_RECOVER_TABLE_JOB_TID == base_table_id_
      || OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID == base_table_id_;
  if (OB_ISNULL(input_row) || OB_ISNULL(input_row->cells_) || OB_ISNULL(scan_param_)
      || (cur_row_.count_ > 0 && OB_ISNULL(cur_row_.cells_))
      || input_row->count_ < cur_row_.count_ + (restore_job ? 1 : 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input row", KR(ret), KP(input_row), K(cur_row_.count_));
  } else if (OB_FAIL(ObIteratePrivateVirtualTable::try_convert_row(input_row, row))) {
    LOG_WARN("convert restore row failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_row_.count_; ++i) {
    const ObObj &input = input_row->get_cell(i);
    const ObString &column = mapping_[scan_param_->column_ids_.at(i)].base_col_name_;
    const bool restore_history_value = (OB_ALL_RESTORE_JOB_HISTORY_TID == base_table_id_ || recover_table)
        && (column == ObString::make_string("`backup_dest`") || column == ObString::make_string("`backup_set_list`")
            || column == ObString::make_string("`backup_piece_list`") || column == ObString::make_string("`restore_option`"));
    const bool recover_secret = recover_table && (column == ObString::make_string("`backup_passwd`")
        || column == ObString::make_string("`external_kms_info`"));
    if (!input.is_null() && ((restore_job && column == ObString::make_string("`value`"))
        || restore_history_value || recover_secret)) {
      ObString value;
      ObString display_value;
      const ObString name = restore_job ? input_row->get_cell(input_row->count_ - 1).get_string()
          : ObString(column.length() - 2, column.ptr() + 1);
      if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&convert_alloc_, input, value))) {
        LOG_WARN("read restore value failed", KR(ret), K(name));
      } else if (OB_FAIL(ObPhysicalRestoreJob::get_display_value(name, value, convert_alloc_, display_value))) {
        LOG_WARN("convert restore value failed", KR(ret), K(name));
      } else {
        ObObj &output = cur_row_.cells_[i];
        output.reset();
        if (input.is_lob_storage()) {
          output.set_lob_value(input.get_type(), display_value.ptr(), display_value.length());
        } else {
          output.set_string(input.get_type(), display_value);
        }
        output.set_collation_type(input.get_collation_type());
      }
    }
  }
  if (OB_FAIL(ret)) {
    row = nullptr;
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
