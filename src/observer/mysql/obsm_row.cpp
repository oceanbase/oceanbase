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

#include "obsm_row.h"

#include "observer/mysql/obsm_utils.h"
#include "common/ob_accuracy.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

ObSMRow::ObSMRow(MYSQL_PROTOCOL_TYPE type, const ObNewRow& obrow, const ObDataTypeCastParams& dtc_params,
    const common::ColumnsFieldIArray* fields, ObSchemaGetterGuard* schema_guard, uint64_t tenant_id)
    : ObMySQLRow(type),
      obrow_(obrow),
      dtc_params_(dtc_params),
      fields_(fields),
      schema_guard_(schema_guard),
      tenant_id_(tenant_id)
{}

int64_t ObSMRow::get_cells_cnt() const
{
  return NULL == obrow_.projector_ ? obrow_.count_ : obrow_.projector_size_;
}

int ObSMRow::encode_cell(int64_t idx, char* buf, int64_t len, int64_t& pos, char* bitmap) const
{
  int ret = OB_SUCCESS;
  if (idx > get_cells_cnt() || idx < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t cell_idx = OB_LIKELY(NULL != obrow_.projector_) ? obrow_.projector_[idx] : idx;
    const ObObj* cell = &obrow_.cells_[cell_idx];

    if (NULL == fields_) {
      ret = ObSMUtils::cell_str(buf, len, *cell, type_, pos, idx, bitmap, dtc_params_, NULL, NULL);
    } else {
      ret = ObSMUtils::cell_str(
          buf, len, *cell, type_, pos, idx, bitmap, dtc_params_, &fields_->at(idx), schema_guard_, tenant_id_);
    }
  }

  return ret;
}
