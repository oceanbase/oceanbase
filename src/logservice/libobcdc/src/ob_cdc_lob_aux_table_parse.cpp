/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_aux_table_parse.h"

namespace oceanbase
{
namespace libobcdc
{
int ObCDCLobAuxMetaParse::parse_aux_lob_meta_table_row(
    ColValueList &cols,
    ObLobId &lob_id,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  ColValue *col_value = cols.head_;

  while (nullptr != col_value) {
    if (AUX_LOB_META_TABLE_LOB_ID_COLUMN_ID == col_value->column_id_) {
      if (OB_FAIL(get_lob_id_(*col_value, lob_id))) {
        LOG_ERROR("get_lob_id_ failed", KR(ret), KPC(col_value));
      }
    } else if (AUX_LOB_META_TABLE_LOB_DATA_COLUMN_ID == col_value->column_id_) {
      lob_data = col_value->value_.get_string().ptr();
      lob_data_len = col_value->value_.get_string().length();
    }

    col_value = col_value->next_;
  } // while

  return ret;
}

int ObCDCLobAuxMetaParse::get_lob_id_(
    ColValue &col_value,
    ObLobId &lob_id)
{
  int ret = OB_SUCCESS;

  lob_id = *reinterpret_cast<ObLobId*>(col_value.value_.get_string().ptr());

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
