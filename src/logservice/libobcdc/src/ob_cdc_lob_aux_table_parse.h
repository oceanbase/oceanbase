/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_PARSE_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_PARSE_H_

#include "common/object/ob_object.h"        // ObLobId
#include "lib/ob_define.h"                  // OB_APP_MIN_COLUMN_ID
#include "ob_log_part_trans_task.h"         // ColValueList

namespace oceanbase
{
namespace libobcdc
{
class ObCDCLobAuxMetaParse
{
public:
  static const int64_t AUX_LOB_META_TABLE_LOB_ID_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID;
  static const int64_t AUX_LOB_META_TABLE_LOB_DATA_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID + 5;

  static int parse_aux_lob_meta_table_row(
      ColValueList &cols,
      ObLobId &lob_id,
      const char *&lob_data,
      int64_t &lob_data_len);

private:
  static int get_lob_id_(
      ColValue &col_val,
      ObLobId &lob_id);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
