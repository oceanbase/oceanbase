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

#include "storage/tx_table/ob_tx_table_interface.h"
#include "storage/tx_table/ob_tx_table.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{

int ObTxTableGuard::init(ObTxTable *tx_table)
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx_data_table is nullptr.");
  } else {
    epoch_ = tx_table->get_epoch();
    tx_table_ = tx_table;
  }
  return ret;
}

} // end namespace transaction
} // end namespace oceanbase

