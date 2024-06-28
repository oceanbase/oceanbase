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

#include "storage/tx/ob_multi_data_source_printer.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx/ob_committer_define.h"

namespace oceanbase
{
namespace transaction
{
const char *ObMultiDataSourcePrinter::to_str_mds_type(const ObTxDataSourceType &mds_type)
{
  const char *str = "INVALID";
  switch(mds_type)
  {
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, UNKNOWN);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, MEM_TABLE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TABLE_LOCK);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, LS_TABLE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, DDL_BARRIER);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, DDL_TRANS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, STANDBY_UPGRADE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, STANDBY_UPGRADE_DATA_VERSION);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, BEFORE_VERSION_4_1);

    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TEST1);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TEST2);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TEST3);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, CREATE_TABLET_NEW_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, DELETE_TABLET_NEW_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, UNBIND_TABLET_NEW_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_OUT);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_IN);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, FINISH_TRANSFER_OUT);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, FINISH_TRANSFER_IN);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TRANSFER_TASK);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_OUT_PREPARE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, START_TRANSFER_OUT_V2);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TRANSFER_MOVE_TX_CTX);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TRANSFER_DEST_PREPARE);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, CHANGE_TABLET_TO_TABLE_MDS);
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, TABLET_BINDING);

    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, MAX_TYPE);
  }
  return str;
}

const char *ObMultiDataSourcePrinter::to_str_notify_type(const NotifyType &notify_type)
{
  const char *str = "INVALID";
  switch(notify_type)
  {
    TRX_ENUM_CASE_TO_STR(NotifyType, UNKNOWN);
    TRX_ENUM_CASE_TO_STR(NotifyType, REGISTER_SUCC);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_REDO);
    TRX_ENUM_CASE_TO_STR(NotifyType, TX_END);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_PREPARE);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_COMMIT);
    TRX_ENUM_CASE_TO_STR(NotifyType, ON_ABORT);
  }
  return str;
}
}
}
