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

#include "ob_multi_data_source_printer.h"
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
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, BEFORE_VERSION_4_1);

    #define NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
    #define _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) \
    TRX_ENUM_CASE_TO_STR(ObTxDataSourceType, ENUM_NAME);
    #include "storage/multi_data_source/compile_utility/mds_register.h"
    #undef _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_
    #undef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION

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
