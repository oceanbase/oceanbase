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
 *
 * TableIdCache update info data struct in OBCDC
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_tic_update_info.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

void TICUpdateInfo::reset()
{
  reason_ = TICUpdateReason::INVALID_REASON;
  database_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
}

bool TICUpdateInfo::is_valid() const
{
  return reason_ != TICUpdateReason::INVALID_REASON && (database_id_ != OB_INVALID_ID
      || table_id_ != OB_INVALID_ID);
}

const char *TICUpdateInfo::print_tic_update_reason(const TICUpdateReason reason)
{
  const char *reason_str = "INVALID";
  switch (reason) {
    case INVALID_REASON:
      reason_str = "INVALID";
      break;

    case DROP_TABLE:
      reason_str = "DROP_TABLE";
      break;

    case CREATE_TABLE:
      reason_str = "CREATE_TABLE";
      break;

    case RENAME_TABLE_ADD:
      reason_str = "RENAME_TABLE_ADD";
      break;

    case RENAME_TABLE_REMOVE:
      reason_str = "RENAME_TABLE_REMOVE";
      break;

    case DROP_DATABASE:
      reason_str = "DROP_DATABASE";
      break;

    default:
      reason_str = "INVALID";
      break;
  }

  return reason_str;
}
}
}