/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

    case RECOVER_TABLE_END:
      reason_str = "RECOVER_TABLE_END";
      break;

    default:
      reason_str = "INVALID";
      break;
  }

  return reason_str;
}
}
}