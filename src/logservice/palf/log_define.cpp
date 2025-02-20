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

#include "log_define.h"
#include "log_block_pool_interface.h"

namespace oceanbase
{
namespace palf
{

bool need_force_purge(PurgeThrottlingType type)
{
  return PURGE_BY_NOTIFY_FETCH_LOG == type;
}

const char *get_purge_throttling_type_str(PurgeThrottlingType type)
{
  const char *result= "UNKNOWN_TYPE";
  switch (type) {
    case INVALID_PURGE_TYPE :
      result = "PURGE_BY_RECONFIRM";
      break;
    case PURGE_BY_RECONFIRM:
      result = "PURGE_BY_RECONFIRM";
      break;
    case PURGE_BY_CHECK_BARRIER_CONDITION:
      result = "PURGE_BY_CHECK_BARRIER_CONDITION";
      break;
    case PURGE_BY_PRE_CHECK_FOR_CONFIG:
      result = "PURGE_BY_PRE_CHECK_FOR_CONFIG";
      break;
    case PURGE_BY_CHECK_SERVERS_LSN_AND_VERSION:
      result = "PURGE_BY_CHECK_SERVERS_LSN_AND_VERSION";
      break;
    case PURGE_BY_GET_MC_REQ:
      result = "PURGE_BY_GET_MC_REQ";
      break;
    case PURGE_BY_NOTIFY_FETCH_LOG:
      result = "PURGE_BY_NOTIFY_FETCH_LOG";
      break;
    default:
      PALF_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid purge type", K(type));
      break;
  }
  return result;
}

} // namespace palf
} // namespace oceanbase
