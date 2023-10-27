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
#include "lib/list/ob_dlist.h"
#include "share/ob_errno.h"
#include "log_block_pool_interface.h"

namespace oceanbase
{
namespace palf
{

int convert_sys_errno()
{
  int ret = OB_IO_ERROR;
  switch (errno) {
  case EACCES:
    ret = OB_FILE_OR_DIRECTORY_PERMISSION_DENIED;
    break;
  case ENOENT:
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    break;
  case EEXIST:
  case ENOTEMPTY:
    ret = OB_FILE_OR_DIRECTORY_EXIST;
    break;
  case EAGAIN:
    ret = OB_EAGAIN;
    break;
  case EDQUOT:
  case ENOSPC:
    ret = OB_ALLOCATE_DISK_SPACE_FAILED;
    break;
  case ENOMEM:
    ret = OB_ALLOCATE_MEMORY_FAILED;
    break;
  default:
    ret = OB_IO_ERROR;
  }
  return ret;
}

int block_id_to_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu", block_id);
  }
  return ret;
}

int block_id_to_tmp_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu.tmp", block_id);
  }
  return ret;
}

int block_id_to_flashback_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu.flashback", block_id);
  }
  return ret;
}

bool is_number(const char* str)
{
	bool bool_ret = true;
  for (int64_t i = 0; true == bool_ret; ++i) {
    if ('\0' == str[i]) {
      break;
    } else if (!isdigit(str[i])) {
      bool_ret = false;
		}
  }
	return bool_ret;
}

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
