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
#define USING_LOG_PREFIX LIB
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace common
{
extern "C" {
int ob_decode_pcode(const char *buf, const int64_t data_len, int64_t *pos, uint32_t *val)
{
  int ret = (NULL != buf  && NULL != pos && data_len - *pos  >= 4) ? OB_SUCCESS : OB_DESERIALIZE_ERROR;
  if (OB_SUCC(ret)) {
    *val = ((static_cast<uint32_t>(*(buf + (*pos)++))) & 0xff) << 24;
    *val |= ((static_cast<uint32_t>(*(buf + (*pos)++))) & 0xff) << 16;
    *val |= ((static_cast<uint32_t>(*(buf + (*pos)++))) & 0xff) << 8;
    *val |= ((static_cast<uint32_t>(*(buf + (*pos)++))) & 0xff);
  }
  return ret;
}

int ob_is_bypass_pcode(uint32_t pcode)
{
  //table_api pcode directly bypass
  int bret = 0;
  if (pcode >= OB_TABLE_API_LOGIN &&  pcode <= OB_TABLE_API_MOVE) { //table_api
    bret = 1;
  } else if (OB_LOG_REQ_START_LSN_BY_TS == pcode || OB_LS_FETCH_LOG2 == pcode
            || OB_LS_FETCH_MISSING_LOG == pcode) { //libcdc or standby cluster
    bret = 1;
  }
  return bret;
}

int ob_is_tableapi_pcode(uint32_t pcode)
{
  int bret = 0;
  if (pcode >= OB_TABLE_API_LOGIN &&  pcode <= OB_TABLE_API_MOVE) { //table_api
    bret = 1;
  }
  return bret;
}

int ob_judge_is_tableapi_pcode_from_raw_packet(const char *buf, ssize_t data_len)
{
  int bret = 0;
  int ret = OB_SUCCESS;
  if (NULL != buf) {
    int demand_length = OB_NET_HEADER_LENGTH + 4;
    if (data_len >= demand_length && 0 == memcmp(buf, ObRpcPacket::MAGIC_HEADER_FLAG,
        sizeof(ObRpcPacket::MAGIC_HEADER_FLAG))) {
      int64_t pos = 0;
      uint32_t pcode = 0;
      if (OB_SUCC(ob_decode_pcode(buf + OB_NET_HEADER_LENGTH, 4, &pos, &pcode))) {
        if (ob_is_tableapi_pcode(pcode)) {
          bret = 1;
        }
      }
    }
  }
  return bret;
}
}
}
}
