/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_cdc_req_struct.h"

namespace oceanbase
{

namespace obrpc
{

OB_SERIALIZE_MEMBER(ObCdcRpcId, client_pid_, client_addr_);

int ObCdcRpcId::init(const uint64_t pid, const ObAddr &addr) {
  int ret = OB_SUCCESS;
  if (pid > 0 && addr.is_valid()) {
    // addr may not be valid
    client_pid_ = pid;
    client_addr_ = addr;
  } else {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid arguments for ObCdcRpcId", KR(ret), K(pid), K(addr));
  }
  return ret;
}

ObCdcFetchLogProtocolType get_fetch_log_protocol_type(const ObString &proto_type_str)
{
  ObCdcFetchLogProtocolType type = ObCdcFetchLogProtocolType::Unknown;
  if (0 == proto_type_str.case_compare("v1")) {
    type = ObCdcFetchLogProtocolType::LogGroupEntryProto;
  } else if (0 == proto_type_str.case_compare("v2")) {
    type = ObCdcFetchLogProtocolType::RawLogDataProto;
  }
  return type;
}

const char *fetch_log_protocol_type_str(const ObCdcFetchLogProtocolType type)
{
  const char *type_str = "UNKNOWN";
  switch (type) {
    case ObCdcFetchLogProtocolType::LogGroupEntryProto:
      type_str = "V1";
      break;
    case ObCdcFetchLogProtocolType::RawLogDataProto:
      type_str = "V2";
      break;
    default:
      type_str = "UNKNOWN";
      break;
  }
  return type_str;
}

}

}