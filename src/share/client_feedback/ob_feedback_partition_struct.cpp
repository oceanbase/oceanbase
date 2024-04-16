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

#define USING_LOG_PREFIX SHARE

#include "share/client_feedback/ob_feedback_partition_struct.h"

namespace oceanbase
{
namespace share
{
using namespace common;

int ObFeedbackReplicaLocation::serialize_struct_content(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  const int32_t version = server_.get_version();
  OB_FB_ENCODE_INT(version);
  if (ObAddr::IPV4 == version) {
    OB_FB_ENCODE_INT(server_.get_ipv4());
  } else if (ObAddr::IPV6 == version) {
    OB_FB_ENCODE_INT(server_.get_ipv6_high());
    OB_FB_ENCODE_INT(server_.get_ipv6_low());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid server", K_(server), K(ret));
  }
  OB_FB_ENCODE_INT(server_.get_port());
  OB_FB_ENCODE_INT(role_);
  OB_FB_ENCODE_INT(replica_type_);
  OB_FB_SER_END;
}

int ObFeedbackReplicaLocation::deserialize_struct_content(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  int32_t version = 0;
  int32_t ipv4 = 0;
  int32_t port = 0;
  uint64_t ipv6_high = 0;
  uint64_t ipv6_low = 0;
  OB_FB_DECODE_INT(version, int32_t);

  if (ObAddr::IPV4 == version) {
    OB_FB_DECODE_INT(ipv4, int32_t);
  } else if (ObAddr::IPV6 == version) {
    OB_FB_DECODE_INT(ipv6_high, uint64_t);
    OB_FB_DECODE_INT(ipv6_low, uint64_t);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ip version", K(version), K(ret));
  }
  OB_FB_DECODE_INT(port, int32_t);
  OB_FB_DECODE_INT(role_, common::ObRole);
  OB_FB_DECODE_INT(replica_type_, common::ObReplicaType);
  if (OB_SUCC(ret)) {
    if (ObAddr::IPV4 == version) {
      server_.set_ipv4_addr(ipv4, port);
    } else if (ObAddr::IPV6 == version){
      server_.set_ipv6_addr(ipv6_high, ipv6_low, port);
    }
  }
  OB_FB_DESER_END;
}

int ObFeedbackPartitionLocation::serialize_struct_content(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  OB_FB_ENCODE_INT(table_id_);
  OB_FB_ENCODE_INT(partition_id_);
  OB_FB_ENCODE_INT(schema_version_);
  OB_FB_ENCODE_STRUCT_ARRAY(replicas_, replicas_.count());
  OB_FB_SER_END;
}

int ObFeedbackPartitionLocation::deserialize_struct_content(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  OB_FB_DECODE_INT(table_id_, uint64_t);
  OB_FB_DECODE_INT(partition_id_, int64_t);
  OB_FB_DECODE_INT(schema_version_, int64_t);
  OB_FB_DECODE_STRUCT_ARRAY(replicas_, ObFeedbackReplicaLocation);
  OB_FB_DESER_END;
}

void ObFeedbackRerouteInfo::assign(const ObFeedbackRerouteInfo &info)
{
  server_ = info.server_;
  role_ = info.role_;
  replica_type_ = info.replica_type_;
  MEMCPY(tbl_name_, info.tbl_name_, info.tbl_name_len_);
  tbl_name_len_ = info.tbl_name_len_;
  tbl_schema_version_ = info.tbl_schema_version_;
  for_session_reroute_ = info.for_session_reroute_;
}

int ObFeedbackRerouteInfo::set_tbl_name(const ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (table_name.length() > OB_MAX_TABLE_NAME_LENGTH) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid table name", K(ret), K(table_name));
  } else {
    MEMCPY(tbl_name_, table_name.ptr(), table_name.length());
    tbl_name_len_ = table_name.length();
  }
  return ret;
}

int ObFeedbackRerouteInfo::serialize_struct_content(char *buf,
                                                    const int64_t len,
                                                    int64_t &pos) const
{
  OB_FB_SER_START;
  const int32_t version = server_.get_version();
  OB_FB_ENCODE_INT(version);
  if (ObAddr::IPV4 == version) {
    OB_FB_ENCODE_INT(server_.get_ipv4());
  } else if (ObAddr::IPV6 == version) {
    OB_FB_ENCODE_INT(server_.get_ipv6_high());
    OB_FB_ENCODE_INT(server_.get_ipv6_low());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid server", K_(server), K(ret));
  }
  OB_FB_ENCODE_INT(server_.get_port());
  OB_FB_ENCODE_INT(role_);
  OB_FB_ENCODE_INT(replica_type_);
  OB_FB_ENCODE_STRING(tbl_name_, tbl_name_len_);
  OB_FB_ENCODE_INT(tbl_schema_version_);
  OB_FB_SER_END;
}

int ObFeedbackRerouteInfo::deserialize_struct_content(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  int32_t version = 0;
  int32_t ipv4 = 0;
  int32_t port = 0;
  uint64_t ipv6_high = 0;
  uint64_t ipv6_low = 0;
  OB_FB_DECODE_INT(version, int32_t);

  if (ObAddr::IPV4 == version) {
    OB_FB_DECODE_INT(ipv4, int32_t);
  } else if (ObAddr::IPV6 == version) {
    OB_FB_DECODE_INT(ipv6_high, uint64_t);
    OB_FB_DECODE_INT(ipv6_low, uint64_t);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ip version", K(version), K(ret));
  }
  OB_FB_DECODE_INT(port, int32_t);
  OB_FB_DECODE_INT(role_, common::ObRole);
  OB_FB_DECODE_INT(replica_type_, common::ObReplicaType);
  if (OB_SUCC(ret)) {
    if (ObAddr::IPV4 == version) {
      server_.set_ipv4_addr(ipv4, port);
    } else {
      // must IPV6, not support yet, TODO open below later
      //server_.set_ipv6_addr(ipv6_high, ipv6_low, port);
      server_.reset();
    }
  }
  OB_FB_DECODE_STRING(tbl_name_, OB_MAX_TABLE_NAME_LENGTH, tbl_name_len_);
  OB_FB_DECODE_INT(tbl_schema_version_, int64_t);
  OB_FB_DESER_END;
}

} // end namespace share
} // end namespace oceanbase
