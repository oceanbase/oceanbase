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

#include "share/ob_proposal_id.h"

#include "lib/json/ob_yson_encode.h"
#include "lib/ob_name_id_def.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace common
{
void ObProposalID::reset()
{
  ts_ = OB_INVALID_TIMESTAMP;
  addr_.reset();
}

bool ObProposalID::is_valid() const
{
  return ts_ != OB_INVALID_TIMESTAMP && addr_.is_valid();
}

int ObProposalID::to_yson(char *buf, const int64_t buf_len, int64_t &pos) const
{
  return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, OB_ID(time_to_usec), ts_, OB_ID(server), addr_);
}

bool ObProposalID::operator < (const ObProposalID &pid) const
{
  return (ts_ == OB_INVALID_TIMESTAMP && pid.ts_ != OB_INVALID_TIMESTAMP)
      || (ts_ < pid.ts_)
      || ((ts_ == pid.ts_) && (addr_ < pid.addr_));
}

bool ObProposalID::operator > (const ObProposalID &pid) const
{
  return (ts_ != OB_INVALID_TIMESTAMP && pid.ts_ == OB_INVALID_TIMESTAMP)
      || (ts_ > pid.ts_)
      || ((ts_ == pid.ts_) && (pid.addr_ < addr_));
}

bool ObProposalID::operator >= (const ObProposalID &pid) const
{
  return !((*this) < pid);
}

bool ObProposalID::operator <= (const ObProposalID &pid) const
{
  return !((*this) > pid);
}

void ObProposalID::operator = (const ObProposalID &pid)
{
  ts_ = pid.ts_;
  addr_ = pid.addr_;
}

bool ObProposalID::operator == (const ObProposalID &pid) const
{
  return (ts_ == pid.ts_) && (addr_ == pid.addr_);
}

bool ObProposalID::operator != (const ObProposalID &pid2) const
{
  return !(*this == pid2);
}

DEFINE_SERIALIZE(ObProposalID)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int8_t current_version = addr_.using_ipv4() ? PROPOSAL_ID_VERSION : PROPOSAL_ID_VERSION6;

  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, current_version)))) {
    CLOG_LOG(WARN, "serialize version error", K(ret), K(buf), K(buf_len), K(pos), K(new_pos));
  }

  if (OB_SUCC(ret)) {
    if (PROPOSAL_ID_VERSION6 == current_version) {
      if ((OB_FAIL(addr_.serialize(buf, buf_len, new_pos)))) {
        CLOG_LOG(WARN, "serialize addr error", K(ret), K(buf), K(buf_len), K(pos), K(new_pos));
      }
    } else {
      if ((OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos,
                                             addr_.get_ipv4_server_id())))) {
        CLOG_LOG(WARN, "serialize addr error", K(ret), K(buf), K(buf_len), K(pos), K(new_pos));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if ((OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, ts_)))) {
    CLOG_LOG(WARN, "serialize timestamp error", K(ret), K(buf), K(buf_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObProposalID)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t server_id = 0;
  int8_t current_version = 0;

  if (OB_ISNULL(buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, &current_version)))) {
    // CLOG_LOG(WARN, "deserialize VERSION error", K(ret), K(buf), K(data_len), K(pos), K(new_pos));
  }

  if (OB_SUCC(ret)) {
    if (PROPOSAL_ID_VERSION6 == current_version) {
      ret = addr_.deserialize(buf, data_len, new_pos);
    } else {
      ret = serialization::decode_i64(buf, data_len, new_pos, &server_id);
      if (OB_SUCC(ret)) {
        addr_.set_ipv4_server_id(server_id);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if ((OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &ts_)))) {
    // CLOG_LOG(WARN, "deserialize timestamp error", K(ret), K(buf), K(data_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObProposalID)
{
  int64_t size = 0;
  int8_t current_version = addr_.using_ipv4() ? PROPOSAL_ID_VERSION : PROPOSAL_ID_VERSION6;
  size += serialization::encoded_length_i8(current_version);
  size += serialization::encoded_length_i64(ts_);
  if (PROPOSAL_ID_VERSION6 == current_version) {
    size += addr_.get_serialize_size();
  } else {
    size += serialization::encoded_length_i64(addr_.get_ipv4_server_id());
  }
  return size;
}

}//end namespace common
}//end namespace oceanbase
