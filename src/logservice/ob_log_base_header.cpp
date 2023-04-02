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

#include "ob_log_base_header.h"
#include "lib/utility/ob_print_utils.h"         // Print*
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace logservice
{
ObLogBaseHeader::ObLogBaseHeader()
{
  reset();
}

ObLogBaseHeader::ObLogBaseHeader(const ObLogBaseType log_type,
                                 const enum ObReplayBarrierType replay_barrier_type,
                                 const int64_t replay_hint)
{
  version_ = BASE_HEADER_VERSION;
  log_type_ = log_type;
  flag_ = 0;
  replay_hint_ = replay_hint;
  switch (replay_barrier_type)
  {
  case ObReplayBarrierType::NO_NEED_BARRIER :
    break;
  case ObReplayBarrierType::PRE_BARRIER :
    flag_ = (flag_ | NEED_PRE_REPLAY_BARRIER_FLAG);
    break;
  case ObReplayBarrierType::STRICT_BARRIER :
    flag_ = (flag_ | NEED_POST_REPLAY_BARRIER_FLAG) | NEED_PRE_REPLAY_BARRIER_FLAG;
    break;
  default:
    flag_ = (flag_ | NEED_POST_REPLAY_BARRIER_FLAG) | NEED_PRE_REPLAY_BARRIER_FLAG;
    CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid replay barrier type", K(log_type), K(replay_barrier_type),
             K(replay_hint));
    break;
  }
}

ObLogBaseHeader::ObLogBaseHeader(const ObLogBaseType log_type,
                                 const enum ObReplayBarrierType replay_barrier_type)
  : ObLogBaseHeader(log_type,
                    replay_barrier_type,
                    common::ObTimeUtility::current_time())
{
}

ObLogBaseHeader::~ObLogBaseHeader()
{
  reset();
}

void ObLogBaseHeader::reset()
{
  version_ = 0;
  log_type_ = ObLogBaseType::INVALID_LOG_BASE_TYPE;
  flag_ = 0;
  replay_hint_ = 0;
}

bool ObLogBaseHeader::is_valid() const
{
  return version_ > 0 && log_type_ > 0;
}

bool ObLogBaseHeader::need_pre_replay_barrier() const
{
  return flag_ & NEED_PRE_REPLAY_BARRIER_FLAG;
}

bool ObLogBaseHeader::need_post_replay_barrier() const
{
  return flag_ & NEED_POST_REPLAY_BARRIER_FLAG;
}

ObLogBaseType ObLogBaseHeader::get_log_type() const
{
  return static_cast<ObLogBaseType>(log_type_);
}

int64_t ObLogBaseHeader::get_replay_hint() const
{
  return replay_hint_;
}

DEFINE_SERIALIZE(ObLogBaseHeader)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, version_))) {
    CLOG_LOG(WARN, "serialize replay_hint_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, log_type_))) {
    CLOG_LOG(WARN, "serialize log_type_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, flag_))) {
    CLOG_LOG(WARN, "serialize flag_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, replay_hint_))) {
    CLOG_LOG(WARN, "serialize replay_hint_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogBaseHeader)
{
  int ret = OB_SUCCESS;
  if ((OB_ISNULL(buf)) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &version_))) {
    CLOG_LOG(WARN, "deserialize replay_hint_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &log_type_))) {
    CLOG_LOG(WARN, "deserialize log_type_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &flag_))) {
    CLOG_LOG(WARN, "deserialize flag_ failed", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &replay_hint_))) {
    CLOG_LOG(WARN, "deserialize replay_hint_ failed", K(ret), KP(buf), K(data_len), K(pos));
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogBaseHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i16(log_type_);
  size += serialization::encoded_length_i32(flag_);
  size += serialization::encoded_length_i64(replay_hint_);
  return size;
}

} // namespace logservice
} // namespace oceanbase
