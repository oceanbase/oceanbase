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

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_barrier_log.h"

namespace oceanbase
{
namespace storage
{
ObTabletBarrierLogState::ObTabletBarrierLogState()
  : state_(TABLET_BARRIER_LOG_INIT), log_ts_(0), schema_version_(0)
{
}

ObTabletBarrierLogStateEnum ObTabletBarrierLogState::to_persistent_state() const
{
  ObTabletBarrierLogStateEnum persistent_state = TABLET_BARRIER_LOG_INIT;
  switch (state_) {
    case TABLET_BARRIER_LOG_INIT:
      // fall through
    case TABLET_BARRIER_LOG_WRITTING:
      persistent_state = TABLET_BARRIER_LOG_INIT;
      break;
    case TABLET_BARRIER_SOURCE_LOG_WRITTEN:
      persistent_state = TABLET_BARRIER_SOURCE_LOG_WRITTEN;
      break;
    case TABLET_BARRIER_DEST_LOG_WRITTEN:
      persistent_state = TABLET_BARRIER_DEST_LOG_WRITTEN;
      break;
  }
  return persistent_state;
}

void ObTabletBarrierLogState::reset()
{
  state_ = TABLET_BARRIER_LOG_INIT;
  log_ts_ = 0;
  schema_version_ = 0;
}

void ObTabletBarrierLogState::set_log_info(
    const ObTabletBarrierLogStateEnum state,
    const int64_t log_ts,
    const int64_t schema_version)
{
  state_ = state;
  log_ts_ = log_ts;
  schema_version_ = schema_version;
}

int ObTabletBarrierLogState::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObTabletBarrierLogStateEnum persistent_state = to_persistent_state();
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, persistent_state))) {
    LOG_WARN("fail to encode state", K(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, log_ts_))) {
    LOG_WARN("encode log ts failed", K(ret));
  }
  return ret;
}

int ObTabletBarrierLogState::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_state = 0;
  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_state))) {
    LOG_WARN("fail to decode state", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &log_ts_))) {
    LOG_WARN("decode log ts failed", K(ret));
  } else {
    state_ = static_cast<ObTabletBarrierLogStateEnum>(tmp_state);
  }
  return ret;
}

int64_t ObTabletBarrierLogState::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(to_persistent_state());
  len += serialization::encoded_length_i64(log_ts_);
  return len;
}
}
}

