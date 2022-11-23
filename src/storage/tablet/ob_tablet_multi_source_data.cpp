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

#include "storage/tablet/ob_tablet_multi_source_data.h"

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_range.h"

using namespace oceanbase::common;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{
ObTabletTxMultiSourceDataUnit::ObTabletTxMultiSourceDataUnit()
  : version_(TX_DATA_VERSION),
    length_(0),
    tx_id_(ObTabletCommon::FINAL_TX_ID),
    tx_log_ts_(ObLogTsRange::MAX_TS),
    tablet_status_()
{
}

ObTabletTxMultiSourceDataUnit::~ObTabletTxMultiSourceDataUnit()
{
  reset();
}

ObTabletTxMultiSourceDataUnit::ObTabletTxMultiSourceDataUnit(const ObTabletTxMultiSourceDataUnit &other)
  : ObIMultiSourceDataUnit(other),
    version_(other.version_),
    length_(other.length_),
    tx_id_(other.tx_id_),
    tx_log_ts_(other.tx_log_ts_),
    tablet_status_(other.tablet_status_)
{
}

ObTabletTxMultiSourceDataUnit &ObTabletTxMultiSourceDataUnit::operator=(const ObTabletTxMultiSourceDataUnit &other)
{
  if (this != &other) {
    ObIMultiSourceDataUnit::operator=(other);
    version_ = other.version_;
    length_ = other.length_;
    tx_id_ = other.tx_id_;
    tx_log_ts_ = other.tx_log_ts_;
    tablet_status_ = other.tablet_status_;
  }
  return *this;
}

int ObTabletTxMultiSourceDataUnit::deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(src));
  } else if (OB_UNLIKELY(MultiSourceDataUnitType::TABLET_TX_DATA != src->type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), KPC(src));
  } else {
    const ObTabletTxMultiSourceDataUnit *data = static_cast<const ObTabletTxMultiSourceDataUnit*>(src);
    version_ = data->version_;
    length_ = data->length_;
    tx_id_ = data->tx_id_;
    tx_log_ts_ = data->tx_log_ts_;
    tablet_status_ = data->tablet_status_;
  }

  return ret;
}

void ObTabletTxMultiSourceDataUnit::reset()
{
  version_ = TX_DATA_VERSION;
  length_ = 0;
  tx_id_.reset();
  tx_log_ts_ = ObLogTsRange::MAX_TS;
  tablet_status_ = ObTabletStatus::MAX;
}

int64_t ObTabletTxMultiSourceDataUnit::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(version),
       K_(length),
       K_(tx_id),
       K_(tx_log_ts),
       K_(tablet_status),
       K_(is_tx_end),
       K_(unsynced_cnt_for_multi_data));
  J_OBJ_END();
  return pos;
}

bool ObTabletTxMultiSourceDataUnit::is_valid() const
{
  return //tx_id_.is_valid()
      // && tx_log_ts_ > OB_INVALID_TIMESTAMP
      tablet_status_.is_valid();
}

int64_t ObTabletTxMultiSourceDataUnit::get_data_size() const
{
  int64_t size = sizeof(ObTabletTxMultiSourceDataUnit);
  return size;
}

int ObTabletTxMultiSourceDataUnit::serialize(
    char *buf,
    const int64_t len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (FALSE_IT(length_ = get_serialize_size())) {
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize version", K(ret), K(len), K(new_pos), K_(version));
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, length_))) {
    LOG_WARN("failed to serialize length", K(ret), K(len), K(new_pos), K_(length));
  } else if (OB_FAIL(tx_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tx id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, tx_log_ts_))) {
    LOG_WARN("failed to serialize tx log ts", K(ret), K(len), K(new_pos), K_(tx_log_ts));
  } else if (OB_FAIL(tablet_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet status", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(pos + length_ != new_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize length does not match member length", K(ret), K(pos), K_(length), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int ObTabletTxMultiSourceDataUnit::deserialize(
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, &version_))) {
    LOG_WARN("failed to deserialize version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, &length_))) {
    LOG_WARN("failed to deserialize length", K(ret), K(len), K(new_pos));
  } else if (TX_DATA_VERSION == version_) {
    if (OB_FAIL(new_pos - pos < length_ && tx_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tx id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &tx_log_ts_))) {
      LOG_WARN("failed to deserialize tx log ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(tablet_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tablet status", K(ret), K(len), K(new_pos));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(pos + length_ != new_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, deserialize size does not match length", K(ret), K(pos), K_(length), K(new_pos));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObTabletTxMultiSourceDataUnit::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i32(version_);
  size += serialization::encoded_length_i32(length_);
  size += tx_id_.get_serialize_size();
  size += serialization::encoded_length_i64(tx_log_ts_);
  size += tablet_status_.get_serialize_size();
  return size;
}

int ObTabletTxMultiSourceDataUnit::set_log_ts(const int64_t log_ts)
{
  tx_log_ts_ = log_ts;

  return OB_SUCCESS;
}

} // namespace storage
} // namespace oceanbase
