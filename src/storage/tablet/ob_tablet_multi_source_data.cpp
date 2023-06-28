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
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
ObTabletTxMultiSourceDataUnit::ObTabletTxMultiSourceDataUnit()
  : version_(TX_DATA_VERSION),
    length_(0),
    tx_id_(ObTabletCommon::FINAL_TX_ID),
    tablet_status_(),
    transfer_seq_(0),
    transfer_ls_id_(),
    transfer_scn_()
{
  tx_scn_.set_max();
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
    tx_scn_(other.tx_scn_),
    tablet_status_(other.tablet_status_),
    transfer_seq_(other.transfer_seq_),
    transfer_ls_id_(other.transfer_ls_id_),
    transfer_scn_(other.transfer_scn_)
{
}

ObTabletTxMultiSourceDataUnit &ObTabletTxMultiSourceDataUnit::operator=(const ObTabletTxMultiSourceDataUnit &other)
{
  if (this != &other) {
    ObIMultiSourceDataUnit::operator=(other);
    version_ = other.version_;
    length_ = other.length_;
    tx_id_ = other.tx_id_;
    tx_scn_ = other.tx_scn_;
    tablet_status_ = other.tablet_status_;
    transfer_seq_ = other.transfer_seq_;
    transfer_ls_id_ = other.transfer_ls_id_;
    transfer_scn_ = other.transfer_scn_;
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
    tx_scn_ = data->tx_scn_;
    tablet_status_ = data->tablet_status_;
    transfer_seq_ = data->transfer_seq_;
    transfer_ls_id_ = data->transfer_ls_id_;
    transfer_scn_ = data->transfer_scn_;
  }

  return ret;
}

void ObTabletTxMultiSourceDataUnit::reset()
{
  version_ = TX_DATA_VERSION;
  length_ = 0;
  tx_id_.reset();
  tx_scn_.reset();
  tx_scn_.set_max();
  tablet_status_ = ObTabletStatus::MAX;
  transfer_seq_ = 0;
  transfer_ls_id_.reset();
  transfer_scn_.reset();
}

int64_t ObTabletTxMultiSourceDataUnit::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(version),
       K_(length),
       K_(tx_id),
       K_(tx_scn),
       K_(tablet_status),
       K_(is_tx_end),
       K_(unsynced_cnt_for_multi_data),
       K_(transfer_seq),
       K_(transfer_ls_id),
       K_(transfer_scn));
  J_OBJ_END();
  return pos;
}

bool ObTabletTxMultiSourceDataUnit::is_valid() const
{
  return //tx_id_.is_valid()
      // && tx_scn_ > OB_INVALID_TIMESTAMP
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
  } else if (OB_FAIL(tx_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tx scn", K(ret), K(len), K(new_pos), K_(tx_scn));
  } else if (OB_FAIL(tablet_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet status", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::encode_i64(buf, len, new_pos, transfer_seq_))) {
    LOG_WARN("failed to serialize tx log ts", K(ret), K(len), K(new_pos), K_(transfer_seq));
  } else if (OB_FAIL(transfer_ls_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize transfer ls id", K(ret), K(len), K(new_pos), K_(transfer_ls_id));
  } else if (OB_FAIL(transfer_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize transfer scn", K(ret), K(len), K(new_pos), K_(transfer_scn));
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
    } else if (new_pos - pos < length_ && OB_FAIL(tx_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tx scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(tablet_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tablet status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &transfer_seq_))) {
      LOG_WARN("failed to deserialize tx log ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(transfer_ls_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize transfer ls id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(transfer_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize transfer scn", K(ret), K(len), K(new_pos));
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
  size += tx_scn_.get_fixed_serialize_size();
  size += tablet_status_.get_serialize_size();
  size += serialization::encoded_length_i64(transfer_seq_);
  size += transfer_ls_id_.get_serialize_size();
  size += transfer_scn_.get_fixed_serialize_size();
  return size;
}

int ObTabletTxMultiSourceDataUnit::set_scn(const SCN &scn)
{
  tx_scn_ = scn;

  return OB_SUCCESS;
}

} // namespace storage
} // namespace oceanbase
