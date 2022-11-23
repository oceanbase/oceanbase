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
#include <algorithm>            // std::max
#include "ob_archive_piece.h"

namespace oceanbase
{
namespace share
{
using namespace std;

ObArchivePiece::ObArchivePiece() :
  interval_(0),
  genesis_ts_(0),
  base_piece_id_(0),
  piece_id_(0)
{
}

ObArchivePiece::~ObArchivePiece()
{
  reset();
}

ObArchivePiece::ObArchivePiece(const int64_t timestamp, const int64_t interval_us, const int64_t genesis_ts, const int64_t base_piece_id)
{
  const int64_t interval =  interval_us * 1000L;
  if (timestamp < 0 || interval < ONE_SECOND || genesis_ts < 0 || base_piece_id < 1) {
    LOG_ERROR("invalid argument", K(timestamp), K(interval), K(genesis_ts), K(base_piece_id));
  } else {
    interval_ = interval;
    genesis_ts_ = genesis_ts;
    base_piece_id_ = base_piece_id;
    piece_id_ = (timestamp - genesis_ts_) / std::max(interval_, 1L) + base_piece_id_;
  }
}

void ObArchivePiece::reset()
{
  interval_ = ONE_DAY;
  genesis_ts_ = 0;
  base_piece_id_ = 0;
  piece_id_ = 0;
}

int ObArchivePiece::set(const int64_t piece_id, const int64_t interval_us, const int64_t genesis_ts, const int64_t base_piece_id)
{
  int ret = OB_SUCCESS;
  const int64_t interval =  interval_us * 1000L;
  if (piece_id < 0 || interval <= 0 || genesis_ts < 0 || base_piece_id < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(piece_id), K(interval), K(genesis_ts), K(base_piece_id));
  } else {
    piece_id_ = piece_id;
    interval_ = interval;
    genesis_ts_ = genesis_ts;
    base_piece_id_ =base_piece_id;
  }
  return ret;
}

bool ObArchivePiece::is_valid() const
{
  return interval_ >= ONE_SECOND
    && genesis_ts_ >= 0
    && base_piece_id_ > 0;
}

int64_t ObArchivePiece::get_piece_lower_limit()
{
  //return convert_timestamp_();
  return (piece_id_ - base_piece_id_) * interval_ + genesis_ts_;
}

ObArchivePiece &ObArchivePiece::operator=(const ObArchivePiece &other)
{
  interval_ = other.interval_;
  genesis_ts_ = other.genesis_ts_;
  base_piece_id_ = other.base_piece_id_;
  piece_id_ = other.piece_id_;
  return *this;
}

ObArchivePiece &ObArchivePiece::operator++()
{
  inc();
  return *this;
}

// 不同interval piece不可比
bool ObArchivePiece::operator==(const ObArchivePiece &other) const
{
  if (interval_ != other.interval_) {
    LOG_ERROR("different piece interval, can not compare", KPC(this), K(other));
  }
  return piece_id_ == other.piece_id_;
}

bool ObArchivePiece::operator!=(const ObArchivePiece &other) const
{
  return ! (*this == other);
}

bool ObArchivePiece::operator>(const ObArchivePiece &other) const
{
  if (interval_ != other.interval_) {
    LOG_ERROR("different piece interval, can not compare", KPC(this), K(other));
  }
  return piece_id_ > other.piece_id_;
}

void ObArchivePiece::inc()
{
  piece_id_++;
}

} // namespace share
} // namespace oceanbase
