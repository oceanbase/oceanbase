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
  interval_us_(0),
  base_piece_id_(0),
  piece_id_(0)
{
  genesis_scn_.set_min();
}

ObArchivePiece::~ObArchivePiece()
{
  reset();
}

ObArchivePiece::ObArchivePiece(const SCN &scn, const int64_t interval_us, const SCN &genesis_scn, const int64_t base_piece_id)
{
  if (!scn.is_valid() || interval_us < ONE_SECOND || !genesis_scn.is_valid() || base_piece_id < 1) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument", K(scn), K(interval_us), K(genesis_scn), K(base_piece_id));
  } else {
    interval_us_ = interval_us;
    genesis_scn_ = genesis_scn;
    base_piece_id_ = base_piece_id;
    piece_id_ = (scn.convert_to_ts() - genesis_scn_.convert_to_ts()) / interval_us_ + base_piece_id_;
  }
}

void ObArchivePiece::reset()
{
  interval_us_ = ONE_DAY;
  genesis_scn_.reset();
  base_piece_id_ = 0;
  piece_id_ = 0;
}

int ObArchivePiece::set(const int64_t piece_id,
                        const int64_t interval_us,
                        const SCN &genesis_scn,
                        const int64_t base_piece_id)
{
  int ret = OB_SUCCESS;
  if (piece_id < 0 || interval_us < ONE_SECOND || !genesis_scn.is_valid() || base_piece_id < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(piece_id), K(interval_us), K(genesis_scn), K(base_piece_id));
  } else {
    piece_id_ = piece_id;
    interval_us_ = interval_us;
    genesis_scn_ = genesis_scn;
    base_piece_id_ = base_piece_id;
  }
  return ret;
}

bool ObArchivePiece::is_valid() const
{
  return interval_us_ >= ONE_SECOND
    && genesis_scn_.is_valid()
    && base_piece_id_ > 0;
}

int ObArchivePiece::get_piece_lower_limit(SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t ts = (piece_id_ - base_piece_id_) * interval_us_  + genesis_scn_.convert_to_ts();
  if (OB_FAIL(scn.convert_from_ts(ts))) {
    LOG_WARN("failed to convert_for_logservice", KPC(this), K(ret), K(ts));
  }
  return ret;
}

ObArchivePiece &ObArchivePiece::operator=(const ObArchivePiece &other)
{
  interval_us_ = other.interval_us_;
  genesis_scn_ = other.genesis_scn_;
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
  if (interval_us_ != other.interval_us_) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "different piece interval, can not compare", KPC(this), K(other));
  }
  return piece_id_ == other.piece_id_;
}

bool ObArchivePiece::operator!=(const ObArchivePiece &other) const
{
  return ! (*this == other);
}

bool ObArchivePiece::operator>(const ObArchivePiece &other) const
{
  if (interval_us_ != other.interval_us_) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "different piece interval, can not compare", KPC(this), K(other));
  }
  return piece_id_ > other.piece_id_;
}

void ObArchivePiece::inc()
{
  piece_id_++;
}

} // namespace share
} // namespace oceanbase
