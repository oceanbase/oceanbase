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

#include "log_block_header.h"
#include "lib/ob_define.h"                                    // PALF_LOG
#include "lib/ob_errno.h"                                     // OB_SUCCESS...
#include "lib/checksum/ob_crc64.h"                            // ob_crc64
#include "lib/utility/serialization.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
LogBlockHeader::LogBlockHeader()
{
  reset();
}

LogBlockHeader::~LogBlockHeader()
{
  reset();
}

bool LogBlockHeader::is_valid() const
{
  return MAGIC == magic_
         && LOG_INFO_BLOCK_VERSION == version_
         && true == min_lsn_.is_valid()
         && true == min_scn_.is_valid()
         && true == is_valid_block_id(curr_block_id_)
         && true == is_valid_palf_id(palf_id_);
}

void LogBlockHeader::reset()
{
  magic_ = MAGIC;
  version_ = LOG_INFO_BLOCK_VERSION;
  flag_ = 0;
  min_lsn_.reset();
  min_scn_.reset();
  max_scn_.reset();
  curr_block_id_ = LOG_INVALID_BLOCK_ID;
  palf_id_ = INVALID_PALF_ID;
  checksum_ = 0;
}

void LogBlockHeader::update_lsn_and_scn(const LSN &lsn,
                                        const SCN &scn)
{
  min_lsn_ = lsn;
  min_scn_ = scn;
}

void LogBlockHeader::update_palf_id_and_curr_block_id(const int64_t palf_id,
                                                      const block_id_t curr_block_id)
{
  palf_id_ = palf_id;
  curr_block_id_ = curr_block_id;
}

void LogBlockHeader::mark_block_can_be_reused(const SCN &max_scn)
{
  flag_ |= REUSED_BLOCK_MASK;
  max_scn_ = max_scn;
}

block_id_t LogBlockHeader::get_curr_block_id() const
{
  return curr_block_id_;
}

const SCN &LogBlockHeader::get_min_scn() const
{
  return min_scn_;
}

LSN LogBlockHeader::get_min_lsn() const
{
  return min_lsn_;
}

const SCN LogBlockHeader::get_scn_used_for_iterator() const
{
  return true == is_reused_block_() ? max_scn_ : min_scn_;
}

void LogBlockHeader::calc_checksum()
{
  checksum_ = calc_checksum_();
}

int64_t LogBlockHeader::calc_checksum_() const
{
  int64_t checksum_len = sizeof(*this) - sizeof(checksum_);
  return static_cast<int64_t>(ob_crc64(this, checksum_len));
}

bool LogBlockHeader::is_reused_block_() const
{
  return 1 == (flag_ & REUSED_BLOCK_MASK);
}

bool LogBlockHeader::check_integrity() const
{
  return checksum_ == calc_checksum_();
}

DEFINE_SERIALIZE(LogBlockHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))
             || OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))
             || OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, flag_))
             || OB_FAIL(min_lsn_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(min_scn_.fixed_serialize(buf, buf_len, new_pos))
             || OB_FAIL(max_scn_.fixed_serialize(buf, buf_len, new_pos))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, curr_block_id_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, palf_id_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, checksum_))) {
    PALF_LOG(ERROR, "serialize failed", K(ret), K(pos), K(buf_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogBlockHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_))
             || OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_))
             || OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &flag_))
             || OB_FAIL(min_lsn_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(min_scn_.fixed_deserialize(buf, data_len, new_pos))
             || OB_FAIL(max_scn_.fixed_deserialize_without_transform(buf, data_len, new_pos))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&curr_block_id_)))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &palf_id_))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &checksum_))) {
    PALF_LOG(ERROR, "serialize failed", K(ret), K(pos), K(data_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogBlockHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i32(flag_);
  size += min_lsn_.get_serialize_size();
  size += min_scn_.get_fixed_serialize_size();
  size += max_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(curr_block_id_);
  size += serialization::encoded_length_i64(palf_id_);
  size += serialization::encoded_length_i64(checksum_);
  return size;
}
} // end namespace palf
} // end namespace oceanbase
