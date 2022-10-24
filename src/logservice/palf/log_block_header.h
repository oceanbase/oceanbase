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

#ifndef OCEANBASE_LOGSERVICE_LOG_BLOCK_HEADER_
#define OCEANBASE_LOGSERVICE_LOG_BLOCK_HEADER_

#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"

namespace oceanbase
{
namespace palf
{
struct LogBlockHeader
{
public:
  LogBlockHeader();
  ~LogBlockHeader();
  bool is_valid() const;
  void reset();
  // NB: not thread safe
  void update_lsn_and_ts(const LSN &lsn, const int64_t log_ts);
  void update_palf_id_and_curr_block_id(const int64_t palf_id, const block_id_t curr_block_id);
  void mark_block_can_be_reused(const int64_t max_timestamp);
  block_id_t get_curr_block_id() const;
  int64_t get_min_timestamp() const;
  LSN get_min_lsn() const;

  // This interface is just used for PalfIterator
  //
  // when iterate LogBlockHeader, this interface will return the min timestamp of this block,
  // if the 'log_ts_' of next LogGroupEntry is smaller than the return value, means this is
  // an empty block.
  //
  // if this block is a reused block, return 'max_timestamp_', otherwise return 'min_timestamp_'.
  int64_t get_timestamp_used_for_iterator() const;
  void calc_checksum();
  bool check_integrity() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(magic), K_(version), K_(min_lsn), K_(min_timestamp), K_(curr_block_id), K_(palf_id));
  // 0x4942 means InfoBlock
  static constexpr int16_t MAGIC= 0x4942;
  static constexpr int16_t LOG_INFO_BLOCK_VERSION = 1;
  static constexpr int32_t REUSED_BLOCK_MASK = 1 << 0;
private:
  int64_t calc_checksum_() const;
  bool is_reused_block_() const;
private:
  int16_t magic_;
  int16_t version_;
  int32_t flag_;
  // The min lsn of curr block
  LSN min_lsn_;
  // The min timstamp of curr block
  int64_t min_timestamp_;
  int64_t max_timestamp_;

  // Assume that, multiple PALF instances share a directory, and each block names with
  // 'block_%d'(the number is monotonic, and it may be not same with 'curr_block_id_').
  //
  // 'curr_block_id_' is the logical block id, and keep it increase monotonically in each
  // palf instance, even if switch block when write failed.
  // NB: to locate logs by LSN, we need keep the pair(physical block name, InfoBlock).
  block_id_t curr_block_id_;
  int64_t palf_id_;
  int64_t checksum_;
};
} // end namespace palf
} // end namespace oceanbase
#endif
