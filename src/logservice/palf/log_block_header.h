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
#include "share/scn.h"

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
  void update_lsn_and_scn(const LSN &lsn, const share::SCN &scn);
  void update_palf_id_and_curr_block_id(const int64_t palf_id, const block_id_t curr_block_id);
  void mark_block_can_be_reused(const share::SCN &max_scn);
  block_id_t get_curr_block_id() const;
  const share::SCN &get_min_scn() const;
  LSN get_min_lsn() const;

  // This interface is just used for PalfIterator
  //
  // when iterate LogBlockHeader, this interface will return the min scn of this block,
  // if the 'scn_' of next LogGroupEntry is smaller than the return value, means this is
  // an empty block.
  //
  // if this block is a reused block, return 'max_scn_', otherwise return 'min_scn_'.
  const share::SCN get_scn_used_for_iterator() const;
  void calc_checksum();
  bool check_integrity() const;
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(magic), K_(version), K_(min_lsn), K_(min_scn), K_(curr_block_id), K_(palf_id));
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
  share::SCN min_scn_;
  share::SCN max_scn_;

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
