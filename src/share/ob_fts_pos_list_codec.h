/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #ifndef OCEANBASE_SHARE_OB_FTS_POS_LIST_CODEC_H_
 #define OCEANBASE_SHARE_OB_FTS_POS_LIST_CODEC_H_

#include "lib/container/ob_iarray.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

struct ObFTSPositionListStore
{
  static constexpr int16_t MAGIC_NUMER = static_cast<int16_t>(0xFACE);
  static constexpr int32_t POS_LIST_CURRENT_VERSION = 1;
public:

  ObFTSPositionListStore()
      : magic_(MAGIC_NUMER)
      , version_(POS_LIST_CURRENT_VERSION)
      , length_(0)
      , checksum_(0)
      , pos_list_cnt_(0)
      , pos_list_str_()
  {
  }

  void reset()
  {
    magic_ = MAGIC_NUMER;
    version_ = POS_LIST_CURRENT_VERSION;
    length_ = 0;
    checksum_ = 0;
    pos_list_cnt_ = 0;
    pos_list_str_.reset();
  }

  int encode_and_serialize(
      ObIAllocator &allocator,
      const ObIArray<int64_t> &pos_list,
      char *&buf,
      int64_t &buf_len);

  int deserialize_and_decode(const char *buf, const int64_t buf_len, ObIArray<int64_t> &pos_list);

  int serialize(const uint64_t data_version, char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len,int64_t &pos);
  int64_t get_serialize_size(const uint64_t data_version) const;

  TO_STRING_KV(
      K_(magic),
      K_(version),
      K_(length),
      K_(checksum),
      K_(pos_list_cnt),
      K_(pos_list_str));

  static int encode_pos_list(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, ObString &pos_list_str);
  static int decode_pos_list(const ObString &pos_list_str, const int64_t pos_count, ObIArray<int64_t> &pos_list);

private:
  static int calculate_checksum(const ObString &pos_list_str, int64_t &checksum);
  void set_pos_list_cnt(const int32_t pos_list_cnt) { pos_list_cnt_ = pos_list_cnt; }
  void set_pos_list_str(const ObString &pos_list_str) { pos_list_str_ = pos_list_str; }
  void set_checksum(const int64_t checksum) { checksum_ = checksum; }
  void set_length(const int32_t length) { length_ = length; }

  static int encode_with_variable_int64(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, ObString &pos_list_str);
  static int decode_with_variable_int64(const ObString &pos_list_str, const int64_t pos_count, ObIArray<int64_t> &pos_list);

  // todo: use this if needed, currently not used by any other code
  static int encode_with_delta_zigzag_pfor(ObIAllocator &allocator, const ObIArray<int64_t> &pos_list, ObString &pos_list_str);
  static int decode_with_delta_zigzag_pfor(const ObString &pos_list_str, const int64_t pos_count, ObIArray<int64_t> &pos_list);

public:
  int16_t magic_; // 0xFACE
  int16_t version_; // 1
  int32_t length_; // length of ObFTSPositionListStore
  int64_t checksum_; // checksum of the pos list str
  int32_t pos_list_cnt_; // count of the pos list
  ObString pos_list_str_; // after encoder, pos_list_str_ is the encoded string
};

 } // end namespace share
 } // end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_FTS_POS_LIST_CODEC_H_