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

#ifndef OB_WORD_SEGMENT_H_
#define OB_WORD_SEGMENT_H_

#include "ob_i_word_segment_impl.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace common {

enum ObTokenizer {
  WS_TAOBAO_CHN = 0,
  WS_INTERNET_CHN,
  WS_DAOGOU_CHN,
  WS_B2B_CHN,
  WS_INTERNET_JPN,
  WS_INTERNET_ENG,
  WS_CHAR,
  WS_MAX,
};

class ObWordSegment {
public:
  ObWordSegment() : inner_allocator_(), segmenter_(NULL){};
  virtual ~ObWordSegment()
  {
    destory();
  }

  int init(const ObString& tokenizer);
  int segment(const ObString& string, ObIArray<ObString>& words);
  int segment(const ObObj& string, ObIArray<ObObj>& words);
  int reset();
  int destory();

private:
  bool is_inited()
  {
    return NULL != segmenter_;
  }

private:
  static const char* TokenizerName[WS_MAX + 1];

private:
  common::ObArenaAllocator inner_allocator_;
  ObIWordSegmentImpl* segmenter_;
};
}  // namespace common
}  // namespace oceanbase

#endif /* OB_WORD_SEGMENT_H_ */
