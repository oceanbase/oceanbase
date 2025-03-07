/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_QUANTIFIER_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_QUANTIFIER_PROCESSOR_H_

#include "lib/allocator/ob_allocator.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/ik/ob_ik_processor.h"

#include <cstdint>
namespace oceanbase
{
namespace storage
{
class ObFTDictHub;
class ObIKQuantifierProcessor : public ObIIKProcessor
{
public:
  ObIKQuantifierProcessor(const ObIFTDict &quan_dict, ObIAllocator &alloc)
      : quan_dict_(quan_dict), count_hits_(alloc), start_(-1), end_(-1), quan_char_cnt_(0)
  {
  }
  ~ObIKQuantifierProcessor() override { count_hits_.reset(); }

  int do_process(TokenizeContext &ctx,
                 const char *ch,
                 const uint8_t char_len,
                 const ObFTCharUtil::CharType type);

private:
  int process_CN_number(TokenizeContext &ctx,
                        const char *ch,
                        const uint8_t char_len,
                        const ObFTCharUtil::CharType type);

  int process_CN_count(TokenizeContext &ctx,
                       const char *ch,
                       const uint8_t char_len,
                       const ObFTCharUtil::CharType type);
  void reset();

private:
  bool need_count_scan(TokenizeContext &ctx);
  int output_num_token(TokenizeContext &ctx);

private:
  const ObIFTDict &quan_dict_;
  ObList<ObDATrieHit, ObIAllocator> count_hits_;
  int64_t start_;
  int64_t end_;
  int64_t quan_char_cnt_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_QUANTIFIER_PROCESSOR_H_
