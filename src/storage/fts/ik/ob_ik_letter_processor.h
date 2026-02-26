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

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_LETTER_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_LETTER_PROCESSOR_H_

#include "storage/fts/ik/ob_ik_processor.h"

namespace oceanbase
{
namespace storage
{
class ObIKLetterProcessor : public ObIIKProcessor
{
public:
  ObIKLetterProcessor();
  ~ObIKLetterProcessor() override {}

  int do_process(TokenizeContext &ctx,
                 const char *ch,
                 const uint8_t char_len,
                 const ObFTCharUtil::CharType type) override;

private:
  int process_english_letter(TokenizeContext &ctx,
                             const char *ch,
                             const uint8_t char_len,
                             const ObFTCharUtil::CharType type);

  int process_arabic_letter(TokenizeContext &ctx,
                            const char *ch,
                            const uint8_t char_len,
                            const ObFTCharUtil::CharType type);

  int process_mix_letter(TokenizeContext &ctx,
                         const char *ch,
                         const uint8_t char_len,
                         const ObFTCharUtil::CharType type);

  void reset_english_state();

  void reset_arabic_state();

  void reset_mix_state();

private:
  int64_t english_start_ = -1;
  int64_t english_end_ = -1;
  int64_t english_char_cnt_ = 0;

  int64_t arabic_start_ = -1;
  int64_t arabic_end_ = -1;
  int64_t arabic_char_cnt_ = 0;
  int64_t arabic_connect_cnt_ = 0;

  int64_t mix_start_ = -1;
  int64_t mix_end_ = -1;
  int64_t mix_char_cnt_ = 0;
};
} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_LETTER_PROCESSOR_H_
