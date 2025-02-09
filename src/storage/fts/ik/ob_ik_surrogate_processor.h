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

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_SURROGATE_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_SURROGATE_PROCESSOR_H_

#include "storage/fts/ik/ob_ik_processor.h"

namespace oceanbase
{
namespace storage
{
// Only for UTF16
class ObIKSurrogateProcessor : public ObIIKProcessor
{
public:
  ObIKSurrogateProcessor() {}
  ~ObIKSurrogateProcessor() override {}

  int do_process(TokenizeContext &ctx,
                 const char *ch,
                 const uint8_t char_len,
                 const ObFTCharUtil::CharType type) override;

private:
  void reset()
  {
    low_offset = -1;
    high_offset_ = -1;
  }

  bool has_high() { return high_offset_ != -1; }

private:
  int64_t high_offset_ = -1;
  int64_t low_offset = -1;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_SURROGATE_PROCESSOR_H_
