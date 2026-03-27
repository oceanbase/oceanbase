/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

  void reuse() override { reset(); }

private:
  void reset()
  {
    low_offset_ = -1;
    high_offset_ = -1;
  }

  bool has_high() { return high_offset_ != -1; }

private:
  int64_t high_offset_ = -1;
  int64_t low_offset_ = -1;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_SURROGATE_PROCESSOR_H_
