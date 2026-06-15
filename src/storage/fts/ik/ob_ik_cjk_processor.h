/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_IK_OB_IK_CJK_PROCESSOR_H_
#define _OCEANBASE_STORAGE_FTS_IK_OB_IK_CJK_PROCESSOR_H_

#include "lib/allocator/ob_allocator.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/ik/ob_ik_processor.h"

namespace oceanbase
{
namespace storage
{
class ObIKCJKProcessor : public ObIIKProcessor
{
public:
  ObIKCJKProcessor(const ObIFTDict &dict_main, ObIAllocator &alloc)
      : hits_(alloc), dict_main_(dict_main)
  {
  }
  ~ObIKCJKProcessor() override { hits_.reset(); }

public:
  int do_process(TokenizeContext &ctx,
                 const char *ch,
                 const uint8_t char_len,
                 const ObFTCharUtil::CharType type) override;

  void reuse() override { hits_.clear(); }

private:
  ObList<ObDATrieHit, ObIAllocator> hits_;
  const ObIFTDict &dict_main_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIKCJKProcessor);
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_IK_OB_IK_CJK_PROCESSOR_H_
