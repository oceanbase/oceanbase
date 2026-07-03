/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STANDARD_TOKENIZER_H_
#define OB_STANDARD_TOKENIZER_H_

#include "storage/fts/analyzer/ob_analyzer.h"

#include <unicode/brkiter.h>
#include <unicode/ubrk.h>
#include <unicode/utext.h>

namespace oceanbase
{
namespace storage
{

struct ObStandardTokenizerSpec : public ObTokenizerSpec
{
  int32_t max_token_length_;

  explicit ObStandardTokenizerSpec(int32_t max_token_length = 255)
    : ObTokenizerSpec(ObTokenizerType::TOKENIZER_TYPE_STANDARD),
      max_token_length_(max_token_length)
  {}
};

class ObStandardTokenizer : public ObITokenizer
{
public:
  ObStandardTokenizer();
  ~ObStandardTokenizer();
  int init(const ObTokenizerSpec &spec, ObIAllocator &alloc) override;
  int set_input(const char *text, int64_t text_len, ObCollationType coll_type) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;
protected:
  int32_t max_token_length_;
  icu::BreakIterator *bi_;
  ObString text_;
  UText *utext_;
  int32_t last_boundary_;
  int32_t last_emitted_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStandardTokenizer);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STANDARD_TOKENIZER_H_
