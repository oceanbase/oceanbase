/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_I_TOKENIZER_H_
#define OCEANBASE_STORAGE_OB_I_TOKENIZER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/fts/analyzer/ob_token_stream.h"

namespace oceanbase
{
namespace storage
{

// ============================================================
// Tokenizer configuration spec structures
// ============================================================

enum class ObTokenizerType
{
  TOKENIZER_TYPE_INVALID = 0,
  TOKENIZER_TYPE_SPACE,     // whitespace tokenizer
  TOKENIZER_TYPE_NGRAM,     // ngram tokenizer (fixed-size)
  TOKENIZER_TYPE_BENG,      // basic english tokenizer
  TOKENIZER_TYPE_IK,        // IK Chinese tokenizer
  TOKENIZER_TYPE_NGRAM2,    // range ngram tokenizer (min-max)
  TOKENIZER_TYPE_STANDARD,  // ICU-based standard tokenizer
  TOKENIZER_TYPE_KEYWORD,   // dummy tokenizer
  TOKENIZER_TYPE_MAX
};

struct ObTokenizerSpec
{
  ObTokenizerType type_;
  ObTokenizerSpec() : type_(ObTokenizerType::TOKENIZER_TYPE_INVALID) {}
  ObTokenizerSpec(const ObTokenizerType type) : type_(type) {}
  virtual ~ObTokenizerSpec() = default;
  VIRTUAL_TO_STRING_KV(K_(type));
};


// ============================================================
// ObITokenizer interface
// ============================================================

// Tokenizer accepts text processed by all CharFilters, and splits it into an initial token stream.
// An Analyzer has exactly one Tokenizer. It also implements ObITokenStream.
class ObITokenizer : public ObITokenStream
{
public:
  ObITokenizer() = default;
  virtual ~ObITokenizer() = default;
  virtual int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) = 0;
  virtual int set_input(const char *text, int64_t text_len, ObCollationType coll_type) = 0;
  virtual int get_next_token(ObTokenAttr &token) override = 0;
  // Reset the object and free its memory. The object must not be used after this call.
  virtual void reset() override = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObITokenizer);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_I_TOKENIZER_H_
