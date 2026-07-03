/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_I_TOKEN_FILTER_H_
#define OCEANBASE_STORAGE_OB_I_TOKEN_FILTER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "object/ob_object.h"
#include "storage/fts/analyzer/ob_token_stream.h"

namespace oceanbase
{
namespace storage
{

// ============================================================
// TokenFilter configuration spec structures
// ============================================================

enum class ObTokenFilterType
{
  TOKEN_FILTER_TYPE_INVALID = 0,
  TOKEN_FILTER_TYPE_LOWERCASE,          // convert tokens to lowercase
  TOKEN_FILTER_TYPE_STOP,               // remove stop words
  TOKEN_FILTER_TYPE_SNOWBALL,           // Snowball stemming algorithm
  TOKEN_FILTER_TYPE_DECIMAL_DIGIT,      // normalize decimal digits to ASCII 0-9
  TOKEN_FILTER_TYPE_POSSESSIVE_ENGLISH, // remove possessive English endings ('s)
  TOKEN_FILTER_TYPE_ICU_NORMALIZATION,  // ICU normalization
  TOKEN_FILTER_TYPE_ICU_FOLDING,        // ICU folding
  TOKEN_FILTER_TYPE_MIN_MAX,            // min/max token length filter
  TOKEN_FILTER_TYPE_LEGACY_STOP,        // legacy stop word filter
  TOKEN_FILTER_TYPE_CHARSET_CONVERT,    // convert tokens from utf8mb4_bin to source collation
  TOKEN_FILTER_TYPE_MAX
};

struct ObTokenFilterSpec
{
  ObTokenFilterType type_;
  ObTokenFilterSpec() : type_(ObTokenFilterType::TOKEN_FILTER_TYPE_INVALID) {}
  ObTokenFilterSpec(const ObTokenFilterType type) : type_(type) {}
  virtual ~ObTokenFilterSpec() = default;
  VIRTUAL_TO_STRING_KV(K_(type));
};

// ============================================================
// ObITokenFilter interface
// ============================================================

// TokenFilter forms a pull chain by holding an upstream ObITokenStream* pointer.
// Each call to get_next_token() pulls a token from the upstream and applies
// component-specific transformation or filtering.
class ObITokenFilter : public ObITokenStream
{
public:
  ObITokenFilter() : input_(nullptr) {}
  virtual ~ObITokenFilter() = default;
  virtual int init(const ObTokenFilterSpec &spec, common::ObIAllocator &alloc) = 0;
  virtual void set_input(ObITokenStream *input) { input_ = input; }
  virtual int get_next_token(ObTokenAttr &token) override = 0;
  // Reset the object and free its memory. The object must not be used after this call.
  virtual void reset() override = 0;
  VIRTUAL_TO_STRING_KV(KP_(input));
protected:
  ObITokenStream *input_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObITokenFilter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_I_TOKEN_FILTER_H_
