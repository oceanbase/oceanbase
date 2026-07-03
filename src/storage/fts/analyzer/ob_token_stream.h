/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TOKEN_STREAM_H_
#define OCEANBASE_STORAGE_OB_TOKEN_STREAM_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{

// Token attributes output by ObITokenStream::get_next_token()
struct ObTokenAttr
{
  const char *token_ptr_;    // token string pointer (does not own memory, points to original text or internal buffer)
  int32_t token_len_;        // token byte length
  int32_t pos_inc_;          // position relative to the previous token
                             // 1 in most cases, 0 for synonyms, etc., and more than 1 if preceding tokens are filtered
  bool is_keyword_;          // whether token is a key word

  ObTokenAttr()
    : token_ptr_(nullptr),
      token_len_(0),
      pos_inc_(0),
      is_keyword_(false)
  {}

  bool is_valid() const { return token_ptr_ != nullptr && token_len_ > 0; }
  common::ObString to_ob_string() const { return common::ObString(token_len_, token_ptr_); }

  TO_STRING_KV(KP_(token_ptr), K_(token_len), K_(pos_inc), K_(is_keyword));
};

// Base pull-model token stream interface.
// All text analysis components (Tokenizer, TokenFilter) inherit this interface,
// exposing a unified pull-model token stream.
class ObITokenStream
{
public:
  ObITokenStream() = default;
  virtual ~ObITokenStream() = default;

  // The token_ptr_ in ObTokenAttr is only valid until the next analyze() execution or until the analyzer is destroyed.
  virtual int get_next_token(ObTokenAttr &token) = 0;
  // Reset the object and free its memory. The object must not be used after this call.
  virtual void reset() = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObITokenStream);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TOKEN_STREAM_H_
