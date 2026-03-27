/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{

bool ObFTSingleToken::operator==(const ObFTSingleToken &other) const
{
  return (this == &other)
         || (token_char_len_ == other.token_char_len_ && 0 == memcmp(token_, other.token_, token_char_len_));
}

int ObFTSingleToken::set_token(const char *token, int32_t token_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == token || token_len <= 0 || token_len > ObCharset::MAX_MB_LEN)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    memcpy(this->token_, token, token_len);
    this->token_char_len_ = token_len;
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
