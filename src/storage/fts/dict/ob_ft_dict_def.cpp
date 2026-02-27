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
