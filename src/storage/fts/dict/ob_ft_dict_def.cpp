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
int64_t ObFTSingleWord::hash(uint64_t &hash_value) const
{
  hash_value = get_word().hash();
  return OB_SUCCESS;
}

common::ObString ObFTSingleWord::get_word() const { return ObString(word_len, word); }
bool ObFTSingleWord::operator==(const ObFTSingleWord &other) const
{
  return (this == &other)
         || (word_len == other.word_len && 0 == memcmp(word, other.word, word_len));
}

int32_t ObFTSingleWord::set_word(const char *word, int32_t word_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(word)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (word_len > ObCharset::MAX_MB_LEN) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    memcpy(this->word, word, word_len);
    this->word_len = word_len;
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
