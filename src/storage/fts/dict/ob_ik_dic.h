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

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_IK_DIC_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_IK_DIC_H_

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"

#include <cstdint>

namespace oceanbase
{
namespace storage
{
class ObIKDictLoader
{
public:
  struct RawDict
  {
    const char **data_;
    int64_t array_size_;
  };
  static RawDict dict_text();
  static RawDict dict_quen_text();
  static RawDict dict_stop();
};

class ObIKDictIterator : public ObIFTDictIterator
{
public:
  ObIKDictIterator(ObIKDictLoader::RawDict dict_text) : dict_text_(dict_text), pos_(-1), size_(0) {}
  ~ObIKDictIterator() {}

  int init();

  // override
public:
  int next() override;

  int get_key(ObString &str) override
  {
    int ret = OB_SUCCESS;
    str = get_str();
    return ret;
  }

  int get_value() override
  {
    int ret = OB_SUCCESS;
    return ret;
  }

  ObString get_str() const { return ObString(dict_text_.data_[pos_]); }
  bool valid() const { return pos_ >= 0 && pos_ < dict_text_.array_size_; }

private:
  ObIKDictLoader::RawDict dict_text_;
  int64_t pos_;
  int32_t size_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_IK_DIC_H_
