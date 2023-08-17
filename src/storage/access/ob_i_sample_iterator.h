/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_I_SAMPLE_ITERATOR_H
#define OCEANBASE_STORAGE_OB_I_SAMPLE_ITERATOR_H

#include "ob_store_row_iterator.h"

namespace oceanbase
{
namespace storage
{

class ObISampleIterator : public ObQueryRowIterator
{
public:
  explicit ObISampleIterator(const common::SampleInfo &sample_info);
  virtual ~ObISampleIterator();
  virtual void reuse() = 0;
protected:
  bool return_this_sample(const int64_t num) const;
protected:
  const common::SampleInfo *sample_info_;
};

}
}

#endif /* OCEANBASE_STORAGE_OB_I_SAMPLE_ITERATOR_H */
