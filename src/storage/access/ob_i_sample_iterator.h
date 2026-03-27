/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
