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

#ifndef OCEANBASE_STORAGE_OB_ROW_SAMPLE_ITERATOR_H
#define OCEANBASE_STORAGE_OB_ROW_SAMPLE_ITERATOR_H

#include "storage/ob_i_store.h"
#include "storage/ob_i_sample_iterator.h"
#include "share/ob_i_data_access_service.h"

namespace oceanbase {
namespace storage {
class ObRowSampleIterator : public ObISampleIterator {
public:
  explicit ObRowSampleIterator(const common::SampleInfo& sample_info);
  virtual ~ObRowSampleIterator();
  int open(ObQueryRowIterator& iterator);
  void reuse();
  virtual int get_next_row(ObStoreRow*& row) override;
  virtual void reset() override;

private:
  ObQueryRowIterator* iterator_;
  int64_t row_num_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_ROW_SAMPLE_ITERATOR_H */
