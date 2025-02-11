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

#ifndef OB_BATCH_DATUM_ROWS_H_
#define OB_BATCH_DATUM_ROWS_H_

#include "lib/container/ob_array.h"
#include "share/vector/ob_i_vector.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace blocksstable
{

class ObBatchDatumRows
{
public:
  ObBatchDatumRows()
    : row_count_(0)
  {
    vectors_.set_tenant_id(MTL_ID());
  }
  ~ObBatchDatumRows() {}
  void reset();

  OB_INLINE int64_t get_column_count() const { return vectors_.count(); }

  TO_STRING_KV(K_(row_flag), K_(mvcc_row_flag), K_(trans_id), K(vectors_.count()), K_(row_count));

public:
  // convert vectors_ to datum_row at row idx = idx
  // performance is low, use it in performance non sensitive position
  int to_datum_row(int64_t idx, ObDatumRow &datum_row) const;

public:
  ObDmlRowFlag row_flag_;
  ObMultiVersionRowFlag mvcc_row_flag_;
  transaction::ObTransID trans_id_;
  common::ObArray<common::ObIVector *> vectors_;
  int64_t row_count_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif /* OB_BATCH_DATUM_ROWS_H_ */
