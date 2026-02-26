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

#ifndef OCEANBASE_DATUM_ROW_ITERATOR_
#define OCEANBASE_DATUM_ROW_ITERATOR_

#include "src/storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace blocksstable
{

class ObDatumRowIterator
{
public:
  typedef common::ObReserveArenaAllocator<1024> ObStorageReserveAllocator;
public:
  ObDatumRowIterator() {}
  virtual ~ObDatumRowIterator() {}
  /**
   * get the next datum row and move the cursor
   *
   * @param row [out]
   *
   * @return OB_ITER_END if end of iteration
   */
  virtual int get_next_row(ObDatumRow *&row) = 0;
  virtual int get_next_rows(ObDatumRow *&rows, int64_t &row_count)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(get_next_row(rows))) {
    } else {
      row_count = 1;
    }
    return ret;
  }
  virtual void reset() {}
  TO_STRING_EMPTY();
};

/// wrap one datum row as an iterator
class ObSingleDatumRowIteratorWrapper: public ObDatumRowIterator
{
public:
  ObSingleDatumRowIteratorWrapper();
  ObSingleDatumRowIteratorWrapper(ObDatumRow *row);
  virtual ~ObSingleDatumRowIteratorWrapper() {}

  void set_row(ObDatumRow *row) { row_ = row; }
  virtual int get_next_row(ObDatumRow *&row);
	virtual void reset() { iter_end_ = false; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleDatumRowIteratorWrapper);
private:
  // data members
  ObDatumRow *row_;
  bool iter_end_;
};

inline ObSingleDatumRowIteratorWrapper::ObSingleDatumRowIteratorWrapper()
    :row_(NULL),
     iter_end_(false)
{}

inline ObSingleDatumRowIteratorWrapper::ObSingleDatumRowIteratorWrapper(ObDatumRow *row)
    :row_(row),
     iter_end_(false)
{}

inline int ObSingleDatumRowIteratorWrapper::get_next_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

}
}

#endif //OCEANBASE_DATUM_ROW_ITERATOR_
