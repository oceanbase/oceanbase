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

#ifndef OCEANBASE_COMMON_ROW_ITERATOR_
#define OCEANBASE_COMMON_ROW_ITERATOR_

#include "common/row/ob_row.h"

namespace oceanbase
{
namespace common
{

class ObNewRowIterator
{
public:
  enum IterType
  {
    Other = 0,
    ObTableScanIterator = 1,
    ObLocalIndexLookupIterator = 2,
    ObGroupLookupOp = 3,
    ObTextRetrievalOp = 4,
  };
public:
  ObNewRowIterator() : type_(Other) {}
  explicit ObNewRowIterator(const IterType type) : type_(type) {}
  virtual ~ObNewRowIterator() {}
  /**
   * get the next row and move the cursor
   *
   * @param row [out]
   *
   * @return OB_ITER_END if end of iteration
   */
  virtual int get_next_row(ObNewRow *&row) = 0;
  virtual int get_next_rows(ObNewRow *&rows, int64_t &row_count)
  {
    int ret = get_next_row(rows);
    if (OB_SUCCESS == ret) {
      row_count = 1;
    } else {
      row_count = 0;
    }
    return ret;
  }

  // Iterate row interface for sql static typing engine.
  virtual int get_next_row()
  {
    int ret = common::OB_NOT_IMPLEMENT;
    COMMON_LOG(WARN, "interface not implement", K(ret));
    return ret;
  }

  // Iterate row interface for vectorized engine.
  virtual int get_next_rows(int64_t &count, int64_t capacity)
  {
    UNUSEDx(count, capacity);
    int ret = common::OB_NOT_IMPLEMENT;
    COMMON_LOG(WARN, "interface not implement", K(ret));
    return ret;
  }

  /// rewind the iterator
  virtual void reset() = 0;
  TO_STRING_EMPTY();

  IterType get_type() const { return type_; }
private:
  const IterType type_;
};

class ObNewIterIterator
{
public:
  enum IterType
  {
    Other = 0,
    ObTableScanIterIterator = 1,
  };
public:
  ObNewIterIterator() : type_(Other) {}
  explicit ObNewIterIterator(const IterType type) : type_(type) {}
  virtual ~ObNewIterIterator() {}
  virtual int get_next_iter(ObNewRowIterator *&iter) = 0;
  virtual void reset() = 0;

  IterType get_type() const { return type_; }
private:
  const IterType type_;
};

/// wrap one row as an iterator
class ObSingleRowIteratorWrapper: public ObNewRowIterator
{
public:
  ObSingleRowIteratorWrapper();
  ObSingleRowIteratorWrapper(ObNewRow *row);
  virtual ~ObSingleRowIteratorWrapper() {}

  void set_row(ObNewRow *row) { row_ = row; }
  virtual int get_next_row(ObNewRow *&row);
	virtual void reset() { iter_end_ = false; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleRowIteratorWrapper);
private:
  // data members
  ObNewRow *row_;
  bool iter_end_;
};

inline ObSingleRowIteratorWrapper::ObSingleRowIteratorWrapper()
    :row_(NULL),
     iter_end_(false)
{}

inline ObSingleRowIteratorWrapper::ObSingleRowIteratorWrapper(ObNewRow *row)
    :row_(row),
     iter_end_(false)
{}

inline int ObSingleRowIteratorWrapper::get_next_row(ObNewRow *&row)
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

class ObOuterRowIterator
{
public:
  ObOuterRowIterator() {}
  virtual ~ObOuterRowIterator() {}
  /**
   * get the next row and move the cursor
   *
   * @param row [out]
   *
   * @return OB_ITER_END if end of iteration
   */
  virtual int get_next_row(ObNewRow &row) = 0;
  /// rewind the iterator
  virtual void reset() = 0;
};

}
}

#endif //OCEANBASE_COMMON_ROW_ITERATOR_
