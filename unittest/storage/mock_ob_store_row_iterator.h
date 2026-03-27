/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef MOCK_OB_STORE_ROW_ITERATOR_H_
#define MOCK_OB_STORE_ROW_ITERATOR_H_

#include "storage/access/ob_store_row_iterator.h"

namespace oceanbase
{
namespace storage
{

class MockObStoreRowIterator : public ObStoreRowIterator
{
public:
  MockObStoreRowIterator() {}
  virtual ~MockObStoreRowIterator() {}
  MOCK_METHOD1(get_next_row, int(const ObStoreRow *&row));
  MOCK_METHOD0(reset, void());
};

}
}



#endif /* MOCK_OB_STORE_ROW_ITERATOR_H_ */

