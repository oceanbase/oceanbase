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

