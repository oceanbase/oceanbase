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

#ifndef MOCK_OB_SSTABLE_H_
#define MOCK_OB_SSTABLE_H_

namespace oceanbase
{
namespace storage
{
class MockObMacroBlockIterator : public ObMacroBlockIterator
{
public:
  MockObMacroBlockIterator() {}
  virtual ~MockObMacroBlockIterator() {}
  MOCK_METHOD1(get_next_macro_block, int(ObMacroBlockDesc &block_desc));
};


class MockObSSTable : public ObSSTable
{
public:
  MockObSSTable() {}
  virtual ~MockObSSTable() {}

  MOCK_CONST_METHOD3(get_macro_range, int(const int64_t index, int64_t &macro_block_id,
                                          common::ObStoreRange &range));
  MOCK_METHOD2(open, int(
                   const share::schema::ObTableSchema &schema,
                   const int64_t data_version));
  MOCK_METHOD1(append, int(const ObStoreRow &row));
  MOCK_METHOD1(append, int(const int64_t macro_block_id));
  MOCK_METHOD0(close, int());
};

}
}






#endif /* MOCK_OB_SSTABLE_H_ */
