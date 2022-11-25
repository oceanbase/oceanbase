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

#ifndef OB_STORAGE_OB_SSTABLE_ROW_EXISTER_H_
#define OB_STORAGE_OB_SSTABLE_ROW_EXISTER_H_

#include "ob_sstable_row_getter.h"
#include "storage/blocksstable/ob_micro_block_row_exister.h"

namespace oceanbase {
namespace storage {

class ObSSTableRowExister : public ObSSTableRowGetter
{
public:
  ObSSTableRowExister()
      : store_row_(),
      micro_exister_(nullptr)
  {}
  virtual ~ObSSTableRowExister();
  virtual void reset();
  virtual void reuse();
protected:
  virtual int fetch_row(ObSSTableReadHandle &read_handle, const blocksstable::ObDatumRow *&store_row);
private:
  int exist_row(ObSSTableReadHandle &read_handle, blocksstable::ObDatumRow &store_row);
  int exist_block_row(ObSSTableReadHandle &read_handle, blocksstable::ObDatumRow &store_row);

private:
  blocksstable::ObDatumRow store_row_;
  blocksstable::ObMicroBlockRowExister *micro_exister_;
};

}
}

#endif //OB_STORAGE_OB_SSTABLE_ROW_EXISTER_H_
