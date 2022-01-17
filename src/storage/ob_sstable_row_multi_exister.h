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

#ifndef OB_SSTABLE_ROW_MULTI_EXISTER_H_
#define OB_SSTABLE_ROW_MULTI_EXISTER_H_

#include "ob_sstable_row_multi_getter.h"

namespace oceanbase {
namespace storage {

class ObSSTableRowMultiExister : public ObSSTableRowMultiGetter {
public:
  ObSSTableRowMultiExister();
  virtual ~ObSSTableRowMultiExister();

protected:
  virtual int fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);

private:
  ObStoreRow store_row_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_SSTABLE_ROW_MULTI_EXISTER_H_ */
