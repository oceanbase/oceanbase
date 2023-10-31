/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_STORAGE_COLUMN_STORE_COSSTABLE_ROW_MULTI_GETTER_H_
#define OB_STORAGE_COLUMN_STORE_COSSTABLE_ROW_MULTI_GETTER_H_
#include "ob_cg_sstable_row_getter.h"

namespace oceanbase
{
namespace storage
{
class ObCOSSTableRowMultiGetter : public ObCGSSTableRowGetter
{
public:
  ObCOSSTableRowMultiGetter() :
      ObCGSSTableRowGetter(),
      prefetcher_()
  {
    type_ = ObStoreRowIterator::IteratorCOMultiGet;
  }
  virtual ~ObCOSSTableRowMultiGetter()
  {}
  void reset() override final;
  void reuse() override final;
  INHERIT_TO_STRING_KV("ObCOSSTableRowMultiGetter", ObCGSSTableRowGetter, K_(prefetcher));
protected:
  virtual int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) final;
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&store_row) final;

protected:
  ObIndexTreeMultiPrefetcher prefetcher_;
};

}
}
#endif
