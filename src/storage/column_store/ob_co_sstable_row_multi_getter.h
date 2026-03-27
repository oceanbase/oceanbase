/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
