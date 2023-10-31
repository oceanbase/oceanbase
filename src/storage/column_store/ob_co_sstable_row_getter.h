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
#ifndef OB_STORAGE_COLUMN_STORE_COSSTABLE_ROW_GETTER_H_
#define OB_STORAGE_COLUMN_STORE_COSSTABLE_ROW_GETTER_H_
#include "ob_cg_sstable_row_getter.h"

namespace oceanbase
{
namespace storage
{
// row cache not supported in ObCOSSTableRowGetter because it is not efficient
class ObCOSSTableRowGetter : public ObCGSSTableRowGetter
{
public:
  ObCOSSTableRowGetter() :
      ObCGSSTableRowGetter(),
      has_fetched_(false),
      nop_pos_(nullptr),
      read_handle_(),
      prefetcher_()
  {
    type_ = ObStoreRowIterator::IteratorCOSingleGet;
  }
  virtual ~ObCOSSTableRowGetter()
  {}
  virtual void reset() override final;
  virtual void reuse() override final;
  void set_nop_pos(const ObNopPos *nop_pos)
  { nop_pos_ = nop_pos; }
  INHERIT_TO_STRING_KV("ObCOSSTableRowGetter", ObCGSSTableRowGetter, K_(prefetcher));
protected:
  virtual int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) final;
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&store_row) final;

private:
  bool has_fetched_;
  // nop_pos is used for opt in single merge, only get columns without update
  const ObNopPos *nop_pos_;
  ObSSTableReadHandle read_handle_;
  ObIndexTreePrefetcher prefetcher_;
};

}
}
#endif
