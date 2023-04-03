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

#ifndef OB_STORAGE_OB_SSTABLE_ROW_MULTI_GETTER_H_
#define OB_STORAGE_OB_SSTABLE_ROW_MULTI_GETTER_H_

#include "storage/blocksstable/ob_micro_block_row_getter.h"
#include "ob_index_tree_prefetcher.h"

namespace oceanbase {
namespace storage {
class ObSSTableRowMultiGetter : public ObStoreRowIterator
{
public:
  ObSSTableRowMultiGetter() :
      ObStoreRowIterator(),
      sstable_(nullptr),
      iter_param_(nullptr),
      access_ctx_(nullptr),
      prefetcher_(),
      macro_block_reader_(),
      is_opened_(false),
      micro_getter_(nullptr)
  {
    type_ = ObStoreRowIterator::IteratorMultiGet;
  }

  virtual ~ObSSTableRowMultiGetter();
  virtual void reset() override;
  virtual void reuse() override;
  TO_STRING_KV(K_(is_opened), K_(prefetcher));
protected:
  int inner_open(
      const ObTableIterParam &access_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range) final;
  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&store_row);
  virtual int fetch_row(ObSSTableReadHandle &read_handle, const blocksstable::ObDatumRow *&store_row);

protected:
  ObSSTable *sstable_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  ObIndexTreeMultiPrefetcher prefetcher_;
  ObMacroBlockReader macro_block_reader_;
private:
  bool is_opened_;
  blocksstable::ObMicroBlockRowGetter *micro_getter_;
};

}
}
#endif //OB_STORAGE_OB_SSTABLE_ROW_MULTI_GETTER_H_
