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

#ifndef OB_STORAGE_OB_SSTABLE_ROW_SCANNER_H_
#define OB_STORAGE_OB_SSTABLE_ROW_SCANNER_H_

#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "ob_index_tree_prefetcher.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {
template<typename PrefetchType = ObIndexTreeMultiPassPrefetcher<>>
class ObSSTableRowScanner : public ObStoreRowIterator
{
public:
  ObSSTableRowScanner() :
      ObStoreRowIterator(),
      is_opened_(false),
      sstable_(nullptr),
      iter_param_(nullptr),
      access_ctx_(nullptr),
      prefetcher_(),
      macro_block_reader_(),
      micro_scanner_(nullptr),
      cur_range_idx_(-1)
  {
    type_ = ObStoreRowIterator::IteratorScan;
  }
  virtual ~ObSSTableRowScanner();
  virtual void reset();
  virtual void reuse();
  TO_STRING_KV(K_(is_opened), K_(cur_range_idx), K_(prefetcher));
protected:
  int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range);
  virtual int inner_get_next_row(const ObDatumRow *&store_row) override;
  virtual int fetch_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row);
  virtual int refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey) override final;
  virtual int get_next_rows() override;
private:
  OB_INLINE int init_micro_scanner();
  OB_INLINE bool can_vectorize() const;
  int open_cur_data_block(ObSSTableReadHandle &read_handle);
  int fetch_rows(ObSSTableReadHandle &read_handle);

protected:
  bool is_opened_;
  ObSSTable *sstable_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  PrefetchType prefetcher_;
  ObMacroBlockReader macro_block_reader_;
  ObIMicroBlockRowScanner *micro_scanner_;
private:
  int64_t cur_range_idx_;
};

}
}
#endif //OB_STORAGE_OB_SSTABLE_ROW_SCANNER_H_
