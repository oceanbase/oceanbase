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
#ifndef OB_STORAGE_COLUMN_STORE_CG_SSTABLE_ROW_GETTER_H_
#define OB_STORAGE_COLUMN_STORE_CG_SSTABLE_ROW_GETTER_H_
#include "ob_cg_iter_param_pool.h"
#include "storage/ob_row_fuse.h"
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/ob_micro_block_row_getter.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

namespace oceanbase
{
namespace storage
{
// bloom filter not supported in ObCGGetter because no primary key exist
class ObCGGetter
{
public:
  ObCGGetter() :
      is_inited_(false),
      is_same_data_block_(false),
      sstable_(nullptr),
      table_wrapper_(),
      iter_param_(nullptr),
      access_ctx_(nullptr),
      read_handle_(),
      micro_getter_(nullptr)
  {}
  virtual ~ObCGGetter();
  void reset();
  void reuse();
  int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObSSTableWrapper &wrapper,
      const blocksstable::ObDatumRowkey &idx_key);
  int get_next_row(ObMacroBlockReader &block_reader, const blocksstable::ObDatumRow *&store_row);
  TO_STRING_KV(K_(is_inited), K_(is_same_data_block), K_(prefetcher), KPC_(sstable));

protected:
  bool is_inited_;
  bool is_same_data_block_;
  ObSSTable *sstable_;
  ObSSTableWrapper table_wrapper_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  ObIndexTreePrefetcher prefetcher_;
private:
  ObSSTableReadHandle read_handle_;
  blocksstable::ObMicroBlockCGRowGetter *micro_getter_;
};

class ObCGSSTableRowGetter : public ObStoreRowIterator
{
public:
  ObCGSSTableRowGetter() :
      ObStoreRowIterator(),
      is_inited_(false),
      row_(),
      co_sstable_(nullptr),
      cg_param_pool_(nullptr),
      access_ctx_(nullptr),
      iter_param_(nullptr),
      reader_(nullptr),
      flat_reader_(nullptr),
      encode_reader_(nullptr),
      cs_encode_reader_(nullptr),
      micro_getter_(nullptr),
      row_idx_datum_(),
      row_idx_key_(),
      macro_block_reader_(),
      row_getters_()
  {}
  virtual ~ObCGSSTableRowGetter();
  virtual void reset() override;
  virtual void reuse() override;
  int init(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObIndexTreePrefetcher &prefetcher,
      ObITable *table,
      const void *query_range);
  int fetch_row(
      ObSSTableReadHandle &read_handle,
      const ObNopPos *nop_pos,
      const blocksstable::ObDatumRow *&store_row);
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(row_idx_datum), K_(row_idx_key));

private:
  int init_cg_param_pool(ObTableAccessContext &context);
  int prepare_reader(const ObRowStoreType store_type);
  int get_row_id(ObSSTableReadHandle &read_handle, ObCSRowId &row_id);
  int prepare_cg_row_getter(const ObCSRowId row_id, const ObNopPos *nop_pos, ObIArray<int32_t> &project_idxs);
  int fetch_rowkey_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row);
  int get_not_exist_row(const ObDatumRowkey &rowkey, ObDatumRow &row);

protected:
  bool is_inited_;
  blocksstable::ObDatumRow row_;

private:
  ObCOSSTableV2 *co_sstable_;
  ObCGIterParamPool *cg_param_pool_;
  ObTableAccessContext *access_ctx_;
  const ObTableIterParam *iter_param_;
  ObIMicroBlockGetReader *reader_;
  ObMicroBlockGetReader *flat_reader_;
  ObEncodeBlockGetReader *encode_reader_;
  ObCSEncodeBlockGetReader *cs_encode_reader_;
  blocksstable::ObMicroBlockRowGetter *micro_getter_;
  ObStorageDatum row_idx_datum_;
  blocksstable::ObDatumRowkey row_idx_key_;
  ObMacroBlockReader macro_block_reader_;
  ObReallocatedFixedArray<ObCGGetter> row_getters_;
};

}
}
#endif
