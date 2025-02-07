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

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_GETTER_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_GETTER_H_

#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "ob_datum_row.h"
#include "ob_row_cache.h"

namespace oceanbase
{
namespace storage {
struct ObSSTableReadHandle;
}

namespace blocksstable
{
class ObMacroBlockReader;
class ObCSEncodeBlockGetReader;

class ObIMicroBlockRowFetcher {
public:
  ObIMicroBlockRowFetcher();
  virtual ~ObIMicroBlockRowFetcher();
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable);
  virtual int switch_context(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable);
  OB_INLINE int64_t get_average_row_length() const
  {
    return nullptr != reader_ && 0 < reader_->row_count()  ?
      reader_->original_data_length() / reader_->row_count() : 0;
  }
protected:
  int prepare_reader(const ObRowStoreType store_type);
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
  const blocksstable::ObSSTable *sstable_;
  ObIMicroBlockGetReader *reader_;
  ObMicroBlockGetReader *flat_reader_;
  ObEncodeBlockGetReader *encode_reader_;
  ObCSEncodeBlockGetReader *cs_encode_reader_;
  const ObITableReadInfo *read_info_;
  ObIAllocator *long_life_allocator_;
  bool is_inited_;
};

class ObMicroBlockRowGetter : public ObIMicroBlockRowFetcher
{
public:
  ObMicroBlockRowGetter() : row_(), cache_project_row_() {};
  virtual ~ObMicroBlockRowGetter() {};
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
  int get_row(
      ObSSTableReadHandle &get_handle,
      const ObDatumRow *&store_row,
      ObMacroBlockReader *block_reader);
  virtual int switch_context(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
private:
  int get_block_row(ObSSTableReadHandle &read_handle, ObMacroBlockReader &block_reader, const ObDatumRow *&store_row);
  int get_cached_row(const ObDatumRowkey &rowkey, const ObRowCacheValue &value, const ObDatumRow *&row);
  int get_not_exist_row(const ObDatumRowkey &rowkey, const ObDatumRow *&row);
  int project_cache_row(const ObRowCacheValue &value, ObDatumRow &row);
  int inner_get_row(
      const MacroBlockId &macro_id,
      const ObDatumRowkey &rowkey,
      const ObMicroBlockData &block_data,
      const ObDatumRow *&row);
private:
  ObDatumRow row_;
  ObDatumRow cache_project_row_;
};

class ObMicroBlockCGRowGetter : public ObIMicroBlockRowFetcher
{
public:
  ObMicroBlockCGRowGetter() : row_() {};
  virtual ~ObMicroBlockCGRowGetter() {};
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
  virtual int switch_context(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
  int get_row(
      ObSSTableReadHandle &read_handle,
      ObMacroBlockReader &block_reader,
      const uint32_t row_idx,
      const ObDatumRow *&store_row);
private:
  int get_block_row(ObSSTableReadHandle &read_handle,
                    ObMacroBlockReader &block_reader,
                    const uint32_t row_idx,
                    const ObDatumRow *&row);
  int get_not_exist_row(const ObDatumRow *&row);

private:
  ObDatumRow row_;
};

}
}
#endif //OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_GETTER_H_
