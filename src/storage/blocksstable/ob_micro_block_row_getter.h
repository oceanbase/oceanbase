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

#ifndef OB_MICRO_BLOCK_ROW_GETTER_H_
#define OB_MICRO_BLOCK_ROW_GETTER_H_

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_sparse_micro_block_reader.h"

namespace oceanbase {
namespace blocksstable {
class ObIMicroBlockRowFetcher {
public:
  ObIMicroBlockRowFetcher();
  virtual ~ObIMicroBlockRowFetcher();
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const storage::ObSSTable* sstable);

protected:
  int prepare_reader(const ObFullMacroBlockMeta& macro_meta);
  const storage::ObTableIterParam* param_;
  storage::ObTableAccessContext* context_;
  const storage::ObSSTable* sstable_;
  ObIMicroBlockGetReader* reader_;
  ObMicroBlockGetReader* flat_reader_;
  ObMultiVersionBlockGetReader* multi_version_reader_;
  ObSparseMicroBlockGetReader* sparse_reader_;
  bool is_multi_version_;
  bool is_inited_;
};

class ObMicroBlockRowGetter : public ObIMicroBlockRowFetcher {
public:
  ObMicroBlockRowGetter();
  virtual ~ObMicroBlockRowGetter();
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const storage::ObSSTable* sstable);
  int get_row(const common::ObStoreRowkey& rowkey, const MacroBlockId macro_id, const int64_t file_id,
      const ObFullMacroBlockMeta& macro_meta, const ObMicroBlockData& block_data,
      const storage::ObSSTableRowkeyHelper* rowkey_helper, const storage::ObStoreRow*& row);
  int get_cached_row(const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
      const ObRowCacheValue& value, const storage::ObStoreRow*& row);
  int get_not_exist_row(const common::ObStoreRowkey& rowkey, const storage::ObStoreRow*& row);

private:
  int project_row(const ObStoreRowkey& rowkey, const ObRowCacheValue& value, const ObColumnMap& column_map,
      const storage::ObStoreRow*& row);
  int project_cache_row(const ObStoreRowkey& rowkey, const ObRowCacheValue& value, const ObColumnMap& column_map,
      const storage::ObStoreRow*& row);
  int project_cache_sparse_row(const ObStoreRowkey& rowkey, const ObRowCacheValue& value, const ObColumnMap& column_map,
      const storage::ObStoreRow*& row);

private:
  common::ObArenaAllocator allocator_;
  ObColumnMap column_map_;
  storage::ObStoreRow row_;
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj)];
  storage::ObStoreRow full_row_;
  char full_row_obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj)];
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MICRO_BLOCK_ROW_GETTER_H_ */
