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

#ifndef OB_PARTITION_MERGE_BUILDER_H_
#define OB_PARTITION_MERGE_BUILDER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_bit_set.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/schema/ob_table_schema.h"
#include "share/stat/ob_column_stat.h"
#include "storage/ob_i_store.h"
#include "storage/ob_sstable.h"
#include "storage/ob_i_partition_group.h"
#include "storage/blocksstable/ob_bloom_filter_data_writer.h"
#include "storage/blocksstable/ob_bloom_filter_data_reader.h"

namespace oceanbase {
namespace storage {
struct ObSSTableMergeInfo;
class ObPartitionStorage;
class ObSSTableMergeContext;
struct ObSSTableMergeCtx;
}  // namespace storage
namespace memtable {
class ObIMemtableCtxFactory;
}

namespace share {
namespace schema {
class ObColDesc;
}
}  // namespace share

namespace compaction {

struct ObCompactRowType {
  enum ObCompactRowTypeEnum { T_INVALID = -1, T_BASE_ROW = 0, T_INSERT_ROW, T_UPDATE_ROW, T_DELETE_ROW };
};

class ObIStoreRowProcessor {
public:
  ObIStoreRowProcessor()
  {}
  virtual ~ObIStoreRowProcessor()
  {}
  virtual int open(storage::ObSSTableMergeCtx& ctx, const int64_t idx, const bool iter_complement,
      const ObIArray<share::schema::ObColDesc>& column_ids,
      const ObIArray<blocksstable::ObMacroBlockInfoPair>* lob_blocks = NULL) = 0;
  virtual int process(const blocksstable::ObMacroBlockCtx& macro_block_ctx) = 0;
  virtual int process(const blocksstable::ObMicroBlock& micro_block) = 0;
  virtual int process(const storage::ObStoreRow& row, const ObCompactRowType::ObCompactRowTypeEnum type) = 0;
  virtual int close() = 0;
  virtual void reset() = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
  virtual const blocksstable::ObDataStoreDesc* get_data_store_desc() const = 0;
  virtual void set_purged_count(const int64_t count) = 0;
};

class ObMacroBlockBuilder : public ObIStoreRowProcessor {
public:
  ObMacroBlockBuilder();
  virtual ~ObMacroBlockBuilder();
  virtual int open(storage::ObSSTableMergeCtx& ctx, const int64_t idx, const bool iter_complement,
      const ObIArray<share::schema::ObColDesc>& column_ids,
      const ObIArray<blocksstable::ObMacroBlockInfoPair>* lob_blocks = NULL) override;
  virtual int init_bloomfilter_if_need(storage::ObSSTableMergeCtx& ctx);
  virtual int process(const blocksstable::ObMacroBlockCtx& macro_block_ctx);
  virtual int process(const blocksstable::ObMicroBlock& micro_block);
  virtual int process(const storage::ObStoreRow& row, const ObCompactRowType::ObCompactRowTypeEnum type);
  virtual int close();
  virtual void reset();
  virtual const blocksstable::ObDataStoreDesc* get_data_store_desc() const
  {
    return &desc_;
  }
  virtual void set_purged_count(const int64_t count);
  TO_STRING_KV(K_(desc), K_(sstable_merge_info), K_(writer));

protected:
  void est_row(const ObCompactRowType::ObCompactRowTypeEnum& type, const bool is_output);
  int check_row_columns(const storage::ObStoreRow& row);
  int check_sparse_row_columns(const storage::ObStoreRow& row);
  int check_flat_row_columns(const storage::ObStoreRow& row);
  OB_INLINE int check_row_column(const storage::ObStoreRow& row, const int64_t idx);
  OB_INLINE int check_sparse_row_column(const common::ObObj& obj, const int64_t idx);
  int append_bloom_filter(const storage::ObStoreRow& row);

private:
  storage::ObMergeType merge_type_;
  blocksstable::ObMacroBlockWriter* writer_;
  blocksstable::ObDataStoreDesc desc_;
  storage::ObSSTableMergeInfo sstable_merge_info_;
  int64_t task_idx_;
  blocksstable::ObBlockMarkDeletionMaker* mark_deletion_maker_;
  storage::ObSSTableMergeContext* merge_context_;
  common::ObArenaAllocator allocator_;
  bool need_build_bloom_filter_;
  blocksstable::ObBloomFilterDataWriter bf_macro_writer_;
  share::schema::ColumnMap* cols_id_map_;
  bool is_opened_;
};

class ObMacroBlockEstimator : public ObIStoreRowProcessor {
public:
  ObMacroBlockEstimator();
  virtual ~ObMacroBlockEstimator();
  virtual int open(storage::ObSSTableMergeCtx& ctx, const int64_t idx, const bool iter_complement,
      const ObIArray<share::schema::ObColDesc>& column_ids,
      const ObIArray<blocksstable::ObMacroBlockInfoPair>* lob_blocks = NULL) override;
  virtual int process(const blocksstable::ObMacroBlockCtx& macro_block_ctx);
  virtual int process(const blocksstable::ObMicroBlock& micro_block);
  virtual int process(const storage::ObStoreRow& row, const ObCompactRowType::ObCompactRowTypeEnum type);
  virtual int close();
  virtual void reset();
  OB_INLINE void set_component(ObIStoreRowProcessor* component)
  {
    component_ = component;
  }
  virtual const blocksstable::ObDataStoreDesc* get_data_store_desc() const
  {
    return is_opened_ ? component_->get_data_store_desc() : NULL;
  }
  virtual void set_purged_count(const int64_t count);
  TO_STRING_KV(KP(component_));

private:
  int update_estimator(const storage::ObStoreRow& row);
  ObIStoreRowProcessor* component_;
  bool is_opened_;
  int64_t partition_id_;
  int64_t stat_sampling_ratio_;
  int64_t stat_sampling_count_;
  common::ObArray<common::ObColumnStat*> column_stats_;
  storage::ObSSTableMergeContext* merge_context_;
  common::ObArenaAllocator allocator_;
};

} /* namespace compaction */
} /* namespace oceanbase */

#endif /* OB_PARTITION_MERGE_BUILDER_H_ */
