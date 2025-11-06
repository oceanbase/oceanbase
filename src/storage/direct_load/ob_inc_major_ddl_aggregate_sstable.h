/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_OB_INC_MAJOR_DDL_AGGREGATE_SSTABLE_H_
#define OB_STORAGE_OB_INC_MAJOR_DDL_AGGREGATE_SSTABLE_H_

#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{
class ObDDLMemtable;

class ObIncMajorDDLAggregateSSTableEmptyRowIterator : public ObStoreRowIterator
{
public:
  ObIncMajorDDLAggregateSSTableEmptyRowIterator();
  virtual ~ObIncMajorDDLAggregateSSTableEmptyRowIterator();
  virtual int get_next_row(const ObDatumRow *&row) override;
};

class ObIIncMajorDDLAggregateSSTable : public ObCOSSTableV2
{
public:
  ObIIncMajorDDLAggregateSSTable() = default;
  virtual ~ObIIncMajorDDLAggregateSSTable() = default;
  virtual int get_sstable(const ObSSTable *&sstable) const = 0;
  virtual int get_sstables(ObIArray<ObSSTable *> &ddl_sstables) const = 0;
  virtual int get_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const = 0;
  virtual int get_rowkey_sstables(ObIArray<ObSSTable *> &ddl_sstables) const = 0;
  virtual int get_rowkey_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const = 0;
  virtual int64_t get_cg_idx() const = 0;
};

class ObIncMajorDDLAggregateCGSSTable : public ObIIncMajorDDLAggregateSSTable
{
public:
  ObIncMajorDDLAggregateCGSSTable();
  virtual ~ObIncMajorDDLAggregateCGSSTable();
  int init(
      ObArenaAllocator &allocator,
      const ObITable::TableKey &table_key,
      ObSSTable &base_table,
      ObSSTableMetaHandle &meta_handle,
      const int64_t snapshot_version,
      const int64_t column_group_cnt,
      const int64_t rowkey_column_cnt,
      const int64_t column_cnt,
      const int64_t row_cnt,
      const int64_t cg_idx);
  int add_table(const int64_t cg_idx, ObITable *table);
  int add_sstable_wrapper(ObSSTableWrapper &sstable_wrapper);
  int set_rowkey_cg_sstable(const ObIncMajorDDLAggregateCGSSTable *rowkey_cg_sstable);
  int init_index_tree_root();
  virtual int get_sstable(const ObSSTable *&sstable) const override;
  virtual int get_sstables(ObIArray<ObSSTable *> &ddl_sstables) const override;
  virtual int get_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const override;
  virtual int get_rowkey_sstables(ObIArray<ObSSTable *> &ddl_sstables) const override;
  virtual int get_rowkey_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const override;
  virtual int64_t get_cg_idx() const override;
  virtual int get_index_tree_root(
              ObMicroBlockData &index_data,
              const bool need_transform = true);
  virtual bool is_empty() const;
  void reset();
  INHERIT_TO_STRING_KV("ObCOSSTableV2", ObCOSSTableV2, KP(this),
                       K_(cg_idx),
                       K_(ddl_sstables),
                       K_(ddl_memtables),
                       KPC_(rowkey_cg_sstable),
                       K_(is_inited));

private:
  template<typename T>
  int find_sstable_meta_with_valid_root_block(
      const common::ObIArray<T *> &sstables,
      blocksstable::ObSSTableMetaHandle &meta_handle);
private:
  int64_t cg_idx_;
  ObArray<ObSSTable *> ddl_sstables_;
  ObArray<ObDDLMemtable *> ddl_memtables_;
  const ObIncMajorDDLAggregateCGSSTable *rowkey_cg_sstable_;
  ObArray<ObSSTableWrapper> sstable_wrappers_; // 管理 cg 的 生命周期
  ObSSTableMetaHandle meta_handle_;
  ObMicroBlockData index_tree_root_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIncMajorDDLAggregateCGSSTable);
};

class ObIncMajorDDLAggregateCOSSTable : public ObIIncMajorDDLAggregateSSTable
{
  using EmptyRowIterator = ObIncMajorDDLAggregateSSTableEmptyRowIterator;
  static const int64_t CG_SSTABLE_COUNT = 128;
public:
  ObIncMajorDDLAggregateCOSSTable();
  virtual ~ObIncMajorDDLAggregateCOSSTable();
  int init(
      ObSSTable &base_table,
      const ObITable::TableKey &table_key,
      const int64_t column_group_cnt,
      const int64_t column_cnt,
      const int64_t snapshot_version,
      const ObCOSSTableBaseType co_base_type,
      ObIArray<ObITable *> &tables);
  virtual int get_sstable(const ObSSTable *&sstable) const override;
  virtual int get_sstables(ObIArray<ObSSTable *> &ddl_sstables) const override;
  virtual int get_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const override;
  virtual int get_rowkey_sstables(ObIArray<ObSSTable *> &ddl_sstables) const override;
  virtual int get_rowkey_memtables(ObIArray<ObDDLMemtable *> &ddl_memtables) const override;
  virtual int64_t get_cg_idx() const override;
  virtual int get_index_tree_root(blocksstable::ObMicroBlockData &index_data,
                                  const bool need_transform = true);
  virtual bool is_empty() const;
  INHERIT_TO_STRING_KV("ObCOSSTableV2", ObCOSSTableV2, KP(this),
                       K_(tablet_id),
                       K_(snapshot_version),
                       KP_(base_sstable),
                       K_(column_group_cnt),
                       K_(rowkey_column_cnt),
                       K_(column_cnt),
                       K_(row_cnt),
                       K_(tx_info),
                       KP_(rowkey_cg_sstable),
                       K_(is_inited));
public:
  // derived from ObCOSSTableV2
  virtual int fetch_cg_sstable(
          const uint32_t cg_idx,
          ObSSTableWrapper &cg_wrapper) const override;
  virtual int get_cg_sstable(const uint32_t cg_idx, ObSSTableWrapper &cg_wrapper) const override;
  virtual int get_all_tables(common::ObIArray<ObSSTableWrapper> &table_wrappers) const override;
  int check_can_access(const ObTableAccessContext &context, bool &can_access) const;
public:
  // derived from ObCOSSTableV2
  // query interfaces
  virtual int scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;
  virtual int get(
      const storage::ObTableIterParam &param,
      ObTableAccessContext &context,
      const ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_get(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;
private:
  int calculate_rowkey_column_cnt_and_row_cnt(ObIArray<ObITable *> &tables);
  int add_table(ObITable *table);
  int init_tx_info(const ObSSTable &sstable);
  int add_ddl_sstable(ObSSTable *sstable);
  int add_ddl_memtable(ObSSTable *memtable);
  int add_cg_sstable(const int64_t cg_idx, ObITable *table);
  int add_cg_sstable_wrapper(const int64_t cg_idx, ObSSTableWrapper &cg_wrapper);
  int prepare_before_query();
  int check_all_cg();
  int init_index_tree_root();
  int set_rowkey_cg_sstable();
private:
  void destroy();
private:
  struct TxInfo {
    TxInfo() : trans_id_(), seq_no_() {};
    TxInfo(transaction::ObTransID &trans_id, transaction::ObTxSEQ &seq_no)
     : trans_id_(trans_id), seq_no_(seq_no) {}
    void reset();
    TO_STRING_KV(K_(trans_id), K_(seq_no));
    transaction::ObTransID trans_id_;
    transaction::ObTxSEQ seq_no_;
  };
private:
  ObArenaAllocator allocator_;
  ObTabletID tablet_id_;
  int64_t snapshot_version_;
  ObSSTable *base_sstable_;
  ObSSTableMetaHandle base_sstable_meta_;
  int64_t column_group_cnt_;
  int64_t rowkey_column_cnt_;
  int64_t column_cnt_;
  int64_t row_cnt_;
  TxInfo tx_info_;
  hash::ObHashMap<int64_t, ObIncMajorDDLAggregateCGSSTable *, common::hash::NoPthreadDefendMode> cg_sstables_;
  ObIncMajorDDLAggregateCGSSTable *rowkey_cg_sstable_;
  ObArray<ObSSTableMetaHandle> co_meta_handles_; // 管理co sstable 的生命周期
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIncMajorDDLAggregateCOSSTable);
};

} // namespace storage
} // namespace oceanbase

#endif /* OB_STORAGE_OB_INC_MAJOR_DDL_AGGREGATE_SSTABLE_H_ */
