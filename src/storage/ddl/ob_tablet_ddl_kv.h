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

#ifndef OCEANBASE_STORAGE_DDL_OB_TABLET_DDL_KV_H_
#define OCEANBASE_STORAGE_DDL_OB_TABLET_DDL_KV_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "share/scn.h"
#include "storage/ob_i_table.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_ddl_index_block_row_iterator.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "storage/memtable/mvcc/ob_keybtree.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/access/ob_store_row_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObSSTableMergeRes;
struct ObDatumRowkey;
struct ObDatumRowkeyWrapper;
struct ObStorageDatumUtils;
class ObDataMacroBlockMeta;
}

namespace storage
{

class ObBlockMetaTreeValue final
{
public:
  ObBlockMetaTreeValue() : co_sstable_row_offset_(0), block_meta_(nullptr), rowkey_(nullptr), header_() {}
  ObBlockMetaTreeValue(const blocksstable::ObDataMacroBlockMeta *block_meta,
                       const blocksstable::ObDatumRowkey *rowkey)
    : co_sstable_row_offset_(0), block_meta_(block_meta), rowkey_(rowkey), header_(){}
  ~ObBlockMetaTreeValue()
  {
    co_sstable_row_offset_ = 0;
    block_meta_ = nullptr;
    rowkey_ = nullptr;
  }
  TO_STRING_KV(K_(co_sstable_row_offset), KPC_(block_meta), KPC_(rowkey), K_(header));

public:
  int64_t co_sstable_row_offset_;
  const blocksstable::ObDataMacroBlockMeta *block_meta_;
  const blocksstable::ObDatumRowkey *rowkey_;
  blocksstable::ObIndexBlockRowHeader header_;
};

struct ObDDLBlockMeta
{
public:
  ObDDLBlockMeta() : block_meta_(nullptr), end_row_offset_(-1) {}
  TO_STRING_KV(KPC(block_meta_), K(end_row_offset_));
public:
  const blocksstable::ObDataMacroBlockMeta *block_meta_;
  int64_t end_row_offset_;
};

class ObBlockMetaTree
{
  typedef keybtree::ObKeyBtree<blocksstable::ObDatumRowkeyWrapper, ObBlockMetaTreeValue *> KeyBtree;
  typedef keybtree::BtreeNodeAllocator<blocksstable::ObDatumRowkeyWrapper, ObBlockMetaTreeValue *> BtreeNodeAllocator;
  typedef keybtree::BtreeRawIterator<blocksstable::ObDatumRowkeyWrapper, ObBlockMetaTreeValue *> BtreeRawIterator;
public:
  ObBlockMetaTree();
  virtual ~ObBlockMetaTree();
  int init(ObTablet &tablet,
           const ObITable::TableKey &table_key,
           const share::SCN &ddl_start_scn,
           const uint64_t data_format_version,
           const ObStorageSchema *storage_schema);
  void destroy();
  void destroy_tree_value();
  int insert_macro_block(const ObDDLMacroHandle &macro_handle,
                         const blocksstable::ObDatumRowkey *rowkey,
                         const blocksstable::ObDataMacroBlockMeta *meta,
                         const int64_t co_sstable_row_offset);
  int locate_key(const blocksstable::ObDatumRange &range,
                 const blocksstable::ObStorageDatumUtils &datum_utils,
                 blocksstable::DDLBtreeIterator &iter,
                 ObBlockMetaTreeValue *&cur_tree_value) const;
  int locate_range(const blocksstable::ObDatumRange &range,
                   const blocksstable::ObStorageDatumUtils &datum_utils,
                   const bool is_left_border,
                   const bool is_right_border,
                   const bool is_reverse_scan,
                   blocksstable::DDLBtreeIterator &iter,
                   ObBlockMetaTreeValue *&cur_tree_value) const;
  int skip_to_next_valid_position(const blocksstable::ObDatumRowkey &rowkey,
                                  const blocksstable::ObStorageDatumUtils &datum_utils,
                                  blocksstable::DDLBtreeIterator &iter,
                                  ObBlockMetaTreeValue *&tree_value) const;
  int get_next_tree_value(blocksstable::DDLBtreeIterator &iter,
                          const int64_t step,
                          ObBlockMetaTreeValue *&tree_value) const;
  int64_t get_macro_block_cnt() const { return macro_blocks_.count(); }
  int get_last_rowkey(const blocksstable::ObDatumRowkey *&last_rowkey);
  int get_sorted_meta_array(ObIArray<ObDDLBlockMeta> &meta_array);
  int exist(const blocksstable::ObDatumRowkey *rowkey, bool &is_exist);
  const blocksstable::ObDataStoreDesc &get_data_desc() const { return data_desc_.get_desc(); }
  bool is_valid() const { return is_inited_; }
  int64_t get_memory_used() const;
  const KeyBtree &get_keybtree() const { return block_tree_; }
  TO_STRING_KV(K(is_inited_), K(macro_blocks_.count()), K(arena_.total()), K(data_desc_));

private:
  int lower_bound(const blocksstable::ObDatumRowkey *target_rowkey,
                  const blocksstable::ObStorageDatumUtils &datum_utils,
                  blocksstable::ObDatumRowkey *&rowkey,
                  ObBlockMetaTreeValue *&tree_value) const;
  int upper_bound(const blocksstable::ObDatumRowkey *target_rowkey,
                  const blocksstable::ObStorageDatumUtils &datum_utils,
                  blocksstable::ObDatumRowkey *&rowkey,
                  ObBlockMetaTreeValue *&tree_value) const;

private:
  struct IndexItem final
  {
  public:
    IndexItem() : rowkey_(nullptr), block_meta_(nullptr) {}
    IndexItem(const blocksstable::ObDatumRowkey *rowkey,
              const blocksstable::ObDataMacroBlockMeta *block_meta)
      : rowkey_(rowkey), block_meta_(block_meta) {}
    TO_STRING_KV(KPC_(rowkey), KPC_(block_meta), K_(header));
    const blocksstable::ObDatumRowkey *rowkey_;
    const blocksstable::ObDataMacroBlockMeta *block_meta_;
    blocksstable::ObIndexBlockRowHeader header_;
  };
  struct CompareFunctor
  {
    CompareFunctor(const blocksstable::ObStorageDatumUtils &datum_utils) : datum_utils_(datum_utils) {}
    bool operator ()(const IndexItem &item, const blocksstable::ObDatumRowkey &rowkey);
    bool operator ()(const blocksstable::ObDatumRowkey &rowkey, const IndexItem &item);
    const blocksstable::ObStorageDatumUtils &datum_utils_;
  };

private:
  bool is_inited_;
  ObArray<ObDDLMacroHandle> macro_blocks_;
  ObArenaAllocator arena_;
  BtreeNodeAllocator tree_allocator_;
  KeyBtree block_tree_;
  blocksstable::ObWholeDataStoreDesc data_desc_;
  blocksstable::ObStorageDatumUtils row_id_datum_utils_;
  blocksstable::ObStorageDatumUtils *datum_utils_;
};

class ObDDLMemtable : public blocksstable::ObSSTable
{
public:
  ObDDLMemtable();
  virtual ~ObDDLMemtable();
  int init(
      ObArenaAllocator &allocator,
      ObTablet &tablet,
      const ObITable::TableKey &table_key,
      const share::SCN &ddl_start_scn,
      const uint64_t data_format_version);
  void reset();
  int insert_block_meta_tree(
      const ObDDLMacroHandle &macro_handle,
      blocksstable::ObDataMacroBlockMeta *data_macro_meta,
      const int64_t co_sstable_row_offset);
  void set_scn_range(
      const share::SCN &start_scn,
      const share::SCN &end_scn);
  const ObBlockMetaTree *get_block_meta_tree() { return &block_meta_tree_; }
  int init_ddl_index_iterator(const blocksstable::ObStorageDatumUtils *datum_utils,
                              const bool is_reverse_scan,
                              blocksstable::ObDDLIndexBlockRowIterator *ddl_kv_index_iter);
  int64_t get_memory_used() const { return block_meta_tree_.get_memory_used(); }
  INHERIT_TO_STRING_KV("ObSSTable", ObSSTable, K(is_inited_), K(block_meta_tree_));
private:
  int init_sstable_param(
      ObTablet &tablet,
      const ObITable::TableKey &table_key,
      const share::SCN &ddl_start_scn,
      ObTabletCreateSSTableParam &sstable_param);
private:
  bool is_inited_;
  ObBlockMetaTree block_meta_tree_;
};

class ObDDLKV : public ObITabletMemtable
{
public:
  ObDDLKV();
  ~ObDDLKV();
// full direct load.
  int init(const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &ddl_start_scn,
           const int64_t snapshot_version,
           const share::SCN &last_freezed_scn,
           const uint64_t data_format_version);

public: // derived from ObIMemtable
  // for read_barrier, it needs to be always false
  virtual bool is_empty() const override { return false; }
  virtual int64_t get_occupied_size() const override;

public : // derived from ObITabletMemtable
  virtual bool is_inited() const override { return is_inited_; }
  virtual int init(const ObITable::TableKey &table_key,
                   ObLSHandle &ls_handle,
                   ObFreezer *freezer,
                   ObTabletMemtableMgr *memtable_mgr,
                   const int64_t schema_version,
                   const uint32_t freeze_clock) override;
  virtual void print_ready_for_flush() override;
  virtual int set_frozen() override { ATOMIC_SET(&is_independent_freezed_, true); return OB_SUCCESS; }
  virtual bool can_be_minor_merged() override;
  virtual int get_schema_info(
    const int64_t input_column_cnt,
    int64_t &max_schema_version_on_memtable,
    int64_t &max_column_cnt_on_memtable) const override;
  // TODO : @suzhi.yt implement ddlkv dump2text
  virtual int dump2text(const char *fname) override { return OB_SUCCESS; }

public:  // derived from ObITable
  virtual bool is_frozen_memtable() override;
  virtual int get_frozen_schema_version(int64_t &schema_version) const;

  virtual int exist(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &is_exist,
    bool &has_found);

  virtual int exist(
      ObRowsInfo &rowsInfo,
      bool &is_exist,
      bool &has_found);

  virtual int scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const blocksstable::ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;

  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  blocksstable::ObDatumRow &row) override;
  virtual int multi_get(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;

  virtual int multi_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;

  int check_row_locked(
      const ObTableIterParam &param,
      const blocksstable::ObDatumRowkey &rowkey,
      ObTableAccessContext &context,
      ObStoreRowLockState &lock_state,
      ObRowState &row_state,
      bool check_exist = false);

  // TODO : @jianyun.sjy ObDDLMemtable adapts check_rows_locked
  int check_rows_locked(
      const bool check_exist,
      storage::ObTableAccessContext &context,
      share::SCN &max_trans_version,
      ObRowsInfo &rows_info);

public: // derived from ObFreezeCheckpoint
  virtual int flush(share::ObLSID ls_id) override;
  virtual bool ready_for_flush() override;
  virtual bool rec_scn_is_stable() override;
  virtual void set_allow_freeze(const bool allow_freeze) override;

public:
  void reset();
  void set_freeze_need_retry() { ATOMIC_STORE(&is_independent_freezed_, false); }
  int set_macro_block(
      ObTablet &tablet,
      const ObDDLMacroBlock &macro_block,
      const int64_t snapshot_version,
      const uint64_t data_format_version,
      const bool can_freeze);

  int freeze(const share::SCN &freeze_scn = share::SCN::min_scn());
  bool is_freezed();
  int close();
  int prepare_sstable(const bool need_check = true);
  int decide_right_boundary();
  bool is_closed() const { return is_closed_; }
  share::SCN get_min_scn() const { return min_scn_; }
  share::SCN get_freeze_scn() const { return freeze_scn_; }
  share::SCN get_ddl_start_scn() const { return ddl_start_scn_; }
  int64_t get_macro_block_cnt() const { return macro_block_count_; }
  // not thread safe, external call are limited to ddl merge task
  int get_ddl_memtable(const int64_t cg_idx, ObDDLMemtable *&ddl_memtable);
  ObIArray<ObDDLMemtable *> &get_ddl_memtables() { return ddl_memtables_; }
  void inc_pending_cnt(); // used by ddl kv pending guard
  void dec_pending_cnt();
  // const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  uint64_t get_data_format_version() const { return data_format_version_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  int64_t get_memory_used() const;
  OB_INLINE bool is_inc_ddl_kv() const { return is_inc_ddl_kv_; }

  int64_t get_row_count() const;
  int get_block_count_and_row_count(
    int64_t &macro_block_count,
    int64_t &micro_block_count,
    int64_t &row_count) const;

  // for inc_ddl_kv only
  template<class _callback>
  int access_first_ddl_memtable(_callback &callback) const;

  INHERIT_TO_STRING_KV("ObITabletMemtable",
                       ObITabletMemtable,
                       K_(is_inited),
                       K_(is_closed),
                       K_(is_inc_ddl_kv),
                       K_(is_independent_freezed),
                       K_(ls_id),
                       K_(tablet_id),
                       K_(ddl_start_scn),
                       K_(snapshot_version),
                       K_(data_format_version),
                       K_(trans_id),
                       K_(data_schema_version),
                       K_(column_count),
                       K_(min_scn),
                       K_(max_scn),
                       K_(freeze_scn),
                       K_(pending_cnt),
                       K_(macro_block_count),
                       K_(ddl_memtables));

private:
  bool is_pending() const { return ATOMIC_LOAD(&pending_cnt_) > 0; }
  bool ready_for_flush_();
  bool data_has_completed_(share::SCN &max_decided_scn);
  int wait_pending();
  int full_load_freeze_(const share::SCN &freeze_scn);
  int inc_load_freeze_();

  int create_ddl_memtable(ObTablet &tablet, const ObITable::TableKey &table_key, ObDDLMemtable *&ddl_memtable);

private:
  static const int64_t TOTAL_LIMIT = 10 * 1024 * 1024 * 1024L;
  static const int64_t HOLD_LIMIT = 10 * 1024 * 1024 * 1024L;
  bool is_inited_;
  bool is_closed_;
  bool is_inc_ddl_kv_;
  bool is_independent_freezed_;
  common::TCRWLock lock_; // lock for block_meta_tree_ and freeze_log_ts_
  common::ObArenaAllocator arena_allocator_;
  common::ObTabletID tablet_id_;
  share::SCN ddl_start_scn_; // the log ts of ddl start log
  int64_t ddl_snapshot_version_; // the snapshot version for major sstable which is completed by ddl
  uint64_t data_format_version_;
  transaction::ObTransID trans_id_; // for incremental direct load only
  int64_t data_schema_version_;
  int64_t column_count_;

  // freeze related
  share::SCN min_scn_; // the min log ts of macro blocks
  share::SCN max_scn_; // the max log ts of macro blocks
  int64_t pending_cnt_; // the amount of kvs that are replaying

  int64_t macro_block_count_;
  ObArray<ObDDLMemtable *> ddl_memtables_;
};

template<class _callback>
int ObDDLKV::access_first_ddl_memtable(_callback &callback) const
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!is_inc_ddl_kv())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "not support get for full direct load", K(ret));
  } else if (ddl_memtables_.count() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (ddl_memtables_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "inc direct load do not support column store yet", K(ret));
  } else {
    ObDDLMemtable *ddl_memtable = ddl_memtables_.at(0);
    ret = callback(ddl_memtable);
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase

#endif
