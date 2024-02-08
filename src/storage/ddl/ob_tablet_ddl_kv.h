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
  ObBlockMetaTreeValue() : block_meta_(nullptr), rowkey_(nullptr), header_() {}
  ObBlockMetaTreeValue(const blocksstable::ObDataMacroBlockMeta *block_meta,
                       const blocksstable::ObDatumRowkey *rowkey)
    : block_meta_(block_meta), rowkey_(rowkey), header_(){}
  ~ObBlockMetaTreeValue()
  {
    block_meta_ = nullptr;
    rowkey_ = nullptr;
  }
  TO_STRING_KV(KPC_(block_meta), KPC_(rowkey), K_(header));

public:
  const blocksstable::ObDataMacroBlockMeta *block_meta_;
  const blocksstable::ObDatumRowkey *rowkey_;
  blocksstable::ObIndexBlockRowHeader header_;
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
           const uint64_t data_format_version);
  void destroy();
  void destroy_tree_value();
  int insert_macro_block(const ObDDLMacroHandle &macro_handle,
                         const blocksstable::ObDatumRowkey *rowkey,
                         const blocksstable::ObDataMacroBlockMeta *meta);
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
  int get_sorted_meta_array(ObIArray<const blocksstable::ObDataMacroBlockMeta *> &meta_array);
  int exist(const blocksstable::ObDatumRowkey *rowkey, bool &is_exist);
  const blocksstable::ObDataStoreDesc &get_data_desc() const { return data_desc_.get_desc(); }
  bool is_valid() const { return is_inited_; }
  int64_t get_memory_used() const;
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
      ObTablet &tablet,
      const ObITable::TableKey &table_key,
      const share::SCN &ddl_start_scn,
      const uint64_t data_format_version);
  void reset();
  int insert_block_meta_tree(
      const ObDDLMacroHandle &macro_handle,
      blocksstable::ObDataMacroBlockMeta *data_macro_meta);
  void set_scn_range(
      const share::SCN &start_scn,
      const share::SCN &end_scn);
  int get_sorted_meta_array(
      ObIArray<const blocksstable::ObDataMacroBlockMeta *> &meta_array);
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
  ObArenaAllocator allocator_;
  ObBlockMetaTree block_meta_tree_;
};

class ObDDLKV
{
public:
  ObDDLKV();
  ~ObDDLKV();
  int init(const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &ddl_start_scn,
           const int64_t snapshot_version,
           const share::SCN &last_freezed_scn,
           const uint64_t data_format_version);
  void reset();
  int set_macro_block(
      ObTablet &tablet,
      const ObDDLMacroBlock &macro_block,
      const int64_t snapshot_version,
      const uint64_t data_format_version,
      const bool can_freeze);

  int freeze(const share::SCN &freeze_scn);
  bool is_freezed() const { return ATOMIC_LOAD(&is_freezed_); }
  int close();
  int prepare_sstable(const bool need_check = true);
  bool is_closed() const { return is_closed_; }
  share::SCN get_min_scn() const { return min_scn_; }
  share::SCN get_freeze_scn() const { return freeze_scn_; }
  share::SCN get_ddl_start_scn() const { return ddl_start_scn_; }
  share::SCN get_start_scn() const { return last_freezed_scn_; }
  share::SCN get_end_scn() const { return freeze_scn_; }
  int64_t get_macro_block_cnt() const { return macro_block_count_; }
  int create_ddl_memtable(ObTablet &tablet, const ObITable::TableKey &table_key, ObDDLMemtable *&ddl_memtable);
  int get_ddl_memtable(const int64_t cg_idx, ObDDLMemtable *&ddl_memtable);
  ObIArray<ObDDLMemtable *> &get_ddl_memtables() { return ddl_memtables_; }
  void inc_pending_cnt(); // used by ddl kv pending guard
  void dec_pending_cnt();
  void inc_ref();
  int64_t dec_ref();
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  int64_t get_snapshot_version() const { return snapshot_version_; }
  int64_t get_memory_used() const;
  TO_STRING_KV(K_(is_inited), K_(is_closed), K_(ref_cnt), K_(ls_id), K_(tablet_id),
      K_(ddl_start_scn), K_(snapshot_version), K_(data_format_version),
      K_(is_freezed), K_(last_freezed_scn), K_(min_scn), K_(max_scn), K_(freeze_scn), K_(pending_cnt),
      K_(macro_block_count), K_(ddl_memtables));
private:
  bool is_pending() const { return ATOMIC_LOAD(&pending_cnt_) > 0; }
  int wait_pending();
private:
  static const int64_t TOTAL_LIMIT = 10 * 1024 * 1024 * 1024L;
  static const int64_t HOLD_LIMIT = 10 * 1024 * 1024 * 1024L;
  bool is_inited_;
  bool is_closed_;
  int64_t ref_cnt_;
  common::TCRWLock lock_; // lock for block_meta_tree_ and freeze_log_ts_
  common::ObArenaAllocator arena_allocator_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN ddl_start_scn_; // the log ts of ddl start log
  int64_t snapshot_version_; // the snapshot version for major sstable which is completed by ddl
  uint64_t data_format_version_;

  // freeze related
  bool is_freezed_;
  share::SCN last_freezed_scn_; // the freezed log ts of last ddl kv. the log ts range of this ddl kv is (last_freezed_log_ts_, freeze_log_ts_]
  share::SCN min_scn_; // the min log ts of macro blocks
  share::SCN max_scn_; // the max log ts of macro blocks
  share::SCN freeze_scn_; // ddl kv refuse data larger than freeze log ts, freeze_log_ts >= max_log_ts
  int64_t pending_cnt_; // the amount of kvs that are replaying

  int64_t macro_block_count_;
  ObArray<ObDDLMemtable *> ddl_memtables_;
};


}  // end namespace storage
}  // end namespace oceanbase

#endif
