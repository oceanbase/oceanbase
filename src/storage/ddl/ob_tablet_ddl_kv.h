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
#include "storage/blocksstable/ob_index_block_builder.h"
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

class ObBlockMetaTree
{
  typedef keybtree::ObKeyBtree<blocksstable::ObDatumRowkeyWrapper, blocksstable::ObDataMacroBlockMeta *> KeyBtree;
  typedef keybtree::BtreeIterator<blocksstable::ObDatumRowkeyWrapper, blocksstable::ObDataMacroBlockMeta *> BtreeIterator;
  typedef keybtree::BtreeNodeAllocator<blocksstable::ObDatumRowkeyWrapper, blocksstable::ObDataMacroBlockMeta *> BtreeNodeAllocator;
  typedef keybtree::BtreeRawIterator<blocksstable::ObDatumRowkeyWrapper, blocksstable::ObDataMacroBlockMeta *> BtreeRawIterator;
public:
  ObBlockMetaTree();
  virtual ~ObBlockMetaTree();
  int init(ObTablet &tablet,
           const ObITable::TableKey &table_key,
           const share::SCN &ddl_start_scn,
           const int64_t data_format_version);
  void destroy();
  int insert_macro_block(const ObDDLMacroHandle &macro_handle,
                         const blocksstable::ObDatumRowkey *rowkey,
                         const blocksstable::ObDataMacroBlockMeta *meta);
  int locate_range(const blocksstable::ObDatumRange &range,
                   const blocksstable::ObStorageDatumUtils &datum_utils,
                   const bool is_left_border,
                   const bool is_right_border,
                   int64_t &begin_idx,
                   int64_t &end_idx);
  int get_index_block_row_header(const int64_t idx,
                                 const blocksstable::ObIndexBlockRowHeader *&header,
                                 const blocksstable::ObDatumRowkey *&endkey);
  int get_macro_block_meta(const int64_t idx,
                           blocksstable::ObDataMacroBlockMeta &macro_meta);
  int64_t get_macro_block_cnt() const { return macro_blocks_.count(); }
  int get_last_rowkey(const blocksstable::ObDatumRowkey *&last_rowkey);
  int build_sorted_rowkeys();
  int get_sorted_meta_array(ObIArray<const blocksstable::ObDataMacroBlockMeta *> &meta_array) const;
  int exist(const blocksstable::ObDatumRowkey *rowkey, bool &is_exist);
  const blocksstable::ObDataStoreDesc &get_data_desc() const { return data_desc_; }
  TO_STRING_KV(K(is_inited_), K(macro_blocks_.count()), K(arena_.total()), K(data_desc_), K(sorted_rowkeys_.count()));
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
  blocksstable::ObDataStoreDesc data_desc_;
  ObArray<IndexItem> sorted_rowkeys_;
};


class ObDDLKV : public blocksstable::ObSSTable
{
public:
  ObDDLKV();
  virtual ~ObDDLKV();
  virtual void inc_ref() override;
  virtual int64_t dec_ref() override;
  virtual int64_t get_ref() const override { return ObITable::get_ref(); }
  int init(ObTablet &tablet,
           const share::SCN &ddl_start_scn,
           const int64_t snapshot_version,
           const share::SCN &last_freezed_scn,
           const int64_t data_format_version);
  void reset();
  int set_macro_block(ObTablet &tablet, const ObDDLMacroBlock &macro_block);

  int freeze(const share::SCN &freeze_scn);
  bool is_freezed() const { return ATOMIC_LOAD(&is_freezed_); }
  int close(ObTablet &tablet);
  int prepare_sstable(const bool need_check = true);
  bool is_closed() const { return is_closed_; }
  share::SCN get_min_scn() const { return min_scn_; }
  share::SCN get_freeze_scn() const { return freeze_scn_; }
  share::SCN get_ddl_start_scn() const { return ddl_start_scn_; }
  share::SCN get_start_scn() const { return last_freezed_scn_; }
  int64_t get_macro_block_cnt() const { return block_meta_tree_.get_macro_block_cnt(); }
  void inc_pending_cnt(); // used by ddl kv pending guard
  void dec_pending_cnt();
  bool is_pending() const { return ATOMIC_LOAD(&pending_cnt_) > 0; }
  int wait_pending();
  INHERIT_TO_STRING_KV("ObSSTable", ObSSTable, K_(is_inited), K_(ls_id), K_(tablet_id), K_(ddl_start_scn), K_(snapshot_version),
      K_(is_freezed), K_(is_closed),
      K_(last_freezed_scn), K_(min_scn), K_(max_scn), K_(freeze_scn),
      K_(pending_cnt), K_(data_format_version), K_(ref_cnt),
      K_(block_meta_tree));
private:
  int insert_block_meta_tree(const ObDDLMacroHandle &macro_handle,
                             blocksstable::ObDataMacroBlockMeta *data_macro_meta);
  int init_sstable_param(ObTablet &tablet,
                         const ObITable::TableKey &table_key,
                         const share::SCN &ddl_start_scn,
                         ObTabletCreateSSTableParam &sstable_param);
private:
  static const int64_t TOTAL_LIMIT = 10 * 1024 * 1024 * 1024L;
  static const int64_t HOLD_LIMIT = 10 * 1024 * 1024 * 1024L;
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN ddl_start_scn_; // the log ts of ddl start log
  int64_t snapshot_version_; // the snapshot version for major sstable which is completed by ddl
  common::TCRWLock lock_; // lock for block_meta_tree_ and freeze_log_ts_
  common::ObArenaAllocator arena_allocator_;
  bool is_freezed_;
  bool is_closed_;
  share::SCN last_freezed_scn_; // the freezed log ts of last ddl kv. the log ts range of this ddl kv is (last_freezed_log_ts_, freeze_log_ts_]
  share::SCN min_scn_; // the min log ts of macro blocks
  share::SCN max_scn_; // the max log ts of macro blocks
  share::SCN freeze_scn_; // ddl kv refuse data larger than freeze log ts, freeze_log_ts >= max_log_ts
  int64_t pending_cnt_; // the amount of kvs that are replaying
  int64_t data_format_version_;
  ObBlockMetaTree block_meta_tree_;
};


}  // end namespace storage
}  // end namespace oceanbase

#endif
