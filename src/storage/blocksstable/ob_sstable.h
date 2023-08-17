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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_H

#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "share/scn.h"
#include "ob_datum_range.h"
#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{
namespace common
{
}
namespace storage
{
class ObAllMicroBlockRangeIterator;
struct ObTabletCreateSSTableParam;
class ObStoreRowIterator;
}
namespace blocksstable
{
class ObSSTableSecMetaIterator;
class ObIMacroBlockIterator;
struct ObMacroBlocksWriteCtx;

class ObSSTableMetaHandle
{

public:
  ObSSTableMetaHandle() : handle_(), meta_(nullptr) {}
  ~ObSSTableMetaHandle() { reset(); }

  void reset();
  int get_sstable_meta(const ObSSTableMeta *&sstable_meta) const;

  OB_INLINE bool is_valid() { return nullptr != meta_ && meta_->is_valid(); }
  OB_INLINE const ObSSTableMeta &get_sstable_meta() const
  {
    OB_ASSERT(nullptr != meta_);
    return *meta_;
  }
  TO_STRING_KV(K_(handle), KPC_(meta));
private:
  friend class ObSSTable;
  ObStorageMetaHandle handle_;
  const ObSSTableMeta *meta_;
};

// SSTable class after version 4.0
class ObSSTable : public ObITable, public ObIStorageMetaObj
{
public:
  ObSSTable();
  virtual ~ObSSTable();

  // From 4.2, the SSTable object reference count will be abandoned. Otherwise, when SSTable
  // will be put in KVCACHE for only read, it will modify the cache memory.
  virtual void inc_ref() override;
  virtual int64_t dec_ref() override;
  virtual int64_t get_ref() const override;

  int init(const ObTabletCreateSSTableParam &param, common::ObArenaAllocator *allocator);
  void reset();

  // Query interfaces
  virtual int scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_get(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;
  virtual int exist(
      const ObTableIterParam &param,
	  ObTableAccessContext &context,
	  const blocksstable::ObDatumRowkey &rowkey,
	  bool &is_exist,
	  bool &has_found) override;
  virtual int exist(
      ObRowsInfo &rows_info,
      bool &is_exist,
      bool &all_rows_found) override;

  int scan_macro_block(
      const ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      blocksstable::ObIMacroBlockIterator *&macro_block_iter,
      const bool is_reverse_scan = false,
      const bool need_record_micro_info = false,
      const bool need_scan_sec_meta = false);
  int scan_micro_block(
      const ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      ObAllMicroBlockRangeIterator *&micro_iter,
      const bool is_reverse_scan = false);
  int scan_secondary_meta(
      ObIAllocator &allocator,
      const ObDatumRange &query_range,
      const ObITableReadInfo &rowkey_read_info,
      const blocksstable::ObMacroBlockMetaType meta_type,
      blocksstable::ObSSTableSecMetaIterator *&meta_iter,
      const bool is_reverse_scan = false,
      const int64_t sample_step = 0) const;
  int bf_may_contain_rowkey(const ObDatumRowkey &rowkey, bool &contain);

  // For transaction
  int check_row_locked(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      ObStoreRowLockState &lock_state);

  int set_upper_trans_version(const int64_t upper_trans_version);
  virtual int64_t get_upper_trans_version() const override
  {
    return upper_trans_version_;
  }
  virtual int64_t get_max_merged_trans_version() const override
  {
    return max_merged_trans_version_;
  }
  OB_INLINE bool contain_uncommitted_row() const
  {
    return contain_uncommitted_row_;
  }
  OB_INLINE share::SCN get_filled_tx_scn() const
  {
    return filled_tx_scn_;
  }
  bool is_empty() const
  {
    return 0 == data_macro_block_count_;
  }
  int set_addr(const ObMetaDiskAddr &addr);
  OB_INLINE const ObMetaDiskAddr &get_addr() const { return addr_; }
  OB_INLINE int64_t get_data_macro_block_count() const { return data_macro_block_count_; }
  OB_INLINE int64_t get_macro_offset() const { return nested_offset_; }
  OB_INLINE int64_t get_macro_read_size() const { return nested_size_; }
  OB_INLINE bool is_small_sstable() const
  {
    return OB_DEFAULT_MACRO_BLOCK_SIZE != nested_size_ && 0 < nested_offset_;
  }
  int64_t get_data_version() const
  {
    return is_major_sstable() ? get_snapshot_version() :
      is_ddl_sstable() ? get_upper_trans_version() :
      get_key().get_end_scn().get_val_for_tx();
  }
  virtual int get_frozen_schema_version(int64_t &schema_version) const override;
  virtual int inc_macro_ref(bool &inc_success) const;
  virtual void dec_macro_ref() const;
  OB_INLINE bool is_valid() const { return valid_for_reading_; }
  OB_INLINE bool is_loaded() const { return nullptr != meta_; }
  int get_meta(ObSSTableMetaHandle &meta_handle, common::ObSafeArenaAllocator *allocator = nullptr) const;
  int set_status_for_read(const ObSSTableStatus status);

  // TODO: get_index_tree_root and get_last_rowkey now required sstable to be loaded
  int get_index_tree_root(
      blocksstable::ObMicroBlockData &index_data,
      const bool need_transform = true);
  int get_last_rowkey(
      common::ObIAllocator &allocator,
      ObDatumRowkey &endkey);

  int deep_copy(ObArenaAllocator &allocator, ObSSTable *&dst) const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    int64_t size = sizeof(ObSSTable);
    if (is_loaded()) {
      size += sizeof(ObSSTableMeta) + meta_->get_variable_size();
    }
    return size;
  }

public:
  int dump2text(
      const char *dir_name,
      const ObStorageSchema &schema,
      const char *fname)
  {
    // TODO: print sstable
    UNUSEDx(dir_name, schema, fname);
    return OB_NOT_SUPPORTED;
  }
  virtual int64_t get_serialize_size() const override;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int deserialize_post_work(
      common::ObIAllocator *allocator);
  int assign_meta(ObSSTableMeta *dest);

  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), K_(addr), K_(upper_trans_version),
      K_(max_merged_trans_version), K_(data_macro_block_count), K_(nested_size),
      K_(nested_offset), K_(contain_uncommitted_row), K_(filled_tx_scn), KPC_(meta), K_(valid_for_reading));

  static const int64_t AARCH64_CP_BUF_ALIGN = 8;
private:
  static bool ignore_ret(const int ret);
  int check_valid_for_reading();
  int add_used_size() const;
  int dec_used_size() const;
  int build_exist_iterator(
      const ObTableIterParam &iter_param,
      const ObDatumRowkey &rowkey,
      ObTableAccessContext &access_context,
      ObStoreRowIterator *&iter);
  int build_multi_exist_iterator(ObRowsInfo &rows_info, ObStoreRowIterator *&iter);
  int init_sstable_meta(const ObTabletCreateSSTableParam &param, common::ObArenaAllocator *allocator);
  int get_last_rowkey(const ObDatumRowkey *&sstable_endkey);
  int serialize_fixed_struct(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_fixed_struct(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_sstable_fix_serialize_size() const;
  int64_t get_sstable_fix_serialize_payload_size() const;
private:
  static const int64_t SSTABLE_VERSION = 1;
  struct StatusForSerialize
  {
    StatusForSerialize()
      : with_fixed_struct_(0),
        with_meta_(0),
        reserved_(0),
        compat_magic_(COMPAT_MAGIC) {}
    OB_INLINE void reset() { new (this) StatusForSerialize(); }
    OB_INLINE bool with_fixed_struct() { return 1 == with_fixed_struct_; }
    OB_INLINE bool with_meta() { return 1 == with_meta_; }

    OB_INLINE void set_with_fixed_struct() { with_fixed_struct_ = 1; }
    OB_INLINE void set_with_meta() { with_meta_ = 1; }
    static const int8_t COMPAT_MAGIC = 0x55;
    union
    {
      uint16_t pack_;
      struct
      {
        uint16_t with_fixed_struct_:1;
        uint16_t with_meta_:1;

        uint16_t reserved_:6;
        uint16_t compat_magic_:8;
      };
    };
  };
protected:
  ObMetaDiskAddr addr_; // serialized in table store
  // serialized data cache
  int64_t upper_trans_version_;
  int64_t max_merged_trans_version_;
  int64_t data_macro_block_count_;
  int64_t nested_size_;
  int64_t nested_offset_;
  bool contain_uncommitted_row_;
  // in-memory
  bool valid_for_reading_;
  bool is_tmp_sstable_;
  share::SCN filled_tx_scn_;
  // serialized
  blocksstable::ObSSTableMeta *meta_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTable);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_SSTABLE_H
