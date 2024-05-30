/*************************************************************************
  * Copyright (c) 2022 OceanBase
  * OceanBase is licensed under Mulan PubL v2.
  * You can use this software according to the terms and conditions of the Mulan PubL v2
  * You may obtain a copy of Mulan PubL v2 at:
  *          http://license.coscl.org.cn/MulanPubL-2.0
  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
  * See the Mulan PubL v2 for more details.
  * File Name   : ob_column_oriented_sstable.h
  * Created  on : 09/05/2022
 ************************************************************************/
#ifndef _OB_COLUMN_ORIENTED_SSTABLE_H
#define _OB_COLUMN_ORIENTED_SSTABLE_H

#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_table_store_util.h"
#include "lib/allocator/page_arena.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"

namespace oceanbase
{

namespace blocksstable
{
class ObSSTable;
}

namespace storage
{
class ObTableHandleV2;
class ObICGIterator;
class ObCOSSTableV2;


/*
 * ObSSTableWrapper is used for guaranteeing the lifetime of cg sstable
 * ONLY CG SSTables need to be guarded by meta_handle
 */
class ObSSTableWrapper
{
public:
  ObSSTableWrapper();
  ~ObSSTableWrapper() { reset(); }
  void reset();
  bool is_valid() const { return sstable_ != nullptr; }
  int set_sstable(blocksstable::ObSSTable *sstable, ObStorageMetaHandle *meta_handle = nullptr);
  // this interface will return the loaded column store type sstable
  int get_loaded_column_store_sstable(blocksstable::ObSSTable *&table);
  int get_merge_row_cnt(const ObTableIterParam &iter_param, int64_t &row_cnt);
  blocksstable::ObSSTable *get_sstable() const { return sstable_; }
  const ObStorageMetaHandle &get_meta_handle() const { return meta_handle_; }
  TO_STRING_KV(KPC_(sstable), K_(meta_handle));
private:
  friend class ObCOSSTableV2;
  ObStorageMetaHandle meta_handle_; // keep the lifetime of cg sstable
  blocksstable::ObSSTable *sstable_;
};


struct ObCOSSTableMeta
{
public:
  ObCOSSTableMeta() { reset(); }
  ~ObCOSSTableMeta() = default;
  OB_INLINE void reset() { MEMSET(this, 0, sizeof(ObCOSSTableMeta)); }
  OB_INLINE bool is_valid() const { return column_group_cnt_ > 0; }
  OB_INLINE bool is_empty() const { return 0 == data_macro_block_cnt_; }
  OB_INLINE uint32_t get_column_group_count() const { return column_group_cnt_; }
  OB_INLINE uint64_t get_total_macro_block_count() const { return data_macro_block_cnt_ + index_macro_block_cnt_; }
  OB_INLINE uint64_t get_total_use_old_macro_block_count() const { return use_old_macro_block_cnt_; }

  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      const char *buf,
      const int64_t data_len,
      int64_t &pos);

  TO_STRING_KV(K_(column_group_cnt), K_(data_macro_block_cnt), K_(use_old_macro_block_cnt), K_(data_micro_block_cnt),
               K_(index_macro_block_cnt), K_(occupy_size), K_(original_size), K_(data_checksum), K_(full_column_cnt));
public:
  uint64_t data_macro_block_cnt_;
  uint64_t use_old_macro_block_cnt_;
  uint64_t data_micro_block_cnt_;
  uint64_t index_macro_block_cnt_;
  uint64_t occupy_size_;
  uint64_t original_size_;
  int64_t data_checksum_;
  uint32_t column_group_cnt_; // Attention ! including all column cg.
  uint32_t full_column_cnt_;
};


enum ObCOSSTableBaseType : int32_t
{
  INVALID_TYPE = 0,
  ALL_CG_TYPE = 1,
  ROWKEY_CG_TYPE = 2,
  MAX_TYPE
};

enum ObCOMajorSSTableStatus: uint8_t {
  INVALID_CO_MAJOR_SSTABLE_STATUS = 0,
  COL_WITH_ALL, // all cg + normal cg
  COL_ONLY_ALL, // all cg only
  PURE_COL, // rowkey cg + normal cg
  PURE_COL_ONLY_ALL, // all cg only
  MAX_CO_MAJOR_SSTABLE_STATUS
};

inline bool is_valid_co_major_sstable_status(const ObCOMajorSSTableStatus& major_sstable_status)
{
  return major_sstable_status > INVALID_CO_MAJOR_SSTABLE_STATUS && major_sstable_status < MAX_CO_MAJOR_SSTABLE_STATUS;
}
inline bool is_rowkey_major_sstable(const ObCOMajorSSTableStatus& major_sstable_status)
{
  return PURE_COL == major_sstable_status;
}
inline bool is_redundant_row_store_major_sstable(const ObCOMajorSSTableStatus& major_sstable_status)
{
  return major_sstable_status == COL_WITH_ALL || major_sstable_status == COL_ONLY_ALL;
}
inline bool is_major_sstable_match_schema(const ObCOMajorSSTableStatus& major_sstable_status)
{
  return major_sstable_status == COL_WITH_ALL || major_sstable_status == PURE_COL;
}

/*
 * The base part of ObCOSSTable maybe
 */
class ObCOSSTableV2 : public blocksstable::ObSSTable
{
public:
  ObCOSSTableV2();
  virtual ~ObCOSSTableV2();
  virtual void reset();
  virtual int init(
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator *allocator) override;

  bool is_row_store_only_co_table() const { return is_cgs_empty_co_ && is_all_cg_base(); }
  bool is_cgs_empty_co_table() const { return is_cgs_empty_co_; }
  int fill_cg_sstables(const common::ObIArray<ObITable *> &cg_tables);
  OB_INLINE const ObCOSSTableMeta &get_cs_meta() const { return cs_meta_; }
  OB_INLINE bool is_all_cg_base() const { return ObCOSSTableBaseType::ALL_CG_TYPE == base_type_; }
  OB_INLINE bool is_rowkey_cg_base() const { return ObCOSSTableBaseType::ROWKEY_CG_TYPE == base_type_; }
  OB_INLINE bool is_inited() const { return is_cgs_empty_co_table() || is_cs_valid(); }
  OB_INLINE bool is_cs_valid() const {
    return valid_for_cs_reading_
        && base_type_ > ObCOSSTableBaseType::INVALID_TYPE && base_type_ < ObCOSSTableBaseType::MAX_TYPE
        && key_.column_group_idx_ < cs_meta_.column_group_cnt_;
  }
  int fetch_cg_sstable(
      const uint32_t cg_idx,
      ObSSTableWrapper &cg_wrapper) const;
  int get_cg_sstable(const uint32_t cg_idx, ObSSTableWrapper &cg_wrapper) const;
  int get_all_tables(common::ObIArray<ObSSTableWrapper> &table_wrappers) const;

  virtual int64_t get_serialize_size() const override;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos) override;

  int deep_copy(
      common::ObArenaAllocator &allocator,
      const common::ObIArray<ObMetaDiskAddr> &cg_addrs,
      ObCOSSTableV2 *&co_sstable);
  virtual int serialize_full_table(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int64_t get_full_serialize_size() const override;

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const override;
  virtual int64_t get_deep_copy_size() const override
  {
    int64_t size = sizeof(ObCOSSTableV2);
    size += ObSSTable::get_deep_copy_size();
    return size;
  }

// Query interfaces
  virtual int scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const blocksstable::ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;
  int cg_scan(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      ObICGIterator *&cg_iter,
      const bool is_projector /* remove later, needed to be used in projector */,
      const bool project_single_row);
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_get(
      const ObTableIterParam &param,
      ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;
  INHERIT_TO_STRING_KV("ObSSTable", ObSSTable, KP(this), K_(cs_meta),
      K_(base_type), K_(is_cgs_empty_co), K_(valid_for_cs_reading));
private:
  int build_cs_meta();
  int build_cs_meta_without_cgs();
protected:
  ObCOSSTableMeta cs_meta_;
  ObCOSSTableBaseType base_type_;
  bool is_cgs_empty_co_; // The co sstable only contains all_cg and cg_sstables_ should be empty. No need to create cg sstable when (case 1) co sstable is co_major and it is empty (case 2) normal cg is redundant
  bool valid_for_cs_reading_;
  common::ObArenaAllocator tmp_allocator_; // TODO(@jiahua.cjh) remove this allocator later
  DISALLOW_COPY_AND_ASSIGN(ObCOSSTableV2);
};


} /* storage */
} /* oceanbase */


#endif
