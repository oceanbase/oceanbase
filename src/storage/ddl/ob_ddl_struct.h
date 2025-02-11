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

#ifndef OCEANBASE_STORAGE_OB_DDL_STRUCT_H_
#define OCEANBASE_STORAGE_OB_DDL_STRUCT_H_

#include "lib/container/ob_array.h"
#include "share/scn.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/ob_i_table.h"
namespace oceanbase
{
namespace storage
{

static const int64_t DDL_FLUSH_MACRO_BLOCK_TIMEOUT = 5 * 1000 * 1000;

static const int64_t SS_DDL_START_SCN_VAL = 100;

enum ObDDLMacroBlockType
{
  DDL_MB_INVALID_TYPE = 0,
  DDL_MB_DATA_TYPE = 1,
  DDL_MB_INDEX_TYPE = 2,
  DDL_MB_SSTABLE_META_TYPE = 3,
  DDL_MB_TABLET_META_TYPE = 4,
  DDL_MB_SS_EMPTY_DATA_TYPE = 5,
};

class ObDDLMacroHandle
{
public:
  ObDDLMacroHandle();
  ObDDLMacroHandle(const ObDDLMacroHandle &other);
  ObDDLMacroHandle &operator=(const ObDDLMacroHandle &other);
  ~ObDDLMacroHandle();
  bool is_valid() const { return block_id_.is_valid(); }
  int set_block_id(const blocksstable::MacroBlockId &block_id);
  int reset_macro_block_ref();
  const blocksstable::MacroBlockId &get_block_id() const { return block_id_; }
  TO_STRING_KV(K_(block_id));
private:
  blocksstable::MacroBlockId block_id_;
};

class ObDDLMacroBlock final
{
public:
  ObDDLMacroBlock();
  ~ObDDLMacroBlock();
  const blocksstable::MacroBlockId &get_block_id() const { return block_handle_.get_block_id(); }
  int deep_copy(ObDDLMacroBlock &dst_block, common::ObIAllocator &allocator) const;
  int set_data_macro_meta(const blocksstable::MacroBlockId &macro_id,
                          const char* macor_block_buf,
                          const int64_t size,
                          const ObDDLMacroBlockType &block_type,
                          const bool force_set_macro_meta = false);
  bool is_valid() const;
  bool is_column_group_info_valid() const;
  TO_STRING_KV(K_(block_handle),
               K_(logic_id),
               K_(block_type),
               K_(ddl_start_scn),
               K_(scn),
               K_(table_key),
               K_(end_row_id),
               K_(trans_id),
               KPC_(data_macro_meta),
               KP_(buf),
               K_(size),
               K_(merge_slice_idx));
public:
  ObArenaAllocator allocator_; // used to hold data_macro_meta_
  ObDDLMacroHandle block_handle_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  ObDDLMacroBlockType block_type_;
  share::SCN ddl_start_scn_;
  share::SCN scn_;
  ObITable::TableKey table_key_;
  int64_t end_row_id_;
  transaction::ObTransID trans_id_; // for incremental direct load only
  blocksstable::ObDataMacroBlockMeta *data_macro_meta_;
  const char* buf_; // only used for warm up
  int64_t size_;
  int64_t merge_slice_idx_;
};

class ObDDLKV;
class ObDDLKVHandle final
{
public:
  ObDDLKVHandle() : ddl_kv_(nullptr), t3m_(nullptr), allocator_(nullptr) {}
  ObDDLKVHandle(const ObDDLKVHandle &other) : ddl_kv_(nullptr), t3m_(nullptr), allocator_(nullptr) { *this = other; }
  ObDDLKVHandle &operator =(const ObDDLKVHandle &other);
  ~ObDDLKVHandle() { reset(); }
  ObDDLKV* get_obj() const { return ddl_kv_; }
  bool is_valid() const;
  // for full direct load
  int set_obj(ObDDLKV *ddl_kv);
  // for incremental direct load
  int set_obj(ObTableHandleV2 &table_handle);
  void reset();
  DECLARE_TO_STRING;
private:
  ObDDLKV *ddl_kv_;
  ObTenantMetaMemMgr *t3m_;
  common::ObIAllocator *allocator_;
};


class ObTablet;
class ObTabletDirectLoadMgrHandle;
class ObDDLKVPendingGuard final
{
public:
  static int set_macro_block(
    ObTablet *tablet,
    const ObDDLMacroBlock &macro_block,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle);
public:
  ObDDLKVPendingGuard(
    ObTablet *tablet,
    const share::SCN &scn,
    const share::SCN &start_scn,
    const int64_t snapshot_version, // used for shared-storage mode.
    const uint64_t data_format_version, // used for shared-storage mode.
    ObTabletDirectLoadMgrHandle &direct_load_mgr_handle);
  ~ObDDLKVPendingGuard();
  int get_ret() const { return ret_; }
  int get_ddl_kv(ObDDLKV *&kv);
  bool can_freeze() { return can_freeze_; }
  TO_STRING_KV(KP(tablet_), K(scn_), K(kv_handle_), K(ret_));
private:
  ObTablet *tablet_;
  share::SCN scn_;
  ObDDLKVHandle kv_handle_;
  int ret_;
  bool can_freeze_;
};


enum ObDirectLoadType {
  DIRECT_LOAD_INVALID = 0,
  DIRECT_LOAD_DDL = 1,
  DIRECT_LOAD_LOAD_DATA = 2,
  DIRECT_LOAD_INCREMENTAL = 3,
  DIRECT_LOAD_DDL_V2 = 4,
  DIRECT_LOAD_LOAD_DATA_V2 = 5,
  DIRECT_LOAD_MAX
};

static inline bool is_valid_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INVALID < type && ObDirectLoadType::DIRECT_LOAD_MAX > type;
}

static inline bool is_ddl_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL == type || ObDirectLoadType::DIRECT_LOAD_DDL_V2 == type;
}

static inline bool is_full_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL == type
      || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA == type
      || ObDirectLoadType::DIRECT_LOAD_DDL_V2 == type
      || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == type;
}

static inline bool is_data_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_LOAD_DATA == type
      || ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == type
      || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == type;
}

static inline bool is_incremental_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == type;
}

static inline bool is_shared_storage_dempotent_mode(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL_V2 == type || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == type;
}

struct ObDDLMacroBlockRedoInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLMacroBlockRedoInfo();
  ~ObDDLMacroBlockRedoInfo() = default;
  bool is_valid() const;
  bool is_column_group_info_valid() const;
  /*
   * For tow conditions:
   *   1. column store table, unnessasery to generate double redo clog.
   *   2. row store table, but unnessasery to process cs replica.
   *     (a) cs replica not exist, may not be created or is creating.
   *     (b) table is not user data table.
   */
  bool is_not_compat_cs_replica() const;
  // If cs replica exist, this redo clog is suitable for F/R replica.
  bool is_cs_replica_row_store() const;
  // If cs replica exist, this redo clog is suitable for C replica.
  bool is_cs_replica_column_store() const;
  void reset();
  TO_STRING_KV(K_(table_key),
               K_(data_buffer),
               K_(block_type),
               K_(logic_id),
               K_(start_scn),
               K_(data_format_version),
               K_(end_row_id),
               K_(type),
               K_(trans_id),
               K_(with_cs_replica),
               K_(macro_block_id),
               K_(parallel_cnt),
               K_(cg_cnt),
               K_(merge_slice_idx));
public:
  storage::ObITable::TableKey table_key_;
  ObString data_buffer_;
  ObDDLMacroBlockType block_type_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  share::SCN start_scn_;
  uint64_t data_format_version_;
  int64_t end_row_id_;
  storage::ObDirectLoadType type_;
  transaction::ObTransID trans_id_; // for incremental direct load only
  bool with_cs_replica_;
  blocksstable::MacroBlockId macro_block_id_; // for shared storage mode
  // for shared storage gc occupy info
  int64_t parallel_cnt_;
  int64_t cg_cnt_;
  int64_t merge_slice_idx_;
};

class ObTabletDirectLoadMgr;
class ObTabletFullDirectLoadMgr;
class ObTabletIncDirectLoadMgr;
class ObTabletDirectLoadMgrHandle final
{
public:
  ObTabletDirectLoadMgrHandle();
  ~ObTabletDirectLoadMgrHandle();
  int set_obj(ObTabletDirectLoadMgr *mgr);
  int assign(const ObTabletDirectLoadMgrHandle &handle);
  ObTabletDirectLoadMgr *get_obj();
  const ObTabletDirectLoadMgr *get_obj() const;
  ObTabletFullDirectLoadMgr *get_full_obj() const;
  ObTabletIncDirectLoadMgr *get_inc_obj() const;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP_(tablet_mgr));
private:
  ObTabletDirectLoadMgr *tablet_mgr_;
};

#ifdef OB_BUILD_SHARED_STORAGE
struct ObDDLFinishLogInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDDLFinishLogInfo();
  ~ObDDLFinishLogInfo() = default;
  bool is_valid() const;
  int assign(const ObDDLFinishLogInfo &other);
  void reset();
  TO_STRING_KV(K_(ls_id), K_(table_key), K_(data_buffer), K_(data_format_version), K_(macro_block_id));
public:
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
  ObString data_buffer_;
  uint64_t data_format_version_;
  blocksstable::MacroBlockId  macro_block_id_;
};
#endif

class ObIDirectLoadRowIterator : public ObIStoreRowIterator
{
public:
  ObIDirectLoadRowIterator() {}
  virtual ~ObIDirectLoadRowIterator() {}
  virtual int get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&row) = 0;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif
