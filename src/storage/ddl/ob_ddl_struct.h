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
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace storage
{

static const int64_t DDL_FLUSH_MACRO_BLOCK_TIMEOUT = 5 * 1000 * 1000;

enum ObDDLMacroBlockType
{
  DDL_MB_INVALID_TYPE = 0,
  DDL_MB_DATA_TYPE = 1,
  DDL_MB_INDEX_TYPE = 2,
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
  bool is_valid() const;
  bool is_column_group_info_valid() const;
  TO_STRING_KV(K_(block_handle),
               K_(logic_id),
               K_(block_type),
               K_(ddl_start_scn),
               K_(scn),
               KP_(buf),
               K_(size),
               K_(table_key),
               K_(end_row_id),
               K_(trans_id));
public:
  ObDDLMacroHandle block_handle_;
  blocksstable::ObLogicMacroBlockId logic_id_;
  ObDDLMacroBlockType block_type_;
  share::SCN ddl_start_scn_;
  share::SCN scn_;
  const char *buf_;
  int64_t size_;
  ObITable::TableKey table_key_;
  int64_t end_row_id_;
  transaction::ObTransID trans_id_; // for incremental direct load only
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
  TO_STRING_KV(KP_(ddl_kv));
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
  DIRECT_LOAD_MAX
};

static inline bool is_valid_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INVALID < type && ObDirectLoadType::DIRECT_LOAD_MAX > type;
}

static inline bool is_ddl_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL == type;
}

static inline bool is_full_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL <= type
      && ObDirectLoadType::DIRECT_LOAD_LOAD_DATA >= type;
}

static inline bool is_data_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_LOAD_DATA <= type
      && ObDirectLoadType::DIRECT_LOAD_INCREMENTAL >= type;
}

static inline bool is_incremental_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == type;
}

struct ObDDLMacroBlockRedoInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLMacroBlockRedoInfo();
  ~ObDDLMacroBlockRedoInfo() = default;
  bool is_valid() const;
  bool is_column_group_info_valid() const;
  void reset();
  TO_STRING_KV(K_(table_key),
               K_(data_buffer),
               K_(block_type),
               K_(logic_id),
               K_(start_scn),
               K_(data_format_version),
               K_(end_row_id),
               K_(type),
               K_(trans_id));
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

}  // end namespace storage
}  // end namespace oceanbase

#endif
