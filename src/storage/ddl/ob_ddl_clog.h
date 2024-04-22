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

#ifndef OCEANBASE_STORAGE_OB_DDL_CLOG_H_
#define OCEANBASE_STORAGE_OB_DDL_CLOG_H_

#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "logservice/ob_append_callback.h"

namespace oceanbase
{

namespace storage
{
enum class ObDDLClogType : int64_t
{
  UNKNOWN = -1,
  DDL_REDO_LOG = 0x1,
  OLD_DDL_COMMIT_LOG = 0x2, // deprecated, only compatable use
  DDL_TABLET_SCHEMA_VERSION_CHANGE_LOG = 0x10,
  DDL_START_LOG = 0x20,
  DDL_COMMIT_LOG = 0x40,// rename from DDL_PREPARE_LOG
};

enum ObDDLClogState : uint8_t
{
  STATE_INIT = 0,
  STATE_SUCCESS = 1,
  STATE_FAILED = 2
};

class ObDDLClogCbStatus final
{
public:
  ObDDLClogCbStatus();
  ~ObDDLClogCbStatus() {}
  void set_state(const ObDDLClogState state) { state_ = state; }
  inline bool is_success() const { return state_ == ObDDLClogState::STATE_SUCCESS; }
  inline bool is_failed() const { return state_ == ObDDLClogState::STATE_FAILED; }
  inline bool is_finished() const { return state_ != ObDDLClogState::STATE_INIT; }
  bool try_set_release_flag();
  void set_ret_code(const int ret_code) { ret_code_ = ret_code; }
  int get_ret_code() const { return ret_code_; }
  TO_STRING_KV(K(the_other_release_this_), K(state_), K(ret_code_));
private:
  bool the_other_release_this_;
  ObDDLClogState state_;
  int ret_code_;
};

class ObDDLClogCb : public logservice::AppendCb
{
public:
  ObDDLClogCb();
  virtual ~ObDDLClogCb() = default;
  virtual int on_success() override;
  virtual int on_failure() override;
  inline bool is_success() const { return status_.is_success(); }
  inline bool is_failed() const { return status_.is_failed(); }
  inline bool is_finished() const { return status_.is_finished(); }
  void try_release();
private:
  ObDDLClogCbStatus status_;
};

class ObDDLStartClogCb : public logservice::AppendCb
{
public:
  ObDDLStartClogCb();
  virtual ~ObDDLStartClogCb() = default;
  int init(const ObITable::TableKey &table_key,
      const uint64_t data_format_version,
      const int64_t execution_id,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      ObDDLKvMgrHandle &lob_kv_mgr_handle,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      const uint32_t lock_tid);
  virtual int on_success() override;
  virtual int on_failure() override;
  inline bool is_success() const { return status_.is_success(); }
  inline bool is_failed() const { return status_.is_failed(); }
  inline bool is_finished() const { return status_.is_finished(); }
  int get_ret_code() const { return status_.get_ret_code(); }
  void try_release();
  TO_STRING_KV(K(is_inited_), K(status_), K_(table_key), K_(data_format_version), K_(execution_id), K_(lock_tid));
private:
  bool is_inited_;
  ObDDLClogCbStatus status_;
  ObITable::TableKey table_key_;
  uint64_t data_format_version_;
  int64_t execution_id_;
  uint32_t lock_tid_;
  ObDDLKvMgrHandle ddl_kv_mgr_handle_;
  ObDDLKvMgrHandle lob_kv_mgr_handle_;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle_;
};

class ObDDLMacroBlockClogCb : public logservice::AppendCb
{
public:
  ObDDLMacroBlockClogCb();
  virtual ~ObDDLMacroBlockClogCb();
  int init(const share::ObLSID &ls_id,
           const storage::ObDDLMacroBlockRedoInfo &redo_info,
           const blocksstable::MacroBlockId &macro_block_id,
           ObTabletHandle &tablet_handle);
  virtual int on_success() override;
  virtual int on_failure() override;
  inline bool is_success() const { return status_.is_success(); }
  inline bool is_failed() const { return status_.is_failed(); }
  inline bool is_finished() const { return status_.is_finished(); }
  int get_ret_code() const { return status_.get_ret_code(); }
  void try_release();
private:
  bool is_inited_;
  ObDDLClogCbStatus status_;
  share::ObLSID ls_id_;
  storage::ObDDLMacroBlockRedoInfo redo_info_;
  blocksstable::MacroBlockId macro_block_id_;
  ObSpinLock data_buffer_lock_;
  bool is_data_buffer_freed_;
  ObTabletHandle tablet_handle_;
};

class ObDDLCommitClogCb : public logservice::AppendCb
{
public:
  ObDDLCommitClogCb();
  virtual ~ObDDLCommitClogCb() = default;
  int init(const share::ObLSID &ls_id,
           const common::ObTabletID &tablet_id,
           const share::SCN &start_scn,
           const uint32_t lock_tid,
           ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
           ObTabletDirectLoadMgrHandle &lob_direct_load_mgr_handle);
  virtual int on_success() override;
  virtual int on_failure() override;
  inline bool is_success() const { return status_.is_success(); }
  inline bool is_failed() const { return status_.is_failed(); }
  inline bool is_finished() const { return status_.is_finished(); }
  int get_ret_code() const { return status_.get_ret_code(); }
  void try_release();
  TO_STRING_KV(K(is_inited_), K(status_), K(ls_id_), K(tablet_id_), K(start_scn_), K_(lock_tid));
private:
  bool is_inited_;
  ObDDLClogCbStatus status_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN start_scn_;
  uint32_t lock_tid_;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle_;
  ObTabletDirectLoadMgrHandle lob_direct_load_mgr_handle_;
};

class ObDDLClogHeader final
{
public:
  static const int64_t DDL_CLOG_HEADER_SIZE = sizeof(ObDDLClogType);

  NEED_SERIALIZE_AND_DESERIALIZE;
  ObDDLClogHeader() : ddl_clog_type_(ObDDLClogType::UNKNOWN) {}
  ObDDLClogHeader(const ObDDLClogType &type) : ddl_clog_type_(type) {}
  const ObDDLClogType & get_ddl_clog_type() { return ddl_clog_type_; };
  TO_STRING_KV(K(ddl_clog_type_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLClogHeader);
  ObDDLClogType ddl_clog_type_;
};

class ObDDLClog
{
public:
  static const uint64_t COMPATIBLE_LOB_META_TABLET_ID = 1;
};

class ObDDLStartLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLStartLog();
  ~ObDDLStartLog() = default;
  int init(const ObITable::TableKey &table_key,
           const uint64_t data_format_version,
           const int64_t execution_id,
           const ObDirectLoadType direct_load_type,
           const ObTabletID &lob_meta_tablet_id);
  bool is_valid() const { return table_key_.is_valid() && data_format_version_ >= 0 && execution_id_ >= 0 && is_valid_direct_load(direct_load_type_); }
  ObITable::TableKey get_table_key() const { return table_key_; }
  uint64_t get_data_format_version() const { return data_format_version_; }
  int64_t get_execution_id() const { return execution_id_; }
  ObDirectLoadType get_direct_load_type() const { return direct_load_type_; }
  const ObTabletID &get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  TO_STRING_KV(K_(table_key), K_(data_format_version), K_(execution_id), K_(direct_load_type), K_(lob_meta_tablet_id));
private:
  ObITable::TableKey table_key_; // use table type to distinguish column store, column group id is valid
  uint64_t data_format_version_; // used for compatibility
  int64_t execution_id_;
  ObDirectLoadType direct_load_type_;
  ObTabletID lob_meta_tablet_id_; // avoid replay get newest mds data
};

class ObDDLRedoLog final
{
public:
  ObDDLRedoLog();
  ~ObDDLRedoLog() = default;
  int init(const storage::ObDDLMacroBlockRedoInfo &redo_info);
  bool is_valid() const { return redo_info_.is_valid(); }
  storage::ObDDLMacroBlockRedoInfo get_redo_info() const { return redo_info_; }
  TO_STRING_KV(K_(redo_info));
  OB_UNIS_VERSION_V(1);
private:
  storage::ObDDLMacroBlockRedoInfo redo_info_;
};

class ObDDLCommitLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLCommitLog();
  ~ObDDLCommitLog() = default;
  int init(const ObITable::TableKey &table_key,
           const share::SCN &start_scn,
           const ObTabletID &lob_meta_tablet_id);
  bool is_valid() const { return table_key_.is_valid() && start_scn_.is_valid(); }
  ObITable::TableKey get_table_key() const { return table_key_; }
  share::SCN get_start_scn() const { return start_scn_; }
  const ObTabletID &get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  TO_STRING_KV(K_(table_key), K_(start_scn), K_(lob_meta_tablet_id));
private:
  ObITable::TableKey table_key_;
  share::SCN start_scn_;
  ObTabletID lob_meta_tablet_id_; // avoid replay get newest mds data
};

class ObTabletSchemaVersionChangeLog final
{
public:
  ObTabletSchemaVersionChangeLog();
  ~ObTabletSchemaVersionChangeLog() = default;
  int init(const common::ObTabletID &tablet_id, const int64_t schema_version);
  bool is_valid() const { return tablet_id_.is_valid() && schema_version_ >= 0; }
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  int64_t get_schema_version() const { return schema_version_; }
  TO_STRING_KV(K_(tablet_id), K_(schema_version));
  OB_UNIS_VERSION_V(1);
private:
  common::ObTabletID tablet_id_;
  int64_t schema_version_;
};

class ObDDLBarrierLog final {
public:
  ObDDLBarrierLog() : ls_id_(), hidden_tablet_ids_() {}
  ~ObDDLBarrierLog() {}
  bool is_valid() const { return ls_id_.is_valid() && hidden_tablet_ids_.count() > 0; }
  TO_STRING_KV(K_(ls_id), K_(hidden_tablet_ids));
  OB_UNIS_VERSION_V(1);
public:
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> hidden_tablet_ids_;
};

} // namespace storage
} // namespace oceanbase
#endif
