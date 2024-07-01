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

#pragma once

#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx/ob_trans_define_v4.h"

namespace oceanbase
{
namespace storage
{

class ObDDLIncLogHandle
{
public:
  static const int64_t DDL_INC_LOG_TIMEOUT = 60 * 1000 * 1000; // 1min
  static const int64_t CHECK_DDL_INC_LOG_FINISH_INTERVAL = 1000; // 1ms
  ObDDLIncLogHandle();
  ~ObDDLIncLogHandle();
  int wait(const int64_t timeout = DDL_INC_LOG_TIMEOUT);
  void reset();
  bool is_valid() const { return nullptr != cb_  && scn_.is_valid_and_not_min(); }
public:
  ObDDLIncClogCb *cb_;
  share::SCN scn_;
};

class ObDDLIncRedoLogWriter final
{
public:
  static const int64_t DEFAULT_RETRY_TIMEOUT_US = 60L * 1000L * 1000L; // 1min
  ObDDLIncRedoLogWriter();
  ~ObDDLIncRedoLogWriter();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id);
  void reset();
  static bool need_retry(int ret_code, bool allow_remote_write);
  int write_inc_start_log_with_retry(
      const ObTabletID &lob_meta_tablet_id,
      transaction::ObTxDesc *tx_desc,
      share::SCN &start_scn);
  int write_inc_redo_log_with_retry(
      const storage::ObDDLMacroBlockRedoInfo &redo_info,
      const blocksstable::MacroBlockId &macro_block_id,
      const int64_t task_id,
      transaction::ObTxDesc *tx_desc);
  int wait_inc_redo_log_finish();
  int write_inc_commit_log_with_retry(
      const bool allow_remote_write,
      const ObTabletID &lob_meta_tablet_id,
      transaction::ObTxDesc *tx_desc);
private:
  int write_inc_start_log(
      const ObTabletID &lob_meta_tablet_id,
      transaction::ObTxDesc *tx_desc,
      share::SCN &start_scn);
  int write_inc_redo_log(
      const storage::ObDDLMacroBlockRedoInfo &redo_info,
      const blocksstable::MacroBlockId &macro_block_id,
      const int64_t task_id,
      transaction::ObTxDesc *tx_desc);
  int write_inc_commit_log(
      const bool allow_remote_write,
      const ObTabletID &lob_meta_tablet_id,
      transaction::ObTxDesc *tx_desc);
  int get_write_store_ctx_guard(
      transaction::ObTxDesc *tx_desc,
      ObStoreCtxGuard &ctx_guard,
      storage::ObLS *&ls);
  int switch_to_remote_write();
  int local_write_inc_start_log(
      ObDDLIncStartLog &log,
      transaction::ObTxDesc *tx_desc,
      share::SCN &start_scn);
  int local_write_inc_redo_log(
      const storage::ObDDLMacroBlockRedoInfo &redo_info,
      const blocksstable::MacroBlockId &macro_block_id,
      const int64_t task_id,
      transaction::ObTxDesc *tx_desc);
  int local_write_inc_commit_log(
      ObDDLIncCommitLog &log,
      transaction::ObTxDesc *tx_desc);
  int retry_remote_write_inc_commit_log(
      const common::ObTabletID lob_meta_tablet_id,
      transaction::ObTxDesc *tx_desc);
  int remote_write_inc_commit_log(
      const common::ObTabletID lob_meta_tablet_id,
      transaction::ObTxDesc *tx_desc);
private:
  bool is_inited_;
  bool remote_write_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObDDLIncLogHandle ddl_inc_log_handle_;
  ObAddr leader_addr_;
  share::ObLSID leader_ls_id_;
  char *buffer_;
};

class ObDDLIncRedoLogWriterCallback : public blocksstable::ObIMacroBlockFlushCallback
{
public:
  ObDDLIncRedoLogWriterCallback();
  virtual ~ObDDLIncRedoLogWriterCallback();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const storage::ObDDLMacroBlockType block_type,
      const ObITable::TableKey &table_key,
      const int64_t task_id,
      const share::SCN &start_scn,
      const uint64_t data_format_version,
      const storage::ObDirectLoadType direct_load_type,
      transaction::ObTxDesc *tx_desc,
      const transaction::ObTransID &trans_id);
  void reset();
  int write(
      blocksstable::ObMacroBlockHandle &macro_handle,
      const blocksstable::ObLogicMacroBlockId &logic_id,
      char *buf,
      const int64_t buf_len,
      const int64_t row_count);
  int wait();
private:
  bool is_inited_;
  storage::ObDDLMacroBlockRedoInfo redo_info_;
  blocksstable::MacroBlockId macro_block_id_;
  ObDDLIncRedoLogWriter ddl_inc_writer_;
  storage::ObDDLMacroBlockType block_type_;
  ObITable::TableKey table_key_;
  int64_t task_id_;
  share::SCN start_scn_;
  uint64_t data_format_version_;
  storage::ObDirectLoadType direct_load_type_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTransID trans_id_;
};

}  // end namespace storage
}  // end namespace oceanbase
