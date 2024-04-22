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

#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{

class ObDDLIncClogCb : public logservice::AppendCb
{
public:
  ObDDLIncClogCb()
  : status_() {}
  virtual ~ObDDLIncClogCb() = default;
  virtual int on_success() override = 0;
  virtual int on_failure() override = 0;
  virtual void try_release() = 0;
  inline bool is_success() const { return status_.is_success(); }
  inline bool is_failed() const { return status_.is_failed(); }
  inline bool is_finished() const { return status_.is_finished(); }
  int get_ret_code() const { return status_.get_ret_code(); }
protected:
  ObDDLClogCbStatus status_;
};

class ObDDLIncStartClogCb : public ObDDLIncClogCb
{
public:
  ObDDLIncStartClogCb();
  virtual ~ObDDLIncStartClogCb() = default;
  int init(const ObDDLIncLogBasic &log_basic);
  virtual int on_success() override;
  virtual int on_failure() override;
  virtual void try_release() override;
  share::SCN get_scn() const { return scn_; }
  TO_STRING_KV(K(is_inited_), K(log_basic_));
private:
  bool is_inited_;
  ObDDLIncLogBasic log_basic_;
  share::SCN scn_;
};

class ObDDLIncRedoClogCb : public ObDDLIncClogCb
{
public:
  ObDDLIncRedoClogCb();
  virtual ~ObDDLIncRedoClogCb();
  int init(const share::ObLSID &ls_id,
           const storage::ObDDLMacroBlockRedoInfo &redo_info,
           const blocksstable::MacroBlockId &macro_block_id,
           storage::ObTabletHandle &tablet_handle);
  virtual int on_success() override;
  virtual int on_failure() override;
  virtual void try_release() override;
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  storage::ObDDLMacroBlockRedoInfo redo_info_;
  blocksstable::MacroBlockId macro_block_id_;
  ObSpinLock data_buffer_lock_;
  bool is_data_buffer_freed_;
  storage::ObTabletHandle tablet_handle_;
};

class ObDDLIncCommitClogCb : public ObDDLIncClogCb
{
public:
  ObDDLIncCommitClogCb();
  virtual ~ObDDLIncCommitClogCb() = default;
  int init(const share::ObLSID &ls_id, const ObDDLIncLogBasic &log_basic);
  virtual int on_success() override;
  virtual int on_failure() override;
  virtual void try_release() override;
  share::SCN get_scn() const { return scn_; }
  TO_STRING_KV(K(is_inited_), K(log_basic_));
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObDDLIncLogBasic log_basic_;
  share::SCN scn_;
};

} // namespace storage
} // namespace oceanbase
