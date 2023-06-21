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

#ifndef OCEANBASE_STORAGE_OB_SYNC_TABLET_SEQ_CLOG_H_
#define OCEANBASE_STORAGE_OB_SYNC_TABLET_SEQ_CLOG_H_

#include "common/ob_tablet_id.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/ob_append_callback.h"
#include "share/ob_ls_id.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{

class ObSyncTabletSeqLog final
{
public:
  ObSyncTabletSeqLog() : tablet_id_(), autoinc_seq_(0) {}
  ~ObSyncTabletSeqLog() = default;
public:
  int init(const common::ObTabletID &tablet_id, const uint64_t autoinc_seq);

  bool is_valid() const { return tablet_id_.is_valid() && autoinc_seq_ >= 0; }
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  uint64_t get_autoinc_seq() const { return autoinc_seq_; }

  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(tablet_id), K_(autoinc_seq));
private:
  common::ObTabletID tablet_id_;
  uint64_t autoinc_seq_;
};

class ObSyncTabletSeqMdsLogCb : public logservice::AppendCb
{
public:
  ObSyncTabletSeqMdsLogCb();
  virtual ~ObSyncTabletSeqMdsLogCb() = default;

  int init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id, const int64_t writer_id);
  virtual int on_success() override;
  virtual int on_failure() override;
  void try_release();
  inline bool is_success() const { return state_ == ObDDLClogState::STATE_SUCCESS; }
  inline bool is_failed() const { return state_ == ObDDLClogState::STATE_FAILED; }
  inline bool is_finished() const { return state_ != ObDDLClogState::STATE_INIT; }
  inline int get_ret_code() const { return ret_code_; }
  mds::MdsCtx &get_mds_ctx() { return mds_ctx_; }
private:
  ObDDLClogState state_;
  bool the_other_release_this_;
  int ret_code_;
  ObTabletHandle tablet_handle_;
  mds::MdsCtx mds_ctx_;
};

} // namespace storage
} // namespace oceanbase
#endif