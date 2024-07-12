
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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRANSFER_TX_CTX
#define OCEANBASE_STORAGE_OB_TABLET_TRANSFER_TX_CTX

namespace oceanbase
{
namespace storage
{

#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tablelock/ob_table_lock_common.h"

struct ObTxCtxMoveArg
{
  OB_UNIS_VERSION(1);
public:
  transaction::ObTransID tx_id_;
  int64_t epoch_;
  uint32_t session_id_;
  uint32_t associated_session_id_;
  transaction::ObTxState tx_state_;
  share::SCN trans_version_;
  share::SCN prepare_version_;
  share::SCN commit_version_;
  uint64_t cluster_id_;
  uint64_t cluster_version_;
  common::ObAddr scheduler_;
  int64_t tx_expired_time_;
  transaction::ObXATransID xid_;
  transaction::ObTxSEQ last_seq_no_;
  transaction::ObTxSEQ max_submitted_seq_no_;
  share::SCN tx_start_scn_;
  share::SCN tx_end_scn_;
  bool is_sub2pc_;
  bool happened_before_;
  transaction::tablelock::ObTableLockInfo table_lock_info_;

  TO_STRING_KV(K_(tx_id), K_(epoch), K_(session_id), K_(tx_state), K_(trans_version), K_(prepare_version), K_(commit_version),
      K_(cluster_id), K_(cluster_version), K_(scheduler), K_(tx_expired_time), K_(xid), K_(last_seq_no), K_(max_submitted_seq_no),
      K_(tx_start_scn), K_(tx_end_scn), K_(is_sub2pc), K_(happened_before), K_(table_lock_info));
};

struct ObTransferMoveTxParam
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransferMoveTxParam(share::ObLSID ls_id, int64_t transfer_epoch, share::SCN transfer_scn,
      share::SCN op_scn, transaction::NotifyType op_type, bool is_replay, bool is_incomplete_replay)
    : src_ls_id_(ls_id),
      transfer_epoch_(transfer_epoch),
      transfer_scn_(transfer_scn),
      op_scn_(op_scn),
      op_type_(op_type),
      is_replay_(is_replay),
      is_incomplete_replay_(is_incomplete_replay) {}
  ~ObTransferMoveTxParam() { reset(); }
  void reset();
  TO_STRING_KV(K_(src_ls_id), K_(transfer_epoch), K_(transfer_scn),
      K_(op_scn), K_(op_type), K_(is_replay), K_(is_incomplete_replay));

  share::ObLSID src_ls_id_;
  int64_t transfer_epoch_;
  share::SCN transfer_scn_;
  share::SCN op_scn_;
  transaction::NotifyType op_type_;
  bool is_replay_;
  bool is_incomplete_replay_;
};

struct ObTransferOutTxParam
{
  ObTransferOutTxParam() { reset(); }
  ~ObTransferOutTxParam() { reset(); }
  void reset();
  TO_STRING_KV(K_(except_tx_id), K_(data_end_scn), K_(op_scn), K_(op_type),
      K_(is_replay), K_(dest_ls_id), K_(transfer_epoch), K_(move_tx_ids));
  int64_t except_tx_id_;
  share::SCN data_end_scn_;
  share::SCN op_scn_;
  transaction::NotifyType op_type_;
  bool is_replay_;
  share::ObLSID dest_ls_id_;
  int64_t transfer_epoch_;
  ObIArray<transaction::ObTransID> *move_tx_ids_;
};

struct CollectTxCtxInfo final
{
  OB_UNIS_VERSION(1);
public:
  CollectTxCtxInfo() { reset(); }
  ~CollectTxCtxInfo() { reset(); }
  bool is_valid() {
    return src_ls_id_.is_valid() &&
           dest_ls_id_.is_valid() &&
           task_id_ > 0 &&
           transfer_epoch_ > 0 &&
           transfer_scn_.is_valid() &&
           args_.count() > 0;
  }
  void reset() {
    src_ls_id_.reset();
    dest_ls_id_.reset();
    task_id_ = 0;
    transfer_epoch_ = 0;
    transfer_scn_.reset();
    args_.reset();
  }
  int assign(const CollectTxCtxInfo& other);
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  int64_t task_id_;
  int64_t transfer_epoch_;
  share::SCN transfer_scn_;
  ObSArray<ObTxCtxMoveArg> args_;

  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(task_id), K_(transfer_epoch), K_(transfer_scn), K_(args));
};

struct ObTransferDestPrepareInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTransferDestPrepareInfo() :task_id_(0),
                               src_ls_id_(),
                               dest_ls_id_()
  {}
  void reset() {
    task_id_ = 0;
    src_ls_id_.reset();
    dest_ls_id_.reset();
  }
  ~ObTransferDestPrepareInfo() {
    reset();
  }
  int assign(const ObTransferDestPrepareInfo& other);
  int64_t task_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;

  bool is_valid() {
    return task_id_ > 0 && src_ls_id_.is_valid() && dest_ls_id_.is_valid();
  }

  TO_STRING_KV(K_(task_id), K_(src_ls_id), K_(dest_ls_id));
};

class ObTransferOutTxCtx : public mds::MdsCtx
{
  OB_UNIS_VERSION(1);
public:
  ObTransferOutTxCtx() { reset();  }
  ~ObTransferOutTxCtx() { reset(); }
  void reset();
  int record_transfer_block_op(const share::ObLSID src_ls_id,
                               const share::ObLSID dest_ls_id,
                               const share::SCN data_end_scn,
                               int64_t transfer_epoch,
                               bool is_replay,
                               bool filter_tx_need_transfer,
                               ObIArray<transaction::ObTransID> &move_tx_ids);
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  bool is_valid();
  int assign(const ObTransferOutTxCtx &other);
  bool empty_tx() { return filter_tx_need_transfer_ && move_tx_ids_.count() == 0; }

  TO_STRING_KV(K_(do_transfer_block),
               K_(src_ls_id),
               K_(dest_ls_id),
               K_(data_end_scn),
               K_(transfer_scn),
               K_(filter_tx_need_transfer),
               K_(move_tx_ids));
private:
  bool do_transfer_block_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN data_end_scn_;
  share::SCN transfer_scn_;
  int64_t transfer_epoch_;
  bool filter_tx_need_transfer_;
  ObSEArray<transaction::ObTransID, 1> move_tx_ids_;
};

OB_SERIALIZE_MEMBER_TEMP(inline,
                         ObTransferOutTxCtx,
                         writer_,
                         do_transfer_block_,
                         src_ls_id_,
                         dest_ls_id_,
                         data_end_scn_,
                         transfer_scn_,
                         transfer_epoch_,
                         filter_tx_need_transfer_,
                         move_tx_ids_);

class ObTransferMoveTxCtx : public mds::BufferCtx
{
  OB_UNIS_VERSION(1);
public:
  ObTransferMoveTxCtx();
  ~ObTransferMoveTxCtx() { reset(); }
  void reset();
  int assign(const ObTransferMoveTxCtx& other);
  virtual const mds::MdsWriter get_writer() const override;
  void set_writer(const mds::MdsWriter &writer);
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  CollectTxCtxInfo &get_collect_tx_info() { return collect_tx_info_; }
  share::SCN get_op_scn() const { return op_scn_; }

  TO_STRING_KV(K_(writer), K_(op_scn), K_(collect_tx_info));
private:
  mds::MdsWriter writer_;
  share::SCN op_scn_;
  CollectTxCtxInfo collect_tx_info_;
};

OB_SERIALIZE_MEMBER_TEMP(inline,
                         ObTransferMoveTxCtx,
                         writer_,
                         op_scn_,
                         collect_tx_info_);

class ObStartTransferMoveTxHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
  static int clean(ObLS *ls,
                   transaction::ObTransID tx_id,
                   CollectTxCtxInfo &collect_tx_info);
};

class ObTransferDestPrepareTxCtx : public mds::BufferCtx
{
  OB_UNIS_VERSION(1);
public:
  ObTransferDestPrepareTxCtx() {
    reset();
  }
  ~ObTransferDestPrepareTxCtx() { reset(); }
  void reset();
  int assign(const ObTransferDestPrepareTxCtx &other);
  virtual const mds::MdsWriter get_writer() const override;
  void set_writer(const mds::MdsWriter &writer);
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  ObTransferDestPrepareInfo &get_info() { return transfer_dest_prepare_info_; }
  share::SCN get_op_scn() const { return op_scn_; }

  TO_STRING_KV(K_(writer), K_(op_scn), K_(transfer_dest_prepare_info));
private:
  mds::MdsWriter writer_;
  share::SCN op_scn_;
  ObTransferDestPrepareInfo transfer_dest_prepare_info_;
};

OB_SERIALIZE_MEMBER_TEMP(inline,
                         ObTransferDestPrepareTxCtx,
                         writer_,
                         op_scn_,
                         transfer_dest_prepare_info_);

class ObStartTransferDestPrepareHelper
{
public:
  static int on_register(
      const char* buf,
      const int64_t len,
      mds::BufferCtx &ctx);
  static int on_replay(
      const char* buf,
      const int64_t len,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
};

} // end storage
} // end oceanbase


#endif
