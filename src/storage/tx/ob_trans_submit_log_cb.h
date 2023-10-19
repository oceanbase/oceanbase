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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_SUBMIT_LOG_CB_
#define OCEANBASE_TRANSACTION_OB_TRANS_SUBMIT_LOG_CB_

#include "lib/list/ob_dlist.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "storage/ob_storage_log_type.h"
#include "share/config/ob_server_config.h"
#include "ob_trans_define.h"
#include "ob_tx_ctx_mds.h"
#include "ob_trans_event.h"
#include "share/ob_ls_id.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_append_callback.h"
#include "lib/list/ob_dlink_node.h"
#include "storage/tx/ob_tx_log.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/memtable/ob_redo_log_generator.h"

namespace oceanbase
{

namespace storage
{
class ObTxData;
}

namespace transaction
{
class ObTransService;
class ObPartTransCtx;
}

namespace transaction
{

class ObTxBaseLogCb : public logservice::AppendCb
{
public:
  ObTxBaseLogCb() { reset(); }
  virtual ~ObTxBaseLogCb() { reset(); }
  void reset();
  void reuse();
public:
  void set_base_ts(const share::SCN &base_ts) { base_ts_ = base_ts; }
  const share::SCN &get_base_ts() const { return base_ts_; }
  int set_log_ts(const share::SCN &log_ts);
  const share::SCN &get_log_ts() const { return log_ts_; }
  int set_lsn(const palf::LSN &lsn);
  palf::LSN get_lsn() const { return lsn_; }
  void set_submit_ts(const int64_t submit_ts) { submit_ts_ = submit_ts; }
  int64_t get_submit_ts() const { return submit_ts_; }
  TO_STRING_KV(K_(base_ts), K_(log_ts), K_(lsn), K_(submit_ts));
protected:
  share::SCN base_ts_;
  share::SCN log_ts_;
  palf::LSN lsn_;
  int64_t submit_ts_;
};

class ObTxLogCb : public ObTxBaseLogCb,
                  public common::ObDLinkBase<ObTxLogCb>
{
public:
  ObTxLogCb() { reset(); }
  ~ObTxLogCb() { destroy(); }
  int init(const share::ObLSID &key,
           const ObTransID &trans_id,
           ObTransCtx *ctx,
           const bool is_dynamic);
  void reset();
  void reuse();
  void destroy() { reset(); }
  ObTxLogType get_last_log_type() const;
  ObTransCtx *get_ctx() { return ctx_; }
  void set_tx_data(ObTxData *tx_data)
  {
    if (OB_ISNULL(tx_data)) {
      tx_data_guard_.reset();
    } else {
      tx_data_guard_.init(tx_data);
    }
  }
  ObTxData* get_tx_data() { return tx_data_guard_.tx_data(); }
  void set_callbacks(const memtable::ObCallbackScope &callbacks) { callbacks_ = callbacks; }
  memtable::ObCallbackScope& get_callbacks() { return callbacks_; }
  void set_callbacked() { is_callbacked_ = true; }
  bool is_callbacked() const { return is_callbacked_; }
  bool is_dynamic() const { return is_dynamic_; }
  ObTxCbArgArray &get_cb_arg_array() { return cb_arg_array_; }
  const ObTxCbArgArray &get_cb_arg_array() const { return cb_arg_array_; }
  bool is_valid() const;
public:
  int on_success();
  int on_failure();
  int64_t get_execute_hint() { return trans_id_.hash(); }
  ObTxMDSRange &get_mds_range() { return mds_range_; }

  void set_first_part_scn(const share::SCN &first_part_scn) { first_part_scn_ = first_part_scn; }
  share::SCN get_first_part_scn() const { return first_part_scn_; }

  int copy(const ObTxLogCb &other);
  //bool is_callbacking() const { return is_callbacking_; }
public:
  INHERIT_TO_STRING_KV("ObTxBaseLogCb",
                       ObTxBaseLogCb,
                       KP(this),
                       K(is_inited_),
                       K_(trans_id),
                       K_(ls_id),
                       KP_(ctx),
                       K_(tx_data_guard),
                       K(is_callbacked_),
                       K(is_dynamic_),
                       K(mds_range_),
                       K(cb_arg_array_),
                       K(first_part_scn_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxLogCb);
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObTransID trans_id_;
  ObTransCtx *ctx_;
  ObTxDataGuard tx_data_guard_;
  memtable::ObCallbackScope callbacks_;
  bool is_callbacked_;
  bool is_dynamic_;
  ObTxMDSRange mds_range_;
  ObTxCbArgArray cb_arg_array_;
  share::SCN first_part_scn_;
  //bool is_callbacking_;
};

struct ObTxLogCbRecord
{
  share::SCN self_scn_;
  share::SCN first_part_scn_;

  ObTxLogCbRecord(const ObTxLogCb &cb)
  {
    self_scn_ = cb.get_log_ts();
    first_part_scn_ = cb.get_first_part_scn();
  }

  ObTxLogCbRecord()
  {
    self_scn_.invalid_scn();
    first_part_scn_.invalid_scn();
  }

  TO_STRING_KV(K(self_scn_), K(first_part_scn_));
};

struct ObTxLogBigSegmentInfo
{
  ObTxBigSegmentBuf segment_buf_;
  share::SCN submit_base_scn_;
  logservice::ObReplayBarrierType submit_barrier_type_;
  ObTxLogCb *submit_log_cb_template_;

 common::ObSEArray<ObTxLogCbRecord, 8>  unsynced_segment_part_cbs_;

  void reset()
  {
    segment_buf_.reset();
    submit_base_scn_.min_scn();
    submit_barrier_type_ = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
    if (OB_NOT_NULL(submit_log_cb_template_)) {
      share::mtl_free(submit_log_cb_template_);
    }
    submit_log_cb_template_ = nullptr;

    if (!unsynced_segment_part_cbs_.empty()) {
      TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "all log cbs need return before reset",
                K(unsynced_segment_part_cbs_.count()), K(unsynced_segment_part_cbs_));
    }
  }

  ObTxLogBigSegmentInfo() : submit_log_cb_template_(nullptr) { reset(); }

  void reuse()
  {
    segment_buf_.reset();
    submit_base_scn_.min_scn();
    submit_barrier_type_ = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
    if (OB_NOT_NULL(submit_log_cb_template_)) {
      submit_log_cb_template_->reset();
    }
  }

  TO_STRING_KV(K(segment_buf_),
               K(submit_base_scn_),
               K(submit_barrier_type_),
               KPC(submit_log_cb_template_),
               K(unsynced_segment_part_cbs_.count()));
};
} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_SUBMIT_LOG_CB_
