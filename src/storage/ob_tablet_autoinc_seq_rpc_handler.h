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

#ifndef OCEANBASE_SHARE_OB_TABLET_AUTOINC_SEQ_RPC_HANDLER_H_
#define OCEANBASE_SHARE_OB_TABLET_AUTOINC_SEQ_RPC_HANDLER_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/ob_small_allocator.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/ob_rpc_struct.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"

namespace oceanbase
{
namespace storage 
{
namespace mds
{
struct BufferCtx;
}

class ObSyncTabletSeqReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObSyncTabletSeqReplayExecutor();
  int init(const uint64_t autoinc_seq,
      const bool is_tablet_creating,
      const share::SCN &replay_scn);

  TO_STRING_KV(K_(seq),
               K_(is_tablet_creating),
               K_(scn));

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return is_tablet_creating_;
  }

  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }

private:
  uint64_t seq_;
  bool is_tablet_creating_;
  share::SCN scn_;
};

class ObTabletAutoincSeqReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletAutoincSeqReplayExecutor()
    : logservice::ObTabletReplayExecutor(), user_ctx_(nullptr), scn_(), data_(nullptr) {}

  int init(mds::BufferCtx &user_ctx, const share::SCN &scn, const ObTabletAutoincSeq &data);

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return true;
  }

  int do_replay_(ObTabletHandle &tablet_handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }

private:
  mds::BufferCtx *user_ctx_;
  share::SCN scn_;
  const share::ObTabletAutoincSeq *data_;
};

class ObTabletAutoincSeqRpcHandler final
{
public:
  static ObTabletAutoincSeqRpcHandler &get_instance();
  int init();
  int fetch_tablet_autoinc_seq_cache(
      const obrpc::ObFetchTabletSeqArg &arg,
      obrpc::ObFetchTabletSeqRes &res);
  int batch_get_tablet_autoinc_seq(
      const obrpc::ObBatchGetTabletAutoincSeqArg &arg,
      obrpc::ObBatchGetTabletAutoincSeqRes &res);
  int batch_set_tablet_autoinc_seq(
      const obrpc::ObBatchSetTabletAutoincSeqArg &arg,
      obrpc::ObBatchSetTabletAutoincSeqRes &res);
  int replay_update_tablet_autoinc_seq(
      const ObLS *ls,
      const ObTabletID &tablet_id,
      const uint64_t autoinc_seq,
      const bool is_tablet_creating,
      const share::SCN &replay_scn);
  int batch_set_tablet_autoinc_seq_in_trans(
      ObLS &ls,
      const obrpc::ObBatchSetTabletAutoincSeqArg &arg,
      const share::SCN &replay_scn,
      mds::BufferCtx &ctx);
private:
  ObTabletAutoincSeqRpcHandler();
  ~ObTabletAutoincSeqRpcHandler();
  int set_tablet_autoinc_seq_in_trans(
      ObLS &ls,
      const ObTabletID &tablet_id,
      const share::ObTabletAutoincSeq &data,
      const share::SCN &replay_scn,
      mds::BufferCtx &ctx);
private:
  static const int64_t BUCKET_LOCK_BUCKET_CNT = 10243L;
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
};

} // end namespace storage
} // end namespace oceanbase
#endif
