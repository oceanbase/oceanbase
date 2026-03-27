/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_REPLAY_EXECUTOR_H_
#define OB_STORAGE_OB_DIRECT_LOAD_AUTO_INC_SEQ_REPLAY_EXECUTOR_H_

#include "logservice/replayservice/ob_tablet_replay_executor.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadAutoIncSeqReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObDirectLoadAutoIncSeqReplayExecutor();
  ~ObDirectLoadAutoIncSeqReplayExecutor() = default;
  int init(mds::BufferCtx &user_ctx,
           const ObDirectLoadAutoIncSeqData &seq_data,
           const share::SCN &scn);
protected:
  virtual int do_replay_(ObTabletHandle &tablet_handle) override;
  bool is_replay_update_tablet_status_() const override;
  virtual bool is_replay_update_mds_table_() const override;
private:
  mds::BufferCtx *user_ctx_;
  const ObDirectLoadAutoIncSeqData *seq_data_;
  share::SCN scn_;
};

} // namespace storage
} // namespace oceanbase

#endif