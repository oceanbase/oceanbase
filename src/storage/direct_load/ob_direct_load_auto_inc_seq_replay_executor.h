/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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