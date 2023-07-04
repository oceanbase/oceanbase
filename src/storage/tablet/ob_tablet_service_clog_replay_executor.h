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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SERVICE_CLOG_REPLAY_EXECUTOR
#define OCEANBASE_STORAGE_OB_TABLET_SERVICE_CLOG_REPLAY_EXECUTOR

#include <stdint.h>
#include "common/ob_tablet_id.h"
#include "logservice/palf/lsn.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObLS;

class ObTabletServiceClogReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletServiceClogReplayExecutor();
  int init(
      const char *buf,
      const int64_t log_size,
      const int64_t pos,
      const share::SCN &scn);

  TO_STRING_KV(KP_(buf),
               K_(buf_size),
               K_(pos),
               K_(scn));

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }

  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return false;
  }

private:
  const char *buf_;
  int64_t buf_size_;
  int64_t pos_;
  share::SCN scn_;
};

}
}
#endif
