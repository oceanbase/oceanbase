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
#include "logservice/palf/lsn.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObLS;

class ObTabletServiceClogReplayExecutor final
{
public:
  static int execute(
      ObLS *ls,
      const char *buf,
      const int64_t log_size,
      const int64_t pos,
      const palf::LSN &lsn,
      const share::SCN &scn);
public:
  TO_STRING_KV(KP(ls_), K(lsn_), K(scn_));

private:
  ObTabletServiceClogReplayExecutor(
      ObLS *ls,
      const palf::LSN &lsn,
      const share::SCN &scn);
  ~ObTabletServiceClogReplayExecutor() = default;
  ObTabletServiceClogReplayExecutor(const ObTabletServiceClogReplayExecutor&) = delete;
  ObTabletServiceClogReplayExecutor &operator=(const ObTabletServiceClogReplayExecutor&) = delete;
private:
  int replay_update_storage_schema(
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      const int64_t pos);
private:
  ObLS *ls_;
  palf::LSN lsn_;
  share::SCN scn_;
};

}
}
#endif
