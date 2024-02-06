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

#ifndef OCEANBASE_STORAGE_OB_LS_LOOP_WORKER
#define OCEANBASE_STORAGE_OB_LS_LOOP_WORKER
#include <stdint.h>

#include <sys/types.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
namespace oceanbase
{
namespace clog
{
class ObIPartitionLogService;
class ObISubmitLogCb;
}

namespace storage
{
class ObLS;
class ObLSWRSHandler
{
public:
  ObLSWRSHandler() { reset(); }
  ~ObLSWRSHandler() { reset(); }
  int init(const share::ObLSID &ls_id);
  void reset();
  int offline();
  int online();
  int generate_ls_weak_read_snapshot_version(oceanbase::storage::ObLS &ls,
                                              bool &need_skip,
                                              bool &is_user_ls,
                                              share::SCN &wrs_version,
                                              const int64_t max_stale_time);
  share::SCN get_ls_weak_read_ts() const { return ls_weak_read_ts_; }
  bool can_skip_ls() const { return !is_enabled_; }

  TO_STRING_KV(K_(is_inited), K_(is_enabled), K_(ls_id), K_(ls_weak_read_ts));

private:
  int generate_weak_read_timestamp_(oceanbase::storage::ObLS &ls, const int64_t max_stale_time, share::SCN &timestamp);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLSWRSHandler);

protected:
  // check enable and protect ls_weak_read_ts_ modify
  common::ObSpinLock lock_;
  bool is_inited_;
  bool is_enabled_;
  share::ObLSID ls_id_;
  share::SCN ls_weak_read_ts_;
};

}
}

#endif // OCEANBASE_TRANSACTION_OB_LS_LOOP_WORKER
