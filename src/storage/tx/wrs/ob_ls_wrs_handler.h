/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LS_LOOP_WORKER
#define OCEANBASE_STORAGE_OB_LS_LOOP_WORKER
#include <stdint.h>

#include <sys/types.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "storage/tx/ob_trans_define.h"

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
  ObLSWRSHandler() : lock_(common::ObLatchIds::OB_LS_WRS_HANDLER_LOCK) { reset(); }
  ~ObLSWRSHandler() { reset(); }
  int init(const share::ObLSID &ls_id);
  void reset();
  int offline();
  int online();
  int generate_ls_weak_read_snapshot_version(oceanbase::storage::ObLS &ls,
                                              bool &need_skip,
                                              bool &is_user_ls,
                                              share::SCN &wrs_version,
                                              const int64_t max_stale_time,
                                              const bool need_print);
  share::SCN get_ls_weak_read_ts() const { return ls_weak_read_ts_; }
  bool can_skip_ls() const { return !is_enabled_; }

  TO_STRING_KV(K_(is_inited), K_(is_enabled), K_(ls_id), K_(ls_weak_read_ts));

private:
  int generate_weak_read_timestamp_(oceanbase::storage::ObLS &ls,
                                    const int64_t max_stale_time,
                                    share::SCN &wrs_scn,
                                    share::SCN &min_log_service_scn,
                                    share::SCN &min_tx_service_scn,
                                    share::SCN &end_scn);

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
