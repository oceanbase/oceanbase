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

#ifndef OCEANBASE_LOGSERVICE_LOG_BLOCK_POOL_INTERFACE_
#define OCEANBASE_LOGSERVICE_LOG_BLOCK_POOL_INTERFACE_
#include <stdint.h>
#include "lib/file/file_directory_utils.h"        // FileDirectoryUtils
#include "log_define.h"                           // block_id_t
#include "log_block_header.h"                     // LogBlockHeader
namespace oceanbase
{
namespace palf
{

// Server-level lifecycle of cross-tenant log block reclaim.
enum class LogReclaimState : int64_t
{
  INITIALIZING = 0, // Restore the persisted mode before planning reclaim.
  DISABLED,         // Each tenant follows its own log disk watermark.
  ENABLED,          // The server may assign cross-tenant reclaim quotas.
  DRAINING          // Stop assigning quotas until every tenant returns to local mode.
};

inline const char *log_reclaim_state_to_string(const LogReclaimState state)
{
  const char *str = "UNKNOWN";
  switch (state) {
    case LogReclaimState::INITIALIZING: str = "INITIALIZING"; break;
    case LogReclaimState::DISABLED: str = "DISABLED"; break;
    case LogReclaimState::ENABLED: str = "ENABLED"; break;
    case LogReclaimState::DRAINING: str = "DRAINING"; break;
    default: break;
  }
  return str;
}

class ILogBlockPool
{
public:
  virtual int create_block(const char *block_path,
                           const int64_t block_size) = 0;
  virtual int remove_block(const char *block_path) = 0;
};

// Server-level coordinator shared by each tenant's PALF block GC loop.
// Owned by the server block manager; tenant PALF environments only borrow it.
class ILogBlockReclaimCoordinator
{
public:
  virtual ~ILogBlockReclaimCoordinator() = default;
  // Advance server-level reclaim state and planning with a monotonic timestamp.
  virtual int try_advance_reclaim(const int64_t now_us) = 0;
  // Return the current server-level reclaim lifecycle state.
  virtual LogReclaimState get_reclaim_state() const = 0;
  // Shared protections remain active until draining completes.
  bool is_shared_log_enabled() const
  {
    return LogReclaimState::DISABLED != get_reclaim_state();
  }
  // Return whether shared-pool usage exceeds the reclaim watermark while enabled.
  virtual bool need_reclaim() const = 0;
  // Return whether the shared pool has reached the hard write-stop watermark while shared protections are active.
  virtual bool is_pool_write_stopped() const = 0;
};

int is_block_used_for_palf(const char *path, bool &result);
int remove_file_at(const char *dir, const char *path, ILogBlockPool *log_block_pool);
int remove_directory_rec(const char *path, ILogBlockPool *log_block_pool);
int remove_tmp_file_or_directory_at(const char *path, ILogBlockPool *log_block_pool);
} // namespace palf
} // namespace oceanbase
#endif
