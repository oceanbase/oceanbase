/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_SERVER_DISK_SPACE_FILTER_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_SERVER_DISK_SPACE_FILTER_H_

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace rootserver
{

// Filters backup servers by disk usage threshold and sorts by (priority asc, disk_usage_pct asc).
class ObBackupServerDiskSpaceFilter
{
public:
  // Raw per-server disk stats. Public so callers can cache and reuse across calls.
  struct RawDiskStat {
    common::ObAddr server_;
    int64_t capacity_;
    int64_t in_use_;
    RawDiskStat() : server_(), capacity_(0), in_use_(0) {}
    TO_STRING_KV(K_(server), K_(capacity), K_(in_use));
  };

  ObBackupServerDiskSpaceFilter();
  int init(common::ObMySQLProxy *sql_proxy);
  // Query all servers' disk stats. Caller may cache the result to skip the SQL.
  int query_all_disk_stats(common::ObIArray<RawDiskStat> &raw_stats);
  // Apply the disk-usage filter against pre-fetched raw stats. Stateless: does not
  // need init(). Useful for callers that maintain their own raw_stats cache.
  static int apply_disk_filter(
      const common::ObIArray<share::ObBackupServer> &input_servers,
      const common::ObIArray<RawDiskStat> &raw_stats,
      common::ObIArray<share::ObBackupServer> &filtered_servers);
private:
  struct ServerWithDiskInfo {
    share::ObBackupServer server_;
    int64_t used_percentage_;
    ServerWithDiskInfo() : server_(), used_percentage_(0) {}
    TO_STRING_KV(K_(server), K_(used_percentage));
    bool operator<(const ServerWithDiskInfo &other) const
    {
      bool bret = false;
      if (server_.priority_ != other.server_.priority_) {
        bret = server_.priority_ < other.server_.priority_;
      } else {
        bret = used_percentage_ < other.used_percentage_;
      }
      return bret;
    }
  };
private:
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupServerDiskSpaceFilter);
};

} // namespace rootserver
} // namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_SERVER_DISK_SPACE_FILTER_H_
