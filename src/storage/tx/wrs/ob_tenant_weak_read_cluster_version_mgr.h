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

#ifndef OCEANBASE_TRANSACTION_OB_WEAK_READ_CLUSTER_VERSION_MGR_H_
#define OCEANBASE_TRANSACTION_OB_WEAK_READ_CLUSTER_VERSION_MGR_H_

#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/lock/ob_spin_rwlock.h"          // SpinRWLock
#include "lib/lock/ob_small_spin_lock.h"      // ObByteLock
#include "lib/container/ob_se_array.h"        // ObSEArray
#include "share/scn.h"

namespace oceanbase
{
namespace transaction
{

class ObTenantWeakReadClusterVersionMgr
{
  /// max server heartbeat interval
  /// If server version had no update for a long time, server version is considered invalid
  ///
  /// This time is big enough to check if the server alive.
  /// After this time, if the heartbeat is still not received, it is considered that there should
  /// be a problem with the server, and there is no need to send the request.
  static const int64_t MAX_SERVER_ALIVE_HEARTBEAT_INTERVAL = 10 * 1000 * 1000L;

public:
  ObTenantWeakReadClusterVersionMgr();
  ~ObTenantWeakReadClusterVersionMgr();

public:
  void reset(const uint64_t tenant_id);

  /// update server version
  /// if the server record exist，update the record
  /// if the server record not exist, insert a new record
  ///
  /// @retval OB_SUCCESS success
  /// @retval OTHER CODE fail
  int update_server_version(const common::ObAddr &addr,
      const share::SCN version,
      const int64_t valid_part_count,
      const int64_t total_part_count,
      const int64_t generate_tstamp,
      bool &is_new_server);

  /// get min server version which not smaller than base_version
  /// if no statisfied server version, return base_version
  share::SCN get_cluster_version(const share::SCN base_version, int64_t &skip_server_count, const bool need_print_server_info) const;

  // get server count in cluster master cached registered servers
  int64_t get_server_count() const;
private:
  class ServerInfo;
  bool find_match_server(int64_t &pre_count, const common::ObAddr &addr, ServerInfo *&psi);
private:
  // Spin Lock
  typedef common::ObByteLock      SpinLock;
  typedef common::ObByteLockGuard SpinLockGuard;

  // read write lock
  typedef common::SpinRWLock      RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;

  struct ServerInfo
  {
    common::ObAddr  addr_;                // server addr
    share::SCN       version_;             // server weak read version
    int64_t         valid_part_count_;    // valid partition count
    int64_t         total_part_count_;    // total partition count
    int64_t         generate_tstamp_;     // server weak read version generation timestamp
    mutable bool    is_skipped_;          // is skipped or not

    mutable SpinLock lock_;

    ServerInfo();
    ServerInfo(const common::ObAddr &addr,
        const share::SCN verison,
        const int64_t valid_part_count,
        const int64_t total_part_count);
    bool match(const common::ObAddr &addr) const;
    void update(const share::SCN version,
        const int64_t valid_part_count,
        const int64_t total_part_count,
        const int64_t generate_tstamp);
    share::SCN get_version(bool &need_skip, bool &is_first_skipped) const;

    TO_STRING_KV(K_(addr), K_(version), K_(valid_part_count), K_(total_part_count),
        K_(generate_tstamp), K_(is_skipped));
  };

  typedef common::ObSEArray<ServerInfo, 16> ServerArray;
private:
  uint64_t      tenant_id_;
  ServerArray   svr_array_;
  RWLock        rwlock_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantWeakReadClusterVersionMgr);
};

}
}

#endif
