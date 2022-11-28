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

#ifndef OCEANBASE_TRANSACTION_OB_TENANT_WEAK_READ_SERVER_VERSION_MGR_H_
#define OCEANBASE_TRANSACTION_OB_TENANT_WEAK_READ_SERVER_VERSION_MGR_H_

#include "lib/lock/ob_spin_rwlock.h"        // SpinRWLock
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV
#include "share/scn.h"

namespace oceanbase
{
namespace transaction
{

class ObTenantWeakReadServerVersionMgr
{
public:
  ObTenantWeakReadServerVersionMgr();
  ~ObTenantWeakReadServerVersionMgr();

  share::SCN get_version() const;
  share::SCN get_version(int64_t &total_part_count, int64_t &valid_part_count) const;

  class ServerVersion;
  void get_version(ServerVersion &sv) const;

  // update SERVER level weak read version based on partition readable snapshot version in partition iteration
  //
  // identify different epoch with epoch_tstamp, reset if epoch changes
  int update_with_part_info(const uint64_t tenant_id,
      const int64_t epoch_tstamp,
      const bool need_skip,
      const bool is_user_part,
      const share::SCN version);

  // generate new SERVER level weak read version after scan all partitions
  int generate_new_version(const uint64_t tenant_id,
      const int64_t epoch_tstamp,
      const share::SCN base_version_when_no_valid_partition,
      const bool need_print_status);

public:
  TO_STRING_KV(K_(server_version), K_(server_version_for_stat));

public:
  struct ServerVersion
  {
    share::SCN version_;                 // server version, including inner and user partitions
    int64_t total_part_count_;        // total partition count
    int64_t valid_inner_part_count_;  // valid inner partition count
    int64_t valid_user_part_count_;   // valid user partition count

    ServerVersion() { reset(); }
    void reset()
    {
      version_.reset();
      total_part_count_ = 0;
      valid_inner_part_count_ = 0;
      valid_user_part_count_ = 0;
    }
    void reset(const share::SCN version,
        const int64_t total_part_count,
        const int64_t valid_inner_part_count,
        const int64_t valid_user_part_count)
    {
      version_ = version;
      total_part_count_ = total_part_count;
      valid_inner_part_count_ = valid_inner_part_count;
      valid_user_part_count_ = valid_user_part_count;
    }

    TO_STRING_KV(K_(version),
        K_(total_part_count),
        K_(valid_inner_part_count),
        K_(valid_user_part_count));
  };

private:
  // Server local version
  struct ServerVersionInner : public ServerVersion
  {
    // server version update epoch, indicate server version generation epoch
    int64_t epoch_tstamp_;

    ServerVersionInner() { reset(); }

    void reset()
    {
      ServerVersion::reset();
      epoch_tstamp_ = 0;
    }

    // update SERVER level weak read version based on readable snapshot version
    void update_with_part_info(const int64_t epoch_tstamp,
        const bool need_skip,
        const bool is_user_part,
        const share::SCN version);

    // update based on all partition statistic result
    int update(const ServerVersionInner &new_version);

    // amend weak read version based on epoch timestamp
    int amend(const uint64_t tenant_id, const int64_t new_epoch_tstamp,
        const share::SCN base_version_when_no_valid_partition);

    TO_STRING_KV(K_(version),
        K_(total_part_count),
        K_(valid_inner_part_count),
        K_(valid_user_part_count),
        K_(epoch_tstamp));
  };

private:
  ServerVersionInner server_version_;     // SERVER level VERSION

  mutable common::SpinRWLock  rwlock_;       // Lock

  // usage for stat SERVER version
  //
  // cache tenant SERVER version in partition scan
  //
  // Assumption: Single thread update
  ServerVersionInner server_version_for_stat_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantWeakReadServerVersionMgr);
};

}
}

#endif
