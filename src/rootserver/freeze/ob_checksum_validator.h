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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_

#include "share/ob_tablet_checksum_iterator.h"
#include "share/ob_tablet_replica_checksum_iterator.h"
#include "share/ob_freeze_info_proxy.h"

namespace oceanbase
{
namespace rootserver
{
class ObZoneMergeManager;
class ObFreezeInfoManager;
class ObServerManager;

class ObChecksumValidatorBase
{
public:
  ObChecksumValidatorBase()
    : is_inited_(false), need_check_(true), tenant_id_(OB_INVALID_TENANT_ID),
      sql_proxy_(NULL)
  {}
  virtual ~ObChecksumValidatorBase() {}
  virtual int init(const uint64_t tenant_id,
                   common::ObMySQLProxy *sql_proxy);

  int check(const share::ObSimpleFrozenStatus &frozen_status);

  void set_need_check(bool need_check) { need_check_ = need_check; }

  static const int64_t MIN_CHECK_INTERVAL = 10 * 1000 * 1000LL;

protected:
  virtual int do_check(const share::ObSimpleFrozenStatus &frozen_status) = 0;

protected:
  bool is_inited_;
  bool need_check_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
};

// Mainly to verify checksum between each tablet replicas in primary/standby cluster
class ObTabletChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObTabletChecksumValidator() {}
  virtual ~ObTabletChecksumValidator() {}

protected:
  virtual int do_check(const share::ObSimpleFrozenStatus &frozen_status) override;
};

// Mainly to verify checksum of cross-cluster's tablet which sync from primary cluster
class ObCrossClusterTableteChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObCrossClusterTableteChecksumValidator() {}
  virtual ~ObCrossClusterTableteChecksumValidator() {}

  // sync data from __all_tablet_replica_checksum to __all_tablet_checksum
  int write_tablet_checksum_item();

protected:
  virtual int do_check(const share::ObSimpleFrozenStatus &frozen_status) override;

private:
  bool is_first_tablet_in_sys_ls(const share::ObTabletReplicaChecksumItem &item) const;

private:
  const static int64_t MAX_BATCH_INSERT_COUNT = 100;
};

// Mainly to verify checksum between (global and local) index table and main table
class ObIndexChecksumValidator : public ObChecksumValidatorBase
{
public:
  ObIndexChecksumValidator() {}
  virtual ~ObIndexChecksumValidator() {}

protected:
  virtual int do_check(const share::ObSimpleFrozenStatus &frozen_status) override;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_CHECKSUM_VALIDATOR_H_