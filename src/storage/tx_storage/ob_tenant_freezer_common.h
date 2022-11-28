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

#include "share/ob_define.h"
#include "common/ob_tablet_id.h"
#include "common/storage/ob_freeze_define.h"

#ifndef OCEABASE_STORAGE_TENANT_FREEZER_COMMON_
#define OCEABASE_STORAGE_TENANT_FREEZER_COMMON_
namespace oceanbase
{
namespace storage
{
struct ObTenantFreezeArg
{
  storage::ObFreezeType freeze_type_; // minor/major
  int64_t try_frozen_scn_;            // frozen scn.

  DECLARE_TO_STRING;
  OB_UNIS_VERSION(1);
};

struct ObRetryMajorInfo
{
  uint64_t tenant_id_;
  int64_t frozen_scn_;

  ObRetryMajorInfo()
    : tenant_id_(UINT64_MAX),
      frozen_scn_(0)
  {}
  bool is_valid() const {
    return UINT64_MAX != tenant_id_;
  }
  void reset() {
    tenant_id_ = UINT64_MAX;
    frozen_scn_ = 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(frozen_scn));
};

// store the tenant info, such as memory limit, memstore limit,
// slow freeze flag, freezing flag and so on.
class ObTenantInfo : public ObDLinkBase<ObTenantInfo>
{
public:
  ObTenantInfo();
  virtual ~ObTenantInfo() { reset(); }
  void reset();
  int update_frozen_scn(int64_t frozen_scn);
  int64_t mem_memstore_left() const;
public:
  uint64_t tenant_id_;
  int64_t mem_lower_limit_;    // the min memory limit
  int64_t mem_upper_limit_;    // the max memory limit
  // mem_memstore_limit will be checked when **leader** partitions
  // perform writing operation (select for update is included)
  int64_t mem_memstore_limit_; // the max memstore limit
  bool is_loaded_;             // whether the memory limit set or not.
  bool is_freezing_;           // is the tenant freezing now.
  int64_t last_freeze_clock_;
  int64_t frozen_scn_;         // used by major, the timestamp of frozen.
  int64_t freeze_cnt_;         // minor freeze times.
  int64_t last_halt_ts_;       // Avoid frequent execution of abort preheating
  bool slow_freeze_;           // Avoid frequent freezing when abnormal
  int64_t slow_freeze_timestamp_; // the last slow freeze time timestamp
  int64_t slow_freeze_min_protect_clock_;
  common::ObTabletID slow_tablet_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantInfo);
};

} // storage
} // oceanbase
#endif
