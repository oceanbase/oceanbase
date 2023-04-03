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

#ifndef OCEANBASE_COMMON_OB_SIMPLE_MEM_LIMIT_GETTER_H_
#define OCEANBASE_COMMON_OB_SIMPLE_MEM_LIMIT_GETTER_H_

#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_i_tenant_mem_limit_getter.h"

namespace oceanbase
{
namespace common
{
class ObSimpleMemLimitGetter : public ObITenantMemLimitGetter
{
public:
  ObSimpleMemLimitGetter() : lock_(ObLatchIds::DEFAULT_SPIN_RWLOCK) {}
  virtual ~ObSimpleMemLimitGetter() {}
  int add_tenant(const uint64_t tenant_id,
                 const int64_t lower_limit,
                 const int64_t upper_limit);
  bool has_tenant(const uint64_t tenant_id) const override;
  int get_all_tenant_id(ObIArray<uint64_t> &tenant_ids) const override;
  int get_tenant_mem_limit(const uint64_t tenant_id,
                           int64_t &lower_limit,
                           int64_t &upper_limit) const override;
  void reset();
private:
  bool has_tenant_(const uint64_t tenant_id) const;
private:
  struct ObTenantInfo
  {
    ObTenantInfo()
      : tenant_id_(common::OB_INVALID_ID),
        mem_lower_limit_(-1),
        mem_upper_limit_(-1) {}

    ObTenantInfo(const uint64_t tenant_id,
                 int64_t lower_limit,
                 int64_t upper_limit)
      : tenant_id_(tenant_id),
        mem_lower_limit_(lower_limit),
        mem_upper_limit_(upper_limit) {}

    TO_STRING_KV(K_(tenant_id), K_(mem_lower_limit), K_(mem_upper_limit));

    uint64_t tenant_id_;
    int64_t mem_lower_limit_;
    int64_t mem_upper_limit_;
  };

private:
  SpinRWLock lock_;
  ObSEArray<ObTenantInfo, 10> tenant_infos_;
};

} // common
} // oceanbase

#endif
