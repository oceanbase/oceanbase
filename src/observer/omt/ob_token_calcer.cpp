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

#define USING_LOG_PREFIX SERVER

#include "ob_token_calcer.h"

#include <cmath>
#include <algorithm>
#include "share/ob_define.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;

double ObTokenCalcer::QUOTA_CONCURRENCY = 1.;

DEF_TO_STRING(ObTokenCalcer::TTC)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("id", tenant_->id(), K_(weight), K_(min_tokens), K_(max_tokens), K_(tokens), K_(done));
  J_OBJ_END();
  return pos;
}

void ObTokenCalcer::TTC::prepare(ObTenant* t)
{
  tenant_ = t;
  tokens_ = 0;
  weight_ = .0;
  done_ = false;
  min_tokens_ = static_cast<int64_t>(floor(min_slice()));
  max_tokens_ = static_cast<int64_t>(floor(max_slice()));
}

inline double ObTokenCalcer::TTC::min_slice() const
{
  return tenant_->unit_min_cpu() * ObTokenCalcer::QUOTA_CONCURRENCY;
}

inline double ObTokenCalcer::TTC::max_slice() const
{
  return tenant_->unit_max_cpu() * ObTokenCalcer::QUOTA_CONCURRENCY;
}

inline void ObTokenCalcer::TTC::done()
{
  done_ = true;
  tenant_->set_sug_token(tokens_);
}

void ObTokenCalcer::TTC::revise_min_max_tokens(int64_t& avail_tokens)
{
  auto& t = tenant_;
  // Maximum reserved quota for accumulation of tenant fractional part
  // of min CPU quota.
  static constexpr double RESERVED_MIN_QUOTA = 1.0;

  //// REVISE MIN TOKENS
  //
  // We assign fractional part of min CPU only if:
  //
  // 1. accumulation of fractional part of min CPU quota reach at
  //    least one.
  // 2. the tenant need token now to process its task.
  // 3. node has available token
  auto new_min_slice = t->acc_min_slice() + min_slice();
  if (new_min_slice > 1.0 && t->has_task() && avail_tokens > 0) {
    avail_tokens -= 1;
    new_min_slice -= 1;
    min_tokens_ += 1;
  } else {
    new_min_slice = std::min(new_min_slice, RESERVED_MIN_QUOTA);
  }
  t->acc_min_slice() = new_min_slice;

  //// REVISE MAX TOKENS
  //
  // Revise tenant max tokens this round if accumulation of fractional
  // part of max CPU reaches one. The number of max tokens revision
  // doesn't affect node available tokens since number of max tokens
  // is virtual that may exceeds node tokens any time.
  auto acc_max_slice = t->acc_max_slice() + max_slice();
  if (acc_max_slice >= 1.0) {
    max_tokens_ = static_cast<int64_t>(floor(acc_max_slice));
    acc_max_slice -= static_cast<double>(max_tokens_);
  }
  t->acc_max_slice() = acc_max_slice;
}

ObTokenCalcer::ObTokenCalcer(ObMultiTenant& omt) : omt_(omt), nttc_(), ttcs_(), node_tokens_()
{}

void ObTokenCalcer::prep_tenants()
{
  const TenantList& orig_tenants = omt_.get_tenant_list();
  auto avail_tokens = node_tokens_;
  for (auto it = orig_tenants.begin(); it != orig_tenants.end(); it++) {
    ObTenant* t = *it;
    if ((t->id() < OB_SERVER_TENANT_ID || t->id() >= OB_USER_TENANT_ID)  // exclude virtual tenant.
        && !t->has_stopped()) {                                          // exclude stopping tenant.
      ttcs_[nttc_].prepare(t);
      ttcs_[nttc_++].revise_min_max_tokens(avail_tokens);
    } else {
      // virtual tenants, always use min CPU quota.
      const auto min_token = static_cast<int64_t>(t->unit_min_cpu() * QUOTA_CONCURRENCY);
      t->set_sug_token(std::max(1L, min_token));
    }
  }
}

int ObTokenCalcer::calculate()
{
  int ret = OB_SUCCESS;
  QUOTA_CONCURRENCY = omt_.get_quota2token();
  node_tokens_ = static_cast<int>(omt_.get_node_quota() * QUOTA_CONCURRENCY);
  nttc_ = 0;

  if (OB_FAIL(omt_.lock_tenant_list())) {
    LOG_ERROR("fail to lock tenant list", K(ret));
  } else {
    prep_tenants();
    calc_tenants();
    if (OB_FAIL(omt_.unlock_tenant_list())) {
      LOG_ERROR("fail to unlock tenant list");
    }
  }
  return ret;
}

void ObTokenCalcer::calc_tenants()
{
  static constexpr double SIGBASE = 1.2589;  // pow(SIGBASE, 10) ~= 10
  static constexpr double BH = 1.;           // fake billing history
  // Calculate weight of each tenant: sig * load * bh
  // TODO: Consider CPU utility of last period.
  double total_weights = .0;
  for (auto idx = 0; idx != nttc_; idx++) {
    auto& ttc = ttcs_[idx];
    ttc.weight() = pow(SIGBASE, ttc.tenant()->significance());  // [1/10,10]
    const double load = std::min(
        std::max(static_cast<double>(ttc.tenant()->waiting_count()), 0.0001), static_cast<double>(ttc.max_tokens()));
    ttc.weight() *= load;
    ttc.weight() *= BH;
    total_weights += ttc.weight();
  }

  auto total_tokens = node_tokens_;
  auto offset = -node_tokens_;  // offset = sum(calculated) - sum(target)
  auto finish = false;          // finish means no new tenant done
                                // within on iteration by function
                                // `adjust_tenants'
  for (int64_t i = 0; i < 10 && offset != 0; i++) {
    finish = adjust_tenants(total_weights, total_tokens, offset);
    if (finish || 0 == offset) {
      break;
    }
  }
  if (0 == offset && !finish) {  // mark all tenants done when
                                 // sum(calculated) == sum(target).
    for (auto idx = 0; idx != nttc_; idx++) {
      auto& ttc = ttcs_[idx];
      ttc.done();
    }
    finish = true;
  }

  if (!finish) {  // Still have tenant hasn't done after full
                  // iterations, use round to determine tokens of
                  // tenant.
    std::random_shuffle(ttcs_, ttcs_ + nttc_);
    auto assigned_tokens = 0L;
    auto assigned_weight = .0;
    for (auto idx = 0; idx != nttc_; idx++) {
      auto& ttc = ttcs_[idx];
      if (!ttc.has_done()) {
        assigned_weight += ttc.weight();
        auto tokens = static_cast<int64_t>(static_cast<double>(total_tokens) * assigned_weight / total_weights);
        ttc.tokens() = std::min(ttc.max_tokens(), std::max(ttc.min_tokens(), tokens - assigned_tokens));
        assigned_tokens += ttc.tokens();
        ttc.done();
      }
    }
  }
}

bool ObTokenCalcer::adjust_tenants(double& total_weights, int64_t& total_tokens, int64_t& offset)
{
  offset = -total_tokens;
  for (auto idx = 0; idx != nttc_; idx++) {
    auto& ttc = ttcs_[idx];
    if (!ttc.has_done()) {
      auto target = std::lround(static_cast<double>(total_tokens) * ttc.weight() / total_weights);
      ttc.tokens() = std::min(ttc.max_tokens(), std::max(ttc.min_tokens(), target));
      offset += ttc.tokens();
    }
  }
  bool finish = true;
  for (auto idx = 0; idx != nttc_; idx++) {
    auto& ttc = ttcs_[idx];
    if (ttc.has_done()) {
      // Ignore done ttc
    } else if (ttc.tokens() == (offset > 0 ? ttc.min_tokens() : ttc.max_tokens())) {
      // It hasn't done before, but OK right now.
      ttc.done();
      total_weights -= ttc.weight();
      total_tokens -= ttc.tokens();
    } else {
      // Set flag to indicate there's TTC hasn't finished yet.
      finish = false;
    }
  }
  return finish;
}
