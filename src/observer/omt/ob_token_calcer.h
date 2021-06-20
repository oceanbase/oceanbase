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

#ifndef _OCEABASE_OBSERVER_OMT_OB_TOKEN_CALCER_H_
#define _OCEABASE_OBSERVER_OMT_OB_TOKEN_CALCER_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {
namespace omt {

// Forward declarations
class ObMultiTenant;
class ObTenant;

class ObTokenCalcer {
  // FIXME: remove assumption
  static constexpr int64_t MAX_TENANT_COUNT = 1024;
  static double QUOTA_CONCURRENCY;

  friend class TTC;
  //  Tenant of Token Cacler
  class TTC {
  public:
    void prepare(ObTenant* tenant);
    void revise_min_max_tokens(int64_t& avail_tokens);

    ObTenant* tenant()
    {
      return tenant_;
    }
    int64_t& tokens()
    {
      return tokens_;
    }
    double& weight()
    {
      return weight_;
    }
    int64_t& min_tokens()
    {
      return min_tokens_;
    }
    int64_t& max_tokens()
    {
      return max_tokens_;
    }
    bool has_done() const
    {
      return done_;
    }
    void done();

    DECLARE_TO_STRING;

  private:
    double min_slice() const;
    double max_slice() const;

  private:
    ObTenant* tenant_;
    double weight_;
    int64_t min_tokens_;
    int64_t max_tokens_;
    int64_t tokens_;
    bool done_;
  };
  using TTCVec = TTC[MAX_TENANT_COUNT];

public:
  explicit ObTokenCalcer(ObMultiTenant& omt);
  int calculate();

private:
  // Prepare tenants who's tokens should be calculated. It includes
  // two steps, filter out legal tenants and calculate min/max tokens
  // this round for tenant.
  //
  // Tenant with one or some of following properties won't show in the
  // list.
  //
  //   1. stopping/deleting tenant.
  //
  //   2. virtual tenant which has fixed token.
  void prep_tenants();

  // Calculate tokens of tenants which are filter out in the routine
  // `prep_tenants'. It contains three steps to determine how many
  // each tenant should get.
  //
  //   1. Gather tenant information and calculate weight of each
  //      tenant by them.
  //
  //   2. Filter out definite tenants which would get min or max
  //      tokens by several iterations. Within each iteration
  //      implement by routine `adjust_tenants', those tenants whose
  //      target number of tokens is over its limitation, larger than
  //      max or less than min, would be picked out and mark as
  //      finished.
  //
  //   3. After the iterations we assume all tenants have fit in its
  //      limitation that a round function would be used to decide
  //      each tokens count. To ensure rest of unassigned tokens can
  //      got by the rest of tenants precisely, no remains or lack of
  //      tokens for some tenants, we use a overall round instead of
  //      just round tenant itself. And to make sure the order of
  //      tenants affect token assigning, shuffling would be made
  //      before the round action.
  void calc_tenants();

  // Used in function `calc_tenants' to pick out tenant with definite
  // number of tokens, target tokens larger than max or less than min.
  bool adjust_tenants(double& total_weights, int64_t& total_tokens, int64_t& offset);

private:
  ObMultiTenant& omt_;
  // Number of tenants that participate in the reassignment of tokens.
  int64_t nttc_;
  // Tenants which participate in the reassignment of tokens.
  TTCVec ttcs_;
  // Number of tokens this node owned. It's calculated at the
  // beginning of every assignment and won't change during the
  // assignment.
  int64_t node_tokens_;
};  // end of class ObTokenCalcer

}  // end of namespace omt
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_TOKEN_CALCER_H_ */
