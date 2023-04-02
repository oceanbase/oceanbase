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

#ifndef OCEANBASE_LOGSERVICE_LOG_LEARNER_H_
#define OCEANBASE_LOGSERVICE_LOG_LEARNER_H_

#include "share/ob_errno.h"                  // KR
#include "lib/net/ob_addr.h"                 // common::ObAddr
#include "lib/ob_define.h"                   // vars...
#include "lib/utility/ob_print_utils.h"      // print...
#include "lib/utility/ob_unify_serialize.h"  // serialize
#include "lib/utility/ob_macro_utils.h"      // some macro
#include "common/ob_region.h"                // common::ObRegion
#include "common/ob_learner_list.h"          // common::BaseLearnerList

namespace oceanbase
{
namespace palf
{

class LogLearner
{
  OB_UNIS_VERSION(1);
public:
  LogLearner();
  LogLearner(const common::ObAddr &server, const int64_t register_time_us);
  LogLearner(const common::ObAddr &server, const common::ObRegion &region, const int64_t register_time_us);
  LogLearner(const LogLearner &child);
  virtual ~LogLearner();
  bool is_valid() const;
  void reset();
  bool is_timeout(const int64_t timeout_us) const;
  const common::ObAddr &get_server() const;
  void update_keepalive_ts();
  bool operator<(const LogLearner &val) const;
  bool operator==(const LogLearner &val) const;
  bool operator!=(const LogLearner &val) const;
  LogLearner &operator=(const LogLearner &val);
  TO_STRING_KV(K_(server), K_(region), K_(register_time_us), K_(keepalive_ts));

public:
  // net addr of LogLearner
  common::ObAddr server_;
  // Region info
  common::ObRegion region_;
  // <parent, child, register_time_us_> uniquely identifies a available
  // registration when this child is in children list of parent
  int64_t register_time_us_;
  // last recv keepalive timestamp
  int64_t keepalive_ts_;
};

typedef common::BaseLearnerList<common::OB_MAX_CHILD_MEMBER_NUMBER, LogLearner> LogLearnerList;
typedef common::BaseLearnerList<common::OB_MAX_CHILD_MEMBER_NUMBER, common::ObMember> LogCandidateList;
} // namespace palf end
} // namespace oceanbase end

#endif
