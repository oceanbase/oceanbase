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

#include "ob_task_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_simple_rate_limiter.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

class ObLogRateLimiter : public lib::ObSimpleRateLimiter {
  friend class ObTaskController;

public:
  bool is_force_allows() const override
  {
    return OB_UNLIKELY(allows_ > 0);
  }
  void reset_force_allows() override
  {
    if (is_force_allows()) {
      allows_--;
    }
  }

private:
  static RLOCAL(int64_t, allows_);
};

RLOCAL(int64_t, ObLogRateLimiter::allows_);

// class ObTaskController
ObTaskController ObTaskController::instance_;

ObTaskController::ObTaskController() : limiters_(), rate_pctgs_(), log_rate_limit_(LOG_RATE_LIMIT)
{}

ObTaskController::~ObTaskController()
{
  destroy();
}

int ObTaskController::init()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < MAX_TASK_ID; i++) {
    limiters_[i] = OB_NEW(ObLogRateLimiter, ObModIds::OB_LOG);
    if (nullptr == limiters_[i]) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
  }
  if (OB_SUCC(ret)) {
#define LOG_PCTG(ID, PCTG)          \
  do {                              \
    set_log_rate_pctg<ID>(PCTG);    \
    get_limiter(ID)->set_name(#ID); \
  } while (0)

    // Set percentage of each task here.
    // @NOTE: Inquire @ before any change.
    LOG_PCTG(ObTaskType::GENERIC, 100.0);        // default limiter
    LOG_PCTG(ObTaskType::USER_REQUEST, 100.0);   // default limiter
    LOG_PCTG(ObTaskType::DATA_MAINTAIN, 100.0);  // default limiter
    LOG_PCTG(ObTaskType::ROOT_SERVICE, 100.0);   // default limiter
    LOG_PCTG(ObTaskType::SCHEMA, 100.0);         // default limiter

#undef LOG_PCTG

    calc_log_rate();
    ObLogger::set_default_limiter(*limiters_[toUType(ObTaskType::GENERIC)]);
    ObLogger::set_tl_type(0);
  } else {
    destroy();
  }

  return ret;
}

void ObTaskController::destroy()
{
  for (int i = 0; i < MAX_TASK_ID; i++) {
    if (nullptr != limiters_[i]) {
      ob_delete(limiters_[i]);
      limiters_[i] = nullptr;
    }
  }
}

void ObTaskController::switch_task(ObTaskType task_id)
{
  ObLogger::set_tl_limiter(*limiters_[toUType(task_id)]);
  ObLogger::set_tl_type(static_cast<int32_t>(toUType(task_id)));
}

void ObTaskController::allow_next_syslog(int64_t count)
{
  ObLogRateLimiter::allows_ += count;
}

void ObTaskController::set_log_rate_limit(int64_t limit)
{
  if (limit != log_rate_limit_) {
    log_rate_limit_ = limit;
    calc_log_rate();
  }
}

void ObTaskController::calc_log_rate()
{
  const double total = std::accumulate(rate_pctgs_, rate_pctgs_ + MAX_TASK_ID, .0);
  for (int i = 0; total != 0 && i < MAX_TASK_ID; i++) {
    limiters_[i]->set_rate(static_cast<int64_t>(rate_pctgs_[i] / total * static_cast<double>(log_rate_limit_)));
  }
}

ObTaskController& ObTaskController::get()
{
  return instance_;
}

}  // namespace share
}  // namespace oceanbase
