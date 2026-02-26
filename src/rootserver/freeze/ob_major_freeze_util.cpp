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

#define USING_LOG_PREFIX RS_COMPACTION


#include "ob_major_freeze_util.h"
#include "share/ob_service_epoch_proxy.h"
#include "rootserver/freeze/ob_major_freeze_service.h"

namespace oceanbase
{
namespace rootserver
{

int ObMajorFreezeUtil::get_major_freeze_service(
    ObPrimaryMajorFreezeService *primary_major_freeze_service,
    ObRestoreMajorFreezeService *restore_major_freeze_service,
    ObMajorFreezeService *&major_freeze_service,
    bool &is_primary_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(primary_major_freeze_service) || OB_ISNULL(restore_major_freeze_service)) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(ERROR, "primary or restore major_freeze_service is nullptr", KR(ret),
           KP(primary_major_freeze_service), KP(restore_major_freeze_service));
  } else {
    bool is_primary_service_paused = primary_major_freeze_service->is_paused();
    bool is_restore_service_paused = restore_major_freeze_service->is_paused();
    if ((is_primary_service_paused && is_restore_service_paused)
        || (!is_primary_service_paused && !is_restore_service_paused)) {
      ret = OB_LEADER_NOT_EXIST;
      RS_LOG(WARN, "both primary and restore major_freeze_service are paused or not paused, may be "
             "switching leader", KR(ret), K(is_primary_service_paused), K(is_restore_service_paused));
    } else if (!is_primary_service_paused) {
      major_freeze_service = primary_major_freeze_service;
      is_primary_service = true;
    } else if (!is_restore_service_paused) {
      major_freeze_service = restore_major_freeze_service;
      is_primary_service = false;
    }
  }
  return ret;
}

int ObMajorFreezeUtil::check_epoch_periodically(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t expected_epoch,
    int64_t &last_check_us)
{
  int ret = OB_SUCCESS;
  int64_t now_us = ObTimeUtil::fast_current_time();
  if ((now_us - last_check_us) > CHECK_EPOCH_INTERVAL_US) {
    bool is_match = true;
    if (OB_FAIL(share::ObServiceEpochProxy::check_service_epoch(sql_proxy, tenant_id,
                share::ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
      LOG_WARN("fail to check freeze service epoch", KR(ret), K(tenant_id), K(expected_epoch));
    } else {
      last_check_us = now_us;
      if (!is_match) {
        ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
        LOG_WARN("freeze_service_epoch mismatch", K(tenant_id), K(expected_epoch));
      }
    }
  }
  return ret;
}

ObFreezeTimeGuard::ObFreezeTimeGuard(
  const char *file,
  const int64_t line,
  const char *func,
  const char *mod)
  : warn_threshold_us_(FREEZE_WARN_THRESHOLD_US),
    start_time_us_(common::ObTimeUtility::fast_current_time()),
    file_(file),
    line_(line),
    func_name_(func),
    log_mod_(mod)
{
}

ObFreezeTimeGuard::~ObFreezeTimeGuard()
{
  int64_t now_us = common::ObTimeUtility::fast_current_time();
  int64_t total_cost_us = now_us - start_time_us_;
  if (OB_UNLIKELY(total_cost_us >= warn_threshold_us_)) {
    constexpr int buffer_size = 256;
    char strbuffer[buffer_size] = { 0 };
    int n = snprintf(strbuffer, buffer_size, "cost too much time: %s (%s:%ld), ",
                     func_name_, strrchr(file_, '/') ? (strrchr(file_, '/') + 1) : file_, line_);
    if (n >= buffer_size) {
      snprintf(&strbuffer[buffer_size - 6], 6, "..., ");
    }
    int ret = OB_ERR_TOO_MUCH_TIME;
    OB_MOD_LOG_RET(log_mod_, WARN, ret, strbuffer, K_(warn_threshold_us), K(total_cost_us), K_(start_time_us), K(now_us));
  }
}

const char * ObMajorFreezeReasonStr[] = {
  "DAILY_MERGE",
  "USER_REQUEST",
  "MAJOR_COMPACT_TRIGGER"
};
const char *major_freeze_reason_to_str(const int64_t freeze_reason)
{
  STATIC_ASSERT(static_cast<int64_t>(MF_REASON_MAX) == ARRAYSIZEOF(ObMajorFreezeReasonStr),
                "major freeze reason str len is mismatch");
  const char *str = "";
  if (OB_UNLIKELY(!is_valid_major_freeze_reason((ObMajorFreezeReason)freeze_reason))) {
    str = "invalid_freeze_reason";
  } else {
    str = ObMajorFreezeReasonStr[freeze_reason];
  }
  return str;
}
bool is_valid_major_freeze_reason(const ObMajorFreezeReason &freeze_reason)
{
  return freeze_reason >= ObMajorFreezeReason::MF_DAILY_MERGE
    && freeze_reason < ObMajorFreezeReason::MF_REASON_MAX;
}

} // end namespace rootserver
} // end namespace oceanbase
