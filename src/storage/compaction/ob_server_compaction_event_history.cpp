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

#define USING_LOG_PREFIX STORAGE
#include "storage/compaction/ob_server_compaction_event_history.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace compaction
{

const char *ObServerCompactionEvent::ObCompactionEventStr[] = {
    "RECEIVE_BROADCAST_SCN",
    "GET_FREEZE_INFO",
    "WEAK_READ_TS_READY",
    "SCHEDULER_LOOP",
    "TABLET_COMPACTION_FINISHED",
    "COMPACTION_REPORT",
};

const char *ObServerCompactionEvent::get_comp_event_str(enum ObCompactionEvent event)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_EVENT_MAX) == ARRAYSIZEOF(ObCompactionEventStr), "events str len is mismatch");
  const char *str = "";
  if (event >= COMPACTION_EVENT_MAX || event < RECEIVE_BROADCAST_SCN) {
    str = "invalid_type";
  } else {
    str = ObCompactionEventStr[event];
  }
  return str;
}

int ObServerCompactionEvent::generate_event_str(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (0 == strlen(comment_)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, "%s", get_comp_event_str(event_)))) {
      LOG_WARN("failed to generate str", K(ret), KPC(this));
    }
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "%s:%s", get_comp_event_str(event_), comment_))) {
    LOG_WARN("failed to generate str", K(ret), KPC(this));
  }
  return ret;
}

int ObServerCompactionEvent::generate_event_str(
    const ObServerCompactionEvent &last_event, char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!last_event.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("last event is invalid", K(ret), K(last_event));
  } else {
    if (last_event.merge_type_ == merge_type_
        && last_event.compaction_scn_ == compaction_scn_
        && last_event.event_ != event_) {
      const int64_t cost_time = timestamp_ - last_event.timestamp_;
      if (0 == strlen(comment_)) {
        if (OB_FAIL(databuff_printf(buf, buf_len, "cost_time:%.2fs | %s", (float)cost_time/1_s, get_comp_event_str(event_)))) {
          LOG_WARN("failed to generate str", K(ret), KPC(this));
        }
      } else if (OB_FAIL(databuff_printf(buf, buf_len, "cost_time:%.2fs | %s:%s", (float)cost_time/1_s, get_comp_event_str(event_), comment_))) {
        LOG_WARN("failed to generate str", K(ret), KPC(this));
      }
    } else {
      ret = generate_event_str(buf, buf_len);
    }
  }
  return ret;
}

int ObServerCompactionEventHistory::mtl_init(ObServerCompactionEventHistory* &event_history)
{
  return event_history->init();
}

int ObServerCompactionEventHistory::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInfoRingArray::init(SERVER_EVENT_MAX_CNT))) {
    STORAGE_LOG(WARN, "failed to init ObInfoRingArray", K(ret));
  }
  return ret;
}

void ObServerCompactionEventHistory::destroy()
{
  ObInfoRingArray::destroy();
}

int ObServerCompactionEventHistory::add_event(const ObServerCompactionEvent &event)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!event.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(event));
  } else if (OB_FAIL(ObInfoRingArray::add(event))) {
    LOG_WARN("failed to add event", K(ret), K(event));
  }
  return ret;
}

int ObServerCompactionEventHistory::get_last_event(ObServerCompactionEvent &event)
{
  int ret = OB_SUCCESS;
  if (size() > 0) {
    if (OB_FAIL(get(get_last_pos(), event))) {
      LOG_WARN("failed to get last event", K(ret), K(get_last_pos()));
    }
  } else {
    event.reset();
  }
  return ret;
}

/*
 * ObServerCompactionEventIterator implement
 * */

int ObServerCompactionEventIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObServerCompactionEventIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) { // sys tenant can get all tenants' info
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
    LOG_WARN("failed to push back tenant_id", K(ret), K(tenant_id));
  }
  for (int i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    if (!is_virtual_tenant_id(all_tenants[i])) { // skip virtual tenant
      MTL_SWITCH(all_tenants[i]) {
        if (OB_FAIL(MTL(ObServerCompactionEventHistory *)->get_list(event_array_))) {
          LOG_WARN("failed to get compaction info", K(ret));
        }
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(all_tenants[i]));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

void ObServerCompactionEventIterator::reset()
{
  event_array_.reset();
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObServerCompactionEventIterator::get_next_info(ObServerCompactionEvent &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= event_array_.count()) {
    ret = OB_ITER_END;
  } else {
    info = event_array_.at(cur_idx_);
    ++cur_idx_;
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
