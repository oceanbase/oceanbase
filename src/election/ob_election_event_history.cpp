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

#include "common/ob_clock_generator.h"
#include "ob_election_event_history.h"
#include "ob_election_async_log.h"

namespace oceanbase {
using namespace common;
namespace election {
//  caution:
//    the limit of name field is 32 bytes
const char* const ObElectionEventHistory::EVENT_NAME_STR[EVENT_MAX] = {"invalid event",
    "leader takevoer",
    "leader takevoer",
    "leader takevoer",
    "leader takevoer",
    "leader takevoer",
    "leader revoke",
    "leader revoke",
    "leader revoke"};

//  caution:
//    the limit of info field is 32 bytes
const char* const ObElectionEventHistory::EVENT_INFO_STR[EVENT_MAX] = {"invalid event info",
    "assign leader",
    "handle devote succ",
    "renew lease succ",
    "change leader succ",
    "query leader succ",
    "lease expire",
    "not candidate anymore",
    "old leader revoke"};

const char* ObElectionEventHistory::get_event_name_cstr() const
{
  return EVENT_NAME_STR[event_type_];
}

const char* ObElectionEventHistory::get_event_info_cstr() const
{
  return EVENT_INFO_STR[event_type_];
}

void ObElectionEventHistory::reset()
{
  timestamp_ = 0;
  partition_.reset();
  addr_.reset();
  current_leader_.reset();
  event_type_ = EVENT_MAX;
}

void ObElectionEventHistoryArray::destroy()
{
  if (NULL != array_) {
    for (int32_t i = 0; i < OB_ELECT_EVENT_ITEM_NUM; i++) {
      array_[i].~ObElectionEventHistory();
    }
    ob_free(array_);
    array_ = NULL;
  }
  is_inited_ = false;
  idx_ = 0;
  self_.reset();
}

int ObElectionEventHistoryArray::init(const ObAddr& self)
{
  int ret = OB_SUCCESS;
  if (!self.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(self));
  } else if (NULL ==
             (array_ = (ObElectionEventHistory*)ob_malloc(OB_ELECT_EVENT_ITEM_NUM * sizeof(ObElectionEventHistory),
                  ObModIds::OB_ELECTION_VIRTAUL_TABLE_EVENT_HISTORY))) {
    ret = OB_ERR_UNEXPECTED;
    ELECT_ASYNC_LOG(WARN, "alloc memory failed", K(ret));
  } else {
    self_ = self;
    for (int32_t i = 0; i < OB_ELECT_EVENT_ITEM_NUM; i++) {
      new (array_ + i) ObElectionEventHistory();
    }
    is_inited_ = true;
    ELECT_ASYNC_LOG(INFO, "ObElectionEventHistoryArray init success ", K_(self));
  }

  return ret;
}

int ObElectionEventHistoryArray::add_event_history_item(
    const ObPartitionKey& pkey, const ObAddr& current_leader, const EventType etype)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionEventHistoryArray not inited", K(ret));
  } else if (!pkey.is_valid() || !ObElectionEventHistory::is_valid_event_type(etype)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(pkey), K(current_leader), K(etype));
  } else {
    int64_t idx = ATOMIC_LOAD(&idx_);
    int64_t new_idx = (idx + 1) % OB_ELECT_EVENT_ITEM_NUM;
    int64_t old_idx = idx;
    while (old_idx != (idx = ATOMIC_CAS(&idx_, idx, new_idx))) {
      new_idx = (idx + 1) % OB_ELECT_EVENT_ITEM_NUM;
      old_idx = idx;
    }
    ObElectionEventHistory* event_history = &array_[idx];
    event_history->timestamp_ = ObClockGenerator::getClock();
    event_history->partition_ = pkey;
    event_history->addr_ = self_;
    event_history->current_leader_ = current_leader;
    event_history->event_type_ = etype;
  }
  return ret;
}

int ObElectionEventHistoryArray::get_prev_event_history(
    const int64_t now, int32_t& idx, ObElectionEventHistory& event_history) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(WARN, "ObElectionEventHistoryArray not inited", K(ret));
  } else {
    event_history.reset();
    idx = (idx + OB_ELECT_EVENT_ITEM_NUM - 1) % OB_ELECT_EVENT_ITEM_NUM;
    if (array_[idx].get_timestamp() > 0 && array_[idx].get_timestamp() < now) {
      event_history = array_[idx];
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

}  // namespace election
}  // namespace oceanbase
