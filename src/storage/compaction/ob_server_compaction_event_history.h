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

#ifndef OB_STORAGE_COMPACTION_SERVER_COMPACTION_EVENT_HISTORY_H_
#define OB_STORAGE_COMPACTION_SERVER_COMPACTION_EVENT_HISTORY_H_
#include "ob_compaction_suggestion.h" // for ObInfoRingArray
#include "ob_compaction_diagnose.h" // for ADD_KV

namespace oceanbase
{
namespace compaction
{

struct ObServerCompactionEvent
{
public:
  enum ObCompactionEvent : uint8_t
  {
    RECEIVE_BROADCAST_SCN = 0,
    GET_FREEZE_INFO,
    WEAK_READ_TS_READY,
    SCHEDULER_LOOP,
    TABLET_COMPACTION_FINISHED,
    COMPACTION_FINISH_CHECK,
    COMPACTION_REPORT,
    RS_REPAPRE_UNFINISH_TABLE_IDS,
    RS_FINISH_CUR_LOOP,
    COMPACTION_EVENT_MAX,
  };
  static const char *get_comp_event_str(enum ObCompactionEvent event);
  enum ObCompactionRole : uint8_t
  {
    TENANT_RS = 0,
    STORAGE,
    COMPACTION_ROLE_MAX
  };
  static const char *get_comp_role_str(enum ObCompactionRole role);
public:
  ObServerCompactionEvent()
   : tenant_id_(OB_INVALID_TENANT_ID),
     merge_type_(INVALID_MERGE_TYPE),
     compaction_scn_(0),
     event_(COMPACTION_EVENT_MAX),
     timestamp_(0),
     comment_("\0")
  {
  }
  ~ObServerCompactionEvent() { reset(); }
  OB_INLINE void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    merge_type_ = INVALID_MERGE_TYPE;
    compaction_scn_ = 0;
    event_ = COMPACTION_EVENT_MAX;
    timestamp_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && compaction_scn_ > 0
        && COMPACTION_EVENT_MAX != event_
        && timestamp_ > 0;
  }
  int generate_event_str(char *buf, const int64_t buf_len) const;
  TO_STRING_KV(K_(tenant_id), "merge_type", merge_type_to_str(merge_type_), K_(compaction_scn),
      "event", get_comp_event_str(event_), "role", get_comp_role_str(role_),
      K_(timestamp), K_(comment));

  int64_t tenant_id_;
  ObMergeType merge_type_;
  int64_t compaction_scn_;
  ObCompactionEvent event_;
  ObCompactionRole role_;
  uint64_t timestamp_;
  char comment_[common::OB_MERGE_COMMENT_INNER_STR_LENGTH];
};

class ObServerCompactionEventHistory : public ObInfoRingArray<ObServerCompactionEvent> {
public:
  static const int64_t SERVER_EVENT_MAX_CNT = 500;

  ObServerCompactionEventHistory()
  : ObInfoRingArray(allocator_)
  {
    allocator_.set_attr(SET_USE_500("CompEventMgr"));
  }
  ~ObServerCompactionEventHistory() {}
  static int mtl_init(ObServerCompactionEventHistory* &event_history);
  int init();
  void destroy();

  int add_event(const ObServerCompactionEvent &event);
  int get_last_event(ObServerCompactionEvent &event);

private:
  ObArenaAllocator allocator_;
};

/*
 * ObServerCompactionEventIterator
 * */

class ObServerCompactionEventIterator
{
public:
  ObServerCompactionEventIterator()
   : event_array_(),
     cur_idx_(0),
     is_opened_(false)
  {
  }
  virtual ~ObServerCompactionEventIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObServerCompactionEvent &info);
  void reset();

private:
  ObArray<ObServerCompactionEvent> event_array_;
  int64_t cur_idx_;
  bool is_opened_;
};

#define ADD_COMPACTION_EVENT(compaction_scn, event, timestamp, ...) \
PUSH_COMPACTION_EVENT(MTL_ID(), MAJOR_MERGE, compaction_scn, event, ObServerCompactionEvent::STORAGE, timestamp, __VA_ARGS__)

#define ADD_RS_COMPACTION_EVENT(compaction_scn, event, timestamp, ...) \
PUSH_COMPACTION_EVENT(MTL_ID(), MAJOR_MERGE, compaction_scn, event, ObServerCompactionEvent::TENANT_RS, timestamp, __VA_ARGS__)

#define DEFINE_COMPACTION_EVENT_PRINT_KV(n)                                    \
  template <LOG_TYPENAME_TN##n>                                                \
  int PUSH_COMPACTION_EVENT(                                                   \
      const int64_t tenant_id, const compaction::ObMergeType merge_type,       \
      const int64_t compaction_scn,                                            \
      const ObServerCompactionEvent::ObCompactionEvent event,                  \
      const ObServerCompactionEvent::ObCompactionRole role,                    \
      const int64_t timestamp, LOG_PARAMETER_KV##n) {                          \
    int64_t __pos = 0;                                                         \
    int ret = OB_SUCCESS;                                                      \
    compaction::ObServerCompactionEvent event_item;                            \
    event_item.tenant_id_ = tenant_id;                                         \
    event_item.merge_type_ = merge_type;                                       \
    event_item.compaction_scn_ = compaction_scn;                               \
    event_item.event_ = event;                                                 \
    event_item.role_ = role;                                                   \
    event_item.timestamp_ = timestamp;                                         \
    char *buf = event_item.comment_;                                           \
    const int64_t buf_size = ::oceanbase::common::OB_DIAGNOSE_INFO_LENGTH;     \
    SIMPLE_TO_STRING_##n if (OB_FAIL(MTL(ObServerCompactionEventHistory *)     \
                                         ->add_event(event_item))) {           \
      STORAGE_LOG(WARN, "failed to add event", K(ret), K(event_item));         \
    }                                                                          \
    else {                                                                     \
      STORAGE_LOG(DEBUG, "success to add event", K(ret), K(event_item));       \
    }                                                                          \
    return ret;                                                                \
  }

DEFINE_COMPACTION_EVENT_PRINT_KV(1)
DEFINE_COMPACTION_EVENT_PRINT_KV(2)
DEFINE_COMPACTION_EVENT_PRINT_KV(3)
DEFINE_COMPACTION_EVENT_PRINT_KV(4)
DEFINE_COMPACTION_EVENT_PRINT_KV(5)
DEFINE_COMPACTION_EVENT_PRINT_KV(6)
DEFINE_COMPACTION_EVENT_PRINT_KV(7)


} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SERVER_COMPACTION_EVENT_HISTORY_H_
