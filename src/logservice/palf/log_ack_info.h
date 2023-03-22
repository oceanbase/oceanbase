// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OCEANBASE_LOGSERVICE_LOG_ACK_INFO_
#define OCEANBASE_LOGSERVICE_LOG_ACK_INFO_

#include "common/ob_member.h"
#include "common/ob_learner_list.h"
#include "lib/container/ob_se_array.h"
#include "log_define.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{

struct LsnTsInfo
{
  LSN lsn_;
  int64_t last_ack_time_us_;
  int64_t last_advance_time_us_;
  LsnTsInfo()
    : lsn_(), last_ack_time_us_(OB_INVALID_TIMESTAMP), last_advance_time_us_(OB_INVALID_TIMESTAMP)
  {}
  LsnTsInfo(const LSN &lsn, const int64_t ack_time_us)
    : lsn_(lsn), last_ack_time_us_(ack_time_us), last_advance_time_us_(ack_time_us)
  {}
  bool is_valid() const {
    return (lsn_.is_valid() && OB_INVALID_TIMESTAMP != last_ack_time_us_);
  }
  void reset()
  {
    lsn_.reset();
    last_ack_time_us_ = OB_INVALID_TIMESTAMP;
    last_advance_time_us_ = OB_INVALID_TIMESTAMP;
  }
  void operator=(const LsnTsInfo &val)
  {
    lsn_ = val.lsn_;
    last_ack_time_us_ = val.last_ack_time_us_;
    last_advance_time_us_ = val.last_advance_time_us_;
  }
  TO_STRING_KV(K_(lsn), K_(last_ack_time_us), K_(last_advance_time_us));
};

struct LogMemberAckInfo
{
  LogMemberAckInfo ()
      : member_(),
        last_ack_time_us_(OB_INVALID_TIMESTAMP),
        last_flushed_end_lsn_()
  { }
  LogMemberAckInfo (const common::ObMember &member,
                    const int64_t last_ack_time_us,
                    const LSN &last_flushed_end_lsn)
      : member_(member),
        last_ack_time_us_(last_ack_time_us),
        last_flushed_end_lsn_(last_flushed_end_lsn)
  { }
  common::ObMember member_;
  // 降级时 double check
  int64_t last_ack_time_us_;
  // 升级时double check
  LSN last_flushed_end_lsn_;

  void operator=(const LogMemberAckInfo &ack_info)
  {
    member_ = ack_info.member_;
    last_ack_time_us_ = ack_info.last_ack_time_us_;
    last_flushed_end_lsn_ = ack_info.last_flushed_end_lsn_;
  }

  TO_STRING_KV(K_(member), K_(last_ack_time_us), K_(last_flushed_end_lsn));
};

typedef common::ObSEArray<LogMemberAckInfo, common::OB_MAX_MEMBER_NUMBER> LogMemberAckInfoList;

template<typename T = LogMemberAckInfo>
inline int64_t ack_info_list_get_index(const common::ObSEArray<T, common::OB_MAX_MEMBER_NUMBER> &list_a,
                                       const common::ObAddr &addr)
{
  int64_t index = -1;
  for (int64_t i = 0; i < list_a.count(); ++i) {
    if (list_a.at(i).member_.get_server() == addr) {
      index = i;
      break;
    }
  }
  return index;
}

template<typename T = LogMemberAckInfo>
inline bool ack_info_list_addr_equal(const common::GlobalLearnerList &list_a,
                                     const common::ObSEArray<T, common::OB_MAX_MEMBER_NUMBER>  &list_b)
{
  bool bool_ret = true;
  if (list_a.get_member_number() != list_b.count()) {
    bool_ret = false;
  } else {
    common::ObAddr addr;
    for (int64_t i = 0; i < list_b.count(); ++i) {
      if (!list_a.contains(list_b.at(i).member_.get_server())) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

template<typename T = LogMemberAckInfo>
inline bool ack_info_list_addr_equal(const common::ObSEArray<T, common::OB_MAX_MEMBER_NUMBER>  &list_a,
                                     const common::ObSEArray<T, common::OB_MAX_MEMBER_NUMBER>  &list_b)
{
  bool bool_ret = true;
  if (list_a.count() != list_b.count()) {
    bool_ret = false;
  } else {
    common::ObAddr addr;
    for (int64_t i = 0; i < list_b.count(); ++i) {
      if (-1 == ack_info_list_get_index(list_a, list_b.at(i).member_.get_server())) {
        bool_ret = false;
        break;
      }
    }
  }
  return bool_ret;
}

} // namespace palf
} // namespace oceanbase
#endif // OCEANBASE_LOGSERVICE_LOG_ACK_INFO_
