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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_ID_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_ID_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_member_list.h"
#include "common/ob_partition_key.h"

namespace oceanbase {

namespace election {
typedef std::pair<int64_t, int64_t> lease_t;

class ObElectionGroupId {
  OB_UNIS_VERSION(1);

public:
  ObElectionGroupId() : server_(), create_time_(common::OB_INVALID_TIMESTAMP), hash_val_(0)
  {}
  ~ObElectionGroupId()
  {}
  int init(const common::ObAddr& server, const int64_t create_time);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  ObElectionGroupId& operator=(const ObElectionGroupId& other);
  int compare(const ObElectionGroupId& other) const;
  bool operator==(const ObElectionGroupId& other) const
  {
    return 0 == compare(other);
  }
  bool operator!=(const ObElectionGroupId& other) const
  {
    return !operator==(other);
  }
  bool operator<(const ObElectionGroupId& other) const
  {
    return -1 == compare(other);
  }
  const common::ObAddr& get_server() const
  {
    return server_;
  }
  int64_t get_create_time() const
  {
    return create_time_;
  }

  TO_STRING_AND_YSON(OB_ID(eg_id_hash), hash_val_, Y_(server), Y_(create_time));

private:
  uint64_t cal_hash_() const;

private:
  common::ObAddr server_;
  int64_t create_time_;
  uint64_t hash_val_;
};

struct PartState {
  int64_t lease_end_;
  int64_t takeover_t1_timestamp_;
  int32_t vote_cnt_;

  PartState()
  {
    reset();
  }
  PartState(const int64_t lease_end, const int64_t takeover_t1_timestamp)
      : lease_end_(lease_end), takeover_t1_timestamp_(takeover_t1_timestamp), vote_cnt_(0)
  {}
  void reset()
  {
    lease_end_ = -1;
    takeover_t1_timestamp_ = -1;
    vote_cnt_ = 0;
  }
  TO_STRING_KV(K_(lease_end), K_(takeover_t1_timestamp), K_(vote_cnt));
};

typedef common::ObSEArray<PartState, 16> ObPartStateArray;
typedef common::ObSEArray<int64_t, 16> ObPartIdxArray;

}  // namespace election
}  // namespace oceanbase
#endif  // OCEANBASE_ELECTION_OB_ELECTION_GROUP_ID_
