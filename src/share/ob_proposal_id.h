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

#ifndef OCEANBASE_COMMON_OB_PROPOSAL_ID_
#define OCEANBASE_COMMON_OB_PROPOSAL_ID_

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObProposalID
{
public:
  const static int8_t PROPOSAL_ID_VERSION = 4;
  const static int8_t PROPOSAL_ID_VERSION6 = 6;

  ObProposalID() : addr_(), ts_(OB_INVALID_TIMESTAMP) {}
  void reset();
  bool is_valid() const;
  int to_yson(char *buf, const int64_t buf_len, int64_t &pos) const;

  bool operator < (const ObProposalID &pid) const;
  bool operator > (const ObProposalID &pid) const;
  bool operator >= (const ObProposalID &pid) const;
  bool operator <= (const ObProposalID &pid) const;
  void operator = (const ObProposalID &pid);
  bool operator == (const ObProposalID &pid) const;
  bool operator != (const ObProposalID &pid2) const;

  ObAddr addr_;
  int64_t ts_;

  TO_STRING_KV(N_TIME_TO_USEC, ts_, N_SERVER, addr_);
  NEED_SERIALIZE_AND_DESERIALIZE;
};

}//end namespace common
}//end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_PROPOSAL_ID_
