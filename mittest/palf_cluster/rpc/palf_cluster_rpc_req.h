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

#ifndef OCEANBASE_PALF_CLUSTER_RPC_REQ_H_
#define OCEANBASE_PALF_CLUSTER_RPC_REQ_H_

#include "lib/utility/ob_unify_serialize.h"                    // OB_UNIS_VERSION
#include "lib/utility/ob_print_utils.h"                        // TO_STRING_KV
#include "common/ob_member_list.h"                             // ObMemberList
#include "logservice/palf/palf_handle_impl.h"                  // PalfStat
#include "share/scn.h"
#include "logservice/palf/log_writer_utils.h"                  // LogWriteBuf

namespace oceanbase
{
namespace palfcluster
{

struct LogCreateReplicaCmd {
  OB_UNIS_VERSION(1);
public:
  LogCreateReplicaCmd();
  LogCreateReplicaCmd(const common::ObAddr &src,
                  const int64_t ls_id,
                  const common::ObMemberList &member_list,
                  const int64_t replica_num,
                  const int64_t leader_idx);
  ~LogCreateReplicaCmd()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(ls_id), K_(member_list), K_(replica_num), K_(leader_idx));
  common::ObAddr src_;
  int64_t ls_id_;
  common::ObMemberList member_list_;
  int64_t replica_num_;
  int64_t leader_idx_;
};

struct SubmitLogCmd {
  OB_UNIS_VERSION(1);
public:
  SubmitLogCmd();
  SubmitLogCmd(const common::ObAddr &src,
               const int64_t ls_id,
               const int64_t client_id,
               const palf::LogWriteBuf &log_buf_);
  ~SubmitLogCmd()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(ls_id));
  common::ObAddr src_;
  int64_t ls_id_;
  int64_t client_id_;
  palf::LogWriteBuf log_buf_;
};

struct SubmitLogCmdResp {
  OB_UNIS_VERSION(1);
public:
  SubmitLogCmdResp();
  SubmitLogCmdResp(const common::ObAddr &src,
                   const int64_t ls_id,
                   const int64_t client_id);
  ~SubmitLogCmdResp()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(ls_id));
  common::ObAddr src_;
  int64_t ls_id_;
  int64_t client_id_;
};

} // end namespace palfcluster
}// end namespace oceanbase

#endif
