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

#define USING_LOG_PREFIX SQL_DTL

#include "ob_dtl_task.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

OB_SERIALIZE_MEMBER(ObDtlChannelInfo, chid_, type_, peer_, role_, tenant_id_);
OB_SERIALIZE_MEMBER(ObDtlChSet, exec_addr_, ch_info_set_);
OB_SERIALIZE_MEMBER(ObDtlTask, jobid_, taskid_, cis_, chans_cnt_);
OB_SERIALIZE_MEMBER(ObDtlExecServer, total_task_cnt_, exec_addrs_, prefix_task_counts_);
OB_SERIALIZE_MEMBER(ObDtlChTotalInfo, start_channel_id_, transmit_exec_server_,
                    receive_exec_server_, channel_count_, tenant_id_, is_local_shuffle_);


int ObDtlChSet::add_channel_info(const ObDtlChannelInfo &info)
{
  int ret = OB_SUCCESS;
  if (ch_info_set_.count() >= MAX_CHANS) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("chan set full", "count", ch_info_set_.count(), K(ret));
  } else if (OB_FAIL(ch_info_set_.push_back(info))) {
    LOG_WARN("fail push back channel info", K(info), K(ret));
  }
  return ret;
}

int ObDtlChSet::get_channel_info(int64_t chan_idx, ObDtlChannelInfo &ci) const
{
  int ret = OB_SUCCESS;
  if (chan_idx < 0 || chan_idx >= ch_info_set_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(chan_idx), "count", ch_info_set_.count());
  } else {
    ci = ch_info_set_.at(chan_idx);
  }
  return ret;
}

int ObDtlChSet::assign(const ObDtlChSet &other)
{
  int ret = OB_SUCCESS;
  exec_addr_ = other.exec_addr_;
  ch_info_set_.reuse();
  if (0 < other.ch_info_set_.count()) {
    if (OB_FAIL(ch_info_set_.prepare_allocate(other.ch_info_set_.count()))) {
      LOG_WARN("failed to prepare alloc", K(ret));
    } else {
      for (int64_t i = 0; i < other.ch_info_set_.count(); ++i) {
        ch_info_set_.at(i) = other.ch_info_set_.at(i);
      }
    }
  }
  return ret;
}

int ObDtlExecServer::assign(const ObDtlExecServer &other)
{
  int ret = OB_SUCCESS;
  total_task_cnt_ = other.total_task_cnt_;
  if (0 < other.exec_addrs_.count()) {
    OZ(exec_addrs_.prepare_allocate(other.exec_addrs_.count()));
    for (int64_t i = 0; i < other.exec_addrs_.count() && OB_SUCC(ret); ++i) {
      exec_addrs_.at(i) = other.exec_addrs_.at(i);
    }
  }
  if (0 < other.prefix_task_counts_.count()) {
    OZ(prefix_task_counts_.prepare_allocate(other.prefix_task_counts_.count()));
    for (int64_t i = 0; i < other.prefix_task_counts_.count() && OB_SUCC(ret); ++i) {
      prefix_task_counts_.at(i) = other.prefix_task_counts_.at(i);
    }
  }
  return ret;
}

int ObDtlChTotalInfo::assign(const ObDtlChTotalInfo &other)
{
  int ret = OB_SUCCESS;
  start_channel_id_ = other.start_channel_id_;
  channel_count_ = other.channel_count_;
  tenant_id_ = other.tenant_id_;
  OZ(transmit_exec_server_.assign(other.transmit_exec_server_));
  OZ(receive_exec_server_.assign(other.receive_exec_server_));
  is_local_shuffle_ = other.is_local_shuffle_;
  return ret;
}

int ObDtlExecServer::add_exec_addr(const common::ObAddr &exec_addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exec_addrs_.push_back(exec_addr))) {
    LOG_WARN("fail push back exec addr", K(exec_addr), K(ret));
  }
  return ret;
}

// int ObDtlTask::link_chans()
// {
//   int ret = OB_SUCCESS;
//   int i = 0;
//   for (i = 0; OB_SUCC(ret) && i < chans_cnt_; i++) {
//     auto &ci = cis_[i];
//     const auto chid = ci.chid_;
//     auto &chan = chans_[i];
//     if (ci.type_ == DTL_CT_LOCAL) {
//       if (ci.role_ == DTL_CR_PUSHER) {
//         // if (OB_FAIL(DTL.create_local_channel(ci.chid_, chan))) {
//         //   LOG_WARN("create local channel fail", K(i), K(ret));
//         // }
//       } else {
//         if (OB_FAIL(DTL.get_channel(ci.chid_, chan))) {
//           LOG_WARN("get channel with ID fail", KP(chid), K(ret));
//           break;
//         } else {
//           chan->pin();
//         }
//       }
//     } else if (ci.role_ == DTL_CR_PUSHER) {
//       if (OB_FAIL(DTL.create_rpc_channel(ci.chid_, chan))) {
//         LOG_WARN("create rpc channel fail", K(i), K(ret));
//       }
//     } else {
//       auto proxy = DTL.get_rpc_proxy().to(GCTX.self_addr());
//       obrpc::ObDtlRpcChanArgs args;
//       args.chid_ = ci.chid_;
//       args.peer_ = ci.peer_;
//       if (OB_FAIL(proxy.create_channel(args))) {
//         LOG_WARN("create rpc channel fail", K(i), K(ret));
//       }
//     }
//   }
//   if (OB_FAIL(ret)) {
//     unlink_chans();
//   }
//   return ret;
// }

// // int ObDtlTask::link_chans()
// // {
// //   int ret = OB_SUCCESS;
// //   for (int i = 0; OB_SUCC(ret) && i < chans_cnt_; i++) {
// //     auto &ci = cis_[i];
// //     const auto chid = ci.chid_;
// //     auto &chan = chans_[i];
// //     if (OB_FAIL(DTL.get_channel(ci.chid_, chan))) {
// //       LOG_WARN("get channel with ID fail", KP(chid), K(ret));
// //       break;
// //     } else {
// //       chan->pin();
// //     }
// //   }
// //   if (OB_FAIL(ret)) {
// //     unlink_chans();
// //   }
// //   return ret;
// // }

// void ObDtlTask::unlink_chans()
// {
//   int ret = OB_SUCCESS;
//   for (int i = 0; i < chans_cnt_; i++) {
//     auto &ci = cis_[i];
//     auto &chan = chans_[i];
//     if (nullptr != chan) {
//       if (!(ci.type_ == DTL_CT_LOCAL && ci.role_ == DTL_CR_PUSHER)) {
//         if (OB_FAIL(DTL.destroy_channel(ci.chid_))) {
//           LOG_WARN("destroy channel fail", K(ret));
//         }
//       } else {
//         DTL.release_channel(chan);
//       }
//       chan = nullptr;
//     }
//   }
// }


}  // dtl
}  // sql
}  // oceanbase
