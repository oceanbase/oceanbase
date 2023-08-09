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

#ifndef OB_PX_INTERRUPTION_H_
#define OB_PX_INTERRUPTION_H_

#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace sql
{

class ObDfo;
class ObPxTask;
class ObPxSqcMeta;


//在px中由于需要记录两个中断ID，px中的中断id均来自此结构体
struct ObPxInterruptID
{
  OB_UNIS_VERSION(1);
public:
  ObPxInterruptID():query_interrupt_id_(0), px_interrupt_id_(0) {}
  void operator=(const ObPxInterruptID &other)
  {
    query_interrupt_id_ = other.query_interrupt_id_;
    px_interrupt_id_ = other.px_interrupt_id_;
  }
  bool operator==(const ObPxInterruptID &other) const
  {
    return query_interrupt_id_ == other.query_interrupt_id_ &&
           px_interrupt_id_ == other.px_interrupt_id_;
  }
  TO_STRING_KV(K_(query_interrupt_id), K_(px_interrupt_id));
  // 用于sqc以及task向qc发送中断的id,query注册中断时也使用该id
  common::ObInterruptibleTaskID query_interrupt_id_;    
  // 用于qc向所属sqc和tasks发送中断的id，sqc以及tasks注册中断时也使用该id
  common::ObInterruptibleTaskID px_interrupt_id_;  
};

class ObPxInterruptGuard
{
public:
  ObPxInterruptGuard(const common::ObInterruptibleTaskID &interrupt_id_);
  ~ObPxInterruptGuard();
private:
  common::ObInterruptibleTaskID interrupt_id_;
};


class ObInterruptUtil
{
public:
  // QC 向 px 下的所有 SQC 以及Tasks发送中断
  static int broadcast_px(common::ObIArray<sql::ObDfo *> &dfos, int code);
  // QC 向 dfo 下的所有 SQC 以及Tasks发送中断
  static int broadcast_dfo(ObDfo *dfo, int code);
  // SQC 向所有 tasks 发送中断，以催促尽快退出。应对 task 丢 qc 中断的场景
  static int interrupt_tasks(ObPxSqcMeta &sqc, int code);
  // DFO 重试时，需要使用新的中断号，避免遇到中断残余，被误中断
  static int regenerate_interrupt_id(ObDfo &dfo);
  static void update_schema_error_code(ObExecContext *exec_ctx, int &code);
  // SQC 以及 Tasks 向 QC 发送中断
  static int interrupt_qc(ObPxSqcMeta &sqc, int code, ObExecContext *exec_ctx);
  static int interrupt_qc(ObPxTask &task, int code, ObExecContext *exec_ctx);
  // 将server_id、execution_id、qc_id共同组成中断id
  static int generate_query_interrupt_id(const uint32_t server_id,
                                         const uint64_t px_sequence_id,
                                         common::ObInterruptibleTaskID &interrupt_id);
  static int generate_px_interrupt_id(const uint32_t server_id,
                                      const uint32_t qc_id,
                                      const uint64_t px_sequence_id,
                                      const int64_t dfo_id,
                                      common::ObInterruptibleTaskID &interrupt_id);
};

class ObDfoInterruptIdGen
{
public:
  ObDfoInterruptIdGen(const common::ObInterruptibleTaskID &query_interrupt_id,
                      const uint32_t server_id,
                      const uint32_t qc_id,
                      const uint64_t px_sequence_id)
      : query_interrupt_id_(query_interrupt_id),
        server_id_(server_id),
        qc_id_(qc_id),
        px_sequence_id_(px_sequence_id)
  {}
  ~ObDfoInterruptIdGen() = default;
  int gen_id(int64_t dfo_id, ObPxInterruptID &int_id) const
  {
    int_id.query_interrupt_id_ = query_interrupt_id_;
    return ObInterruptUtil::generate_px_interrupt_id(server_id_,
                                                     qc_id_,
                                                     px_sequence_id_,
                                                     dfo_id,
                                                     int_id.px_interrupt_id_);
  }
  static void inc_seqnum(common::ObInterruptibleTaskID &px_interrupt_id);
  uint64_t get_px_sequence_id() const { return px_sequence_id_; }
private:
  const common::ObInterruptibleTaskID &query_interrupt_id_;
  const uint32_t server_id_;
  const uint32_t qc_id_;
  const uint64_t px_sequence_id_;
};

}
}

#endif // OB_PX_INTERRUPTION_H_
