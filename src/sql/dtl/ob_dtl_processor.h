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

#ifndef OB_DTL_PROCESSOR_H
#define OB_DTL_PROCESSOR_H

#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/oblog/ob_log.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl_msg.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "common/row/ob_row_iterator.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlInterruptProc {
public:
  virtual int process(const common::ObInterruptCode& ic) = 0;
};

class ObDtlMsgIterator {
public:
  virtual bool has_next() = 0;
  virtual void set_iterator_end() = 0;
  virtual int get_next_row(common::ObNewRow& row) = 0;
  virtual int get_next_row(const ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx) = 0;
};

class ObDtlPacketProcBase {
public:
  virtual ~ObDtlPacketProcBase() = default;
  virtual ObDtlMsgType get_proc_type() const = 0;
  virtual OB_INLINE void set_iter(ObDtlMsgIterator* iter)
  {
    UNUSED(iter);
  }
  virtual OB_INLINE bool has_iter()
  {
    return false;
  }
  virtual int process(const ObDtlLinkedBuffer& buffer, ObDtlMsgIterator* iter = nullptr) = 0;
};

template <class Packet>
class ObDtlPacketProc : public ObDtlPacketProcBase {
public:
  ObDtlMsgType get_proc_type() const override
  {
    return Packet::type();
  }
  int decode(const ObDtlLinkedBuffer& buffer);
  int process(const ObDtlLinkedBuffer& buffer, ObDtlMsgIterator* iter = nullptr) override;
  virtual int init(const ObDtlLinkedBuffer& buffer)
  {
    UNUSED(buffer);
    return common::OB_SUCCESS;
  }

private:
  virtual int process(const Packet& pkt) = 0;

private:
  Packet pkt_;
};

template <class Packet>
int ObDtlPacketProc<Packet>::decode(const ObDtlLinkedBuffer& buffer)
{
  using common::OB_SUCCESS;
  int ret = OB_SUCCESS;
  const char* buf = buffer.buf();
  int64_t size = buffer.size();
  int64_t& pos = buffer.pos();
  if (OB_FAIL(common::serialization::decode(buf, size, pos, pkt_))) {
    SQL_DTL_LOG(WARN, "decode DTL packet fail", K(size), K(pos));
  } else {
  }
  return ret;
}

template <class Packet>
int ObDtlPacketProc<Packet>::process(const ObDtlLinkedBuffer& buffer, ObDtlMsgIterator* iter)
{
  int ret = common::OB_SUCCESS;
  if (nullptr != iter) {
    set_iter(iter);
    ret = process(pkt_);
  } else {
    if (buffer.pos() == buffer.size()) {
      // last row
      ret = OB_ITER_END;
    } else if (OB_SUCC(decode(buffer))) {
      ret = process(pkt_);
    }
  }
  // need to free memory after packet processing or it might cause memory leak
  // need to reset packet regardless success or failure
  pkt_.reset();
  return ret;
}

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase

#endif /* OB_DTL_PROCESSOR_H */
