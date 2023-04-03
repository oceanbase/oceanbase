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

#ifndef _SRC_SQL_ENGINE_SEQENCE_OB_SEQUENCE_OP_H
#define _SRC_SQL_ENGINE_SEQENCE_OB_SEQUENCE_OP_H 1
#include "sql/engine/ob_operator.h"
#include "share/sequence/ob_sequence_cache.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{

class ObSequenceSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObSequenceSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(nextval_seq_ids));

  /*
   * 将 nextval sequence id 添加到 ObSequence 中，
   * 每次迭代一行，对这些 id 取 nextval，保存到 session，
   * 供 ObSeqNextvalExpr 读取
   *
   * 注意：为了避免重复计算，每个 id 只能添加一次。
   * 例如：查询 select s.nextval as c1, s.nextval as c2 from dual;
   * 输出的值一定满足 c1 = c2
   */
  int add_uniq_nextval_sequence_id(uint64_t seq_id);
  common::ObFixedArray<uint64_t, common::ObIAllocator> nextval_seq_ids_;
};

class ObSequenceOp : public ObOperator
{
public:
  ObSequenceOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObSequenceOp();

  virtual int inner_get_next_row() override;
  virtual int inner_open() override;
  virtual int inner_close() override;

  void reset()
  {
    sequence_cache_ = NULL;
    seq_schemas_.reset();
  }

  virtual void destroy() override
  {
    sequence_cache_ = NULL;
    seq_schemas_.reset();
    ObOperator::destroy();
  }
private:
  int init_op();
  /**
   * 对于 select、update 语句，sequence 有 child
   * 对于 insert 语句，sequence 没有 child
   * 本函数根据 child 个数决定是否从 child 取下一行
   */
  int try_get_next_row();
private:
  // sequence 暴露给用户层的是一个 cache
  // cache 底层负责做 sequence 的缓存更新以及全局的协调
  share::ObSequenceCache *sequence_cache_;
  // schema 放入 context 中是为了利用它的 cache 能力
  common::ObSEArray<share::schema::ObSequenceSchema, 1> seq_schemas_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _SRC_SQL_ENGINE_SEQENCE_OB_SEQUENCE_OP_H */
