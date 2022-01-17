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

namespace oceanbase {
namespace sql {

class ObSequenceSpec : public ObOpSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObSequenceSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(nextval_seq_ids));

  int add_uniq_nextval_sequence_id(uint64_t seq_id);
  common::ObFixedArray<uint64_t, common::ObIAllocator> nextval_seq_ids_;
};

class ObSequenceOp : public ObOperator {
public:
  ObSequenceOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
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
  bool is_valid();
  int init_op();
  /**
   * @brief overload add_filter to prevent any filter expression add to sequence
   * @param expr[in] any expr
   * @return always return OB_NOT_SUPPORTED
   */
  int add_filter(ObSqlExpression* expr);

  int try_get_next_row();

private:
  share::ObSequenceCache* sequence_cache_;
  common::ObSEArray<share::schema::ObSequenceSchema, 1> seq_schemas_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _SRC_SQL_ENGINE_SEQENCE_OB_SEQUENCE_OP_H */
