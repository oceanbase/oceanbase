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

#ifndef OB_SET_OPERATOR_H
#define OB_SET_OPERATOR_H

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
class ObSetOperator : public ObPhyOperator {
public:
  ObSetOperator();
  explicit ObSetOperator(common::ObIAllocator& alloc);
  ~ObSetOperator();
  virtual void reset();
  virtual void reuse();
  virtual int init(int64_t count);

  virtual void set_distinct(bool is_distinct);
  bool is_distinct() const;
  int add_collation_type(common::ObCollationType cs_type);

protected:
  int init_collation_types(int64_t count);

  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief create operator context, only child operator can know it's specific operator type,
   * so must be overwrited by child operator,
   * @param ctx[in], execute context
   * @param op_ctx[out], the pointer of operator context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const = 0;

  virtual int set_child(int32_t child_idx, ObPhyOperator& child_operator) override;
  virtual int create_child_array(int64_t child_op_size) override;
  virtual ObPhyOperator* get_child(int32_t child_idx) const override;
  virtual int32_t get_child_num() const override
  {
    return child_num_;
  }
  virtual int accept(ObPhyOperatorVisitor& visitor) const override;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSetOperator);

protected:
  bool distinct_;
  common::ObFixedArray<common::ObCollationType, common::ObIAllocator> cs_types_;
  int32_t child_num_;
  // Reminder: serialization ofdistinct_/cs_types_/child_num_ are implemented in
  // ObHashSetOperator and ObMergeSetOperator
  // To add more fileds, you need add their serialization of members in these subclasses
  ObPhyOperator** child_array_;
};
inline bool ObSetOperator::is_distinct() const
{
  return distinct_;
}
inline int ObSetOperator::add_collation_type(common::ObCollationType cs_type)
{
  return cs_types_.push_back(cs_type);
}
inline int ObSetOperator::init_collation_types(int64_t count)
{
  return init_array_size<>(cs_types_, count);
}

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OB_SET_OPERATOR_H
