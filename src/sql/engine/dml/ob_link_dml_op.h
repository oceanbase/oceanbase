/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_LINK_DML_OP_H_
#define OCEANBASE_SQL_ENGINE_LINK_DML_OP_H_

#include "sql/engine/dml/ob_link_op.h"

namespace oceanbase
{
namespace sql
{

class ObLinkDmlSpec : public ObLinkSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObLinkDmlSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObLinkDmlOp : public ObLinkOp
{
public:
  explicit ObLinkDmlOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObLinkDmlOp() { destroy(); }

  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;

  virtual void reset();
  int inner_execute_link_stmt(const char *link_stmt);
  int send_reverse_link_info(transaction::ObTransID &tx_id);

private:
  virtual void reset_dblink() override;
private:
  int64_t affected_rows_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_LINK_DML_ */
