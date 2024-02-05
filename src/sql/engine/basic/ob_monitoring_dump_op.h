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

#ifndef SRC_SQL_ENGINE_BASIC_OB_MONITORING_DUMP_OP_H_
#define SRC_SQL_ENGINE_BASIC_OB_MONITORING_DUMP_OP_H_

#include "sql/engine/ob_operator.h"
#include "share/datum/ob_datum.h"

namespace oceanbase
{
namespace sql
{

class ObMonitoringDumpSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObMonitoringDumpSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(flags), K_(dst_op_id));

  uint64_t flags_;
  uint64_t dst_op_id_;

};

class ObMonitoringDumpOp : public ObOperator
{
public:
  ObMonitoringDumpOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override { ObOperator::destroy(); }

  int calc_hash_value();
  int64_t get_monitored_row_count() const
  {
    return rows_;
  }

private:
  common::ObDatum op_name_;
  common::ObDatum tracefile_identifier_;
  uint64_t open_time_;
  uint64_t rows_;
  uint64_t first_row_time_;
  uint64_t last_row_time_;
  bool first_row_fetched_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> output_hash_;
};

}
}

#endif /* SRC_SQL_ENGINE_BASIC_OB_MONITORING_DUMP_OP_H_ */

