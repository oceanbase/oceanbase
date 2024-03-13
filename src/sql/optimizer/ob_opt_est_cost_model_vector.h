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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_VECTOR_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_VECTOR_
#include "ob_opt_est_cost_model.h"

namespace oceanbase
{
namespace sql
{

class ObOptEstVectorCostModel : public ObOptEstCostModel {

public:
    ObOptEstVectorCostModel(const ObOptCostModelParameter &cost_params,
                            const OptSystemStat &stat)
		:ObOptEstCostModel(cost_params, stat)
	{}
  virtual ~ObOptEstVectorCostModel()=default;
protected:
  virtual int cost_range_scan(const ObCostTableScanInfo &est_cost_info,
                              bool is_scan_index,
                              double row_count,
                              double &cost)override;
};

}
}

#endif  //OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_VECTOR_