/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
};

}
}

#endif  //OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_VECTOR_