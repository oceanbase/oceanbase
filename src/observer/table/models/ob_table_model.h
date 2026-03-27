/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_TABLE_MODELS_OB_TABLE_MODEL_H_
#define OCEANBASE_OBSERVER_TABLE_MODELS_OB_TABLE_MODEL_H_

#include "ob_i_model.h"

namespace oceanbase
{
namespace table
{

class ObTableModel : public ObIModel
{
public:
  ObTableModel(){}
  virtual ~ObTableModel() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableModel);
};

} // end of namespace table
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_MODELS_OB_TABLE_MODEL_H_ */
