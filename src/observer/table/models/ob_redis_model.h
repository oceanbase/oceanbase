/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_MODELS_OB_REDIS_MODEL_H_
#define OCEANBASE_OBSERVER_TABLE_MODELS_OB_REDIS_MODEL_H_

#include "ob_i_model.h"

namespace oceanbase
{   
namespace table
{

class ObRedisModel : public ObIModel
{
public:
  ObRedisModel() {}
  virtual ~ObRedisModel() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRedisModel);
};

} // end of namespace table
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_MODELS_OB_REDIS_MODEL_H_ */
