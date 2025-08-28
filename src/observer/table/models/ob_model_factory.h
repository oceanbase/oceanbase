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

#ifndef OCEANBASE_OBSERVER_TABLE_MODELS_OB_MODEL_FACTORY_H_
#define OCEANBASE_OBSERVER_TABLE_MODELS_OB_MODEL_FACTORY_H_

#include "ob_table_model.h"
#include "ob_hbase_model.h"
#include "ob_redis_model.h"

namespace oceanbase
{   
namespace table
{

class ObModelFactory
{
public:
  static int get_model_guard(common::ObIAllocator &alloc, const ObTableEntityType &type, ObModelGuard &guard)
  {
    int ret = OB_SUCCESS;
    ObIModel *tmp_model = nullptr;
    char *buf = nullptr;

    switch (type) {
      case ObTableEntityType::ET_KV: {
        if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(sizeof(ObTableModel))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "fail to alloc ObTableModel", K(ret), K(sizeof(ObTableModel)));
        } else {
          tmp_model = new (buf) ObTableModel();
        }
        break;
      }
      case ObTableEntityType::ET_HKV: {
        if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(sizeof(ObHBaseModel))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "fail to alloc ObHBaseModel", K(ret), K(sizeof(ObHBaseModel)));
        } else {
          tmp_model = new (buf) ObHBaseModel();
        }
        break;
      }
      case ObTableEntityType::ET_REDIS: {
        if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(sizeof(ObRedisModel))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "fail to alloc ObRedisModel", K(ret), K(sizeof(ObRedisModel)));
        } else {
          tmp_model = new (buf) ObRedisModel();
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "invalid entity type", K(ret), K(type));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      guard.set_allocator(&alloc);
      guard.set_model(tmp_model);
    }

    return ret;
  }
};

} // end of namespace table
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_MODELS_OB_MODEL_FACTORY_H_ */
