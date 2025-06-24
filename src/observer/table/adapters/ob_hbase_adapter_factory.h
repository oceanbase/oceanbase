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

#ifndef _OB_HBASE_ADAPTER_FACTORY_H
#define _OB_HBASE_ADAPTER_FACTORY_H

#include "ob_i_adapter.h"
#include "ob_hbase_normal_adapter.h"
#include "ob_hbase_series_adapter.h"
#include "observer/table/utils/ob_htable_utils.h"

namespace oceanbase
{
namespace table
{

class ObHbaseAdapterFactory
{
public:
  static int alloc_hbase_adapter(ObIAllocator &alloc, const ObTableExecCtx &exec_ctx, ObIHbaseAdapter *&adapter)
  {
    int ret = OB_SUCCESS;
    ObHbaseModeType mode_type = exec_ctx.get_schema_cache_guard().get_hbase_mode_type();
    if (mode_type == ObHbaseModeType::OB_INVALID_MODE_TYPE) {
      ret = OB_SCHEMA_ERROR;
      SERVER_LOG(WARN, "invalid hbase mode type", K(ret));
    } else {
      if (mode_type == ObHbaseModeType::OB_HBASE_NORMAL_TYPE) {
        if (OB_ISNULL(adapter = OB_NEWx(ObHNormalAdapter, &alloc))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(WARN, "fail to alloc hbase normal adapter", K(ret));
        }
      } else if (mode_type == ObHbaseModeType::OB_HBASE_SERIES_TYPE) {
        if (OB_ISNULL(adapter = OB_NEWx(ObHSeriesAdapter, &alloc))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(WARN, "fail to alloc hbase series adapter", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected hbase mode type", K(ret), K(mode_type));
      }
    }

    return ret;
  }
};

class ObHbaseAdapterGuard
{
public:
  ObHbaseAdapterGuard(ObIAllocator &alloc, const ObTableExecCtx &exec_ctx)
    : allocator_(alloc), exec_ctx_(exec_ctx), hbase_adapter_(nullptr)
  {}
  ~ObHbaseAdapterGuard()
  {
    OB_DELETEx(ObIHbaseAdapter, &allocator_, hbase_adapter_);
  }
  int get_hbase_adapter(ObIHbaseAdapter *&hbase_adapter);
private:
  common::ObIAllocator &allocator_;
  const ObTableExecCtx &exec_ctx_;
  ObIHbaseAdapter *hbase_adapter_;
};


} // end of namespace table
} // end of namespace oceanbase

#endif
