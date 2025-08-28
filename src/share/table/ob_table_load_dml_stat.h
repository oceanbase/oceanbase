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

#pragma once

#include "share/stat/ob_stat_define.h"

namespace oceanbase
{
namespace common
{
class ObOptDmlStat;
} // namespace common
namespace table
{
struct ObTableLoadDmlStat
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadDmlStat() : allocator_("TLD_Dmlstat")
  {
    dml_stat_array_.set_tenant_id(MTL_ID());
    allocator_.set_tenant_id(MTL_ID());
  }
  ~ObTableLoadDmlStat() { reset(); }
  void reset()
  {
    for (int64_t i = 0; i < dml_stat_array_.count(); ++i) {
      ObOptDmlStat *col_stat = dml_stat_array_.at(i);
      if (col_stat != nullptr) {
        col_stat->~ObOptDmlStat();
      }
    }
    dml_stat_array_.reset();
    allocator_.reset();
  }
  bool is_empty() const { return dml_stat_array_.empty(); }
  int allocate_dml_stat(ObOptDmlStat *&dml_stat)
  {
    int ret = OB_SUCCESS;
    ObOptDmlStat *new_dml_stat = OB_NEWx(ObOptDmlStat, (&allocator_));
    if (OB_ISNULL(new_dml_stat)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate buffer", KR(ret));
    } else if (OB_FAIL(dml_stat_array_.push_back(new_dml_stat))) {
      OB_LOG(WARN, "fail to push back", KR(ret));
    } else {
      dml_stat = new_dml_stat;
    }
    if (OB_FAIL(ret)) {
      if (new_dml_stat != nullptr) {
        new_dml_stat->~ObOptDmlStat();
        allocator_.free(new_dml_stat);
        new_dml_stat = nullptr;
      }
    }
    return ret;
  }
  int merge(const ObTableLoadDmlStat &other)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.dml_stat_array_.count(); i++) {
      ObOptDmlStat *dml_stat = other.dml_stat_array_.at(i);
      ObOptDmlStat *new_dml_stat = nullptr;
      if (OB_ISNULL(dml_stat)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected dml stat is null", KR(ret));
      } else if (OB_FAIL(allocate_dml_stat(new_dml_stat))) {
        OB_LOG(WARN, "fail to allocate dml stat", KR(ret));
      } else {
        *new_dml_stat = *dml_stat;
      }
    }    
    return ret;
  }
  TO_STRING_KV(K_(dml_stat_array));
public:
  common::ObArray<ObOptDmlStat *> dml_stat_array_;
  common::ObArenaAllocator allocator_;
};

} // namespace table
} // namespace oceanbase
