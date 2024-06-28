/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX CLIENT

#include "ob_table_load_dml_stat.h"

namespace oceanbase
{
namespace table
{

OB_DEF_SERIALIZE(ObTableLoadDmlStat)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(dml_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < dml_stat_array_.count(); i++) {
    ObOptDmlStat *dml_stat = dml_stat_array_.at(i);
    if (OB_ISNULL(dml_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dml stat is null", KR(ret));
    } else {
      OB_UNIS_ENCODE(*dml_stat);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTableLoadDmlStat)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  reset();
  OB_UNIS_DECODE(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObOptDmlStat *dml_stat = nullptr;
    if (OB_FAIL(allocate_dml_stat(dml_stat))) {
      LOG_WARN("fail to allocate dml stat", KR(ret));
    } else {
      OB_UNIS_DECODE(*dml_stat);
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableLoadDmlStat)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_ADD_LEN(dml_stat_array_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < dml_stat_array_.count(); i++) {
    ObOptDmlStat *dml_stat = dml_stat_array_.at(i);
    if (OB_ISNULL(dml_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dml stat is null", KR(ret));
    } else {
      OB_UNIS_ADD_LEN(*dml_stat);
    }
  }
  return len;
}

} // namespace table
} // namespace oceanbase
