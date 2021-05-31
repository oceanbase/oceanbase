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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_range_hash_key_getter.h"
#include "share/schema/ob_table_schema.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObRangeHashKeyGetter::get_part_subpart_obj_idxs(int64_t& part_obj_idx, int64_t& subpart_obj_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == repartition_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice table id is invalid", K(ret));
  } else {
    if (OB_SUCC(ret) && repart_columns_.count() > 0) {
      if (repart_columns_.count() > 1) {
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("multi columns partition key is not supported now", K(ret));
      } else {
        part_obj_idx = repart_columns_.at(0).index_;
      }
    } else {
      part_obj_idx = -1;
    }
    if (OB_SUCC(ret) && repart_sub_columns_.count() > 0) {
      if (repart_sub_columns_.count() > 1) {
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("multi columns subpartition key is not supported now", K(ret));
      } else {
        subpart_obj_idx = repart_sub_columns_.at(0).index_;
      }
    } else {
      subpart_obj_idx = -1;
    }
  }
  return ret;
}
/*
int ObRangeHashKeyGetter::get_part_subpart_idx(const ObTableSchema *table_schema,
                                             int64_t slice_idx,
                                             int64_t &part_idx,
                                             int64_t &subpart_idx) const
{
  int ret = OB_SUCCESS;
  if (NULL != table_schema) {
    int64_t part_obj_idx = -1;
    int64_t subpart_obj_idx = -1;
    if (OB_FAIL(get_part_subpart_obj_idxs(part_obj_idx, subpart_obj_idx))) {
      LOG_WARN("fail to get part and subpart obj idxs", K(ret));
    }
    if (part_obj_idx < 0 && subpart_obj_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part or subpart obj idx should not less than 0", K(part_obj_idx), K(subpart_obj_idx), K(ret));
    } else if (part_obj_idx >= 0 && subpart_obj_idx >= 0) {
      part_idx = slice_idx / table_schema->get_sub_part_num();
      subpart_idx = slice_idx % table_schema->get_sub_part_num();
    } else {
      part_idx = (part_obj_idx >= 0) ? slice_idx : -1;
      subpart_idx = (subpart_obj_idx >= 0) ? slice_idx : -1;
    }
  } else {
    part_idx = 0;
    subpart_idx = 0;
  }
  return ret;
}*/
