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

#ifndef Oceanbase_SQL_OPTIMIZER_DEFAULT_STAT_H_
#define Oceanbase_SQL_OPTIMIZER_DEFAULT_STAT_H_

namespace oceanbase {
namespace common {
const double LN_2 = 0.69314718055994530941723212145818;
#define LOGN(x) (log(x))
#define LOG2(x) (LOGN(static_cast<double>(x)) / LN_2)

const double TYPICAL_BLOCK_CACHE_HIT_RATE = 0.6;

const double DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO = 0.5;

const double DEFAULT_CACHE_HIT_RATE = 0.8;

const double VIRTUAL_INDEX_GET_COST = 20;

const int64_t DEFAULT_ROW_SIZE = 200;

const int64_t DEFAULT_MICRO_BLOCK_SIZE = 16L * 1024;

const int64_t DEFAULT_MACRO_BLOCK_SIZE = 2L * 1024 * 1024;

const int64_t OB_EST_DEFAULT_ROW_COUNT = 100000;

const int64_t EST_DEF_COL_NUM_DISTINCT = 100;

const double EST_DEF_COL_NULL_RATIO = 0.01;

const double EST_DEF_COL_NOT_NULL_RATIO = 1 - EST_DEF_COL_NULL_RATIO;

const double EST_DEF_VAR_EQ_SEL = EST_DEF_COL_NOT_NULL_RATIO / EST_DEF_COL_NUM_DISTINCT;

const double OB_COLUMN_DISTINCT_RATIO = 2.0 / 3.0;

const int64_t OB_EST_DEFAULT_NUM_NULL =
    static_cast<int64_t>(static_cast<double>(OB_EST_DEFAULT_ROW_COUNT) * EST_DEF_COL_NULL_RATIO);

const int64_t OB_EST_DEFAULT_DATA_SIZE = OB_EST_DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE;

const int64_t OB_EST_DEFAULT_MACRO_BLOCKS = OB_EST_DEFAULT_DATA_SIZE / DEFAULT_MACRO_BLOCK_SIZE;

const int64_t OB_EST_DEFAULT_MICRO_BLOCKS = OB_EST_DEFAULT_DATA_SIZE / DEFAULT_MICRO_BLOCK_SIZE;

const double OB_DEFAULT_HALF_OPEN_RANGE_SEL = 0.1;

const double OB_DEFAULT_CLOSED_RANGE_SEL = 0.05;

const double DEFAULT_EQ_SEL = 0.005;

const double DEFAULT_INEQ_SEL = 1.0 / 3.0;

const double DEFAULT_SEL = 0.5;

const int64_t UPDATE_ONE_ROW_COST = 1;

const int64_t DELETE_ONE_ROW_COST = 1;

const double DEFAULT_AGG_RANGE = 0.05;

const double DEFAULT_AGG_EQ = 0.01;

const double DEFAULT_CLOB_LIKE_SEL = 0.05;

}  // namespace common
}  // namespace oceanbase

#endif /* Oceanbase_SQL_OPTIMIZER_DEFAULT_STAT_H_ */