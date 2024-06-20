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

namespace oceanbase
{
namespace common
{
const double LN_2 = 0.69314718055994530941723212145818;
#define LOGN(x) (log(x))
#define LOG2(x) (LOGN(static_cast<double>(x)) / LN_2)

const double DEFAULT_CACHE_HIT_RATE = 0.8;

const int64_t DEFAULT_TABLE_ROW_COUNT = 1;

const int64_t DEFAULT_ROW_SIZE = 200;

const int64_t DEFAULT_COLUMN_SIZE = 10;

const int64_t DEFAULT_MICRO_BLOCK_SIZE = 16L * 1024;

const int64_t DEFAULT_MACRO_BLOCK_SIZE_MB = 2L;

const int64_t DEFAULT_MACRO_BLOCK_SIZE = DEFAULT_MACRO_BLOCK_SIZE_MB * 1024 * 1024;

const int64_t OB_EST_DEFAULT_VIRTUAL_TABLE_ROW_COUNT = 100000;

const double EST_DEF_COL_NULL_RATIO = 0.01;

const double EST_DEF_COL_NOT_NULL_RATIO = 1 - EST_DEF_COL_NULL_RATIO;

const double EST_DEF_VAR_EQ_SEL = EST_DEF_COL_NOT_NULL_RATIO;

const int64_t OB_EST_DEFAULT_DATA_SIZE = DEFAULT_ROW_SIZE;

const double OB_DEFAULT_HALF_OPEN_RANGE_SEL = 0.1;

const double OB_DEFAULT_CLOSED_RANGE_SEL = 0.05;

/**
 *@brief  等值表达式如("A = b")的默认选择率
 */
const double DEFAULT_EQ_SEL = 0.005;

/**
 *@brief　非等值表达式(如"A < b")的默认选择率
 */
const double DEFAULT_INEQ_SEL = 1.0 / 3.0;

/**
 *@brief　空间表达式的默认选择率: 1 / OB_GEO_S2REGION_OPTION_MAX_CELL
 */
const double DEFAULT_SPATIAL_SEL = 0.25;

/**
 *@brief　猜都没办法猜的默认选择率：一半一半
 */
const double DEFAULT_SEL = 0.5;

// [agg(expr) <|>|btw cosnt]的默认选择率，参考oracle
const double DEFAULT_AGG_RANGE = 0.05;
// [aggr(expr) = const]的默认选择率，参考oracle
const double DEFAULT_AGG_EQ = 0.01;
// clob/blob like "xxx" 的默认选择率
const double DEFAULT_CLOB_LIKE_SEL = 0.05;
const double DEFAULT_ANTI_JOIN_SEL = 0.01;
// 范围谓词越界部分选择率，参考 SQLserver
const double DEFAULT_OUT_OF_BOUNDS_SEL = 0.3;
const double DEFAULT_INEQ_JOIN_SEL = 0.05;

} // namespace common
} // namespace oceanabse






#endif /* Oceanbase_SQL_OPTIMIZER_DEFAULT_STAT_H_ */
