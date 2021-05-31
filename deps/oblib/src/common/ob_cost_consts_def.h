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

#ifndef _OB_COST_CONSTS_DEF_H_
#define _OB_COST_CONSTS_DEF_H_ 1

namespace oceanbase {
namespace common {

const double COST_PARAM_INVALID = -1;

/**
 * Calculate the cpu cost of a basic operator
 */
const int64_t CPU_OPERATOR_COST = 1;

const int64_t NO_ROW_FAKE_COST = 100;
/// The basic cost of a rpc
const int64_t NET_BASE_COST = 100;
/// The basic cost of rpc sending a row of data
const double NET_PER_ROW_COST = 0.4;

/// The cost of updating a row of data. Temporarily written
const int64_t UPDATE_ONE_ROW_COST = 1;
/// The cost of DELETE a row of data. Temporarily written
const int64_t DELETE_ONE_ROW_COST = 1;
/**
 * The cpu cost of processing a row of data
 */
const int64_t CPU_TUPLE_COST = 1;

/**
 * The default selection rate of equivalent expressions such as ("A = b")
 */
const double DEFAULT_EQ_SEL = 0.005;

/**
 * The default selection rate for non-equivalent expressions (such as "A <b")
 */
const double DEFAULT_INEQ_SEL = 1.0 / 3.0;

/**
 * The default selection rate that can't be guessed: half and half
 */
const double DEFAULT_SEL = 0.5;

/**
 * The default distinct number
 */
const int64_t DEFAULT_DISTINCT = 10;

// default sizes and counts when stat is not available
const int64_t DEFAULT_ROW_SIZE = 100;
const int64_t DEFAULT_MICRO_BLOCK_SIZE = 16L * 1024;
const int64_t DEFAULT_MACRO_BLOCK_SIZE = 2L * 1024 * 1024;
// Oracle give range that exceeds limit a additional selectivity
// For get, it starts with 1 / DISTINCT at boarder, linearly decrease across another limit range.
// For scan, the probability of get row decrease with the same formula.

/*
 *                     -------------------
 *                    /|                 | \
 *                   / |                 |x \
 *                  /  |        A        |xx|\
 *                 /   |                 |xx|y\
 *                ------------------------------
 *          min - r   min                max a   max + r
 * */
// We can control the decreasing range r by changine DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO
// r = (max - min) * DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO
// For scan, assuming even distribution, area A represents probability of row falling in range (min,max), which is 1.0
// if we have range i > a, then its probability is area of y
// if we have range i < a, then its probability is A plus x
// the logic is in ObOptimizerCost::do_calc_range_selectivity

const double DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO = 0.5;

const double DEFAULT_GET_EXIST_RATE = 1.0;
const double DEFAULT_CACHE_HIT_RATE = 0.8;
// quoted from qushan
const double TYPICAL_BLOCK_CACHE_HIT_RATE = 0.6;
const double TYPICAL_ROW_CACHE_HIT_RATE = 0.3;

const double LN_2 = 0.69314718055994530941723212145818;
#define LOGN(x) (log(x))
#define LOG2(x) (LOGN(static_cast<double>(x)) / LN_2)

/**************************************
 * Note on cost model of sstable access
 ***************************************/
const double STANDARD_SCHEMA_AVG_ROWSZ = 1127;
const double STANDARD_SCHEMA_COL_COUNT = 50;
const double STANDARD_SCHEMA_ROW_PER_MICRO = DEFAULT_MICRO_BLOCK_SIZE / STANDARD_SCHEMA_AVG_ROWSZ;
/*
Note:
In order to simplify the model, and make it more decoupled from implementation detail,
this version of cost model use a different approach to model the performance of storage layer.
The cost model no longer try to simulate the full process of table scan/get. By contrast, it
consider cost of each 'path' of storage access to be (basically) linear.  Here by 'paths', we
mean the different places a row resides before it is accessed. After calculating costs of each
path, we combine them together with probabilities of each path.

The following are the diverging points and main differences of different paths.

Paths of GET (4 in total):
A\Row cache.
  A rowkey is first checked against row cache. The corresponding row will be read from row cache
  if row cache hits.
B\Not exist.
  If path A is not taken, the rowkey will be checked against bloomfilter cache (BF). If BF says the
  row is non-existent, just make a 'non-existent row' and all done for this rowkey.
C\Block cache.
  If path A and B not taken, it means the specified rowkey (may) resides in block. To determine the
  block to check, we need the info of Micro Block Index. Micro Block Index resides on disk, but the
  Micro Block Index Cache is designed to be constantly in memory, and will cause severe performance
  penalty if the cache is washed. After Micro Block Index tells us which micro block(s) to look for
  the rowkey, we access Micro Block Cache to see whether the block is in memory. After obtaining the
  block, we decompress it and read the row or announce the non-existence of the row. Lastly, according
  to the existency of the row, Row Cache or Bloomfilter will be updated.
D\IO.
  If Block Cache misses, a IO request is inevitable. Additional steps of this path is basically a io
  request and a second  access to Block Cache (filling of Block Cache is done in io callback).

Paths of SCAN(2 in total):
A\Block cache
  If Block Cache hits, the block is fetched from Block Cache and decompressed then scanned.
B\IO
  If Block Cache misses, an additional IO request and an additional Block Cache access is performed.
  (filling of Block Cache is done in io callback)

Effect of prefetching:
All IO are basically done via prefetching. Prefetching make io and cpu activity act like a pipline.
We may get 'free' IO (IO without waiting) if this IO piplining works well.
A\Get
  Prefetching of get is done in a per-row basis. During opening, each rowkey will trigger an IO
  request to be sent. IO Merging is an optimization : if two rowkeys resides in the same microblock,
  only one IO will be sent.
B\Scan
  Prefetching of get is done in a per-macroblock basis. During opening a new macroblock, all microblocks
  in this macroblock that needs to be accessed is prefetched.

Basic assumption of sstable cost model:
A\Table schema used in test
  All params are measured on a standard table, which consists of bigint, varchar(32), double, timestamp
  and number, 10 columns of each type, 50 columns in total. I believe this is a common schema in our
  corporation's use case (tens of columns, mixed types).
B\Column type not considered in the model.
  Mainly for simplicity. If upper layer has such need as choosing from two indexes of different type,
  column type may be added to the model.
  BTW: varchar of different length is also tested, no difference is noticed in deserializing time.
C\Block Index Cache always hit.
  Block Index is designed to be always in memory. If not, there is a bug, nothing to do with model.
D\Bloom Filter not considered(considered always hit).
  Bloom Filter is a pure overhead in select. Select operation is considered to only select rows that
  exist.
E\Not exist rows not considered
  In our use cases, get requests usually hit an existent row. During index back, all rows got are
  existent. In other cases, requests usually follow a 'insert-update-select-update-select-....-delete'
  pattern. Only ad-hoc queries by human user will frequently hit a non-existent row.
  This path can be added if needed.

*/
/**************************************
 * Cost params for table scan
 ***************************************/
/*
Tscan_row_cost = Nrow * (SCAN_ROW_COST_T + SCAN_ROW_COST_SIZE_RELATED_T * table_width_factor)
Tscan_col_cost = Nrow * Ncol * SCAN_COL_COST_T
Tscan_cpu = SCAN_BASE_T + Tscan_row_cost + Tscan_col_cost
Tscan_io = Nrow * SCAN_IO_WAIT_PER_ROW_T * table_width_factor
         - Nrow * (SCAN_IO_ROW_PIPLINE_FACTOR_T + Ncol * SCAN_IO_COL_PIPLINE_FACTOR_T)
Tscan_io = Tscan_io > 0 ? Tscan_io : 0
Tscan = Tscan_cpu + Tscan_io

Explaination:
  Items in the formula:
    SCAN_BASE_T: Fixed cost for every scan request
    SCAN_ROW_COST_T: The portion of cpu cost for each row that is irrelevant to table width. This
      include operations like processing each output row, etc, which is irrelevant to  neither
      table width nor column count.
    SCAN_ROW_COST_SIZE_RELATED_T: The portion of cpu cost for each row that is relative to table
      width. This include operations like processing micro/macro block, block index, etc.
    table_width_factor: The relative row width to the standard row width, which is 1157 bytes. If a
      row's width is 500 bytes, table_width_factor would be 500 / 1157 = 0.43. table_width_factor
      affects performance by affecting micro/macro blocks needed to be processed, etc.
    SCAN_COL_COST_T: Portion of cpu cost for each column
    SCAN_IO_WAIT_PER_ROW_T: IO cost per row
    SCAN_IO_ROW_PIPLINE_FACTOR_T: The portion of foreground cpu time while AIO is on the fly that
      is relative to row count.
    SCAN_IO_COL_PIPLINE_FACTOR_T: The portion of foreground cpu time while AIO is on the fly that
      is relative to column count.

  Observations show that total io wait is basically linear to row count.
  Cost of scan consists of two part: IO cost and cpu cost. Tscan_cpu is common between io path and
  block cache path, while the block cache path has no IO cost.
*/
/*
Scan cache fit result
[[Fit Statistics]]
# function evals   = 25
# data points      = 570
# variables        = 3
chi-square         = 108583019589.568
reduced chi-square = 191504443.721
[[Variables]]
Tper_row:                0.29672148 +/- 0.005366 (1.81%) (init= 1)
Tper_row_size_related:   0.17095158 +/- 0.002221 (1.30%) (init= 1)
Tper_col:                0.03680852 +/- 9.92e-05 (0.27%) (init= 1)

Scan IO fit result
[[Fit Statistics]]
# function evals   = 35
# data points      = 570
# variables        = 6
chi-square         = 384204896905.762
reduced chi-square = 681214356.216
[[Variables]]
Tper_col:                  0.03375619 +/- 0.000613 (1.82%) (init= 0.1)
Tper_row_pipline_factor:   0.41046073 +/- 0.027885 (6.79%) (init= 0.1)
Tper_row:                  0.26501238 +/- 0.024792 (9.36%) (init= 0.1)
Tper_row_size_related:     0.40146272 +/- 0.017351 (4.32%) (init= 0.1)
Tper_row_io_wait:          0.88876127 +/- 0.018297 (2.06%) (init= 1)
Tper_col_pipline_factor:   0.02989413 +/- 0.000667 (2.23%) (init= 0.05)
*/
const double SCAN_BC_BASE_T = 25.0;
const double SCAN_BC_ROW_COST_T = 0.29672148;
const double SCAN_BC_ROW_COST_SIZE_RELATED_T = 0.17095158;
const double SCAN_BC_COL_COST_T = 0.03680852;

const double SCAN_IO_BASE_T = 503.0;
const double SCAN_IO_ROW_COST_T = 0.26501238;
const double SCAN_IO_ROW_COST_SIZE_RELATED_T = 0.40146272;
const double SCAN_IO_COL_COST_T = 0.03375619;
// technically, io wait is basically linear to microblock count
const double SCAN_IO_WAIT_PER_ROW_T = 0.88876127;
const double SCAN_IO_ROW_PIPLINE_FACTOR_T = 0.41046073;
const double SCAN_IO_COL_PIPLINE_FACTOR_T = 0.02989413;

/**************************************
 * Cost params for table get
 ***************************************/
/*
Tget = GET_BASE_T + Nrow * (Tper_row + Ncol * Tper_col)
Explaination:
  Items in the formula:
    GET_BASE_T: Fixed cost for every get request.
    Tper_row: The portion of cost related to row count.
    Tper_col: The portion of cost related to column count.

  According to observation, io wait time of getting is not as much affected by column count as
  that of scanning. This is partly because between consuming two prefetched microblocks, only
  1 row is processed in foreground, while scanning processes all rows in a microblock. As a result,
  no need to separate io cost from cpu cost, just use a single linear model for them together.
*/
/*
Get Row Cache
[[Fit Statistics]]
# function evals   = 18
# data points      = 84
# variables        = 2
chi-square         = 891361.397
reduced chi-square = 10870.261
[[Variables]]
Tper_row:   3.65179254 +/- 0.034538 (0.95%) (init= 10)
Tper_col:   0.06121923 +/- 0.001029 (1.68%) (init= 1)

Get Block Cache
[[Fit Statistics]]
# function evals   = 16
# data points      = 84
# variables        = 2
chi-square         = 2307154.480
reduced chi-square = 28136.030
[[Variables]]
Tper_row:   12.3097448 +/- 0.055566 (0.45%) (init= 10)
Tper_col:   0.06997847 +/- 0.001656 (2.37%) (init= 1)

Get IO
[[Fit Statistics]]
# function evals   = 19
# data points      = 84
# variables        = 2
chi-square         = 36984718.378
reduced chi-square = 451033.151
[[Variables]]
Tper_row:   23.9272937 +/- 0.222479 (0.93%) (init= 10)
Tper_col:   0.02160751 +/- 0.006632 (30.69%) (init= 1)
*/

const double GET_RC_BASE_T = 30.0;
const double GET_RC_PER_ROW_T = 3.65179254;
const double GET_RC_PER_COL_T = 0.06121923;

const double GET_BC_BASE_T = 210.0;
const double GET_BC_PER_ROW_T = 12.3097448;
const double GET_BC_PER_COL_T = 0.06997847;

const double GET_IO_BASE_T = 520.0;
const double GET_IO_PER_ROW_T = 23.9272937;
const double GET_IO_PER_COL_T = 0.02160751;

const int64_t MICRO_PER_MACRO_16K_2M = 115;

// if stat is not available, consider relatively small table
// note that default micro block size is 16KB, 128 micros per macro
const int64_t DEFAULT_MACRO_COUNT = 10;

const int64_t DEFAULT_MICRO_COUNT = DEFAULT_MACRO_COUNT * MICRO_PER_MACRO_16K_2M;

const double DEFAULT_IO_RTT_T = 2000;

const int64_t OB_EST_DEFAULT_ROW_COUNT = 1000;

const int64_t EST_DEF_COL_NUM_DISTINCT = 500;

const double EST_DEF_COL_NULL_RATIO = 0.01;

const double EST_DEF_COL_NOT_NULL_RATIO = 1 - EST_DEF_COL_NULL_RATIO;

const double EST_DEF_VAR_EQ_SEL = EST_DEF_COL_NOT_NULL_RATIO / EST_DEF_COL_NUM_DISTINCT;

const double OB_COLUMN_DISTINCT_RATIO = 2.0 / 3.0;

const int64_t OB_EST_DEFAULT_NUM_NULL =
    static_cast<int64_t>(static_cast<double>(OB_EST_DEFAULT_ROW_COUNT) * EST_DEF_COL_NULL_RATIO);

const int64_t OB_EST_DEFAULT_DATA_SIZE = OB_EST_DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE;

const int64_t OB_EST_DEFAULT_MACRO_BLOCKS = OB_EST_DEFAULT_DATA_SIZE / DEFAULT_MACRO_BLOCK_SIZE;

const int64_t OB_EST_DEFAULT_MICRO_BLOCKS = OB_EST_DEFAULT_DATA_SIZE / DEFAULT_MICRO_BLOCK_SIZE;

const double OB_DEFAULT_HALF_OPEN_RANGE_SEL = 0.2;

const double OB_DEFAULT_CLOSED_RANGE_SEL = 0.1;

const double ESTIMATE_UNRELIABLE_RATIO = 0.25;

// these are cost params for operators

/**************************************
 * Cost params for ObRowStore Write
 ***************************************/
// Cost model for rowstore write is:
//  Trowstore_write = RS_WRITE_ONCE + Nrow * (RS_WRITE_ROW_ONCE + sum(RS_WRITE_COL))
const double RS_WRITE_ROW_ONCE = 0.072;
// this is an average case for the testcase used in test.
// for int with 8 bytes, this is 0.03281816
// for int with 1 bytes, this is 0.02272876
// the difference mainly results from pref difference of ObRowWriter::get_int_byte,
// in which big ints goes the last branch, while small ints go the first branch.
// in the same time, larger space occupation leads to more cache misses
const double RS_WRITE_BI = 0.0266846;
const double RS_WRITE_DOUBLE = 0.02970336;     // Basically same as int
const double RS_WRITE_FLOAT = 0.02512819;      // Basically same as int
const double RS_WRITE_NUMBER = 0.08238981;     // this should be a func(precision), tested with fixed
const double RS_WRITE_TIMESTAMP = 0.02998249;  // Basically same as int
// For varchar, write cost is a func(len)
// for varchar(1), RS_WRITE_VC1_REAL = 0.04504297
// for varchar(128), RS_WRITE_VC128_REAL = 0.22601192
// use linear interpolation to calc:
// RS_WRITE_VC32_EST = 0.089216
// RS_WRITE_VC64_EST = 0.134814
// matches observation:
// RS_WRITE_VC32_REAL = 0.08476897
// RS_WRITE_VC64_REAL = 0.13678196
const double RS_WRITE_VC1 = 0.04504297;
const double RS_WRITE_VC_CHAR = 0.00142495;

const double RS_WRITE_ONCE = 10;
/**************************************
 * Cost params for ObRowStore Read
 ***************************************/
// Cost model for rowstore read is:
//  Trowstore_read = RS_READ_ONCE + Nrow * (RS_READ_ROW_ONCE + sum(RS_READ_COL))
// read is generally slower than write, because reading needs to manipulate ObObj
const double RS_READ_ROW_ONCE = 0.085;

// this is also an average case, the difference results from the switch in READ_INT.
// the range is relatively small, 0.03463276 for big ints and 0.03346543 for small ints
const double RS_READ_BI = 0.0335466;
const double RS_READ_DOUBLE = 0.03306249;  // Basically same as int
const double RS_READ_FLOAT = 0.03280787;   // Basically same as int
const double RS_READ_NUMBER = 0.04573405;
const double RS_READ_TIMESTAMP = 0.03302134;  // Basically same as int
// For varchar, experiment shows that read cost varies between different lengths, but not linear.
// It cannot be explained by reading code. Here I think it results from cache miss.
const double RS_READ_VC1 = 0.03953656;

const double RS_READ_ONCE = 2;
/**************************************
 * Cost params for ObArray used in sort
 ***************************************/

const double SORT_ARR_ELEM_PER_PAGE = 1024.0;
const double SORT_ARR_ELEM_PUSH = 0.00898860;
// happen when array extends
// copy_cnt = ELEM_PER_PAGE * (pow(2, extend_cnt) - 1)
const double SORT_ARR_ELEM_COPY = 0.00631888;

/**************************************
 * Cost params for Sorting
 ***************************************/
// Cost model for in-memory sorting is:
//  Tsort = Trowstore_write + Trowstore_read + Treal_sort
//          + Tappend_array(neglected) + Tcache_miss(not considered yet)
//  Treal_sort = SORT_ONCE_T + Tcompare * Nrow * log2(Nrow)
//  Tcompare = Tcmp_col0 + Tcmp_col1 / NDV_col0 + Tcmp_col2 / (NDV_col1 * NDV_col0 ) ......

const double SORT_CMP_BI = 0.0262552;
const double SORT_CMP_DOUBLE = 0.02645678;     // Basically same as int
const double SORT_CMP_FLOAT = 0.02649361;      // Basically same as int
const double SORT_CMP_NUMBER = 0.05426962;     // tested with about 10 digits
const double SORT_CMP_TIMESTAMP = 0.02762868;  // slightly higher than atomic types, for TZ
const double SORT_CMP_VC = 0.055899;           // tested with common prefix of 10 chars

const double SORT_ONCE_T = 15;
/**************************************
 * Note of cache miss during sorting
 ***************************************/
/*
  ObSort stores row date in ObRowStore, and pointers to them in an array for sorting.
  This method leads to large numbers of TLB miss/Cache miss.
  cost-of-cache came in three major ways:
  1\During sorting, std::sort access rows through pointers. At the begining, logical sequence
    is different from physical seq, but access is more likely according to phy-seq. As the sorting
    process going, access-seq gets more and more diverged from phy-seq, and in this way TLB miss occurs.
  2\During getting results, access is through pointers too, and access-seq is totally differant from phy-seq.
    So a lot of TLB misses would occur.
  3\During sorting, comparator-function access sort columns through indexes, and they are unlikely to fit
    into a single cache line. In in-memory sorting, sort columns are cached in stored_row structure in
    access order, so this problem is partially solved. But in top-n sort, this problem is untouched and
    generates a lot of cache misses (Effect is to be tested yet).
  Effect of cache/TLB miss is not yet considered in the model.
*/

// fixed setup time for calc other conds(filters) in join
const double JOIN_OTHER_COND_INIT_T = 0.15;
// average cost for calc a filter (sampled with filter t1.a + t2.a < K).
// can be inaccurate
const double JOIN_OTHER_COND_SINGLE_T = 0.17;

/*
 * EQUAL conditions are much cheaper than OTHER conds, because they don't use
 * post expr mechanism. But calc counts may be larger
 */

// fixed setup time for calc equal conds(filters) in join
const double JOIN_EQUAL_COND_INIT_T = 0.054;
// average cost for calc a equal cond
const double JOIN_EQUAL_COND_SINGLE_T = 0.04;

// cost params for material
const double MATERIAL_ONCE_T = 38;
const double MATERIAL_ROW_READ_ONCE_T = 0.15;
const double MATERIAL_ROW_COL_READ_T = 0.023;
const double MATERIAL_ROW_WRITE_ONCE_T = 0.15;
const double MATERIAL_ROW_COL_WRITE_T = 0.023;

// cost params for nestloop join
const double NESTLOOP_ONCE_T = 25.465184;
// each left scan cause a right rescan and etc, so more expensive
const double NESTLOOP_ROW_LEFT_T = 0.186347;
const double NESTLOOP_ROW_RIGHT_T = 0.033857;
const double NESTLOOP_ROW_OUT_T = 0.249701;
const double NESTLOOP_ROW_QUAL_FAIL_T = 0.128629;

const double BLK_NESTLOOP_ONCE_START_T = 8.030292;
const double BLK_NESTLOOP_ONCE_RES_T = 0.249750;
const double BLK_NESTLOOP_QUAL_FAIL_T = 0.129177;
const double BLK_NESTLOOP_ROW_LEFT_T = 0.19983;
const double BLK_NESTLOOP_ROW_RIGHT_T = 0.034;
const double BLK_NESTLOOP_CACHE_SCAN_T = 0.264018;
const double BLK_NESTLOOP_CACHE_COUNT_T = 54.348476;

// cost params for merge join
const double MERGE_ONCE_T = 0.005255;
// cost for scan left rows
// for simplicity, dont separate used from unused, fitting result is acceptable
const double MERGE_ROW_LEFT_T = 0.005015;
// cost for right rows.
// unlike NL, this splits into three type
// because right rows can come from "right cache"
const double MERGE_ROW_RIGHT_OP_T = 0.000001;
const double MERGE_ROW_RIGHT_CACHE_T = 0.000001;
const double MERGE_ROW_RIGHT_UNUSED_T = 0.006961;  // very small, can ignore?
const double MARGE_ROW_QUAL_FAIL_T = 0.132598;
// there is a cache clear operation each match group
const double MERGE_MATCH_GROUP_T = 0.808002;
const double MERGE_ROW_ONCE_RES_T = 0.751524;

const double HJ_ONCE_T = 0;
// build hashtable
const double HJ_BUILD_HTABLE_T = 0.74497774;
// read right row, cac hash value, read_hash_row
const double HJ_RIGHT_ROW_DEAL_T = 0.26678144;
// convert tuple -> row
const double HJ_CONVERT_TUPLE_T = 0.86340381;
// join matched row
const double HJ_JOIN_ROW_T = 0.28939532;

// for each input row, there are:
// cost for judging whether it starts a new group(Proportional to Ngroup_col)
// cost for calc each aggr column(Proportional to Naggr_col)
// for each output row(i.e. a group), there are
// cost for deep copy first input row twice(Proportional to Ninput_col)
// cost for calc and copy each aggr column(Proportional to Naggr_col)
// Here, we use AVG aggr func as the sampling target.

/* Fitting raw data (based on code of 2016.7.13, run on hudson@10.101.192.90)
 * Merge
[[Model]]
    Model(mg_model_arr)
[[Fit Statistics]]
    # function evals   = 51
    # data points      = 600
    # variables        = 6
    chi-square         = 135494260.647
    reduced chi-square = 228104.816
[[Variables]]
    Taggr_prepare_result:   3.43226110 +/- 0.001761 (0.05%) (init= 0.1)
    Tgroup_cmp_col:         0.01851318 +/- 0.000861 (4.65%) (init= 0.1)
    Tres_once:              2.29158957 +/- 0.031264 (1.36%) (init= 0.1)
    Trow_once:              0.14544762 +/- 0.009362 (6.44%) (init= 0.1)
    Tcopy_col:              0.01696773 +/- 0.001833 (10.80%) (init= 0.1)
    Taggr_process:          0.05854551 +/- 0.000879 (1.50%) (init= 0.1)


HASH
[[Model]]
    Model(mg_model_arr)
[[Fit Statistics]]
    # function evals   = 58
    # data points      = 600
    # variables        = 7
    chi-square         = 107057464.424
    reduced chi-square = 180535.353
[[Variables]]
    Taggr_prepare_result:   3.44343785 +/- 0.001563 (0.05%) (init= 0.1)
    Tcopy_col:              0.02159315 +/- 0.001680 (7.78%) (init= 0.1)
    Tstartup:               550.106907 +/- 22.61977 (4.11%) (init= 0.1)
    Tgroup_hash_col:        0.03631451 +/- 0.000589 (1.62%) (init= 0.1)
    Trow_once:              0.13340641 +/- 0.008364 (6.27%) (init= 0.1)
    Tres_once:              1.50742628 +/- 0.027948 (1.85%) (init= 0.1)
    Taggr_process:          0.05501125 +/- 0.000778 (1.41%) (init= 0.1)
 *
 * */

// cost for group by common operations
const double GB_AGGR_PREPARE_RESULT = 3.4;
const double GB_ROW_ONCE = 0.14;
const double GB_COPY_COL = 0.2;
const double GB_AGGR_PROCESS = 0.055;
// cost params that are different between hash and merge group by
const double HGB_STARTUP = 550;  // build hash buckets, this can be optimized further
const double MGB_CMP_COL = 0.018;
const double HGB_HASH_COL = 0.036;  //
const double MGB_RES_ONCE = 2.3;
const double HGB_RES_ONCE = 1.5;

const int64_t MGB_DEFAULT_GROUP_COL_COUNT = 1;
const int64_t MGB_DEFAULT_INPUT_COL_COUNT = 10;

// cost param for distinct.Refering Merge group by
// Old cost only for compatible with old observer code version
const double MERGE_DISTINCT_STARTUP_T = 50;
const double MERGE_DISTINCT_ONE_T = 1.5;
const double MERGE_DISTINCT_COL_T = 0.018;
// New cost params
const double MERGE_DISTINCT_NEW_STARTUP_T = 6.69406;
// Include compare row.
const double MERGE_DISTINCT_INPUT_CARD_T = 0.053765;
const double MERGE_DISTINCT_COL_INPUT_T = 0.025118;
// Include storing row.
const double MERGE_DISTINCT_OUTPUT_CARD_T = 0.112583;
const double MERGE_DISTINCT_COL_OUTPUT_T = 0.024513;

const double HASH_DISTINCT_STARTUP_T = 26.7906;
// Include calc hash and compare row.
const double HASH_DISTINCT_INPUT_CARD_T = 0.197776;
const double HASH_DISTINCT_COL_INPUT_T = 0.025118;
// Include create hash buckets and storing row.
const double HASH_DISTINCT_OUTPUT_CARD_T = 0.300810;
const double HASH_DISTINCT_COL_OUTPUT_T = 0.031524;

// cost param for virtual table
const double VIRTUAL_INDEX_GET_COST = 20;  // typical get latency for hash table

const double SCALAR_STARTUP_T = 177.0;
const double SCALAR_INPUT_ONCE_T = 0.144;
const double SCALAR_INPUT_AGGR_COL_T = 0.073;

const int64_t HALF_OPEN_RANGE_MIN_ROWS = 20;
const int64_t CLOSE_RANGE_MIN_ROWS = 10;
}  // namespace common
}  // namespace oceanbase

#endif /* _OB_COST_CONSTS_DEF_H_ */