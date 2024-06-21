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

#ifndef OCEANBASE_SQL_OB_SQL_DEFINE_H_
#define OCEANBASE_SQL_OB_SQL_DEFINE_H_

#include "lib/container/ob_fixed_array.h"
#include "share/datum/ob_datum.h"
#include "common/object/ob_object.h"
#include "share/ob_define.h"
#include "deps/oblib/src/lib/container/ob_array.h"
#include "src/share/rc/ob_tenant_base.h"
#include "deps/oblib/src/lib/container/ob_2d_array.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObSchemaObjVersion;
}
}
using namespace common;

namespace sql
{
const int64_t OB_SQL_MAX_CHILD_OPERATOR_NUM = 16;
const int64_t OB_MIN_PARALLEL_TASK_COUNT = 13; //期望每一个并行度最低持有的task数量
const int64_t OB_MAX_PARALLEL_TASK_COUNT = 100; //期望每一个并行度最大持有task数量
const int64_t OB_MIN_MARCO_COUNT_IN_TASK = 1; //每个task最少负责的宏块个数
const int64_t OB_INVAILD_PARALLEL_TASK_COUNT = -1;
const int64_t OB_EXPECTED_TASK_LOAD = 100; //MB, one task will get 100MB data from disk
const int64_t OB_GET_MACROS_COUNT_BY_QUERY_RANGE = 1;
const int64_t OB_GET_BLOCK_RANGE = 2;
const int64_t OB_BROADCAST_THRESHOLD = 100;
const int64_t OB_PARTITION_COUNT_PRE_SQL = 16; //用于开hash表的桶大小或者SEAarry的默认大小，假定一般SQL不会超过16个partition
const int64_t OB_MAX_RECURSIVE_SQL_LEVELS = 50; //compatible with oracle

typedef common::ObFixedArray<share::schema::ObSchemaObjVersion, common::ObIAllocator> DependenyTableStore;
typedef common::ParamStore ParamStore;

// ob_rowkey_info.h oceanbase::common::ObOrderType also defined
enum ObOrderDirection
{
  NULLS_FIRST_ASC = 0,   //顺序，正向扫描,Forward, NULLs first
  NULLS_LAST_ASC, // 顺序，正向扫描,Forward, NULLs last
  NULLS_FIRST_DESC, //倒序，反向扫描,Backward, NULLs first
  NULLS_LAST_DESC,   //倒序，反向扫描,Backward, NULLs last
  UNORDERED, //不排序，保持现状
  MAX_DIR, //非法值
};

inline bool is_null_first(ObOrderDirection order_direction) {
  return NULLS_FIRST_ASC == order_direction || NULLS_FIRST_DESC == order_direction;
}

extern ObOrderDirection default_asc_direction();
extern ObOrderDirection default_desc_direction();

inline bool is_ascending_direction(const ObOrderDirection direction)
{
  return (NULLS_FIRST_ASC == direction || NULLS_LAST_ASC == direction);
}

inline bool is_descending_direction(const ObOrderDirection direction)
{
  return (NULLS_FIRST_DESC == direction || NULLS_LAST_DESC == direction);
}

enum ObJoinType
{
  UNKNOWN_JOIN = 0,
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  RIGHT_OUTER_JOIN,
  FULL_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  RIGHT_SEMI_JOIN,
  LEFT_ANTI_JOIN,
  RIGHT_ANTI_JOIN,
  CONNECT_BY_JOIN, //used for hierarchical query
  MAX_JOIN_TYPE
};

enum SlaveMappingType {
  SM_NONE = 0,
  SM_PWJ_HASH_HASH,
  SM_PPWJ_HASH_HASH,
  SM_PPWJ_BCAST_NONE,
  SM_PPWJ_NONE_BCAST,
};

enum PathType
{
  INVALID = 0,
  ACCESS,
  JOIN,
  SUBQUERY,
  FAKE_CTE_TABLE_ACCESS,
  FUNCTION_TABLE_ACCESS,
  TEMP_TABLE_ACCESS,
  JSON_TABLE_ACCESS,
  VALUES_TABLE_ACCESS
};

enum JtColType {
  INVALID_COL_TYPE = 0,
  COL_TYPE_ORDINALITY, // 1
  COL_TYPE_EXISTS, // 2
  COL_TYPE_QUERY, // 3
  COL_TYPE_VALUE, // 4
  NESTED_COL_TYPE, // 5
  COL_TYPE_QUERY_JSON_COL, // 6
  COL_TYPE_VAL_EXTRACT_XML, // 7
  COL_TYPE_XMLTYPE_XML, // 8
  COL_TYPE_ORDINALITY_XML = 9,
};

enum ObNameTypeClass
{
  OB_TABLE_NAME_CLASS = 0, //table name, database names, table alias names ,affected by system variable lower_case_table_names
  OB_COLUMN_NAME_CLASS = 1,// column name,column alias name. index name , stored using lettercase and comparisons are case insensitive
  OB_USER_NAME_CLASS = 2,// user names, tenant names and other names, stored using lettercase and comparisons are case sensitive
};

enum ObMatchAgainstMode {
  NATURAL_LANGUAGE_MODE = 0,
  NATURAL_LANGUAGE_MODE_WITH_QUERY_EXPANSION = 1,
  BOOLEAN_MODE = 2,
  WITH_QUERY_EXPANSION = 3,
  MAX_MATCH_AGAINST_MODE = 4,
};

#define IS_JOIN(type) \
(((type) == PHY_MERGE_JOIN) || \
 ((type) == PHY_NESTED_LOOP_JOIN) || \
 ((type) == PHY_HASH_JOIN) || \
 ((type) == PHY_BLOCK_BASED_NESTED_LOOP_JOIN))

#define IS_OUTER_JOIN(join_type) \
  ((join_type) == LEFT_OUTER_JOIN || \
   (join_type) == RIGHT_OUTER_JOIN || \
   (join_type) == FULL_OUTER_JOIN)

#define IS_SEMI_ANTI_JOIN(join_type) \
  ((join_type) == LEFT_SEMI_JOIN || \
   (join_type) == RIGHT_SEMI_JOIN || \
   (join_type) == LEFT_ANTI_JOIN || \
   (join_type) == RIGHT_ANTI_JOIN)

#define IS_SEMI_JOIN(join_type) \
  ((join_type) == LEFT_SEMI_JOIN || \
   (join_type) == RIGHT_SEMI_JOIN)

#define IS_ANTI_JOIN(join_type) \
  ((join_type) == LEFT_ANTI_JOIN || \
   (join_type) == RIGHT_ANTI_JOIN)

#define IS_LEFT_SEMI_ANTI_JOIN(join_type) \
  ((join_type) == LEFT_SEMI_JOIN || \
   (join_type) == LEFT_ANTI_JOIN)

#define IS_RIGHT_SEMI_ANTI_JOIN(join_type) \
   ((join_type) == RIGHT_SEMI_JOIN || \
   (join_type) == RIGHT_ANTI_JOIN)

#define IS_OUTER_OR_CONNECT_BY_JOIN(join_type) (IS_OUTER_JOIN(join_type) || CONNECT_BY_JOIN == join_type)

#define IS_LEFT_STYLE_JOIN(join_type) \
  ((join_type) == LEFT_SEMI_JOIN || \
   (join_type) == LEFT_ANTI_JOIN || \
   (join_type) == LEFT_OUTER_JOIN || \
   (join_type) == FULL_OUTER_JOIN)

#define IS_RIGHT_STYLE_JOIN(join_type) \
  ((join_type) == RIGHT_SEMI_JOIN || \
   (join_type) == RIGHT_ANTI_JOIN || \
   (join_type) == RIGHT_OUTER_JOIN || \
   (join_type) == FULL_OUTER_JOIN)

#define IS_SET_PHY_OP(type) \
(((type) == PHY_MERGE_UNION) || \
 ((type) == PHY_HASH_UNION) || \
 ((type) == PHY_HASH_INTERSECT) || \
 ((type) == PHY_MERGE_INTERSECT) || \
 ((type) == PHY_HASH_EXCEPT) || \
 ((type) == PHY_MERGE_EXCEPT))


inline ObJoinType get_opposite_join_type(ObJoinType type)
{
  ObJoinType oppo_type = UNKNOWN_JOIN;
  switch (type) {
  case INNER_JOIN:
    oppo_type = INNER_JOIN;
    break;
  case LEFT_OUTER_JOIN:
      oppo_type = RIGHT_OUTER_JOIN;
      break;
  case RIGHT_OUTER_JOIN:
      oppo_type = LEFT_OUTER_JOIN;
      break;
  case FULL_OUTER_JOIN:
      oppo_type = FULL_OUTER_JOIN;
      break;
  case LEFT_SEMI_JOIN:
      oppo_type = RIGHT_SEMI_JOIN;
      break;
  case RIGHT_SEMI_JOIN:
      oppo_type = LEFT_SEMI_JOIN;
      break;
  case LEFT_ANTI_JOIN:
      oppo_type = RIGHT_ANTI_JOIN;
      break;
  case RIGHT_ANTI_JOIN:
      oppo_type = LEFT_ANTI_JOIN;
      break;
  case CONNECT_BY_JOIN:
      oppo_type = CONNECT_BY_JOIN;
      break;
  default:
      break;
  }
  return oppo_type;
}

inline const ObString &ob_join_type_str(ObJoinType join_type)
{
  static const ObString join_type_str_st[] =
  {
    "UNKNOWN JOIN",
    "JOIN",
    "OUTER JOIN",
    "RIGHT OUTER JOIN",
    "FULL OUTER JOIN",
    "SEMI JOIN",
    "RIGHT SEMI JOIN",
    "ANTI JOIN",
    "RIGHT ANTI JOIN",
    "CONNECT BY"
  };

  if (OB_LIKELY(join_type >= UNKNOWN_JOIN) && OB_LIKELY(join_type <= CONNECT_BY_JOIN)) {
    return join_type_str_st[join_type];
  } else {
    return join_type_str_st[0];
  }
}

enum ObTableLocationType
{
  OB_TBL_LOCATION_UNINITIALIZED = 0,
  OB_TBL_LOCATION_LOCAL,
  OB_TBL_LOCATION_REMOTE,
  OB_TBL_LOCATION_DISTRIBUTED,
  OB_TBL_LOCATION_ALL//like EXPRESSION, match all
};

enum ObRepartitionType
{
  OB_REPARTITION_NO_REPARTITION = 0,//不重分区
  OB_REPARTITION_ONE_SIDE_ONE_LEVEL,//只有一边按照一级分区做repartition,不repartition的一边是一级分区
  OB_REPARTITION_ONE_SIDE_TWO_LEVEL,//只有一边按照二级分区做repartition, 不repartition的一边是二级分区
  OB_REPARTITION_BOTH_SIDE_ONE_LEVEL,//两边都按照一级分区方式做repartition, 分区键是连接键
  OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST,//只有一边按照另外一边(二级分区表)的一级分区做repartition
  OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB,//只有一边按照另外一边(二级分区)的二级分区做repartition
};

enum ObRepartitionScope
{
  OB_REPARTITION_NONE_SIDE = 0,
  OB_REPARTITION_LEFT_SIDE,
  OB_REPARTITION_RIGHT_SIDE,
  OB_REPARTITION_BOTH_SIDE,
};

// enum ObPLCacheObjectType start wich OB_PHY_PLAN_UNCERTAIN value.
// if it need add enum value to ObPhyPlanType, need skip ObPLCacheObjectType max value.
enum ObPhyPlanType
{
  OB_PHY_PLAN_UNINITIALIZED = 0,
  OB_PHY_PLAN_LOCAL,
  OB_PHY_PLAN_REMOTE,
  OB_PHY_PLAN_DISTRIBUTED,
  OB_PHY_PLAN_UNCERTAIN
};

inline const ObString &ob_plan_type_str(ObPhyPlanType plan_type)
{
  static const ObString plan_type_str_st[] =
  {
    "UNINITIALIZED",
    "LOCAL",
    "REMOTE",
    "DISTRIBUTED",
  };

  if (OB_LIKELY(plan_type >= OB_PHY_PLAN_UNINITIALIZED)
      && OB_LIKELY(plan_type <= OB_PHY_PLAN_DISTRIBUTED)) {
    return plan_type_str_st[plan_type];
  } else {
    return plan_type_str_st[0];
  }
}

enum ExplainType
{
  EXPLAIN_UNINITIALIZED = 0,
  EXPLAIN_OUTLINE,
  EXPLAIN_EXTENDED,
  EXPLAIN_PARTITIONS,
  EXPLAIN_TRADITIONAL,
  EXPLAIN_FORMAT_JSON,
  EXPLAIN_BASIC,
  EXPLAIN_PLANREGRESS,
  EXPLAIN_EXTENDED_NOADDR,
  EXPLAIN_DBLINK_STMT,
  EXPLAIN_HINT_FORMAT,
};

enum DiagnosticsType
{
  DIAGNOSTICS_UNINITIALIZED = 0,
  GET_CURRENT_COND,
  GET_CURRENT_INFO,
  GET_STACKED_COND,
  GET_STACKED_INFO
};

enum OutlineType
{
  OUTLINE_TYPE_UNINIT = 0,
  USED_HINT,
  OUTLINE_DATA
};

enum ObPlanLocationType
{
  UNINITIALIZED = 0,
  LOCAL,
  REMOTE,
  DISTRIBUTED
};

struct ObPQDistributeMethod
{
#define PQ_DIST_METHOD_DEF(DEF) \
    DEF(NONE,) \
    DEF(PARTITION,) \
    DEF(RANDOM,) \
	  DEF(RANDOM_LOCAL,) \
    DEF(HASH,) \
    DEF(BROADCAST,) \
    \
    /* distribute in two level: SQC && PX worker*/ \
    /* BROADCAST for SQC, RANDOM for PX worker */ \
    DEF(BC2HOST,) \
    DEF(SM_BROADCAST,) \
    DEF(PARTITION_HASH,) \
    DEF(DROP,) \
    /*PARTITION_RANDOM：PDML情况下处理分区表分区内并行；数据按照partition所在的SQC进行划分，*/ \
    /*并行度不受partition个数的限制；但是PARTITION对应的pkey分区，分区粒度为partition，*/ \
    /*实际并行度受partition的个数限制，无法在PDML场景下充分利用并行*/ \
    DEF(PARTITION_RANDOM,) \
    DEF(RANGE,)\
    DEF(PARTITION_RANGE,)\
    DEF(HYBRID_HASH_BROADCAST,) /* aka PX SEND HYBRID HASH */ \
    DEF(HYBRID_HASH_RANDOM,) /* aka PX SEND HYBRID HASH */ \
    DEF(LOCAL,) /* represents pull to local */

DECLARE_ENUM(Type, type, PQ_DIST_METHOD_DEF, static);

  static ObPQDistributeMethod::Type get_print_dist(ObPQDistributeMethod::Type method) {
    ObPQDistributeMethod::Type print_method = ObPQDistributeMethod::LOCAL;
    if (ObPQDistributeMethod::Type::SM_BROADCAST == method) {
      print_method = ObPQDistributeMethod::Type::BROADCAST;
    } else if (ObPQDistributeMethod::Type::PARTITION_HASH == method) {
      print_method = ObPQDistributeMethod::Type::PARTITION;
    } else {
      print_method = method;
    }
    return print_method;
  }
};

struct ObNullDistributeMethod
{
/*
 * ObNullDistributeMethod::NONE - neither DROP nor RANDOM, do null distribution as ObSliceCalc said
 */
#define NULL_DIST_METHOD_DEF(DEF) \
  DEF(NONE,) \
  DEF(DROP,) \
  DEF(RANDOM,)

DECLARE_ENUM(Type, type, NULL_DIST_METHOD_DEF, static);
  static ObNullDistributeMethod::Type get_print_dist(ObNullDistributeMethod::Type method) {
    return method;
  }
};

enum PartitionFilterType {
  Uninitialized,
  Forbidden,
  OneLevelPartitionKey,
  TwoLevelPartitionKey,
  WholePartitionKey,
 };

enum DistinctType
{
  T_DISTINCT_NONE = 0,
  T_HASH_DISTINCT = 1,
  T_MERGE_DISTINCT = 2
};

enum class ObPDMLOption {
  NOT_SPECIFIED = -1,
  ENABLE,
  DISABLE,
  MAX_VALUE
};

enum OrderingFlag
{
  NOT_MATCH = 0,
  JOIN_MATCH = 1,
  GROUP_MATCH = 1 << 1,
  WINFUNC_MATCH = 1 << 2,
  DISTINCT_MATCH = 1 << 3,
  SET_MATCH = 1 << 4,
  ORDERBY_MATCH = 1 << 5,
  POTENTIAL_MATCH = 1 << 6 // ordering可能在subplan scan后用到
};

enum OrderingCheckScope
{
  NOT_CHECK = 0,
  CHECK_GROUP = 1,
  CHECK_WINFUNC = 1 << 1,
  CHECK_DISTINCT = 1 << 2,
  CHECK_SET = 1 << 3,
  CHECK_ORDERBY = 1 << 4,
  CHECK_ALL = (1 << 5) - 1
};


enum ObExecuteMode
{
  EXECUTE_INVALID = 0,
  EXECUTE_INNER,
  EXECUTE_LOCAL,
  EXECUTE_REMOTE,
  EXECUTE_DIST,
  EXECUTE_PS_PREPARE,//prepare statement local server
  EXECUTE_PS_EXECUTE,//prepare statement local server
  EXECUTE_PS_FETCH,
  EXECUTE_PS_SEND_PIECE,
  EXECUTE_PS_GET_PIECE,
  EXECUTE_PS_SEND_LONG_DATA,
  EXECUTE_PL_EXECUTE
};


enum PartitionIdCalcType {
  CALC_INVALID = -1,          // invalid calc type
  CALC_NORMAL = 0,            // calc both part id and subpart id
  CALC_IGNORE_FIRST_PART = 1, // only calc subpart id
  CALC_IGNORE_SUB_PART =2     // only calc part id
};

enum class ObParamOption {
  NOT_SPECIFIED = -1,
  FORCE,
  EXACT,
  MAX_VALUE
};

enum DominateRelation
{
  OBJ_LEFT_DOMINATE = 0,
  OBJ_RIGHT_DOMINATE,
  OBJ_EQUAL,
  OBJ_UNCOMPARABLE
};

// for parallel precedence, refer to
// https://docs.oracle.com/cd/E11882_01/server.112/e41573/hintsref.htm#PFGRF94937
enum PXParallelRule
{
  USE_PX_DEFAULT = 0, // default disable parallel
  MANUAL_HINT, // /*+ parallel(3) */
  SESSION_FORCE_PARALLEL, // alter session force parallel query parallel 3;
  MANUAL_TABLE_DOP, // create table t1 (...) parallel 3;
  AUTO_DOP, // /*+ parallel(auto) */ or alter session set parallel_degree_policy = 'auto';
  // force disable parallel below
  PL_UDF_DAS_FORCE_SERIALIZE, //stmt has_pl_udf will use das, force serialize;
  DBLINK_FORCE_SERIALIZE, //stmt has dblink will use das, force seialize;
  MAX_OPTION
};

inline const char *ob_px_parallel_rule_str(PXParallelRule px_parallel_ruel)
{
  const char *ret = "USE_PX_DEFAULT";
  static const char *parallel_rule_type_to_str[] =
  {
    "USE_PX_DEFAULT",
    "MANUAL_HINT",
    "SESSION_FORCE_PARALLEL",
    "MANUAL_TABLE_DOP",
    "AUTO_DOP",
    "PL_UDF_DAS_FORCE_SERIALIZE",
    "DBLINK_FORCE_SERIALIZE",
    "MAX_OPTION",
  };
  if (OB_LIKELY(px_parallel_ruel >= USE_PX_DEFAULT)
      && OB_LIKELY(px_parallel_ruel <= MAX_OPTION)) {
    ret = parallel_rule_type_to_str[px_parallel_ruel];
  }
  return ret;
}

static const int64_t PDML_DOP_LIMIT_PER_PARTITION = 10;
static const int64_t ROW_COUNT_THRESHOLD_PER_DOP = 10000;  // zhanyuetodo: need adjust this by test

enum OpParallelRule
{
  OP_GLOBAL_DOP = 0, /* use DOP from global parallel rule except MANUAL_TABLE_DOP or AUTO_DOP */
  OP_DAS_DOP, /*+ DAS use DOP = 1 */
  OP_HINT_DOP, /* use parallel hint for table: parallel(t1, 3) */
  OP_TABLE_DOP, /*+ use table parallel property: create table t1 (...) parallel 3; */
  OP_AUTO_DOP, /*+ DOP is calculated by AUTO_DOP */
  OP_INHERIT_DOP, /*+ inherited from other op or determined by other op in the same DFO */
  OP_DOP_RULE_MAX
};

typedef common::ObDmlEventType ObDmlEventType;

enum MayAddIntervalPart {
  NO, // do nothing, just set part id to 0
  YES, // add an interval partition, and set err code to force retry
  PART_CHANGE_ERR, // set err code, make query stop and report and error msg to client
};

enum ObIDPAbortType
{
  IDP_DEFAULT_ABORT = -1,
  IDP_INVALID_HINT_ABORT = 0,
  IDP_STOPENUM_EXPDOWN_ABORT = 1,
  IDP_STOPENUM_LINEARDOWN_ABORT = 2,
  IDP_ENUM_FAILED_ABORT = 3,
  IDP_NO_ABORT = 4
};

struct ObSqlDatumArray
{
  ObSqlDatumArray()
    : data_(nullptr),
      count_(0),
      element_()
  {
  }
  typedef common::ObArrayWrap<common::ObDatum> DatumArray;
  static ObSqlDatumArray *alloc(common::ObIAllocator &allocator, int64_t count);
  TO_STRING_KV("data", DatumArray(data_, count_),
               K_(count),
               K_(element));
  common::ObDatum *data_;
  int64_t count_;
  common::ObDataType element_;
};

OB_INLINE ObSqlDatumArray *ObSqlDatumArray::alloc(common::ObIAllocator &allocator, int64_t count)
{
  ObSqlDatumArray *array_obj = nullptr;
  void *array_buf = nullptr;
  void *data_buf = nullptr;
  int64_t array_size = sizeof(ObSqlDatumArray) + sizeof(common::ObDatum) * count;
  if (OB_NOT_NULL(array_buf = allocator.alloc(array_size))) {
    array_obj = new (array_buf) ObSqlDatumArray();
    data_buf = static_cast<char*>(array_buf) + sizeof(ObSqlDatumArray);
    array_obj->data_ = new (data_buf) common::ObDatum[count];
    array_obj->count_ = count;
  }
  return array_obj;
}

// Window function optimization settings. (_windowfunc_optimization_settings sys variable)
struct ObWinfuncOptimizationOpt
{
  ObWinfuncOptimizationOpt() : v_(0) {}
  union {
    struct {
      uint64_t disable_range_distribution_:1;
      uint64_t disable_reporting_wf_pushdown_:1;
      // add more options here.
    };
    uint64_t v_;
  };
};

// class full name: ob tenant memory array.
// Used to solve the following problem:
// the initial memory allocation of ObSEArray is relatively large, leading to memory inflation issues in some scenarios.
// Default to using the MTL_ID() tenant,
// and the lifecycle of this class cannot cross tenants.
template<typename T, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false, typename CallBack = ObArrayDefaultCallBack<T>, typename ItemEncode = DefaultItemEncode<T> >
class ObTMArray final : public ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>
{
public:
  using ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::ObArrayImpl;
  ObTMArray(int64_t block_size = std::min(static_cast<int64_t>(4 * sizeof(T)), OB_MALLOC_NORMAL_BLOCK_SIZE),
        const BlockAllocatorT &alloc = BlockAllocatorT("TMArray"));
};

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
ObTMArray<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::ObTMArray(int64_t block_size,
                                                           const BlockAllocatorT &alloc)
    : ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>(block_size, alloc)
{
  this->set_tenant_id(MTL_ID());
}

template <typename T, int max_block_size = OB_MALLOC_BIG_BLOCK_SIZE,
          typename BlockAllocatorT = ModulePageAllocator,
          bool auto_free = false,
          typename BlockPointerArrayT = ObSEArray<T *, OB_BLOCK_POINTER_ARRAY_SIZE,
                                                  BlockAllocatorT, auto_free> >
class ObTMSegmentArray final : public Ob2DArray<T, max_block_size, BlockAllocatorT, auto_free,
          BlockPointerArrayT>
{
public:
  ObTMSegmentArray(const BlockAllocatorT &alloc = BlockAllocatorT("TMSegmentArray"));
  int assign(const ObTMSegmentArray &other) { return this->inner_assign(other); }
};

template <typename T, int max_block_size,
          typename BlockAllocatorT, bool auto_free,
          typename BlockPointerArrayT>
ObTMSegmentArray<T, max_block_size, BlockAllocatorT, auto_free,
          BlockPointerArrayT>::ObTMSegmentArray(const BlockAllocatorT &alloc)
    : Ob2DArray<T, max_block_size, BlockAllocatorT, auto_free,
          BlockPointerArrayT>(alloc)
{
  this->set_tenant_id(MTL_ID());
}

inline const ObString &ob_match_against_mode_str(const ObMatchAgainstMode mode)
{
  static const ObString ma_mode_str[] =
  {
    "NATURAL LANGUAGE MODE",
    "NATURAL LANGUAGE MODE WITH QUERY EXPANSION",
    "BOOLEAN MODE",
    "WITH QUERY EXPANSION",
    "UNKNOWN MATCH MODE"
  };

  if (OB_LIKELY(mode >= ObMatchAgainstMode::NATURAL_LANGUAGE_MODE)
      && OB_LIKELY(mode < ObMatchAgainstMode::MAX_MATCH_AGAINST_MODE)) {
    return ma_mode_str[mode];
  } else {
    return ma_mode_str[ObMatchAgainstMode::MAX_MATCH_AGAINST_MODE];
  }
}

static bool is_fixed_length(ObObjType type) {
  bool is_fixed = true;
  ObObjTypeClass tc = ob_obj_type_class(type);
  OB_ASSERT(tc >= ObNullTC && tc < ObMaxTC);
  if (ObNumberTC == tc
      || ObExtendTC == tc
      || ObTextTC == tc
      || ObStringTC == tc
      || ObEnumSetInnerTC == tc
      || ObRawTC == tc
      || ObRowIDTC == tc
      || ObLobTC == tc
      || ObJsonTC == tc
      || ObGeometryTC == tc
      || ObUserDefinedSQLTC == tc
      || ObDecimalIntTC == tc
      || ObRoaringBitmapTC == tc) {
    is_fixed = false;
  }
  return is_fixed;
}

static int16_t get_type_fixed_length(ObObjType type) {
  int16_t len = 0;
  ObObjTypeClass tc = ob_obj_type_class(type);
  OB_ASSERT(tc >= ObNullTC && tc < ObMaxTC);
  switch (tc)
  {
    case ObUIntTC:
    case ObIntTC:
    case ObDoubleTC:
    case ObDateTimeTC:
    case ObTimeTC:
    case ObBitTC:
    case ObEnumSetTC:
    {
      len = 8;
      break;
    }
    case ObDateTC:
    case ObFloatTC:
    {
      len = 4;
      break;
    }
    case ObYearTC:
    {
      len = 1;
      break;
    }
    case ObOTimestampTC: {
      len = (type == ObTimestampTZType) ? 12 : 10;
      break;
    }
    case ObIntervalTC:
    {
      len = (type == ObIntervalYMType) ? 8 : 12;
      break;
    }
    default:
      break;
  }
  return len;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_OB_SQL_DEFINE_H_ */
