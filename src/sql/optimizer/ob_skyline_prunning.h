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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_SKYLINE_PRUNNING_H_
#define OCEANBASE_SQL_OPTIMIZER_OB_SKYLINE_PRUNNING_H_ 1

#include "share/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/container/ob_array.h"
#include "sql/optimizer/ob_sharding_info.h"

namespace oceanbase
{
namespace sql
{

class ObRawExpr;

/*
 * skyline dimension
 */

class ObSkylineDim
{
public:
  enum Dimension {
    INDEX_BACK = 0,
    INTERESTING_ORDER,
    QUERY_RANGE,
    SHARDING_INFO,
    UNIQUE_RANGE,
    MAX_DIM //max dimension
  };
  enum CompareStat {
    UNCOMPARABLE = -2, //two dimension can't not compare
    RIGHT_DOMINATED = -1, // right dominate left, at least one dimension of right is better than left
    EQUAL = 0,
    LEFT_DOMINATED = 1,   //left dominate right, at least one dimension of left is better right
  };

  explicit ObSkylineDim(Dimension dim_type) : dim_type_(dim_type) {}
  virtual ~ObSkylineDim() {}
  /*
   * shoule return the four stat of CompareStat
   * */
  virtual int compare(const ObSkylineDim &other, CompareStat &status) const = 0;
  Dimension get_dim_type() const { return dim_type_; }
  static const int64_t DIM_COUNT = static_cast<int64_t>(MAX_DIM);;
  VIRTUAL_TO_STRING_KV(K(dim_type_));
private:
  const Dimension dim_type_;
};

class ObIndexBackDim : public ObSkylineDim
{
public:
  friend class ObOptimizerTraceImpl;
  ObIndexBackDim() : ObSkylineDim(INDEX_BACK), need_index_back_(false)
  {}

  virtual ~ObIndexBackDim() {}
  void set_index_back(const bool index_back) { need_index_back_ = index_back; }
  virtual int compare(const ObSkylineDim &other, CompareStat &status) const;
  VIRTUAL_TO_STRING_KV(K_(need_index_back));
private:
  bool need_index_back_;
};


//consider interesting order prefix
/**
 * group by c2,c3,c1
 * order by c2,c3
 * select distinct(c2,c3)
 * key prefix can use by sort
 * we save the column_ids that can be use
 */
class ObInterestOrderDim : public ObSkylineDim
{
public:
  friend class ObOptimizerTraceImpl;
  ObInterestOrderDim() : ObSkylineDim(INTERESTING_ORDER),
    is_interesting_order_(false),
    column_cnt_(0),
    need_index_back_(false),
    can_extract_range_(false),
    filter_column_cnt_(0)
  { MEMSET(column_ids_, 0, sizeof(uint64_t) * common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    MEMSET(filter_column_ids_, 0, sizeof(uint64_t) * common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER); }
  virtual ~ObInterestOrderDim() {}
  void set_interesting_order(const bool interesting_order) { is_interesting_order_ = interesting_order; }
  int add_interest_prefix_ids(const common::ObIArray<uint64_t> &column_ids);
  int add_const_column_info(const common::ObIArray<bool> &const_column_info);
  void set_index_back(const bool index_back) { need_index_back_ = index_back; }
  void set_extract_range(const bool can) { can_extract_range_ = can; }
  int add_filter_column_ids(const common::ObIArray<uint64_t> &filter_column_ids);
  virtual int compare(const ObSkylineDim &other, CompareStat &status) const;
  VIRTUAL_TO_STRING_KV(K_(is_interesting_order),
               K_(column_cnt),
               "column_ids", common::ObArrayWrap<uint64_t>(column_ids_, column_cnt_),
               K_(need_index_back),
               K_(can_extract_range),
               K_(filter_column_cnt),
               "filter column_ids", common::ObArrayWrap<uint64_t>(filter_column_ids_, filter_column_cnt_));
private:
  bool is_interesting_order_;
  int64_t column_cnt_;
  uint64_t column_ids_[common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER];
  bool const_column_info_[common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER];
  // some filter conditions on index columns
  bool need_index_back_;
  bool can_extract_range_;
  int64_t filter_column_cnt_;
  uint64_t filter_column_ids_[common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER];
};

//consider query range subset
//group by, order by, distinct
/**
 * range(100,MAX,MAX ; 200,MIN,MIN) columns (c2, c3, c1)
 * only the c2 can use, save the column_id of c2
 * id in ascending order, for quick compare
 */
class ObQueryRangeDim: public ObSkylineDim
{
public:
  friend class ObOptimizerTraceImpl;
  ObQueryRangeDim() : ObSkylineDim(QUERY_RANGE),
    column_cnt_(0),
    contain_always_false_(false)
  { MEMSET(column_ids_, 0, sizeof(uint64_t) * common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER);}
  virtual ~ObQueryRangeDim() {}
  virtual int compare(const ObSkylineDim &other, CompareStat &status) const;
  int add_rowkey_ids(const common::ObIArray<uint64_t> &column_ids);
  void set_contain_always_false(bool contain_always_false) { contain_always_false_ = contain_always_false; }
  VIRTUAL_TO_STRING_KV(K_(column_cnt), K_(contain_always_false),
               "rowkey_ids", common::ObArrayWrap<uint64_t>(column_ids_, column_cnt_));
private:
  int64_t column_cnt_;
  uint64_t column_ids_[common::OB_USER_MAX_ROWKEY_COLUMN_NUMBER];
  bool contain_always_false_;
};

class ObUniqueRangeDim: public ObSkylineDim
{
public:
  friend class ObOptimizerTraceImpl;
  ObUniqueRangeDim() : ObSkylineDim(UNIQUE_RANGE),
    range_cnt_(0) {}
  virtual ~ObUniqueRangeDim() {}
  virtual int compare(const ObSkylineDim &other, CompareStat &status) const;
  void set_range_count(int64_t range_cnt)
  {
    range_cnt_ = range_cnt;
  }
  VIRTUAL_TO_STRING_KV(K_(range_cnt));
private:
  int64_t range_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObUniqueRangeDim);
};

class ObShardingInfoDim: public ObSkylineDim
{
public:
  friend class ObOptimizerTraceImpl;
  ObShardingInfoDim() : ObSkylineDim(SHARDING_INFO),
    sharding_info_(NULL),
    is_single_get_(false),
    is_global_index_(false),
    is_index_back_(false),
    can_extract_range_(false)
  {}
  virtual ~ObShardingInfoDim() {}
  void set_sharding_info(ObShardingInfo *sharding_info) { sharding_info_ = sharding_info; }
  void set_is_single_get(bool is_single_get) { is_single_get_ = is_single_get; }
  virtual int compare(const ObSkylineDim &other, CompareStat &status) const;
  void set_is_global_index(bool is_global_index) { is_global_index_ = is_global_index; }
  void set_is_index_back(bool is_index_back) { is_index_back_ = is_index_back; }
  void set_can_extract_range(bool can_extract_range) { can_extract_range_ = can_extract_range; }
  /*
   * a global index that cannot extract range and is index back is unstable,
   * it may have highly variable cost for different queries.
   */
  inline bool is_unstable_global_index() const 
  { return is_global_index_ && !can_extract_range_ && is_index_back_; }
private:
  ObShardingInfo *sharding_info_;
  bool is_single_get_;
  bool is_global_index_;
  bool is_index_back_;
  bool can_extract_range_;
};

struct KeyPrefixComp
{
  KeyPrefixComp() : status_(ObSkylineDim::UNCOMPARABLE) {}
  int operator()(const uint64_t *left, const bool *left_const,
                 const int64_t left_cnt, const uint64_t *right,
                 const bool *right_const, const int64_t right_cnt);
  ObSkylineDim::CompareStat get_result() { return status_; }
private:
  static int do_compare(const uint64_t *left, const int64_t left_cnt,
                        const uint64_t *right, const bool *right_const,
                        const int64_t right_cnt, ObSkylineDim::CompareStat &status);
  ObSkylineDim::CompareStat status_;
};

struct RangeSubsetComp
{
public:
  RangeSubsetComp() : status_(ObSkylineDim::UNCOMPARABLE) {}
  int operator()(const uint64_t *left, const int64_t left_cnt,
                 const uint64_t *right, const int64_t right_cnt);
  ObSkylineDim::CompareStat get_result() { return status_; }
private:
  static int do_compare(const uint64_t *left, const int64_t left_cnt,
                        const uint64_t *right, const int64_t right_cnt,
                        ObSkylineDim::CompareStat &status);
  ObSkylineDim::CompareStat status_;
};


//dimension for each index
class ObIndexSkylineDim
{
public:
  ObIndexSkylineDim() : index_id_(common::OB_INVALID_ID),
    dim_count_(ObSkylineDim::DIM_COUNT),
    can_prunning_(true),
    is_get_(false)
  { MEMSET(skyline_dims_, 0, sizeof(const ObSkylineDim *) * ObSkylineDim::DIM_COUNT); }
  virtual ~ObIndexSkylineDim() {}
  int compare(const ObIndexSkylineDim &other, ObSkylineDim::CompareStat &status) const;
  uint64_t get_index_id() const { return index_id_; }
  int add_skyline_dim(const ObSkylineDim &dim);
  void set_index_id(const uint64_t index_id) { index_id_ = index_id; }
  int add_index_back_dim(const bool is_index_back,
                         common::ObIAllocator &allocator);
  int add_interesting_order_dim(const bool is_index_back,
                                const bool can_extract_range,
                                const common::ObIArray<uint64_t> &filter_column_ids,
                                const common::ObIArray<uint64_t> &interest_column_ids,
                                const common::ObIArray<bool> &const_column_info,
                                common::ObIAllocator &allocator);
  int add_query_range_dim(const common::ObIArray<uint64_t> &prefix_range_ids,
                          common::ObIAllocator &allocator,
                          bool contain_always_false);
  int add_unique_range_dim(int64_t range_cnt, ObIAllocator &allocator);
  int add_sharding_info_dim(ObShardingInfo *sharding_info, bool is_single_get, bool is_global_index, bool is_index_back, bool can_extract_range, ObIAllocator &allocator);
  bool can_prunning() const { return can_prunning_; }
  void set_can_prunning(const bool can) { can_prunning_ = can; }
  void set_is_get(bool is_get) { is_get_ = is_get; }
  TO_STRING_KV(K_(index_id), K_(is_get), K_(dim_count),
               "dims", common::ObArrayWrap<const ObSkylineDim *>(skyline_dims_, dim_count_));
private:
  uint64_t index_id_;
  const int64_t dim_count_;
  bool can_prunning_; //whether this index can prunning other index or not
  const ObSkylineDim *skyline_dims_[ObSkylineDim::DIM_COUNT];
  bool is_get_;
};

class ObSkylineDimRecorder
{
public:
  ObSkylineDimRecorder() {}
  virtual ~ObSkylineDimRecorder() {}
  int add_index_dim(const ObIndexSkylineDim &dim, bool &has_add);
  int has_dominate_dim(const ObIndexSkylineDim &dim,
                       common::ObIArray<int64_t> &remove_idxs,
                       bool &need_add);
  int get_dominated_idx_ids(common::ObIArray<uint64_t> &dominated_idxs);
  int64_t get_dim_count() const { return index_dims_.count(); }
  static int extract_column_ids(const common::ObIArray<ObRawExpr*> &keys,
                                const int64_t prefix_count,
                                common::ObIArray<uint64_t> &column_ids);

private:
  common::ObArray<const ObIndexSkylineDim *> index_dims_;
};

class ObSkylineDimFactory
{
public:
  static ObSkylineDimFactory &get_instance();
  template<typename SkylineDimType>
  int create_skyline_dim(common::ObIAllocator &allocator,
                         SkylineDimType *&skyline_dim);
private:
  ObSkylineDimFactory() {}
  DISALLOW_COPY_AND_ASSIGN(ObSkylineDimFactory);
};

template<typename SkylineDimType>
int ObSkylineDimFactory::create_skyline_dim(common::ObIAllocator &allocator,
                                            SkylineDimType *&skyline_dim)
{
  int ret = common::OB_SUCCESS;
  void *ptr = allocator.alloc(sizeof(SkylineDimType));
  skyline_dim = NULL;
  if (OB_ISNULL(ptr)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SQL_OPT_LOG(WARN, "allocate memory for skyline dim failed", K(ret));
  } else {
    skyline_dim = new (ptr) SkylineDimType();
  }
  return ret;
}


} //end of namespace sql
} //end of namespace oceanbase

#endif //end of OCEANBASE_SQL_OPTIMIZER_OB_SKYLINE_PRUNNING_H_
