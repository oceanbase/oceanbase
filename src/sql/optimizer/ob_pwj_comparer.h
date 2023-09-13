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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_PWJ_COMPARER_H
#define OCEANBASE_SQL_OPTIMIZER_OB_PWJ_COMPARER_H 1
#include "lib/container/ob_array.h"
#include "share/partition_table/ob_partition_location.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_sharding_info.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObOptimizerContext;
class ObDMLStmt;
class ObLogPlan;

struct PwjTable {
  PwjTable()
    : phy_table_loc_info_(NULL),
      server_list_(),
      part_level_(share::schema::PARTITION_LEVEL_ZERO),
      part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
      subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
      is_partition_single_(false),
      is_subpartition_single_(false),
      partition_array_(NULL),
      part_number_(-1)
  {}

  virtual ~PwjTable() {}

  int assign(const PwjTable &other);

  int init(const ObShardingInfo &info);
  int init(const share::schema::ObTableSchema &table_schema,
           const ObCandiTableLoc &phy_tbl_info);
  int init(const ObIArray<ObAddr> &server_list);

  TO_STRING_KV(K_(part_level), K_(part_type), K_(subpart_type),
               K_(part_number),
               K_(is_partition_single), K_(is_subpartition_single),
               K_(all_partition_indexes), K_(all_subpartition_indexes),
               K_(server_list));

  const ObCandiTableLoc *phy_table_loc_info_;
  ObSEArray<common::ObAddr, 8, common::ModulePageAllocator, true> server_list_;
  // 分区级别
  share::schema::ObPartitionLevel part_level_;
  // 一级分区类型
  share::schema::ObPartitionFuncType part_type_;
  // 二级分区类型
  share::schema::ObPartitionFuncType subpart_type_;
  // 二级分区表的phy_table_location_info_中，是否只涉及到一个一级分区
  bool is_partition_single_;
  // 二级分区表的phy_table_location_info_中，是否每个一级分区都只涉及一个二级分区
  bool is_subpartition_single_;
  // 一级分区array
  share::schema::ObPartition **partition_array_;
  // 一级分区数量
  int64_t part_number_;
  // phy_table_location_info_中所有的partition_id(物理分区id)
  // ObPwjComparer在生成_id的映射关系时要按照左表这个数组中的partition_id的顺序生成
  common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> ordered_tablet_ids_;
  // phy_table_location_info_中每一个partition_id(物理分区id)的
  // part_id(一级逻辑分区id)在part_array中的偏移
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> all_partition_indexes_;
  // phy_table_location_info_中每一个partition_id(物理分区id)的
  // subpart_id(二级逻辑分区id)在subpart_array中的偏移
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> all_subpartition_indexes_;
};

// TODO yibo 用PartitionIdArray的指针作为value, 否则每次get都要拷贝一次array
typedef common::ObSEArray<uint64_t, 8> TabletIdArray;
typedef common::hash::ObHashMap<uint64_t, TabletIdArray, common::hash::NoPthreadDefendMode> PWJTabletIdMap;
typedef common::hash::ObHashMap<uint64_t, uint64_t, common::hash::NoPthreadDefendMode,
                                common::hash::hash_func<uint64_t>,
                                common::hash::equal_to<uint64_t>,
                                common::hash::SimpleAllocer<typename common::hash::HashMapTypes<uint64_t, uint64_t>::AllocType>,
                                common::hash::NormalPointer,
                                oceanbase::common::ObMalloc,
                                2> TabletIdIdMap;
typedef common::hash::ObHashMap<uint64_t, const ObCandiTabletLoc *,
                                common::hash::NoPthreadDefendMode> TabletIdLocationMap;

class ObPwjComparer
{
public:
  ObPwjComparer(bool is_strict)
    : is_strict_(is_strict), pwj_tables_() {};
  virtual ~ObPwjComparer() {};
  virtual void reset();

  inline bool get_is_strict() { return is_strict_; }

  inline common::ObIArray<PwjTable> &get_pwj_tables() { return pwj_tables_; }

  /**
   * 向ObPwjComparer中添加一个PwjTable，会以第一个添加的PwjTable为基准与后续添加的PwjTable进行比较
   * 其中严格比较会生成tablet_id的映射。
   */
  virtual int add_table(PwjTable &table, bool &is_match_pwj);

  /**
   * 从phy_table_location_info中提取以下分区相关的信息
   * @param all_partition_ids:
   *    phy_table_location_info中所有的partition_id(物理分区id)
   * @param all_partition_indexes:
   *    phy_table_location_info中每一个partition_id(物理分区id)的
   *    part_id(一级逻辑分区id)在part_array中的偏移
   * @param all_subpartition_indexes:
   *    phy_table_location_info中每一个partition_id(物理分区id)的
   *    subpart_id(二级逻辑分区id)在subpart_array中的偏移
   * @param is_partition_single:
   *    二级分区表的phy_table_location_info_中，是否只涉及到一个一级分区
   * @param is_subpartition_single:
   *    二级分区表的phy_table_location_info_中，是否每个一级分区都只涉及一个二级分区
   */
  static int extract_all_partition_indexes(const ObCandiTableLoc &phy_table_location_info,
                                           const share::schema::ObTableSchema &table_schema,
                                           ObIArray<uint64_t> &all_tablet_ids,
                                           ObIArray<int64_t> &all_partition_indexes,
                                           ObIArray<int64_t> &all_subpartition_indexes,
                                           bool &is_partition_single,
                                           bool &is_subpartition_single);

  /**
   * 检查l_partition和r_partition的定义是否相等
   */
  static int is_partition_equal(const share::schema::ObPartition *l_partition,
                                const share::schema::ObPartition *r_partition,
                                const bool is_range_partition,
                                bool &is_equal);

  /**
   * 检查l_subpartition和r_subpartition的定义是否相等
   */
  static int is_subpartition_equal(const share::schema::ObSubPartition *l_subpartition,
                                   const share::schema::ObSubPartition *r_subpartition,
                                   const bool is_range_partition,
                                   bool &is_equal);

  static int is_row_equal(const ObRowkey &first_row,
                          const ObRowkey &second_row,
                          bool &is_equal);

  static int is_list_partition_equal(const share::schema::ObBasePartition *first_partition,
                                     const share::schema::ObBasePartition *second_partition,
                                     bool &is_equal);

  static int is_row_equal(const common::ObNewRow &first_row,
                          const common::ObNewRow &second_row,
                          bool &is_equal);

  static int is_obj_equal(const common::ObObj &first_obj,
                          const common::ObObj &second_obj,
                          bool &is_equal);

  TO_STRING_KV(K_(is_strict), K_(pwj_tables));

protected:
  // 是否以严格模式检查partition wise join
  // 严格模式要求两个基表的分区逻辑上和物理上都相等
  // 非严格模式要求两个基表的数据分布节点相同
  bool is_strict_;
  // 保存一组pwj约束涉及到的基表信息
  common::ObSEArray<PwjTable, 4, common::ModulePageAllocator, true> pwj_tables_;
  static const int64_t MIN_ID_LOCATION_BUCKET_NUMBER;
  static const int64_t DEFAULT_ID_ID_BUCKET_NUMBER;
  DISALLOW_COPY_AND_ASSIGN(ObPwjComparer);
};

class ObStrictPwjComparer : public ObPwjComparer
{
public:
  ObStrictPwjComparer();
  virtual ~ObStrictPwjComparer();
  virtual void reset() override;
  inline common::ObIArray<TabletIdArray> &get_tablet_id_group() { return tablet_id_group_;}

  /**
   * 向ObPwjComparer中添加一个PwjTable，会以第一个添加的PwjTable为基准与后续添加的PwjTable进行严格比较
   * 并生成tablet_id的映射。
   */
  virtual int add_table(PwjTable &table, bool &is_match_pwj) override;

  /**
   * 检查l_table和r_table的分区是否逻辑上相等，并计算出逻辑上相等的partition_id(物理分区id)的映射
   */
  int check_logical_equal_and_calc_match_map(const PwjTable &l_table,
                                             const PwjTable &r_table,
                                             bool &is_match);

  /**
   * 检查l_table和r_table的一级分区是否逻辑上相等
   */
  int is_first_partition_logically_equal(const PwjTable &l_table,
                                         const PwjTable &r_table,
                                         bool &is_equal);

  /**
   * 从所有(sub)part_index中按照升序取出用到的(sub)part_index
   * 例如 all_partition_indexes = [0,0,3,3,1] 可以得到 used_partition_indexes = [0,1,3]
   */
  int get_used_partition_indexes(const int64_t part_count,
                                 const ObIArray<int64_t> &all_partition_indexes,
                                 ObIArray<int64_t> &used_partition_indexes);

  /**
   * 检查l_table和r_table的二级分区是否逻辑上相等
   */
  int is_sub_partition_logically_equal(const PwjTable &l_table,
                                       const PwjTable &r_table,
                                       bool &is_equal);

  /**
   * 获取指定part_index下的所有用到的二级分区的subpart_index，并按升序排列
   */
  int get_subpartition_indexes_by_part_index(const PwjTable &table,
                                             const int64_t part_index,
                                             ObIArray<int64_t> &used_subpart_indexes);

  /**
   * 检查一级hash/key分区是否逻辑上相等, 要求:
   * 1. 左右表分区数量一致
   * 2. 左表每一个part_index都有一个相等的右表part_index
   */
  int check_hash_partition_equal(const PwjTable &l_table,
                                 const PwjTable &r_table,
                                 const ObIArray<int64_t> &l_indexes,
                                 const ObIArray<int64_t> &r_indexes,
                                 ObIArray<std::pair<uint64_t,uint64_t> > &part_tablet_id_map,
                                 ObIArray<std::pair<int64_t,int64_t> > &part_index_map,
                                 bool &is_equal);

  /**
   * 检查二级hash/key分区是否逻辑上相等, 要求:
   * 1. 左右表分区数量一致
   * 2. 左表每一个subpart_index都有一个相等的右表subpart_index
   */
  int check_hash_subpartition_equal(share::schema::ObSubPartition **l_subpartition_array,
                                    share::schema::ObSubPartition **r_subpartition_array,
                                    const ObIArray<int64_t> &l_indexes,
                                    const ObIArray<int64_t> &r_indexes,
                                    ObIArray<std::pair<uint64_t,uint64_t> > &subpart_tablet_id_map,
                                    bool &is_equal);

  bool is_same_part_type(const ObPartitionFuncType part_type1,
                         const ObPartitionFuncType part_type2);

  /**
   * 检查一级range分区是否逻辑上相等, 要求:
   * 1. 左右表对应分区的上界相同
   */
  int check_range_partition_equal(share::schema::ObPartition **left_partition_array,
                                  share::schema::ObPartition **right_partition_array,
                                  const ObIArray<int64_t> &left_indexes,
                                  const ObIArray<int64_t> &right_indexes,
                                  ObIArray<std::pair<uint64_t,uint64_t> > &part_tablet_id_map,
                                  ObIArray<std::pair<int64_t,int64_t> > &part_index_map,
                                  bool &is_equal);

  /**
   * 检查二级range分区是否逻辑上相等, 要求:
   * 1. 左右表对应分区的上界相同
   */
  int check_range_subpartition_equal(share::schema::ObSubPartition **left_subpartition_array,
                                     share::schema::ObSubPartition **right_subpartition_array,
                                     const ObIArray<int64_t> &left_indexes,
                                     const ObIArray<int64_t> &right_indexes,
                                     ObIArray<std::pair<uint64_t,uint64_t> > &subpart_tablet_id_map,
                                     bool &is_equal);

  /**
   * 检查一级list分区是否逻辑上相等, 要求:
   * 1. 左表的每一个分区能在右表找到边界相同的分区
   */
  int check_list_partition_equal(share::schema::ObPartition **left_partition_array,
                                 share::schema::ObPartition **right_partition_array,
                                 const ObIArray<int64_t> &left_indexes,
                                 const ObIArray<int64_t> &right_indexes,
                                 ObIArray<std::pair<uint64_t,uint64_t> > &part_tablet_id_map,
                                 ObIArray<std::pair<int64_t,int64_t> > &part_index_map,
                                 bool &is_equal);

  /**
   * 检查二级list分区是否逻辑上相等, 要求:
   * 1. 左表的每一个分区能在右表找到边界相同的分区
   */
  int check_list_subpartition_equal(share::schema::ObSubPartition **left_subpartition_array,
                                    share::schema::ObSubPartition **right_subpartition_array,
                                    const ObIArray<int64_t> &left_indexes,
                                    const ObIArray<int64_t> &right_indexes,
                                    ObIArray<std::pair<uint64_t,uint64_t> > &subpart_tablet_id_map,
                                    bool &is_equal);

  /**
   * 根据phy_part_map_中的partition_id(物理分区id)的映射关系，检查两表对应的分区是否物理位置相同
   */
  int is_physically_equal_partitioned(const PwjTable &l_table,
                                      const PwjTable &r_table,
                                      bool &is_physical_equal);

  /**
   * 获取part_index(一级逻辑分区在part_array中的偏移)对应分区的part_id(一级逻辑分区id)
   */
  int get_part_tablet_id_by_part_index(const PwjTable &table,
                                       const int64_t part_index,
                                       uint64_t &tablet_id);

  /**
   * 获取二级分区表某个part_index对应的二级分区的subpart_id(二级逻辑分区id)
   */
  int get_sub_part_tablet_id(const PwjTable &table,
                             const int64_t &part_index,
                             uint64_t &sub_part_tablet_id);

private:
  // 保存基表part_id(一级逻辑分区id)的映射关系
  common::ObSEArray<std::pair<uint64_t, uint64_t>, 8, common::ModulePageAllocator, true> part_tablet_id_map_;
  // 保存基表part_index(一级逻辑分区在part_array中的偏移)的映射关系
  common::ObSEArray<std::pair<int64_t, int64_t>, 8, common::ModulePageAllocator, true> part_index_map_;
  // 保存基表subpart_id(二级逻辑分区id)的映射关系
  common::ObSEArray<std::pair<uint64_t, uint64_t>, 8, common::ModulePageAllocator, true> subpart_tablet_id_map_;
  // 保存基表partition_id(物理分区id)的映射关系
  TabletIdIdMap phy_part_map_;
  // 保存一组pwj约束中基表的tablet_id的映射关系
  // 例如 pwj约束中包括[t1,t2,t3],
  //      t1,t2 的tablet_id映射关系是 [0,1,2] <-> [1,2,0]
  //      t2,t3 的tablet_id映射关系是 [0,1,2] <-> [2,1,0]
  // tablet_id_group_ = [[0,1,2], [1,2,0], [1,0,2]]
  common::ObSEArray<TabletIdArray, 4, common::ModulePageAllocator, true> tablet_id_group_;
  DISALLOW_COPY_AND_ASSIGN(ObStrictPwjComparer);
};

class ObNonStrictPwjComparer : public ObPwjComparer
{
public:
  ObNonStrictPwjComparer()
    : ObPwjComparer(false) {};
  virtual ~ObNonStrictPwjComparer() {};
  /**
   * 向ObPwjComparer中添加一个PwjTable，会以第一个添加的PwjTable为基准与后续添加的PwjTable进行非严格比较
   */
  virtual int add_table(PwjTable &table, bool &is_match_nonstrict_pw) override;
  /**
   * check left table and right table have at least one partition on corresponding server
   */
  int is_match_non_strict_partition_wise(PwjTable &l_table,
                                         PwjTable &r_table,
                                         bool &is_match_nonstrict_pw);
private:
  DISALLOW_COPY_AND_ASSIGN(ObNonStrictPwjComparer);
};
}
}
#endif
