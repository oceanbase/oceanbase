//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_TABLE_CKM_ITEMS_H_
#define OB_SHARE_COMPACTION_TABLE_CKM_ITEMS_H_
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_tablet_replica_checksum_operator.h"
namespace oceanbase
{
namespace compaction
{
struct ObTabletLSPairCache;
struct ObIndexCkmValidatePair
{
public:
  ObIndexCkmValidatePair()
    : data_table_id_(0),
      index_table_id_(0)
  {}
  ObIndexCkmValidatePair(
    const uint64_t data_table_id,
    const uint64_t index_table_id)
    : data_table_id_(data_table_id),
      index_table_id_(index_table_id)
  {}
  ~ObIndexCkmValidatePair() {}

  TO_STRING_KV(K_(data_table_id), K_(index_table_id));

public:
  uint64_t data_table_id_;
  uint64_t index_table_id_;
};

struct ObColumnIdToIdx
{
public:
  ObColumnIdToIdx()
    : column_id_(OB_INVALID_ID),
      idx_(OB_INVALID_INDEX)
  {}
  ObColumnIdToIdx(const int64_t column_id)
    : column_id_(column_id),
      idx_(OB_INVALID_INDEX)
  {}
  ~ObColumnIdToIdx() = default;

  bool operator <(const ObColumnIdToIdx &b) const
  {
    return column_id_ < b.column_id_;
  }
  TO_STRING_KV(K_(column_id), K_(idx));

  int64_t column_id_;
  int64_t idx_;
};

struct ObSortColumnIdArray
{
public:
  ObSortColumnIdArray()
    : is_inited_(false),
      build_map_flag_(false),
      array_()
  {}
  ~ObSortColumnIdArray() { reset(); }
  int build(const uint64_t tenant_id, const share::schema::ObTableSchema &table_schema);
  bool is_inited() const { return is_inited_; }
  static int get_array_idx_by_column_id(ObSortColumnIdArray& sort_array, const int64_t column_id, int64_t &array_idx)
  {
    return NULL == sort_array.get_func_ ? -1 : sort_array.get_func_(sort_array, column_id, array_idx);
  }
  void reset();
  TO_STRING_KV(K_(is_inited), K_(build_map_flag), K_(array), "map_size", map_.size());

private:
  int build_hash_map(const uint64_t tenant_id, const ObIArray<share::schema::ObColDesc> &column_descs);
  int build_sort_array(const ObIArray<share::schema::ObColDesc> &column_descs);
  static int32_t get_func_from_array(ObSortColumnIdArray &sort_array, const int64_t column_id, int64_t &input_array_idx);
  static int32_t get_func_from_map(ObSortColumnIdArray &sort_array, const int64_t column_id, int64_t &input_array_idx);
  typedef int32_t (*GET_FUNC)(ObSortColumnIdArray&, const int64_t, int64_t&);
  typedef hash::ObHashMap<int64_t, int64_t> ColIdToIdxMap;
  typedef common::ObSEArray<ObColumnIdToIdx, share::ObTabletReplicaReportColumnMeta::DEFAULT_COLUMN_CNT> ColIdToIdxArray;

  static const int64_t BUILD_HASH_MAP_TABLET_CNT_THRESHOLD = 2048;
  bool is_inited_;
  bool build_map_flag_;
  GET_FUNC get_func_;
  ColIdToIdxArray array_;
  ColIdToIdxMap map_;
};

struct ObTableCkmItems
{
public:
  ObTableCkmItems(const uint64_t tenant_id = MTL_ID());
  ~ObTableCkmItems();
  bool is_inited() const { return is_inited_; }
  void set_is_fts_index(const bool is_fts_index) { is_fts_index_ = is_fts_index; }
  void clear();
  void reset();
  int64_t get_table_id() const { return table_id_; }
  const share::schema::ObTableSchema * get_table_schema() const { return table_schema_; }
  const common::ObIArray<share::ObTabletReplicaChecksumItem> &get_ckm_items() const { return ckm_items_; }
  const common::ObIArray<share::ObTabletLSPair> &get_tablet_ls_pairs() const { return tablet_pairs_; }
  int build(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObSimpleTableSchemaV2 &simple_schema,
    const ObArray<share::ObTabletLSPair> &input_tablet_pairs,
    const ObArray<share::ObTabletReplicaChecksumItem> &input_ckm_items);
  int build(
    const uint64_t table_id,
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const compaction::ObTabletLSPairCache &tablet_ls_pair_cache);
  int build_column_ckm_sum_array(
    const share::SCN &compaction_scn,
    const share::schema::ObTableSchema &table_schema,
    int64_t &row_cnt);
  typedef int (*VALIDATE_CKM_FUNC)(
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm);
  static const int64_t FUNC_CNT = 2;
  static VALIDATE_CKM_FUNC validate_ckm_func[FUNC_CNT];
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(table_id), "tablet_cnt", tablet_pairs_.count(),
    "ckm_item_cnt", ckm_items_.count(), K_(sort_col_id_array),
    "col_ckm_sum_array_size", ckm_sum_array_.count());

private:
  static int validate_column_ckm_sum(
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm);
  static int validate_tablet_column_ckm(
    const share::SCN &compaction_scn,
    common::ObMySQLProxy &sql_proxy,
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm);
  static int compare_ckm_by_column_ids(
    ObTableCkmItems &data_ckm,
    ObTableCkmItems &index_ckm,
    const share::schema::ObTableSchema &data_table_schema,
    const share::schema::ObTableSchema &index_table_schema,
    const ObIArray<int64_t> &data_replica_ckm_array,
    const ObIArray<int64_t> &index_replica_ckm_array,
    share::ObColumnChecksumErrorInfo &ckm_error_info);
  int64_t get_replica_checksum_idx(
    const int64_t last_tablet_idx,
    const ObTabletID &tablet_id) const;

  static const int64_t DEFAULT_COLUMN_CNT = 64;
  static const int64_t DEFAULT_TABLET_CNT = 16;
  bool is_inited_;
  bool is_fts_index_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t row_count_;
  const share::schema::ObTableSchema *table_schema_;
  common::ObSEArray<share::ObTabletLSPair, DEFAULT_TABLET_CNT> tablet_pairs_;
  common::ObArray<share::ObTabletReplicaChecksumItem> ckm_items_; // order by TableSchema::tablet_ids
  ObSortColumnIdArray sort_col_id_array_; // column_id -> array_idx
  common::ObSEArray<int64_t, DEFAULT_COLUMN_CNT> ckm_sum_array_; // order by TableSchema::tablet_ids
};

typedef common::ObArray<ObIndexCkmValidatePair> ObIndexCkmValidatePairArray;

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_TABLE_CKM_ITEMS_H_
