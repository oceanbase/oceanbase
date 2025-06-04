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


#ifndef OCEANBASE_SHARE_VECTOR_INDEX_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "share/schema/ob_table_schema.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "share/vector_type/ob_vector_common_util.h"

namespace oceanbase
{
namespace share
{

enum VecColType {
  IVF_CENTER_ID_COL = 0,
  IVF_CENTER_VECTOR_COL,
  IVF_FLAT_DATA_VECTOR_COL,
  IVF_SQ8_DATA_VECTOR_COL,
  IVF_META_ID_COL,
  IVF_META_VECTOR_COL,
  IVF_PQ_CENTER_ID_COL,
  IVF_PQ_CENTER_IDS_COL,
  IVF_PQ_CENTER_VECTOR_COL,
  MAX_COL_TYPE
};

enum ObVecAuxTableIdx { //FARM COMPAT WHITELIST
  VALID_VID_SCAN_IDX = 0,
  FIRST_VEC_AUX_TBL_IDX = 1,
  SECOND_VEC_AUX_TBL_IDX = 2,
  THIRD_VEC_AUX_TBL_IDX = 3,
  FOURTH_VEC_AUX_TBL_IDX = 4,
  FIFTH_VEC_AUX_TBL_IDX = 5,
};

enum ObVectorIndexDistAlgorithm
{
  VIDA_L2 = 0,
  VIDA_IP = 1,
  VIDA_COS = 2,
  VIDA_MAX
};

enum ObVectorIndexAlgorithmLib
{
  VIAL_VSAG = 0,
  VIAL_OB,
  VIAL_MAX
};

enum ObVectorIndexType
{
  VIT_HNSW_INDEX = 0,
  VIT_IVF_INDEX = 1,
  VIT_SPIV_INDEX = 2,
  VIT_MAX
};

enum ObVectorIndexAlgorithmType : uint16_t
{
  VIAT_HNSW = 0,
  VIAT_HNSW_SQ,
  VIAT_IVF_FLAT,
  VIAT_IVF_SQ8,
  VIAT_IVF_PQ,
  VIAT_HNSW_BQ,
  VIAT_HGRAPH,
  VIAT_SPIV,
  VIAT_MAX
};

enum ObKmeansAlgoType
{
  KAT_ELKAN = 0,
  KAT_MAX
};
const static double VEC_ESTIMATE_MEMORY_FACTOR = 2.0;
constexpr static uint32_t VEC_INDEX_MIN_METRIC = 8;
constexpr const static char* const VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_MAX] = {
  "l2",
  "ip",
  "cosine",
};

struct ObIvfConstant {
  static const int SQ8_META_STEP_SIZE = 255;
  static const int SQ8_META_ROW_COUNT = 3; // max, min, step
  static const int SQ8_META_MIN_IDX = 0;
  static const int SQ8_META_MAX_IDX = 1;
  static const int SQ8_META_STEP_IDX = 2;
};

struct ObVectorIndexAlgorithmHeader
{
  ObVectorIndexAlgorithmType type_;
  OB_UNIS_VERSION(1);
};

// TODO: opt struct
struct ObVectorIndexParam
{
  ObVectorIndexParam() :
    type_(VIAT_MAX), lib_(VIAL_MAX), dim_(0), m_(0), ef_construction_(0), ef_search_(0), nlist_(0), sample_per_nlist_(0), extra_info_max_size_(0), extra_info_actual_size_(0)
  {}
  void reset() {
    type_ = VIAT_MAX;
    lib_ = VIAL_MAX;
    dist_algorithm_ = VIDA_MAX;
    dim_ = 0;
    m_ = 0;
    ef_construction_ = 0;
    ef_search_ = 0;
    nlist_ = 0;
    sample_per_nlist_ = 0;
    extra_info_max_size_ = 0;
    extra_info_actual_size_ = 0;
  };
  ObVectorIndexAlgorithmType type_;
  ObVectorIndexAlgorithmLib lib_;
  ObVectorIndexDistAlgorithm dist_algorithm_;
  int64_t dim_;
  int64_t m_;
  int64_t ef_construction_;
  int64_t ef_search_;
  int64_t nlist_;
  int64_t sample_per_nlist_;
  // 0: close, 1: open, else: max_size
  // default: 1024
  int64_t extra_info_max_size_;
  int64_t extra_info_actual_size_;
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(type), K_(lib), K_(dist_algorithm), K_(dim), K_(m), K_(ef_construction), K_(ef_search),
    K_(nlist), K_(sample_per_nlist), K_(extra_info_max_size), K_(extra_info_actual_size));
};

class ObExprVecIvfCenterIdCache
{
public:
  ObExprVecIvfCenterIdCache()
    : table_id_(ObCommonID::INVALID_ID),
      tablet_id_(),
      centers_(),
      allocator_("IvfCIdCache", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  virtual ~ObExprVecIvfCenterIdCache() {}
  bool hit(ObTableID table_id, ObTabletID tablet_id) { return table_id == table_id_ && tablet_id == tablet_id_; }
  int get_centers(ObIArray<float*> &centers) { return centers.assign(centers_); }
  int update_cache(ObTableID table_id, ObTabletID tablet_id, ObIArray<float*> &centers)
  {
    table_id_ = table_id;
    tablet_id_ = tablet_id;
    return centers_.assign(centers);
  }
  ObArenaAllocator &get_allocator() { return allocator_; }
  void reuse() { table_id_ = ObCommonID::INVALID_ID; tablet_id_.reset(); centers_.reuse(); allocator_.reuse(); }
private:
  ObTableID table_id_;
  ObTabletID tablet_id_;
  ObSEArray<float*, 8> centers_;
  ObArenaAllocator allocator_;
};

struct IvfIndexTableInfo {
  IvfIndexTableInfo() : table_id_(OB_INVALID_ID), schema_version_(OB_INVALID_ID) {}
  IvfIndexTableInfo(const uint64_t table_id, const uint64_t schema_version)
    : table_id_(table_id), schema_version_(schema_version) {}
  ~IvfIndexTableInfo() {}
  TO_STRING_KV(K(table_id_), K(schema_version_));
  uint64_t table_id_;
  uint64_t schema_version_;
};

class ObVectorIndexUtil final
{
  static const int64_t DEFAULT_VEC_INSERT_BATCH_SIZE = 10;
  class ObExprVecIvfCenterIdCtx : public sql::ObExprOperatorCtx
  {
  public:
    ObExprVecIvfCenterIdCtx()
      : ObExprOperatorCtx(),
        cache_(),
        pq_cache_()
    {}
    virtual ~ObExprVecIvfCenterIdCtx() {}
    ObExprVecIvfCenterIdCache *get_cache() { return &cache_; }
    ObExprVecIvfCenterIdCache *get_pq_cache() { return &pq_cache_; }
  private:
    ObExprVecIvfCenterIdCache cache_;
    ObExprVecIvfCenterIdCache pq_cache_;
  };
public:
  static int construct_rebuild_index_param(
      const ObString &old_index_params,
      ObString &new_index_params,
      common::ObIAllocator *allocator);
  static int check_extra_info_size(
      const ObTableSchema &tbl_schema,
      const sql::ObSQLSessionInfo *session_info,
      bool is_extra_max_size_set,
      int64_t extra_info_max_size,
      int64_t& extra_info_actual_size);
  static int update_param_extra_actual_size(const ObTableSchema &data_schema, ObTableSchema &index_schema);
  static int check_vec_index_param(
      const uint64_t tenant_id,
      const ParseNode *option_node,
      common::ObIAllocator &allocator,
      const ObTableSchema &tbl_schema,
      ObString &index_params,
      ObString &vec_column_name,
      ObIndexType &vec_index_type,
      sql::ObSQLSessionInfo *session_info);
  static int parser_params_from_string(
      const ObString &origin_string,
      ObVectorIndexType vector_index_type,
      ObVectorIndexParam &param,
      const bool set_default=true);
  static int filter_index_param(
    const ObString &index_param_str,
    const char *to_filter,
    char *filtered_param_str,
    int32_t &res_len);
  static int print_index_param(
      const ObTableSchema &table_schema,
      char *buf,
      const int64_t &buf_len,
      int64_t &pos);
  static int check_distance_algorithm_match(
      ObSchemaGetterGuard &schema_guard,
      const schema::ObTableSchema &table_schema,
      const ObString &index_column_name,
      const ObItemType type,
      bool &is_match);
  static int insert_index_param_str(
      const ObString &new_add_param,
      ObIAllocator &allocator,
      ObString &current_index_param);
  static int get_index_name_prefix(
      const schema::ObTableSchema &index_schema,
      ObString &prefix);
  static int check_ivf_lob_inrow_threshold(
    const int64_t tenant_id,
    const ObString &database_name,
    const ObString &table_name,
    ObSchemaGetterGuard &schema_guard,
    const int64_t lob_inrow_threshold);
  static int check_table_has_vector_of_fts_index(
      const ObTableSchema &data_table_schema,
      ObSchemaGetterGuard &schema_guard,
      bool &has_fts_index,
      bool &has_vec_index);
  static int check_column_has_vector_index(
      const ObTableSchema &data_table_schema,
      ObSchemaGetterGuard &schema_guard,
      const int64_t col_id,
      bool &is_column_has_vector_index,
      ObIndexType& index_type);
  static int check_has_extra_info(
      const ObTableSchema &data_table_schema,
      ObSchemaGetterGuard &schema_guard,
      bool &has_extra_info);
  static int check_vec_aux_index_deleted(
      ObSchemaGetterGuard &schema_guard,
      const schema::ObTableSchema &table_schema,
      bool &is_all_deleted);
  static int check_vector_index_by_column_name(
      ObSchemaGetterGuard &schema_guard,
      const schema::ObTableSchema &table_schema,
      const ObString &index_column_name,
      bool &is_valid);
  static int get_vector_index_column_name(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      ObIArray<ObString> &col_names);
  static bool is_match_index_column_name(
      const schema::ObTableSchema &table_schema,
      const schema::ObTableSchema &index_schema,
      const ObString &index_column_name);
  static int get_vector_index_column_id(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      ObIArray<uint64_t> &col_ids);

  static int get_extra_info_column_id(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      ObSEArray<uint64_t, 4> &extra_col_ids);

  static int get_vector_index_column_dim(const ObTableSchema &index_table_schema, int64_t &dim);
  static int get_vector_index_column_dim(
      const ObTableSchema &index_table_schema,
      const ObTableSchema &data_table_schema,
      int64_t &dim);
  static int check_rowkey_cid_table_readable(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const uint64_t column_id,
      uint64_t &tid,
      const bool allow_unavailable = false);
  static int get_right_index_tid_in_rebuild(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const ObIndexType index_type,
      const int64_t base_col_id,
      const ObColumnSchemaV2 *column_schema,
      uint64_t &tid);
  static int get_vector_index_tid(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const ObIndexType index_type,
      const int64_t col_id, // index col id
      uint64_t &tid);
  static int get_latest_avaliable_index_tids_for_hnsw(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const int64_t col_id,
    uint64_t &inc_tid,
    uint64_t &vbitmap_tid,
    uint64_t &snapshot_tid);
  static int get_vector_index_tid_with_index_prefix(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const ObIndexType index_type,
    const int64_t col_id,
    ObString &index_prefix,
    uint64_t &tid);
  static int get_vector_index_tid_check_valid(
      sql::ObSqlSchemaGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const ObIndexType index_type,
      const int64_t vec_cid_col_id,
      uint64_t &tid);
  static int get_vector_index_tids(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const ObIndexType index_type,
      const int64_t col_id,
      ObIArray<IvfIndexTableInfo> &tids);
  static int get_vector_index_param(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const int64_t col_id,
      ObVectorIndexParam &param);
  static int get_vector_index_type(
      sql::ObRawExpr *&raw_expr,
      const ObVectorIndexParam &param,
      ObIArray<ObIndexType> &type_array);
  static int get_vector_domain_index_type(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const int64_t col_id, // index col id
      ObIndexType &index_type);
  static int is_sparse_vec_col(
      const ObIArray<ObString> &extend_type_info,
      bool &is_sparse_vec_col);
  static int get_vector_dim_from_extend_type_info(
      const ObIArray<ObString> &extend_type_info,
      int64_t &dim);
  static int generate_new_index_name(
      ObIAllocator &allocator,
      ObString &new_index_name);
  static int generate_switch_index_names(
      const ObString &old_domain_index_name,
      const ObString &new_domain_index_name,
      const ObIndexType index_type,
      ObIAllocator &allocator,
      ObIArray<ObString> &old_table_names,
      ObIArray<ObString> &new_table_names);
  static int update_index_tables_status(
      const int64_t tenant_id,
      const int64_t database_id,
      const ObIArray<ObString> &old_table_names,
      const ObIArray<ObString> &new_table_names,
      rootserver::ObDDLOperator &ddl_operator,
      ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      ObIArray<ObTableSchema> &table_schemas);
  static int update_index_tables_attributes(
      const int64_t tenant_id,
      const int64_t database_id,
      const int64_t data_table_id,
      const int64_t expected_update_table_cnt,
      const ObIArray<ObString> &old_table_names,
      const ObIArray<ObString> &new_table_names,
      rootserver::ObDDLOperator &ddl_operator,
      ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      ObIArray<ObTableSchema> &table_schemas);
  static int construct_new_column_schema_from_exist(
      const ObColumnSchemaV2 *old_column_ptr,
      const ObColumnSchemaV2 *&new_column_ptr,
      const VecColType col_type,
      ObColumnSchemaV2 &new_column,
      uint64_t &available_col_id);
  static int set_new_index_column(
      ObTableSchema &new_index_schema,
      const ObColumnSchemaV2 *old_column_ptr,
      const ObColumnSchemaV2 *&new_column_ptr);
  static int reconstruct_ivf_index_schema_in_rebuild(
      rootserver::ObDDLSQLTransaction &trans,
      rootserver::ObDDLService &ddl_service,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const ObTableSchema &data_table_schema,
      ObTableSchema &new_index_schema);
  static int generate_index_schema_from_exist_table(
      rootserver::ObDDLSQLTransaction &trans,
      const int64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      rootserver::ObDDLService &ddl_service,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const ObTableSchema &data_table_schema,
      ObTableSchema &new_index_schema);
  static int get_dropping_vec_index_invisiable_table_schema(
      const ObTableSchema &index_table_schema,
      const uint64_t data_table_id,
      const bool is_vec_inner_drop,
      share::schema::ObSchemaGetterGuard &schema_guard,
      rootserver::ObDDLOperator &ddl_operator,
      common::ObMySQLTransaction &trans,
      common::ObIArray<share::schema::ObTableSchema> &new_aux_schemas);
  static int check_drop_vec_indexs_ith_valid(
      const ObIndexType index_type, const int64_t schema_count,
      int64_t &rowkey_vid_ith, int64_t &vid_rowkey_ith,
      int64_t &domain_index_ith, int64_t &index_id_ith,
      int64_t &snapshot_data_ith, int64_t &centroid_ith,
      int64_t &cid_vector_ith, int64_t &rowkey_cid_ith,
      int64_t &sq_meta_ith, int64_t &pq_centroid_ith,
      int64_t &pq_code_ith);

  static int add_dbms_vector_jobs(common::ObISQLClient &sql_client, const uint64_t tenant_id,
                                  const uint64_t vidx_table_id,
                                  const common::ObString &exec_env);
  static int remove_dbms_vector_jobs(common::ObISQLClient &sql_client, const uint64_t tenant_id,
                                     const uint64_t vidx_table_id);
  static int get_dbms_vector_job_info(common::ObISQLClient &sql_client,
                                      const uint64_t tenant_id,
                                      const uint64_t vidx_table_id,
                                      common::ObIAllocator &allocator,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      dbms_scheduler::ObDBMSSchedJobInfo &job_info);
  static bool has_multi_index_on_same_column(
      ObIArray<uint64_t> &vec_index_cols,
      const uint64_t col_id);
  static int check_table_exist(
      const ObTableSchema &data_table_schema,
      const ObString &domain_index_name);
  static int get_rebuild_drop_index_id_and_name(
      share::schema::ObSchemaGetterGuard &schema_guard,
      obrpc::ObDropIndexArg &arg);
  static int calc_residual_vector(
      ObIAllocator &alloc,
      int dim,
      ObIArray<float *> &centers,
      float *vector,
      ObVectorNormalizeInfo *norm_info,
      float *&residual);
  static int calc_residual_vector(
      ObIAllocator &alloc,
      int dim,
      const float *vector,
      const float *center_vec,
      float *&residual
  );
  static int calc_location_ids(sql::ObEvalCtx &eval_ctx,
                               sql::ObExpr *table_id_expr,
                               sql::ObExpr *part_id_expr,
                               ObTableID &table_id,
                               ObTabletID &tablet_id);
  static int eval_ivf_centers_common(ObIAllocator &allocator,
                                    const sql::ObExpr &expr,
                                    sql::ObEvalCtx &eval_ctx,
                                    ObIArray<float*> &centers,
                                    ObTableID &table_id,
                                    ObTabletID &tablet_id,
                                    ObVectorIndexDistAlgorithm &dis_algo,
                                    bool &contain_null,
                                    ObIArrayType *&arr);
  static ObExprVecIvfCenterIdCache* get_ivf_center_id_cache_ctx(const uint64_t& id, sql::ObExecContext *exec_ctx);
  static void get_ivf_pq_center_id_cache_ctx(const uint64_t& id, sql::ObExecContext *exec_ctx, ObExprVecIvfCenterIdCache *&cache, ObExprVecIvfCenterIdCache *&pq_cache);
  static int get_ivf_aux_info(share::ObPluginVectorIndexService *service,
                                  ObExprVecIvfCenterIdCache *cache,
                                  const ObTableID &table_id,
                                  const ObTabletID &tablet_id,
                                  common::ObIAllocator &allocator,
                                  ObIArray<float*> &centers);
  static int split_vector(ObIAllocator &alloc, int pq_m, int dim, float *vector, ObIArray<float *> &splited_arrs);
  static int set_extra_info_actual_size_param(ObIAllocator *allocator, const ObString &old_param, int64_t actual_size,
                                       ObString &new_param);
  static bool column_id_asc_compare(uint64_t lhs, uint64_t rhs) { return lhs < rhs; }
  static bool rowexpr_asc_compare(sql::ObRawExpr *lhs, sql::ObRawExpr *rhs)
  {
    return static_cast<sql::ObColumnRefRawExpr *>(lhs)->get_column_id() <
           static_cast<sql::ObColumnRefRawExpr *>(rhs)->get_column_id();
  }
  static int64_t get_hnswsq_type_metric(int64_t origin_metric) {
    return origin_metric / 2 > VEC_INDEX_MIN_METRIC ? origin_metric / 2 : VEC_INDEX_MIN_METRIC;
  }
  static bool check_vector_index_memory(
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema &index_schema,
      const uint64_t tenant_id,
      const int64_t row_count);
  static int estimate_vector_memory_used(
      ObSchemaGetterGuard &schema_guard,
      const ObTableSchema &index_schema,
      const uint64_t tenant_id,
      const int64_t tablet_row_count,
      int64_t &estimate_memory);

private:
  static void save_column_schema(
      const ObColumnSchemaV2 *&old_column,
      const ObColumnSchemaV2 *&new_column,
      const ObColumnSchemaV2 *cur_column);
  static int check_index_param(
      const ParseNode *option_node,
      common::ObIAllocator &allocator,
      const int64_t vector_dim,
      const bool is_sparse_vec,
      ObString &index_params,
      ObIndexType &out_index_type,
      const ObTableSchema &tbl_schema,
      sql::ObSQLSessionInfo *session_info);
  static int generate_hnsw_switch_index_names(
      const ObString &old_domain_index_name,
      const ObString &new_domain_index_name,
      ObIAllocator &allocator,
      ObIArray<ObString> &old_table_names,
      ObIArray<ObString> &new_table_names);
  static int generate_ivfflat_switch_index_names(
      const ObString &old_domain_index_name,
      const ObString &new_domain_index_name,
      ObIAllocator &allocator,
      ObIArray<ObString> &old_table_names,
      ObIArray<ObString> &new_table_names);
  static int generate_ivfsq8_switch_index_names(
      const ObString &old_domain_index_name,
      const ObString &new_domain_index_name,
      ObIAllocator &allocator,
      ObIArray<ObString> &old_table_names,
      ObIArray<ObString> &new_table_names);
  static int generate_ivfpq_switch_index_names(
      const ObString &old_domain_index_name,
      const ObString &new_domain_index_name,
      ObIAllocator &allocator,
      ObIArray<ObString> &old_table_names,
      ObIArray<ObString> &new_table_names);
  static bool is_expr_type_and_distance_algorithm_match(
      const ObItemType expr_type, const ObVectorIndexDistAlgorithm algorithm);
  static int has_same_cascaded_col_id(
      const ObTableSchema &data_table_schema,
      const ObColumnSchemaV2 &col_schema,
      const int64_t col_id,
      bool &has_same_col_id);
  static bool check_is_match_index_type(
      const ObIndexType type1, const ObIndexType type2);
};

// For vector index snapshot write data
class ObVecIdxSnapshotDataWriteCtx final
{
public:
  ObVecIdxSnapshotDataWriteCtx()
    : ls_id_(), data_tablet_id_(), lob_meta_tablet_id_(), lob_piece_tablet_id_(),
      vals_()
  {}
  ~ObVecIdxSnapshotDataWriteCtx() {}
  ObLSID& get_ls_id() { return ls_id_; }
  const ObLSID& get_ls_id() const { return ls_id_; }
  ObTabletID& get_data_tablet_id() { return data_tablet_id_; }
  const ObTabletID& get_data_tablet_id() const { return data_tablet_id_; }
  ObTabletID& get_lob_meta_tablet_id() { return lob_meta_tablet_id_; }
  const ObTabletID& get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  ObTabletID& get_lob_piece_tablet_id() { return lob_piece_tablet_id_; }
  const ObTabletID& get_lob_piece_tablet_id() const { return lob_piece_tablet_id_; }
  ObIArray<ObString>& get_vals() { return vals_; }
  void reset();
  TO_STRING_KV(K(ls_id_), K(data_tablet_id_), K(lob_meta_tablet_id_), K(lob_piece_tablet_id_), K(vals_));
public:
  ObLSID ls_id_;
  ObTabletID data_tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  ObTabletID lob_piece_tablet_id_;
  ObArray<ObString> vals_;
};

typedef struct ObExtraInfoIdxType {
  ObExtraInfoIdxType() : idx_(0), type_() {}
  ObExtraInfoIdxType(const int64_t idx, const common::ObObjMeta type) : idx_(idx), type_(type) {}
  int64_t idx_;
  common::ObObjMeta type_;
  TO_STRING_KV(K_(idx), K_(type));
} ObExtraIdxType;

class ObVecExtraInfoBuffer : public ObStringBuffer {
public:
  ObVecExtraInfoBuffer() : alloctor_("ExtraInfoB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()) {
    set_allocator(&alloctor_);
  }
  virtual ~ObVecExtraInfoBuffer() {}
private:
  common::ObArenaAllocator alloctor_;
};
struct ObVecExtraInfoPtr {
  ObVecExtraInfoPtr() : buf_(nullptr), extra_info_actual_size_(0), count_(0) {}
  int init(ObIAllocator *allocator, const char *src_buf, int64_t extra_info_actual_size, int64_t count);
  int init(ObIAllocator *allocator, int64_t extra_info_actual_size, int64_t count);
  inline void reset() {
    buf_ = nullptr;
    extra_info_actual_size_ = 0;
    count_ = 0;
  }
  inline bool is_null() const { return buf_ == nullptr || count_ == 0; }
  inline const char *operator[](int64_t idx) const
  {
    return (buf_ == nullptr && idx >= count_) ? nullptr : buf_[idx];
  }
  inline int set_with_copy(int64_t idx, const char *src_buf, int64_t extra_info_actual_size)
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(idx >= count_ || OB_ISNULL(src_buf) || extra_info_actual_size != extra_info_actual_size_ ||
                    OB_ISNULL(buf_))) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(idx), K(count_), K(extra_info_actual_size), K(extra_info_actual_size_),
              KP(src_buf), KP(buf_));
    } else {
      MEMCPY(const_cast<char *>(buf_[idx]), src_buf, extra_info_actual_size);
    }

    return ret;
  }
  inline int set_no_copy(int64_t idx, const char *src_buf)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(idx >= count_ || OB_ISNULL(src_buf) || OB_ISNULL(buf_))) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(idx), K(count_), K(extra_info_actual_size_), KP(src_buf), KP(buf_));
    } else {
      buf_[idx] = src_buf;
    }

    return ret;
  }
  const char **buf_;
  int64_t extra_info_actual_size_;
  int64_t count_;
  TO_STRING_KV(KP(buf_), K(extra_info_actual_size_), K(count_));
};

struct ObVecExtraInfoObj {
  ObVecExtraInfoObj() : ptr_(nullptr), len_(0), obj_map_type_(common::ObObjDatumMapType::OBJ_DATUM_NULL) {}
  inline void reset()
  {
    ptr_ = nullptr;
    len_ = 0;
    obj_map_type_ = common::ObObjDatumMapType::OBJ_DATUM_NULL;
  }
  int from_datum(const ObDatum &datum, const common::ObObjMeta &type, ObIAllocator *allocator = nullptr);
  int from_obj(const ObObj &obj, ObIAllocator *allocator = nullptr);
  const char *ptr_;
  int32_t len_;
  common::ObObjDatumMapType obj_map_type_;
  TO_STRING_KV(KP(ptr_), K(len_), K(obj_map_type_));
};

struct ObVecExtraInfo {
public:
  static int extra_infos_to_buf(ObIAllocator &allocator, const ObVecExtraInfoObj *extra_info_obj,
                                int64_t extra_column_count, int64_t extra_info_actual_size, int64_t count, char *&buf);
  static int extra_buf_to_obj(const char *buf, int64_t data_len, int64_t extra_column_count, ObObj *obj);
  static int64_t get_encode_size(const ObIArray<ObVecExtraInfoObj> &extra_obj);
  inline static bool is_obj_type_supported(const ObObjType obj_type)
  {
    const common::ObObjDatumMapType &obj_map_type = common::ObDatum::get_obj_datum_map_type(obj_type);
    bool res = false;
    if (obj_type > 64) {
      res = false;
    } else if (is_fixed_length_type(obj_map_type)) {
      res = true;
    } else if (((int64_t(1) << obj_type) & VARIABLE_LENGTH_TYPE_SUPPROT)) {
      res = true;
    }
    return res;
  }

  inline static bool is_fixed_length_type(const common::ObObjDatumMapType obj_map_type)
  {
    return obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_8BYTE_DATA ||
           obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_4BYTE_DATA ||
           obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_1BYTE_DATA;
  }

  static int extra_info_to_buf(ObIAllocator &allocator, const ObVecExtraInfo *extra_info, int64_t count,
                                  int64_t one_extra_info_size, char *&buf);

private:
  static int extra_info_to_buf(const ObVecExtraInfoObj *extra_obj, int64_t extra_column_count, char *buf, const int64_t buf_len,
                               int64_t &pos);
  static int64_t get_to_buf_size(const ObVecExtraInfoObj *extra_obj, int64_t extra_column_count);
  inline static bool is_legal(const ObVecExtraInfoObj *extra_obj, int64_t extra_column_count)
  {
    return !(nullptr == extra_obj && extra_column_count > 0);
  }

public:
  const static int64_t UNIS_VERSION = 1;
  // now extra_info supports fixed-length types (1 byte, 4 bytes, 8 bytes)
  // as well as some variable-length types.
  const static int64_t VARIABLE_LENGTH_TYPE_SUPPROT = int64_t(1) << common::ObObjType::ObVarcharType;
  const static int64_t FIXED_TYPE_LENGTH = 8;
  const static int64_t EXTRA_INFO_PARAM_MAX_VALUE = 16384;
};


struct ObVecTidCandidate
{
  ObVecTidCandidate() : version_(OB_INVALID_ID), inc_tid_(OB_INVALID_ID) {}
  ObVecTidCandidate(int64_t version, uint64_t inc_tid, ObString &index_prefix)
    : version_(version), inc_tid_(inc_tid), index_prefix_(index_prefix) {}
  int64_t version_;
  uint64_t inc_tid_;
  ObString index_prefix_;
  TO_STRING_KV(K_(version), K_(inc_tid), K_(index_prefix));
};

struct ObVecTidCandidateMaxCompare
{
  bool operator()(const ObVecTidCandidate &lhs, const ObVecTidCandidate &rhs)
  {
    return lhs.version_ < rhs.version_ ? true : false;
  }
  int get_error_code() const { return OB_SUCCESS; }
};

}  // namespace share
}  // namespace oceanbase

#endif
