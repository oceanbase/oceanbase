/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_PART_CALC_H_
#define OCEANBASE_OBSERVER_OB_TABLE_PART_CALC_H_
#include "observer/table/ob_table_cache.h"
#include "ob_table_part_clip.h"

namespace oceanbase
{

namespace table
{

class ObTablePartCalculator
{
public:
  explicit ObTablePartCalculator(ObTableApiSessGuard &sess_guard,
                                 ObKvSchemaCacheGuard &kv_schema_guard,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 const share::schema::ObSimpleTableSchemaV2 *simple_schema,
                                 ObTablePartClipType clip_type = ObTablePartClipType::NONE)
      : allocator_("TbPartCalc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        sess_guard_(sess_guard),
        kv_schema_guard_(kv_schema_guard),
        schema_guard_(schema_guard),
        tb_ctx_(nullptr),
        simple_schema_(simple_schema),
        table_schema_(nullptr),
        clip_type_(clip_type)
  {}
  explicit ObTablePartCalculator(ObTableApiSessGuard &sess_guard,
                                 ObKvSchemaCacheGuard &kv_schema_guard,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 const share::schema::ObTableSchema *table_schema,
                                 ObTablePartClipType clip_type = ObTablePartClipType::NONE)
      : allocator_("TbPartCalc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        sess_guard_(sess_guard),
        kv_schema_guard_(kv_schema_guard),
        schema_guard_(schema_guard),
        tb_ctx_(nullptr),
        simple_schema_(nullptr),
        table_schema_(table_schema),
        clip_type_(clip_type)
  {}

  explicit ObTablePartCalculator(ObTableApiSessGuard &sess_guard,
                                 ObKvSchemaCacheGuard &kv_schema_guard,
                                 share::schema::ObSchemaGetterGuard &schema_guard,
                                 ObTablePartClipType clip_type = ObTablePartClipType::NONE)
      : allocator_("TbPartCalc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        sess_guard_(sess_guard),
        kv_schema_guard_(kv_schema_guard),
        schema_guard_(schema_guard),
        tb_ctx_(nullptr),
        simple_schema_(nullptr),
        table_schema_(nullptr),
        clip_type_(clip_type)
  {}

  ~ObTablePartCalculator()
  {
    if (OB_NOT_NULL(tb_ctx_)) {
      tb_ctx_->~ObTableCtx();
    }
  }
public:
  // 计算单个 entity 的 tablet id
  int calc(uint64_t table_id,
           const ObITableEntity &entity,
           common::ObTabletID &tablet_id);
  // 计算单个 entity 的 tablet id
  int calc(const common::ObString table_name,
           const ObITableEntity &entity,
           common::ObTabletID &tablet_id);
  // 计算多个 entity 的 tablet id
  int calc(uint64_t table_id,
           const common::ObIArray<const ObITableEntity*> &entities,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  // 计算多个 entity 的 tablet id
  int calc(const common::ObString table_name,
           const common::ObIArray<const ObITableEntity*> &entities,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  // 计算单个 range 的 tablet id
  int calc(uint64_t table_id,
           const common::ObNewRange &range,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  // 计算单个 range 的 tablet id
  int calc(const common::ObString table_name,
           const common::ObNewRange &range,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  // 计算多个 range 的 tablet id
  int calc(uint64_t table_id,
           const common::ObIArray<ObNewRange> &ranges,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  // 计算多个 range 的 tablet id
  int calc(const common::ObString table_name,
           const common::ObIArray<ObNewRange> &ranges,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  OB_INLINE void set_clip_type(ObTablePartClipType clip_type) { clip_type_ = clip_type; }
  int calc(const share::schema::ObSimpleTableSchemaV2 *simple_schema,
           ObHCfRows &same_cf_rows,
           bool &is_same_ls,
           ObLSID &ls_id);
private:
  void clear_evaluated_flag();
  int init_tb_ctx(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                  const ObITableEntity &entity,
                  bool need_das_ctx);
  int create_plan(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                  const ObITableEntity &entity,
                  bool need_das_ctx,
                  ObTableApiSpec *&spec);
  int get_simple_schema(const common::ObString table_name,
                        const share::schema::ObSimpleTableSchemaV2 *&simple_schema);
  int get_simple_schema(uint64_t table_id,
                        const share::schema::ObSimpleTableSchemaV2 *&simple_schema);
  int get_table_schema(const common::ObString table_name,
                       const share::schema::ObTableSchema *&table_schema);
  int get_table_schema(uint64_t table_id,
                       const share::schema::ObTableSchema *&table_schema);
  int calc(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
           const ObITableEntity &entity,
           common::ObTabletID &tablet_id);
  int eval(const common::ObIArray<sql::ObExpr *> &new_row,
           sql::ObExpr &part_id_expr,
           const ObITableEntity &entity,
           common::ObTabletID &tablet_id);
  int calc(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
           const common::ObIArray<const ObITableEntity*> &entities,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  int calc(const ObTableSchema &table_schema,
           const ObIArray<ObITableEntity*> &entities,
           ObIArray<ObTabletID> &tablet_ids);
  int calc(const share::schema::ObTableSchema &table_schema,
           const ObITableEntity &entity,
           ObTabletID &tablet_id);
  int calc(const share::schema::ObTableSchema &table_schema,
           const common::ObNewRange &range,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  int calc(const share::schema::ObTableSchema &table_schema,
           const common::ObNewRange &part_range,
           const common::ObNewRange &subpart_range,
           sql::ObDASTabletMapper &tablet_mapper,
           common::ObIArray<common::ObTabletID> &tablet_ids);
  int get_hash_like_object(const common::ObObj &part_obj,
                           common::ObObj &hash_obj);
  int get_hash_like_tablet_id(sql::ObDASTabletMapper &tablet_mapper,
                              schema::ObPartitionLevel part_level,
                              const common::ObNewRange &part_range,
                              common::ObObjectID part_id,
                              common::ObIArray<common::ObTabletID> &tablet_ids,
                              common::ObIArray<common::ObObjectID> &part_ids);
  int construct_part_range(const share::schema::ObTableSchema &table_schema,
                           const common::ObNewRange &range,
                           common::ObNewRange &part_range,
                           common::ObNewRange &subpart_range);
  int construct_part_range(const share::schema::ObTableSchema &table_schema,
                           const common::ObNewRange &range,
                           const common::ObIArray<uint64_t> &col_ids,
                           common::ObNewRange &part_range);
  int get_part_column_ids(const ObPartitionKeyInfo &key_info,
                          common::ObIArray<uint64_t> &col_ids);
  int construct_entity(const common::ObObj *objs,
                       int64_t obj_cnt,
                       ObITableEntity &entity);
  int construct_part_row(const ObTableSchema &table_schema,
                         const ObITableEntity &entity,
                         common::ObNewRow &part_row,
                         common::ObNewRow &subpart_row);
  int construct_part_row(const ObTableSchema &table_schema,
                         const ObITableEntity &entity,
                         const ObIArray<uint64_t> &col_ids,
                         ObNewRow &part_row);
  int calc_partition_id(const ObPartitionLevel part_level,
                        const ObObjectID part_id,
                        const common::ObNewRow &row,
                        sql::ObDASTabletMapper &tablet_mapper,
                        common::ObTabletID &tablet_id,
                        ObObjectID &object_id,
                        const bool is_hash_like);
  int eval(const common::ObIArray<sql::ObExpr *> &new_row,
          const ObITableEntity &entity,
          sql::ObExpr &expr,
          const ObTableColumnInfo &col_info,
          common::ObObj &result);
  int calc_generated_col(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                         const common::ObNewRange &range,
                         const ObTableColumnInfo &col_info,
                         common::ObObj &start,
                         common::ObObj &end);
  int calc_generated_col(const ObSimpleTableSchemaV2 &simple_schema,
                         const ObITableEntity &entity,
                         const ObTableColumnInfo &col_info,
                         bool need_das_ctx,
                         ObObj &gen_col_value);
  int get_ctdef_by_table_id();
  int check_param(const ObTableColumnInfo &col_info,
                  const common::ObIArray<sql::ObExpr *> &new_row,
                  ObRowkey &rowkey,
                  bool &is_min,
                  bool &is_max);
  int construct_series_entity(const ObITableEntity &entity,
                              ObTableEntity &series_entity);
  bool is_single_rowkey(const common::ObNewRange &range) const;
  int clip(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
           common::ObIArray<common::ObTabletID> &tablet_ids);
private:
  common::ObArenaAllocator allocator_;
  ObTableApiSessGuard &sess_guard_;
  ObKvSchemaCacheGuard &kv_schema_guard_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  ObTableCtx *tb_ctx_;
  ObTableApiSpec *spec_ = nullptr;
  observer::ObReqTimeGuard req_time_guard_; // libcache relies on ObReqTimeGuard for elimination
  ObTableApiCacheGuard cache_guard_;
  const share::schema::ObSimpleTableSchemaV2 *simple_schema_;
  const share::schema::ObTableSchema *table_schema_;
  ObTablePartClipType clip_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTablePartCalculator);
};


} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_PART_CALC_H_ */
