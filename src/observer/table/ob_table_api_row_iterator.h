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

#ifndef OB_TABLE_API_ROW_ITERATOR_H_
#define OB_TABLE_API_ROW_ITERATOR_H_

#include "ob_table_service.h"
#include "common/row/ob_row_iterator.h"

namespace oceanbase {
namespace observer {

class ObTableApiRowIterator : public common::ObNewRowIterator {
public:
  ObTableApiRowIterator();
  virtual ~ObTableApiRowIterator();
  int init(
      storage::ObPartitionService &partition_service,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObTableServiceCtx &ctx);
  virtual void reset();
  OB_INLINE common::ObIArray<uint64_t> &get_column_ids() { return column_ids_; }
  OB_INLINE common::ObIArray<common::ObString> &get_properties() { return properties_; }
  OB_INLINE int64_t get_schema_version() { return schema_version_; }
  OB_INLINE int64_t get_rowkey_column_cnt() { return rowkey_column_cnt_; }
protected:
  int check_row(common::ObNewRow &row);
  int entity_to_row(const table::ObITableEntity &entity, common::ObIArray<ObObj> &row);
  int cons_all_columns(const table::ObITableEntity &entity,
                       const bool ignore_missing_column = false,
                       const bool allow_rowkey_in_properties = false);
  int cons_missing_columns(const table::ObITableEntity &entity);
  int fill_get_param(
      ObTableServiceCtx &ctx,
      const table::ObTableOperationType::Type op_type,
      ObRowkey &rowkey,
      storage::ObTableScanParam &scan_param,
      share::schema::ObTableParam &table_param);
  int fill_multi_get_param(
      ObTableServiceCtx &ctx,
      const ObTableBatchOperation &batch_operation,
      storage::ObTableScanParam &scan_param,
      share::schema::ObTableParam &table_param);
  int fill_generate_columns(common::ObNewRow &row);
  virtual bool is_read() const { return false; }
private:
  int check_table_supported(const share::schema::ObTableSchema *table_schema);
  int check_column_type(const sql::ObExprResType &column_type, common::ObObj &obj);
  int fill_range(const ObRowkey &rowkey, ObIArray<common::ObNewRange> &ranges);
  int fill_flag(ObTableServiceCtx &ctx, storage::ObTableScanParam &scan_param);
  int add_column_type(const share::schema::ObColumnSchemaV2 &column_schema);
  int cons_column_type(const share::schema::ObColumnSchemaV2 &column_schema, sql::ObExprResType &column_type);
protected:
  static const int64_t COMMON_COLUMN_NUM = 16;
  storage::ObPartitionService *part_service_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObTableServiceCtx *ctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema *table_schema_;
  int64_t table_id_;
  int64_t tenant_id_;
  int64_t schema_version_;
  int64_t rowkey_column_cnt_;
  common::ObSEArray<common::ObString, COMMON_COLUMN_NUM> properties_;
  common::ObSEArray<uint64_t, COMMON_COLUMN_NUM> column_ids_;
  common::ObSEArray<sql::ObExprResType, COMMON_COLUMN_NUM> columns_type_;
  common::ObSEArray<share::schema::ObColDesc, COMMON_COLUMN_NUM> column_descs_;
  common::ObSEArray<common::ObObj, COMMON_COLUMN_NUM> row_objs_;
  common::ObSEArray<common::ObObj, COMMON_COLUMN_NUM> missing_default_objs_;
  common::ObSEArray<common::ObISqlExpression*, COMMON_COLUMN_NUM> generate_column_exprs_;
  common::ObSEArray<int64_t, COMMON_COLUMN_NUM> generate_column_idxs_;
  common::ObExprCtx expr_ctx_;
  common::ObNewRow row_;
  common::ObArenaAllocator stmt_allocator_;
  common::ObArenaAllocator row_allocator_;
  const table::ObITableEntity *entity_;
  bool has_generate_column_;
  bool is_inited_;
};


class ObTableApiInsertRowIterator : public ObTableApiRowIterator
{
public:
  ObTableApiInsertRowIterator();
  virtual ~ObTableApiInsertRowIterator();
  int open(const ObTableOperation &table_operation);
  virtual int get_next_row(common::ObNewRow *&row);
protected:
  int cons_row(const table::ObITableEntity &entity, common::ObNewRow *&row);
  virtual bool is_read() const override { return false; }
private:
  bool is_iter_end_;
};


class ObTableApiMultiInsertRowIterator : public ObTableApiInsertRowIterator
{
public:
  ObTableApiMultiInsertRowIterator();
  virtual ~ObTableApiMultiInsertRowIterator();
  virtual void reset();
  int open(const ObTableBatchOperation &table_operation);
  virtual int get_next_row(common::ObNewRow *&row);
  OB_INLINE void continue_iter() { is_iter_pause_ = false; }
private:
  const ObTableBatchOperation *batch_operation_;
  int64_t row_idx_;
  int64_t batch_cnt_;
  bool is_iter_pause_;
};


class ObTableApiUpdateRowIterator : public ObTableApiRowIterator
{
public:
  ObTableApiUpdateRowIterator();
  virtual ~ObTableApiUpdateRowIterator();
  virtual void reset();
  int open(const ObTableOperation &table_operation,
      const ObRowkey &rowkey, bool need_update_rowkey = false);
  virtual int get_next_row(common::ObNewRow *&row);
  OB_INLINE common::ObIArray<uint64_t> &get_update_column_ids() { return update_column_ids_; }
  OB_INLINE common::ObNewRow *get_cur_new_row() { return new_row_; }
protected:
  int cons_update_columns(bool need_update_rowkey);
  int cons_new_row(const ObTableOperation &table_operation, common::ObNewRow *&row);
  virtual bool is_read() const override { return false; }
private:
  int obj_increment(
      const common::ObObj &delta,
      const common::ObObj &src,
      const sql::ObExprResType &target_type,
      common::ObObj &target);
  int obj_append(
      const common::ObObj &delta,
      const common::ObObj &src,
      const sql::ObExprResType &target_type,
      common::ObObj &target);
  int int_add_int_with_check(
      int64_t old_int,
      int64_t delta_int,
      common::ObObjType result_type,
      common::ObObj &result);
  int uint_add_int_with_check(
      uint64_t old_uint,
      int64_t delta_int,
      common::ObObjType result_type,
      common::ObObj &result);
protected:
  storage::ObTableScanParam scan_param_;
  share::schema::ObTableParam table_param_;
  common::ObSEArray<uint64_t, COMMON_COLUMN_NUM> update_column_ids_;
  common::ObNewRowIterator *scan_iter_;
  common::ObNewRow *old_row_;
  common::ObNewRow *new_row_;
  int64_t row_idx_;
  bool need_update_rowkey_;
private:
  const ObTableOperation *table_operation_;
};


class ObTableApiMultiUpdateRowIterator : public ObTableApiUpdateRowIterator
{
public:
  ObTableApiMultiUpdateRowIterator();
  virtual ~ObTableApiMultiUpdateRowIterator();
  virtual void reset();
  int open(const ObTableBatchOperation &batch_operation);
  virtual int get_next_row(common::ObNewRow *&row);
  OB_INLINE void continue_iter() { is_iter_pause_ = false; }
  OB_INLINE int64_t get_cur_update_idx() { return cur_update_idx_; }
  OB_INLINE bool has_finished() { return batch_idx_ >= batch_cnt_; }
private:
  const ObTableBatchOperation *batch_operation_;
  int64_t batch_cnt_;
  int64_t batch_idx_;
  int64_t cur_update_idx_;
  bool is_iter_pause_;
};


class ObTableApiDeleteRowIterator : public ObTableApiRowIterator
{
public:
  ObTableApiDeleteRowIterator();
  virtual ~ObTableApiDeleteRowIterator();
  virtual void reset();
  int open(const ObTableOperation &table_operation);
  virtual int get_next_row(common::ObNewRow *&row);
  OB_INLINE common::ObIArray<uint64_t> &get_delete_column_ids() { return column_ids_; }
protected:
  virtual bool is_read() const override { return false; }
protected:
  storage::ObTableScanParam scan_param_;
  share::schema::ObTableParam table_param_;
  common::ObNewRowIterator *scan_iter_;
};


class ObTableApiMultiDeleteRowIterator : public ObTableApiDeleteRowIterator
{
public:
  ObTableApiMultiDeleteRowIterator();
  virtual ~ObTableApiMultiDeleteRowIterator();
  virtual void reset();
  int open(const ObTableBatchOperation &table_operation);
  virtual int get_next_row(common::ObNewRow *&row);
  OB_INLINE void continue_iter() { is_iter_pause_ = false; }
  OB_INLINE int64_t get_cur_delete_idx() { return cur_delete_idx_; }
  OB_INLINE bool has_finished() { return batch_idx_ >= batch_cnt_; }
private:
  const ObTableBatchOperation *batch_operation_;
  int64_t batch_cnt_;
  int64_t batch_idx_;
  int64_t cur_delete_idx_;
  bool is_iter_pause_;
};


class ObTableApiGetRowIterator : public ObTableApiRowIterator
{
public:
  ObTableApiGetRowIterator();
  virtual ~ObTableApiGetRowIterator();
  virtual void reset();
  int open(const ObTableOperation &table_operation);
  virtual int get_next_row(common::ObNewRow *&row);
protected:
  virtual bool is_read() const override { return true; }
protected:
  storage::ObTableScanParam scan_param_;
  share::schema::ObTableParam table_param_;
  common::ObNewRowIterator *scan_iter_;
};


class ObTableApiMultiGetRowIterator : public ObTableApiGetRowIterator
{
public:
  ObTableApiMultiGetRowIterator();
  virtual ~ObTableApiMultiGetRowIterator();
  int open(const ObTableBatchOperation &table_operation);
};


}
}

#endif /* OB_TABLE_API_ROW_ITERATOR_H_ */
