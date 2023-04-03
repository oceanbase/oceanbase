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

#ifndef OCEANBASE_COMMON_OB_ALL_VIRTUAL_TABLE_ITERATOR_
#define OCEANBASE_COMMON_OB_ALL_VIRTUAL_TABLE_ITERATOR_

#include "lib/container/ob_se_array.h"
#include "common/row/ob_row_iterator.h"
#include "common/ob_common_types.h"
#include "share/object/ob_obj_cast.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObSchemaGetterGuard;
}
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace common
{
class ObIAllocator;
class ObVTableScanParam;
class ObVirtualTableIterator : public ObNewRowIterator
{
  static const int64_t VT_COLUMN_COUNT = 64;
public:
  ObVirtualTableIterator()
    : allocator_(NULL),
      output_column_ids_(),
      reserved_column_cnt_(0),
      schema_guard_(NULL),
      table_schema_(NULL),
      index_schema_(NULL),
      cur_row_(),
      session_(NULL),
      row_calc_buf_("VTITER_CALC_BUF"),
      effective_tenant_id_(common::OB_INVALID_TENANT_ID),
      convert_alloc_(),
      cast_ctx_(),
      convert_row_(),
      need_convert_(false),
      scan_param_(NULL)
    {}
  virtual ~ObVirtualTableIterator() {}
  virtual void reset();
  inline void set_allocator(common::ObIAllocator *allocator);
  inline void set_reserved_column_cnt(int64_t count);
  inline int set_output_column_ids(const common::ObIArray<uint64_t> &column_ids);
  inline void set_schema_guard(share::schema::ObSchemaGetterGuard *schema_guard);
  inline share::schema::ObSchemaGetterGuard *get_schema_guard() const;
  inline void set_table_schema(const share::schema::ObTableSchema *table_schema);
  inline void set_index_schema(const share::schema::ObTableSchema *index_schema);
  inline void set_scan_param(const ObVTableScanParam *scan_param) { scan_param_ = scan_param; }
  virtual int open();
  virtual int inner_open() { return common::OB_SUCCESS; };
  virtual int get_next_row(common::ObNewRow *&row);
  virtual int inner_get_next_row(common::ObNewRow *&row) = 0;
  virtual int get_next_row() override; // interface for static typing engine.
  virtual int close();
  virtual int inner_close() { return common::OB_SUCCESS; }
  virtual inline void set_session(sql::ObSQLSessionInfo *session) { session_ = session; }
  virtual inline void set_convert_flag() { need_convert_ = true; }
  virtual void set_effective_tenant_id(const uint64_t tenant_id);
  virtual inline int set_key_ranges(const common::ObIArray<common::ObNewRange> &key_ranges)
  {
    return key_ranges_.assign(key_ranges);
  }
  virtual const common::ObIArray<common::ObNewRange> &get_key_ranges() { return key_ranges_; }
  virtual int check_priv(const ObString &level_str, const ObString &db_name,
                         const ObString &table_name, int64_t tenant_id, bool &passed);
  void set_scan_flag(const common::ObQueryFlag &scan_flag) { scan_flag_ = scan_flag; }
  bool is_reverse_scan() const { return common::ObQueryFlag::Reverse == scan_flag_.scan_order_; }
  VIRTUAL_TO_STRING_KV(K_(output_column_ids));
private:
  int init_convert_ctx();
  int convert_key_ranges();
  int get_key_cols(common::ObIArray<const share::schema::ObColumnSchemaV2*> &key_cols);
  int convert_key(const common::ObRowkey &src, common::ObRowkey &dst, common::ObIArray<const share::schema::ObColumnSchemaV2*> &key_cols);
  int free_convert_ctx();
  void reset_convert_ctx();
  int convert_output_row(ObNewRow *&cur_row);
  int get_all_columns_schema();
protected:
  common::ObIAllocator *allocator_;
  common::ObSEArray<uint64_t, VT_COLUMN_COUNT> output_column_ids_;
  int64_t reserved_column_cnt_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  const share::schema::ObTableSchema *table_schema_;
  const share::schema::ObTableSchema *index_schema_;
  common::ObNewRow cur_row_;
  common::ObQueryFlag scan_flag_;
  sql::ObSQLSessionInfo *session_;
  common::ObSEArray<common::ObNewRange, 16> key_ranges_;
  common::ObSEArray<common::ObNewRange, 16> saved_key_ranges_;
  common::ObArenaAllocator row_calc_buf_;
  uint64_t effective_tenant_id_;
private:
  common::ObArenaAllocator convert_alloc_;
  common::ObCastCtx cast_ctx_;
  common::ObNewRow convert_row_;
  bool need_convert_;
  common::ObSEArray<const share::schema::ObColumnSchemaV2 *, 16> cols_schema_;
  const ObVTableScanParam *scan_param_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVirtualTableIterator);
};

void ObVirtualTableIterator::set_allocator(common::ObIAllocator *allocator)
{
  allocator_ = allocator;
}

void ObVirtualTableIterator::set_reserved_column_cnt(int64_t count)
{
  reserved_column_cnt_ = count;
}

int ObVirtualTableIterator::set_output_column_ids(const common::ObIArray<uint64_t> &column_ids)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCCESS != (ret = output_column_ids_.assign(column_ids))) {
    SQL_ENG_LOG(WARN, "fail to assign output column ids", K(ret), K(column_ids));
  }
  return ret;
}

void ObVirtualTableIterator::set_schema_guard(share::schema::ObSchemaGetterGuard *schema_guard)
{
  schema_guard_ = schema_guard;
}

share::schema::ObSchemaGetterGuard * ObVirtualTableIterator::get_schema_guard() const
{
  return schema_guard_;
}

void ObVirtualTableIterator::set_table_schema(const share::schema::ObTableSchema *table_schema)
{
  table_schema_ = table_schema;
}

void ObVirtualTableIterator::set_index_schema(const share::schema::ObTableSchema *index_schema)
{
  index_schema_ = index_schema;
}

}//common
}//oceanbase
#endif /* OCEANBASE_COMMON_OB_ALL_VIRTUAL_TABLE_ITERATOR_ */
