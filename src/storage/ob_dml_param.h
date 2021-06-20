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

#ifndef OCEANBASE_STORAGE_OB_DML_PARAM_
#define OCEANBASE_STORAGE_OB_DML_PARAM_

#include "lib/container/ob_iarray.h"
#include "common/ob_common_types.h"
#include "share/ob_i_data_access_service.h"
#include "share/datum/ob_datum.h"

namespace oceanbase {
namespace sql {
class ObTableLocation;
class ObEvalCtx;
class ObEvalInfo;
using common::ObDatum;
}  // namespace sql
namespace share {
namespace schema {
class ObTableParam;
class ObTableDMLParam;
}  // namespace schema
}  // namespace share
namespace storage {
class ObIPartitionGroupGuard;

//
// Project storage output row to expression array, the core project logic is:
//
//   if (cells[projector.at(i)].is_nop()) {
//     nop_pos[nop_cnt++] = i;
//   } else {
//     exprs.at(i).locate_datum_for_write().from_obj(cells[projector.at(i)])
//   }
//
// We introduce ObRow2ExprsProjector for optimization:
// 1. Save datum pointer of expression, locate datum once for each expression.
// 2. Project number, string, integer (OBJ_DATUM_8BYTE_DATA) by groups, to reduce type detection.
class ObRow2ExprsProjector {
public:
  explicit ObRow2ExprsProjector(common::ObIAllocator& alloc)
      : other_idx_(0),
        has_virtual_(false),
        outputs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(alloc))
  {}
  ~ObRow2ExprsProjector()
  {
    destroy();
  }

  int init(const sql::ObExprPtrIArray& exprs, sql::ObEvalCtx& eval_ctx, const common::ObIArray<int32_t>& projector);

  int project(const sql::ObExprPtrIArray& exprs, const common::ObObj* cells, int16_t* nop_pos, int64_t& nop_cnt);

  void destroy()
  {
    outputs_.reset();
  }
  bool has_virtual() const
  {
    return has_virtual_;
  }

private:
  struct Item {
    int32_t obj_idx_;
    int32_t expr_idx_;
    sql::ObDatum* datum_;
    sql::ObEvalInfo* eval_info_;
    const char* data_;

    Item() = default;
    DECLARE_TO_STRING;
  };

  template <common::ObObjDatumMapType OBJ_DATUM_MAP_TYPE, bool NEED_RESET_PTR>
  struct MapConvert {
    int32_t start_;
    int32_t end_;

    const static common::ObObjDatumMapType map_type_ = OBJ_DATUM_MAP_TYPE;
    MapConvert() : start_(0), end_(0)
    {}

    OB_INLINE void project(const Item* items, const common::ObObj* cells, int16_t* nop_pos, int64_t& nop_cnt) const
    {
      // performance critical, no parameter validity check.
      for (int32_t i = start_; i < end_; i++) {
        const Item& item = items[i];
        const common::ObObj& cell = cells[item.obj_idx_];
        if (OB_UNLIKELY(cell.is_nop_value())) {
          nop_pos[nop_cnt++] = item.expr_idx_;
        } else if (OB_UNLIKELY(cell.is_null())) {
          item.datum_->set_null();
        } else {
          if (NEED_RESET_PTR) {
            if (OB_UNLIKELY(item.datum_->ptr_ != item.data_)) {
              item.datum_->ptr_ = item.data_;
            }
          }
          item.datum_->obj2datum<OBJ_DATUM_MAP_TYPE>(cell);
        }
      }
    }
  };

  MapConvert<common::OBJ_DATUM_NUMBER, true> num_;
  MapConvert<common::OBJ_DATUM_STRING, false> str_;
  MapConvert<common::OBJ_DATUM_8BYTE_DATA, true> int_;
  int32_t other_idx_;
  bool has_virtual_;  // has virtual column
  common::ObSEArray<Item, 4> outputs_;
};

class ObTableScanParam : public common::ObVTableScanParam {
public:
  ObTableScanParam()
      : common::ObVTableScanParam(),
        trans_desc_(NULL),
        table_param_(NULL),
        allocator_(&CURRENT_CONTEXT.get_arena_allocator()),
        part_filter_(NULL),
        part_mgr_(NULL),
        column_orders_(nullptr),
        need_scn_(false),
        fuse_row_cache_hit_rate_(0),
        block_cache_hit_rate_(0),
        ref_table_id_(common::OB_INVALID_ID),
        partition_guard_(NULL),
        iterator_mementity_(nullptr)
  {}
  explicit ObTableScanParam(transaction::ObTransDesc& trans_desc)
      : common::ObVTableScanParam(),
        trans_desc_(&trans_desc),
        table_param_(NULL),
        allocator_(&CURRENT_CONTEXT.get_arena_allocator()),
        part_filter_(NULL),
        part_mgr_(NULL),
        column_orders_(nullptr),
        need_scn_(false),
        fuse_row_cache_hit_rate_(0),
        block_cache_hit_rate_(0),
        ref_table_id_(common::OB_INVALID_ID),
        partition_guard_(NULL),
        iterator_mementity_(nullptr)
  {}
  virtual ~ObTableScanParam()
  {}

public:
  transaction::ObTransDesc* trans_desc_;  // transaction handle
  const share::schema::ObTableParam* table_param_;
  common::ObArenaAllocator* allocator_;
  common::SampleInfo sample_info_;
  const sql::ObTableLocation* part_filter_;
  common::ObPartMgr* part_mgr_;
  common::ObArray<common::ObOrderType>* column_orders_;
  bool need_scn_;
  int16_t fuse_row_cache_hit_rate_;
  int16_t block_cache_hit_rate_;
  uint64_t ref_table_id_;  // main table id
  ObIPartitionGroupGuard* partition_guard_;
  lib::MemoryContext* iterator_mementity_;
  OB_INLINE virtual bool is_valid() const
  {
    return (NULL != trans_desc_ && trans_desc_->is_valid_or_standalone_stmt() && ObVTableScanParam::is_valid());
  }
  OB_INLINE common::ObIArray<common::ObOrderType>* get_rowkey_column_orders() const
  {
    return column_orders_;
  }

private:
  virtual int init_rowkey_column_orders();
  // TO_STRING_KV(N_TRANS_DESC, trans_desc_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableScanParam);
};

struct ObDMLBaseParam {
  ObDMLBaseParam()
      : timeout_(-1),
        schema_version_(-1),
        query_flag_(),
        sql_mode_(DEFAULT_OCEANBASE_MODE),
        is_total_quantity_log_(false),
        tz_info_(NULL),
        only_data_table_(false),
        table_param_(NULL),
        tenant_schema_version_(OB_INVALID_VERSION),
        is_ignore_(false),
        duplicated_rows_(0),
        prelock_(false),
        output_exprs_(NULL),
        op_(NULL),
        op_filters_(NULL),
        row2exprs_projector_(NULL)
  {}

  int64_t timeout_;
  int64_t schema_version_;
  common::ObQueryFlag query_flag_;
  ObSQLMode sql_mode_;
  bool is_total_quantity_log_;
  const common::ObTimeZoneInfo* tz_info_;
  bool only_data_table_;
  common::ObColumnExprArray virtual_columns_;
  common::ObExprCtx expr_ctx_;
  const share::schema::ObTableDMLParam* table_param_;
  int64_t tenant_schema_version_;
  bool is_ignore_;
  mutable int64_t duplicated_rows_;
  bool prelock_;

  // output for sql static typing engine, NULL for old sql engine scan.
  const sql::ObExprPtrIArray* output_exprs_;
  sql::ObOperator* op_;
  const sql::ObExprPtrIArray* op_filters_;
  ObRow2ExprsProjector* row2exprs_projector_;

  bool is_valid() const
  {
    return (timeout_ > 0 && schema_version_ >= 0);
  }
  TO_STRING_KV(N_TIMEOUT, timeout_, N_SCHEMA_VERSION, schema_version_, N_SCAN_FLAG, query_flag_, N_SQL_MODE, sql_mode_,
      N_IS_TOTAL_QUANTITY_LOG, is_total_quantity_log_, K_(only_data_table), KP_(table_param), K_(tenant_schema_version),
      K_(is_ignore), K_(duplicated_rows), K_(prelock));
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_DML_PARAM_
