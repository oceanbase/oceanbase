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

#include "share/ob_i_tablet_scan.h"
#include "lib/container/ob_iarray.h"
#include "common/ob_common_types.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "storage/tx/ob_trans_define_v4.h"

namespace oceanbase
{
namespace sql
{
class ObOperator;
class ObTableLocation;
class ObEvalCtx;
class ObEvalInfo;
using common::ObDatum;
}  // namespace sql
namespace share
{
namespace schema
{
class ObTableParam;
class ObTableDMLParam;
}  // namespace schema
}  // namespace share
namespace blocksstable
{
struct ObStorageDatum;
}
namespace storage
{
class ObStoreCtxGuard;

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
class ObRow2ExprsProjector
{
public:
  explicit ObRow2ExprsProjector(common::ObIAllocator &alloc)
      : other_idx_(0), has_virtual_(false), op_(NULL),
      outputs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(alloc))
  {}
  ~ObRow2ExprsProjector()
  {
    destroy();
  }

  int init(const sql::ObExprPtrIArray &exprs,
           sql::ObPushdownOperator &op,
           const common::ObIArray<int32_t> &projector);

  int project(const sql::ObExprPtrIArray &exprs,
              const blocksstable::ObStorageDatum *datums,
              int16_t *nop_pos, int64_t &nop_cnt);

  void destroy()
  {
    outputs_.reset();
  }
  bool has_virtual() const { return has_virtual_; }
private:
  struct Item {
    int32_t obj_idx_;
    int32_t expr_idx_;
    sql::ObDatum *datum_;
    sql::ObEvalInfo *eval_info_;
    sql::ObBitVector *eval_flags_;
    const char *data_;

    Item() = default;
    DECLARE_TO_STRING;
  };

  template <common::ObObjDatumMapType OBJ_DATUM_MAP_TYPE, bool NEED_RESET_PTR>
  struct MapConvert {
    int32_t start_;
    int32_t end_;

    const static common::ObObjDatumMapType map_type_ = OBJ_DATUM_MAP_TYPE;
    MapConvert() : start_(0), end_(0) {}

    OB_INLINE void project(const Item *items, const blocksstable::ObStorageDatum *datums,
                           int16_t *nop_pos, int64_t &nop_cnt) const;
    OB_INLINE void project_batch_datum(const Item *items, const blocksstable::ObStorageDatum *datums,
                                       int16_t *nop_pos, int64_t &nop_cnt,
                                       const int64_t idx) const;
  };

  MapConvert<common::OBJ_DATUM_NUMBER, true> num_;
  MapConvert<common::OBJ_DATUM_STRING, false> str_;
  MapConvert<common::OBJ_DATUM_8BYTE_DATA, true> int_;
  int32_t other_idx_;
  bool has_virtual_; // has virtual column
  sql::ObPushdownOperator *op_;
  common::ObSEArray <Item, 4> outputs_;
};

class ObTableScanParam : public common::ObVTableScanParam
{
public:
  ObTableScanParam()
      : common::ObVTableScanParam(),
        trans_desc_(NULL),
        snapshot_(),
        fb_read_tx_uncommitted_(false),
        tx_id_(),
        tx_lock_timeout_(-1),
        table_param_(NULL),
        allocator_(&CURRENT_CONTEXT->get_arena_allocator()),
        need_scn_(false),
        need_switch_param_(false),
        is_mds_query_(false),
        is_thread_scope_(true)
  {}
  virtual ~ObTableScanParam() {}
public:
  transaction::ObTxDesc *trans_desc_;      // transaction handle
  transaction::ObTxReadSnapshot snapshot_;
  bool fb_read_tx_uncommitted_;
  transaction::ObTransID tx_id_;           // used when read-latest
  int64_t tx_lock_timeout_;
  const share::schema::ObTableParam *table_param_;
  common::ObIAllocator *allocator_; //stmt level allocator, only be free at the end of query
  common::SampleInfo sample_info_;
  bool need_scn_;
  bool need_switch_param_;
  bool is_mds_query_;
  OB_INLINE virtual bool is_valid() const {
    return  snapshot_.valid_ && ObVTableScanParam::is_valid();
  }
  OB_INLINE bool use_index_skip_scan() const {
    return (1 == ss_key_ranges_.count()) && (!ss_key_ranges_.at(0).is_whole_range());
  }
  bool is_thread_scope_;
  ObRangeArray ss_key_ranges_;  // used for index skip scan, use as postfix range for ObVTableScanParam::key_ranges_

  DECLARE_VIRTUAL_TO_STRING;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableScanParam);
};

struct ObDMLBaseParam
{
  ObDMLBaseParam()
      : timeout_(-1),
        schema_version_(-1),
        sql_mode_(DEFAULT_OCEANBASE_MODE),
        tz_info_(NULL),
        table_param_(NULL),
        tenant_schema_version_(OB_INVALID_VERSION),
        is_total_quantity_log_(false),
        is_ignore_(false),
        prelock_(false),
        is_batch_stmt_(false),
        dml_allocator_(nullptr),
        store_ctx_guard_(nullptr),
        encrypt_meta_(NULL),
        encrypt_meta_legacy_(),
        spec_seq_no_(),
        snapshot_(),
        branch_id_(0),
        direct_insert_task_id_(0),
        write_flag_(),
        check_schema_version_(true),
        ddl_task_id_(0),
        lob_allocator_(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {
  }

  ~ObDMLBaseParam()
  {
  }

  int64_t timeout_;
  int64_t schema_version_;
  ObSQLMode sql_mode_;
  const common::ObTimeZoneInfo *tz_info_;
  const share::schema::ObTableDMLParam *table_param_;
  int64_t tenant_schema_version_;
  bool is_total_quantity_log_;
  bool is_ignore_;
  bool prelock_;
  bool is_batch_stmt_;
  mutable common::ObIAllocator *dml_allocator_;
  mutable ObStoreCtxGuard *store_ctx_guard_;
  // table_id_, local_index_id_ and its encrypt_meta
  const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_;
  common::ObSEArray<transaction::ObEncryptMetaCache, 1> encrypt_meta_legacy_;

  // specified seq_no
  transaction::ObTxSEQ spec_seq_no_;
  // transaction snapshot
  transaction::ObTxReadSnapshot snapshot_;
  // parallel dml write branch id
  int16_t branch_id_;
  int64_t direct_insert_task_id_; // 0 means no direct insert
  // write flag for inner write processing
  concurrent_control::ObWriteFlag write_flag_;
  bool check_schema_version_;
  int64_t ddl_task_id_;
  mutable ObArenaAllocator lob_allocator_;
  bool is_valid() const { return (timeout_ > 0 && schema_version_ >= 0) && nullptr != store_ctx_guard_; }
  bool is_direct_insert() const { return (direct_insert_task_id_ > 0); }
  DECLARE_TO_STRING;
};

} // end namespace storage
} // end namespace oceanbase

#endif //OCEANBASE_STORAGE_OB_DML_PARAM_
