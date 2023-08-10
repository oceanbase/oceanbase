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

#define USING_LOG_PREFIX STORAGE
#include "ob_lob_locator.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/tx/ob_trans_service.h"
#include "share/ob_lob_access_utils.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObLobLocatorHelper::ObLobLocatorHelper()
  : table_id_(OB_INVALID_ID),
    tablet_id_(OB_INVALID_ID),
    ls_id_(OB_INVALID_ID),
    read_snapshot_(),
    rowid_version_(ObURowIDData::INVALID_ROWID_VERSION),
    rowid_project_(nullptr),
    rowid_objs_(),
    locator_allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    rowkey_str_(),
    enable_locator_v2_(),
    is_inited_(false)
{
}

ObLobLocatorHelper::~ObLobLocatorHelper()
{
}

void ObLobLocatorHelper::reset()
{
  table_id_ = OB_INVALID_ID;
  tablet_id_ = OB_INVALID_ID;
  ls_id_ = OB_INVALID_ID;
  read_snapshot_.reset();
  rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
  rowid_project_ = nullptr;
  rowid_objs_.reset();
  locator_allocator_.reset();
  rowkey_str_.reset();
  enable_locator_v2_ = false;
  is_inited_ = false;
}

int ObLobLocatorHelper::init(const ObTableScanParam &scan_param,
                             const ObStoreCtx &ctx,
                             const share::ObLSID &ls_id,
                             const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam &table_param = *scan_param.table_param_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobLocatorHelper init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(!table_param.use_lob_locator() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObLobLocatorHelper", K(ret), K(table_param), K(snapshot_version));
  } else if (table_param.get_rowid_version() != ObURowIDData::INVALID_ROWID_VERSION
             && table_param.get_rowid_projector().empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected empty rowid projector", K(ret), K(table_param));
  } else {
    if (OB_UNLIKELY(!table_param.enable_lob_locator_v2())
        && OB_UNLIKELY(!lib::is_oracle_mode() || is_sys_table(table_param.get_table_id()))) {
      // only oracle mode user table support lob locator if lob locator v2 not enabled
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected tenant mode to init ObLobLocatorHelper", K(ret), K(table_param));
    } else if (table_param.get_rowid_version() != ObURowIDData::INVALID_ROWID_VERSION
               && table_param.get_rowid_projector().empty()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected empty rowid projector", K(ret), K(table_param));
    } else {
      rowid_version_ = table_param.get_rowid_version();
      rowid_project_ = &table_param.get_rowid_projector();
      table_id_ = table_param.get_table_id();
      tablet_id_ = scan_param.tablet_id_.id();
      ls_id_ = ls_id.id();
      read_snapshot_ = ctx.mvcc_acc_ctx_.snapshot_;
      enable_locator_v2_ = table_param.enable_lob_locator_v2();
      if (snapshot_version != read_snapshot_.version_.get_val_for_tx()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "snapshot version mismatch",
          K(snapshot_version), K(read_snapshot_));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObLobLocatorHelper::init(const ObTableStoreStat &table_store_stat,
                             const ObStoreCtx &ctx,
                             const share::ObLSID &ls_id,
                             const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobLocatorHelper init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObLobLocatorHelper", K(ret), K(ls_id), K(snapshot_version));
  } else {
    rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
    rowid_project_ = NULL;
    // table id只用来判断是不是systable, 这个接口创建的locator不会构造真正的rowid
    table_id_ = table_store_stat.table_id_;
    tablet_id_ = table_store_stat.tablet_id_.id();
    ls_id_ = ls_id.id();
    read_snapshot_ = ctx.mvcc_acc_ctx_.snapshot_;
    enable_locator_v2_ = true; // must be called en locator v2 enabled
    OB_ASSERT(ob_enable_lob_locator_v2() == true);
    is_inited_ = true;
    // OB_ASSERT(snapshot_version == ctx.mvcc_acc_ctx_.snapshot_.version_);
    // snapshot_version mismatch in test_multi_version_sstable_single_get
  }

  return ret;
}

int ObLobLocatorHelper::fill_lob_locator(ObDatumRow &row,
                                         bool is_projected_row,
                                         const ObTableAccessParam &access_param)
{
  int ret = OB_SUCCESS;
  const ObColDescIArray *col_descs = nullptr;
  const common::ObIArray<int32_t> *out_project = access_param.iter_param_.out_cols_project_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
  } else if (OB_ISNULL(out_project)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to fill lob locator", K(ret), KP(out_project));
  } else if (OB_ISNULL(col_descs = access_param.iter_param_.get_out_col_descs())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null col_descs", K(ret), K(access_param.iter_param_));
  } else if (!lib::is_oracle_mode() || is_sys_table(access_param.iter_param_.table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Only oracle mode need build lob locator", K(ret));
  } else if (OB_ISNULL(access_param.output_exprs_) || OB_ISNULL(access_param.get_op())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "output expr or op is null", K(ret), K(access_param));
  } else {
    STORAGE_LOG(DEBUG, "start to fill lob locator", K(row));
    //ObLobLocatorHelper is inited, we always cound find a lob cell in projected row

    if (OB_FAIL(build_rowid_obj(row, rowkey_str_, is_projected_row, *col_descs, *out_project,
                                access_param.iter_param_.tablet_id_))) {
      STORAGE_LOG(WARN, "Failed to build rowid obj", K(ret), K(rowkey_str_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < out_project->count(); i++) {
        int64_t obj_idx = is_projected_row ? i : out_project->at(i);
        int32_t idx = 0;
        if (obj_idx < 0 || obj_idx >= row.count_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected out project idx", K(ret), K(i), K(obj_idx),
                      K(is_projected_row), KPC(out_project), K(row));
        } else if (FALSE_IT(idx = out_project->at(i))) {
        } else if (idx < 0 || idx >= col_descs->count()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected out project idx", K(ret), K(i), K(idx), KPC(col_descs));
        } else if (OB_UNLIKELY(i >= access_param.output_exprs_->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index", K(ret), K(i), KPC(access_param.output_exprs_));
        } else if (OB_ISNULL(access_param.get_op())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("access_param.op is null", K(ret), K(access_param));
        } else {
          sql::ObExpr *expr = access_param.output_exprs_->at(i);
          if (is_lob_locator(expr->datum_meta_.type_)) {
            ObLobLocator *locator = NULL;
            sql::ObDatum &datum = expr->locate_expr_datum(access_param.get_op()->get_eval_ctx());
            if (datum.is_null()) {
              // do nothing.
            } else if (OB_FAIL(build_lob_locator(datum.get_string(), col_descs->at(idx).col_id_,
                                                 rowkey_str_, locator))) {
              STORAGE_LOG(WARN, "Failed to build lob locator", K(ret), K(rowkey_str_));
            } else {
              datum.set_lob_locator(*locator);
              STORAGE_LOG(DEBUG, "succ to fill lob locator and update datum", KPC(locator));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObLobLocatorHelper::fill_lob_locator_v2(ObDatumRow &row,
                                            const ObTableAccessContext &access_ctx,
                                            const ObTableAccessParam &access_param)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnParam *> *out_cols_param = access_param.iter_param_.get_col_params();
  const ObColDescIArray *col_descs = access_param.iter_param_.get_out_col_descs();
  const common::ObIArray<int32_t> *out_project = access_param.iter_param_.out_cols_project_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
  } else if (OB_ISNULL(out_cols_param) || OB_ISNULL(col_descs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null cols param", K(ret), KP(out_cols_param), KP(col_descs));
  } else if (out_cols_param->count() != row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid col count", K(row), KPC(out_cols_param));
  } else {
    if (OB_FAIL(build_rowid_obj(row, rowkey_str_, false, *col_descs, *out_project, access_param.iter_param_.tablet_id_))) {
      STORAGE_LOG(WARN, "Failed to build rowid obj", K(ret), K(rowkey_str_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
        blocksstable::ObStorageDatum &datum = row.storage_datums_[i];
        ObObjMeta datum_meta = out_cols_param->at(i)->get_meta_type();
        ObLobLocatorV2 locator;
        if (datum_meta.is_lob_storage()) {
          if (datum.is_null() || datum.is_nop()) {
          } else if (OB_FAIL(build_lob_locatorv2(locator,
                                                 datum.get_string(),
                                                 out_cols_param->at(i)->get_column_id(),
                                                 rowkey_str_,
                                                 access_ctx,
                                                 datum_meta.get_collation_type(),
                                                 false,
                                                 is_sys_table(access_param.iter_param_.table_id_)))) {
            STORAGE_LOG(WARN, "Lob: Failed to build lob locator v2", K(ret), K(i), K(datum));
          } else {
            datum.set_string(locator.ptr_, locator.size_);
            STORAGE_LOG(DEBUG, "Lob: Succeed to load lob obj", K(datum), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLobLocatorHelper::fuse_mem_lob_header(ObObj &def_obj, uint64_t col_id, bool is_systable)
{
  OB_ASSERT(enable_locator_v2_ == true);
  int ret = OB_SUCCESS;
  if (!enable_locator_v2_) {
  } else if (!(def_obj.is_lob_storage()) || def_obj.is_nop_value() || def_obj.is_null()) {
  } else {
    // must be called after fill_lob_locator, should not reuse/reset locator_allocator_ or rowkey_str_
    ObLobLocatorV2 locator;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
    } else if (OB_UNLIKELY(!is_valid_id(col_id))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to fuse lob header", K(ret), K(col_id));
    } else {
      // default values must be inrow lobs
      int64_t payload_size = def_obj.get_string().length();
      payload_size += sizeof(ObLobCommon);
      // mysql inrow lobs & systable lobs do not have extern fields
      bool has_extern = (lib::is_oracle_mode() && !is_systable);
      ObMemLobExternFlags extern_flags(has_extern);
      ObLobCommon lob_common;
      int64_t full_loc_size = ObLobLocatorV2::calc_locator_full_len(extern_flags,
                                                                    rowkey_str_.length(),
                                                                    payload_size,
                                                                    false);
      char *buf = nullptr;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(locator_allocator_.alloc(full_loc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for lob locator", K(ret), K(full_loc_size));
      } else if (FALSE_IT(MEMSET(buf, 0, full_loc_size))) {
      } else {
        locator.assign_buffer(buf, full_loc_size);
        if (OB_FAIL(locator.fill(PERSISTENT_LOB,
                                 extern_flags,
                                 rowkey_str_,
                                 &lob_common,
                                 payload_size,
                                 0,
                                 false))) {
          STORAGE_LOG(WARN, "Lob: init locator in build_lob_locatorv2", K(ret), K(col_id));
        } else if (OB_FAIL(locator.set_payload_data(&lob_common, def_obj.get_string()))) {
        } else if (has_extern) {
          ObMemLobTxInfo tx_info(read_snapshot_.version_.get_val_for_tx(),
                                 read_snapshot_.tx_id_.get_id(),
                                 read_snapshot_.scn_.cast_to_int());
          ObMemLobLocationInfo location_info(tablet_id_, ls_id_, def_obj.get_collation_type());
          if (OB_FAIL(locator.set_table_info(table_id_, col_id))) { // ToDo: @gehao should be column idx
            STORAGE_LOG(WARN, "Lob: set table info failed", K(ret), K(table_id_), K(col_id));
          } else if (extern_flags.has_tx_info_ && OB_FAIL(locator.set_tx_info(tx_info))) {
            STORAGE_LOG(WARN, "Lob: set transaction info failed", K(ret), K(tx_info));
          } else if (extern_flags.has_location_info_ && OB_FAIL(locator.set_location_info(location_info))) {
            STORAGE_LOG(WARN, "Lob: set location info failed", K(ret), K(location_info));
          }
        }
        if (OB_SUCC(ret)) {
          def_obj.set_string(def_obj.get_type(), buf, full_loc_size);
          def_obj.set_has_lob_header();
        }
      }
    }
  }
  return ret;
}

int ObLobLocatorHelper::build_rowid_obj(ObDatumRow &row,
                                        common::ObString &rowid_str,
                                        bool is_projected_row,
                                        const ObColDescIArray &col_descs,
                                        const common::ObIArray<int32_t> &out_project,
                                        const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  rowid_objs_.reset();
  rowid_str.reset();
  if (rowid_version_ == ObURowIDData::INVALID_ROWID_VERSION) {
    // use empty string for rowid
  } else if (OB_ISNULL(rowid_project_) || rowid_project_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null or empty rowid project", K(ret), K(*this));
  } else {
    ObObj tmp_obj;
    if (is_projected_row) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_project_->count(); i++) {
        int64_t idx = rowid_project_->at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column project idx", K(ret), K(idx));
        } else if (OB_FAIL(row.storage_datums_[idx].to_obj(tmp_obj, col_descs.at(idx).col_type_))) {
          STORAGE_LOG(WARN, "Failed to transform datum to obj", K(ret));
        } else if (OB_FAIL(rowid_objs_.push_back(tmp_obj))) {
          STORAGE_LOG(WARN, "Failed to push back rowid object", K(ret), K(idx), K(row));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_project_->count(); i++) {
        int64_t idx = rowid_project_->at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= out_project.count())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column project idx", K(ret), K(idx), K(out_project));
        } else if (FALSE_IT(idx = out_project.at(idx))) {
        } else if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column project idx", K(ret), K(idx), K(row));
        } else if (OB_FAIL(row.storage_datums_[idx].to_obj(tmp_obj, col_descs.at(idx).col_type_))) {
          STORAGE_LOG(WARN, "Failed to transform datum to obj", K(ret));
        } else if (OB_FAIL(rowid_objs_.push_back(tmp_obj))) {
          STORAGE_LOG(WARN, "Failed to push back rowid object", K(ret), K(idx), K(row));
        }
      }
    }
    // append tablet id to build heap table rowid
    if (OB_SUCC(ret) && (ObURowIDData::HEAP_TABLE_ROWID_VERSION == rowid_version_ || ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION == rowid_version_)) {
      tmp_obj.set_int(tablet_id.id());
      if (OB_FAIL(rowid_objs_.push_back(tmp_obj))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      common::ObURowIDData rowid;
      int64_t rowid_buf_size = 0;
      int64_t rowid_str_size = 0;
      char *rowid_buf = nullptr;
      if (OB_FAIL(rowid.set_rowid_content(rowid_objs_, rowid_version_, locator_allocator_))) {
        STORAGE_LOG(WARN, "Failed to set rowid content", K(ret), K(*this));
      } else if (FALSE_IT(rowid_buf_size = rowid.needed_base64_buffer_size())) {
      } else if (OB_ISNULL(rowid_buf = reinterpret_cast<char *>(locator_allocator_.alloc(rowid_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for rowid base64 str", K(ret), K(rowid_buf_size));
      } else if (OB_FAIL(rowid.get_base64_str(rowid_buf, rowid_buf_size, rowid_str_size))) {
        STORAGE_LOG(WARN, "Failed to get rowid base64 string", K(ret));
      } else {
        rowid_str.assign_ptr(rowid_buf, rowid_str_size);
      }
    }
  }
  STORAGE_LOG(DEBUG, "build rowid for lob locator", K(ret), K(rowid_str));

  return ret;
}

int ObLobLocatorHelper::build_lob_locator(common::ObString payload,
                                          const uint64_t column_id,
                                          const common::ObString &rowid_str,
                                          ObLobLocator *&locator)
{
  int ret = OB_SUCCESS;
  locator = nullptr;
  if (OB_UNLIKELY(!is_valid_id(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build lob locator", K(ret), K(column_id));
  } else {
    char *buf = nullptr;
    int64_t locator_size = sizeof(ObLobLocator) + payload.length() + rowid_str.length();
    if (OB_ISNULL(buf = reinterpret_cast<char *>(locator_allocator_.alloc(locator_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory for lob locator", K(ret), K(locator_size));
    } else if (FALSE_IT(MEMSET(buf, 0, locator_size))) {
    } else if (FALSE_IT(locator = reinterpret_cast<ObLobLocator *>(buf))) {
    } else if (OB_FAIL(locator->init(table_id_, column_id, read_snapshot_.version_.get_val_for_tx(),
                                     LOB_DEFAULT_FLAGS, rowid_str, payload))) {
      STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(*this), K(rowid_str));
    } else {
      STORAGE_LOG(DEBUG, "succ to build lob locator", KPC(locator));
    }
  }

  return ret;
}

// Notice: payload is full disk locator
int ObLobLocatorHelper::build_lob_locatorv2(ObLobLocatorV2 &locator,
                                            const common::ObString &payload,
                                            const uint64_t column_id,
                                            const common::ObString &rowid_str,
                                            const ObTableAccessContext &access_ctx,
                                            const ObCollationType cs_type,
                                            bool is_simple,
                                            bool is_systable)
{
  OB_ASSERT(enable_locator_v2_ == true);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build lob locator", K(ret), K(column_id));
  } else {
    char *buf = nullptr;
    const ObLobCommon *lob_common =
      (payload.length() == 0 ? NULL : reinterpret_cast<const ObLobCommon *>(payload.ptr()));
    int64_t out_payload_len = payload.length();
    int64_t byte_size = lob_common->get_byte_size(out_payload_len);
    bool is_src_inrow = (is_simple ? true : lob_common->in_row_);
    // systable read always get full lob data and output inrow lobs
    bool is_dst_inrow = ((is_systable) ? true : is_src_inrow);
    bool is_enable_force_inrow = false;
    if (is_enable_force_inrow && (byte_size <= LOB_FORCE_INROW_SIZE)) {
      // if lob is smaller than datum allow size
      // let lob obj force inrow for hash/cmp cannot handle error
      is_dst_inrow = true;
    }
    // oracle user table lobs and mysql user table outrow lobs need extern.
    bool has_extern = (!is_simple) && (lib::is_oracle_mode() || !is_dst_inrow);
    ObMemLobExternFlags extern_flags(has_extern);

    bool padding_char_size = false;
    if (!lob_common->in_row_ && is_dst_inrow &&
        out_payload_len == ObLobManager::LOB_WITH_OUTROW_CTX_SIZE) {
      // for 4.0 lob, outrow disk lob locator do force inrow
      // not have char len, should do padding
      out_payload_len += sizeof(uint64_t);
      padding_char_size = true;
    }

    if (!is_src_inrow && is_dst_inrow) {
      // read outrow lobs but output as inrow lobs, need to calc the output payload lens
      // get byte size of out row lob, and calc total disk lob handle size if it is inrow
      out_payload_len += byte_size; // need whole disk locator
    }

    int64_t full_loc_size = ObLobLocatorV2::calc_locator_full_len(extern_flags,
                                                                  rowid_str.length(),
                                                                  out_payload_len,
                                                                  is_simple);
    if (full_loc_size > OB_MAX_LONGTEXT_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "Failed to get lob data over size", K(ret), K(full_loc_size),
                  K(rowid_str.length()), K(out_payload_len));
    } else if (OB_ISNULL(buf = reinterpret_cast<char *>(locator_allocator_.alloc(full_loc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory for lob locator", K(ret), K(full_loc_size));
    } else if (FALSE_IT(MEMSET(buf, 0, full_loc_size))) {
    } else {
      ObMemLobCommon *mem_lob_common = NULL;
      locator.assign_buffer(buf, full_loc_size);
      if (OB_FAIL(locator.fill(PERSISTENT_LOB,
                               extern_flags,
                               rowid_str,
                               lob_common,
                               out_payload_len,
                               is_dst_inrow ? 0 : payload.length(),
                               is_simple))) {
        STORAGE_LOG(WARN, "Lob: init locator in build_lob_locatorv2", K(ret), K(column_id));
      } else if (OB_SUCC(locator.get_mem_locator(mem_lob_common))) {
        mem_lob_common->set_has_inrow_data(is_dst_inrow);
        mem_lob_common->set_read_only(false);
      }
      if (OB_FAIL(ret)) {
      } else if (is_simple) {
        if (OB_FAIL(locator.set_payload_data(payload))) {
          STORAGE_LOG(WARN, "Lob: fill payload failed", K(ret), K(column_id));
        }
      } else if (has_extern) {
        ObMemLobTxInfo tx_info(read_snapshot_.version_.get_val_for_tx(),
                               read_snapshot_.tx_id_.get_id(),
                               read_snapshot_.scn_.cast_to_int());
        ObMemLobLocationInfo location_info(tablet_id_, ls_id_, cs_type);
        if (has_extern && OB_FAIL(locator.set_table_info(table_id_, column_id))) { // should be column idx
          STORAGE_LOG(WARN, "Lob: set table info failed", K(ret), K(table_id_), K(column_id));
        } else if (extern_flags.has_tx_info_ && OB_FAIL(locator.set_tx_info(tx_info))) {
          STORAGE_LOG(WARN, "Lob: set transaction info failed", K(ret), K(tx_info));
        } else if (extern_flags.has_location_info_ && OB_FAIL(locator.set_location_info(location_info))) {
          STORAGE_LOG(WARN, "Lob: set location info failed", K(ret), K(location_info));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (is_simple) {
      } else {
        if (payload.length() == 0) {
          // build fake diskLobCommone
          ObString disk_loc_str;
          if (OB_FAIL(locator.get_disk_locator(disk_loc_str))) {
            STORAGE_LOG(WARN, "Lob: get disk locator failed", K(ret), K(column_id));
          } else {
            OB_ASSERT(disk_loc_str.length() == sizeof(ObLobCommon));
            ObLobCommon *fake_lob_common = new (disk_loc_str.ptr()) ObLobCommon();
          }
        } else if (is_src_inrow == is_dst_inrow ) {
          OB_ASSERT(payload.length() >= sizeof(ObLobCommon));
          if (OB_FAIL(locator.set_payload_data(payload))) {
            STORAGE_LOG(WARN, "Lob: fill payload failed", K(ret), K(column_id));
          }
        } else if ((!is_src_inrow) && is_dst_inrow) { //src outrow, load to inrow result
          OB_ASSERT(payload.length() >= sizeof(ObLobCommon));
          storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);
          ObString disk_loc_str;
          if (OB_FAIL(locator.get_disk_locator(disk_loc_str))) {
            STORAGE_LOG(WARN, "Lob: get disk locator failed", K(ret), K(column_id));
          } else if (OB_ISNULL(lob_mngr)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Lob: get ObLobManager null", K(ret));
          } else {
            char *buffer = disk_loc_str.ptr();
            MEMCPY(buffer, lob_common, payload.length());
            int64_t offset = payload.length();
            uint64_t *char_len_ptr = nullptr;
            if (padding_char_size) {
              char_len_ptr = reinterpret_cast<uint64_t*>(buffer + offset);
              offset += sizeof(uint64_t);
            }

            // read full data to new locator
            // use tmp allocator for read lob col instead of batch level allocator
            ObArenaAllocator tmp_lob_allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
            ObLobAccessParam param;
            param.tx_desc_ = NULL;
            param.snapshot_.core_ = read_snapshot_;
            param.snapshot_.valid_= true;
            param.snapshot_.source_ = transaction::ObTxReadSnapshot::SRC::LS;
            param.snapshot_.snapshot_lsid_ = share::ObLSID(ls_id_);
            param.ls_id_ = share::ObLSID(ls_id_);
            param.sql_mode_ = access_ctx.sql_mode_;
            param.tablet_id_ = ObTabletID(tablet_id_);

            param.allocator_ = &tmp_lob_allocator;
            param.lob_common_ = const_cast<ObLobCommon *>(lob_common);
            param.handle_size_ = payload.length();
            param.byte_size_ = lob_common->get_byte_size(payload.length());
            param.coll_type_ = cs_type;
            param.timeout_ = access_ctx.timeout_;
            param.scan_backward_ = false;
            param.offset_ = 0;
            param.len_ = param.byte_size_;
            ObString output_data;
            output_data.assign_buffer(buffer + offset, param.len_);
            if (OB_FAIL(lob_mngr->query(param, output_data))) {
              COMMON_LOG(WARN,"Lob: falied to query lob tablets.", K(ret), K(param));
            } else if (padding_char_size) {
              ObString data_str;
              if (OB_FAIL(locator.get_inrow_data(data_str))) {
                STORAGE_LOG(WARN, "Lob: read lob data failed",
                  K(ret), K(column_id), K(data_str), K(data_str.length()), K(full_loc_size), K(payload));
              } else if (OB_ISNULL(char_len_ptr)) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "Lob: get null char len ptr when need padding char len",
                  K(ret), K(column_id), K(data_str), K(data_str.length()), K(full_loc_size), K(payload));
              } else {
                *char_len_ptr = ObCharset::strlen_char(param.coll_type_, data_str.ptr(), data_str.length());
              }
            }
          }
        } else if (is_src_inrow && (!is_dst_inrow)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "Lob: fatal error", K(ret), K(locator), K(is_src_inrow), K(is_dst_inrow));
        }
        if (OB_FAIL(ret)) {
          if (ret != OB_TIMEOUT && ret != OB_NOT_MASTER) {
            STORAGE_LOG(WARN, "Lob: failed to build lob locator v2", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
