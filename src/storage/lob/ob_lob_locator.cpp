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


namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObLobLocatorHelper::ObLobLocatorHelper()
  : table_id_(OB_INVALID_ID),
    snapshot_version_(0),
    rowid_version_(ObURowIDData::INVALID_ROWID_VERSION),
    rowid_project_(nullptr),
    rowid_objs_(),
    locator_allocator_(),
    is_inited_(false)
{
}

ObLobLocatorHelper::~ObLobLocatorHelper()
{
}

void ObLobLocatorHelper::reset()
{
  table_id_ = OB_INVALID_ID;
  snapshot_version_ = 0;
  rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
  rowid_project_ = nullptr;
  rowid_objs_.reset();
  locator_allocator_.reset();
  is_inited_ = false;
}

void ObLobLocatorHelper::reuse()
{
  locator_allocator_.reuse();
  rowid_objs_.reuse();
}

int ObLobLocatorHelper::init(const share::schema::ObTableParam &table_param,
                             const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobLocatorHelper init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(!table_param.use_lob_locator() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObLobLocatorHelper", K(ret), K(table_param), K(snapshot_version));
  } else if (OB_UNLIKELY(!lib::is_oracle_mode() || is_sys_table(table_param.get_table_id()))) {
    // only oracle mode and user table support lob locator
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
    snapshot_version_ = snapshot_version;
    is_inited_ = true;
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
  } else if (OB_ISNULL(access_param.output_exprs_) || OB_ISNULL(access_param.op_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "output expr or op is null", K(ret), K(access_param));
  } else {
    STORAGE_LOG(DEBUG, "start to fill lob locator", K(row));
    //ObLobLocatorHelper is inited, we always cound find a lob cell in projected row
    locator_allocator_.reuse();
    common::ObString rowid;

    if (OB_FAIL(build_rowid_obj(row, rowid, is_projected_row, *col_descs, *out_project, access_param.iter_param_.tablet_id_))) {
      STORAGE_LOG(WARN, "Failed to build rowid obj", K(ret), K(rowid));
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
        } else if (OB_ISNULL(access_param.op_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("access_param.op is null", K(ret), K(access_param));
        } else {
          sql::ObExpr *expr = access_param.output_exprs_->at(i);
          if (is_lob_locator(expr->datum_meta_.type_)) {
            ObLobLocator *locator = NULL;
            sql::ObDatum &datum = expr->locate_expr_datum(access_param.op_->get_eval_ctx());
            if (datum.is_null()) {
              // do nothing.
            } else if (OB_FAIL(build_lob_locator(datum.get_string(), col_descs->at(idx).col_id_,
                                                  rowid, locator))) {
              STORAGE_LOG(WARN, "Failed to build lob locator", K(ret), K(rowid));
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
    //
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
    } else if (OB_FAIL(locator->init(table_id_, column_id, snapshot_version_,
                                     LOB_DEFAULT_FLAGS, rowid_str, payload))) {
      STORAGE_LOG(WARN, "Failed to init lob locator", K(ret), K(*this), K(rowid_str));
    } else {
      STORAGE_LOG(DEBUG, "succ to build lob locator", KPC(locator));
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
