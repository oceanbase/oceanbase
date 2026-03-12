/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/container/ob_se_array.h"
#include "share/ob_errno.h"
#define USING_LOG_PREFIX SQL_DAS

#include "ob_das_search_index_utils.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_dml_vec_iter.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "storage/tx/ob_trans_service.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
int ObSearchIndexDMLIterator::init(const ObDASDMLBaseCtDef *das_ctdef,
                                   const ObDASDMLBaseCtDef *main_ctdef,
                                   const IntFixedArray *row_projector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init search index dml iterator twice", K(ret), K(is_inited_));
  } else if (OB_ISNULL(das_ctdef) || OB_ISNULL(main_ctdef) || OB_ISNULL(row_projector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(das_ctdef), K(main_ctdef), K(row_projector), K(ret));
  } else {
    ObString index_properties;
    const share::schema::ObTableSchemaParam &table_param = das_ctdef->table_param_.get_data_table();
    const common::ObIArray<uint64_t> &included_cids = table_param.get_search_index_included_cids();
    const common::ObIArray<int32_t> &included_cid_idxes = table_param.get_search_index_included_cid_idxes();
    const common::ObIArray<ObCollectionArrayType*> &included_arr_types = table_param.get_search_index_arr_types();
    int64_t doc_proj = -1;
    common::ObSEArray<ObObjMeta, 8> included_cid_metas;
    if (OB_UNLIKELY(!table_param.is_search_index())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, it isn't search index", K(ret), K(table_param));
    } else if (included_cids.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search index included column ids is empty", K(ret));
    } else if (included_cid_idxes.count() > 0 && included_cid_idxes.count() != included_cids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search index included cid idxes size mismatch", K(ret), K(included_cids.count()), K(included_cid_idxes.count()));
    } else if (included_cids.count() + 1 != row_projector->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row projector", K(included_cids), KPC(row_projector), K(ret));
    } else {
      doc_proj = row_projector->at(included_cids.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < included_cids.count(); ++i) {
        ObObjMeta src_meta;
        bool found_meta = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found_meta && j < main_ctdef->column_ids_.count(); ++j) {
          if (main_ctdef->column_ids_.at(j) == included_cids.at(i)) {
            src_meta = main_ctdef->column_types_.at(j);
            found_meta = true;
          }
        }
        if (!found_meta) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find src meta for search index included column", K(ret), K(i), K(included_cids.at(i)));
        } else if (OB_FAIL(included_cid_metas.push_back(src_meta))) {
          LOG_WARN("failed to push meta", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(row_generator_.init(included_cid_idxes, included_cid_metas, *row_projector,
                  index_properties, false, &included_arr_types))) {
        LOG_WARN("failed to init row generator", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObSearchIndexDMLIterator::generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  blocksstable::ObDatumRow *row_array = nullptr;
  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(store_row));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_search_data_index())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, it isn't search index data index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
  } else if (OB_FAIL(row_generator_.set_row_projector(row_projector_))) {
    LOG_WARN("failed to set row projector", K(ret));
  } else if (OB_FAIL(row_generator_.generate_rows(store_row, row_array, row_cnt))) {
    LOG_WARN("failed to generate rows", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      if (OB_FAIL(rows_.push_back(&row_array[i]))) {
        LOG_WARN("fail to push back row", K(ret));
      }
    }
  }
  LOG_DEBUG("generate search index domain rows", K(ret), K(rows_), KPC(store_row));
  return ret;
}
} // end namespace storage
} // end namespace oceanbase
