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

#include "lib/objectpool/ob_server_object_pool.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "ob_lob_persistent_adaptor.h"
#include "ob_lob_piece.h"
#include "ob_lob_meta.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace storage
{

int ObPersistentLobApator::prepare_table_param(
  const ObLobAccessParam &param,
  ObTableScanParam &scan_param,
  bool is_meta)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObTableParam *table_param = NULL;
  HEAP_VAR(ObTableSchema, table_schema, param.allocator_) {
    // FIXME: use convert with ObStorageSchema intead of hard-code schema
    if (OB_UNLIKELY(scan_param.table_param_ != NULL)) {
      //do nothing
    } else if (is_meta && OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_meta_schema(table_schema))) {
      LOG_WARN("get lob meta schema failed", K(ret));
    } else if (!is_meta && OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_piece_schema(table_schema))) {
      LOG_WARN("get lob piece schema failed", K(ret));
    } else {
      // table_schema.set_tablet_id();
      if (NULL == (buf = param.allocator_->alloc(sizeof(ObTableParam)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory", K(ret));
      } else {
        table_param = new (buf) ObTableParam(*param.allocator_);
        if (OB_FAIL(table_param->convert(table_schema, scan_param.column_ids_))) {
          LOG_WARN("Fail to convert table param", K(ret));
        } else {
          scan_param.table_param_ = table_param;
        }
      }
    }
  }
  return ret;
}

int ObPersistentLobApator::scan_lob_meta(
  ObLobAccessParam &param,
  ObTableScanParam &scan_param,
  common::ObNewRowIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;
  if (OB_FAIL(get_lob_tablets(param, data_tablet, lob_meta_tablet, lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    // 2. prepare tablet scan param
    scan_param.tablet_id_ = lob_meta_tablet.get_obj()->get_tablet_meta().tablet_id_;
    scan_param.schema_version_ = lob_meta_tablet.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
    const uint64_t table_id = 0;
    scan_param.table_param_ = param.meta_tablet_param_;
    if (OB_FAIL(build_common_scan_param(param, table_id, ObLobMetaUtil::LOB_META_COLUMN_CNT, scan_param))) {
      LOG_WARN("build common scan param failed.", K(ret));
    } else if (OB_FAIL(prepare_table_param(param, scan_param, true))) {
      LOG_WARN("prepare lob meta table param failed.", K(ret));
    } else {
      // build key range
      void *buf = param.allocator_->alloc(sizeof(ObObj) * 4);
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alloc range obj failed.", K(ret));
      } else {
        ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
        const char *lob_id_ptr_ = reinterpret_cast<char*>(&param.lob_data_->id_);
        row_objs[0].reset();
        row_objs[0].set_varchar(lob_id_ptr_, sizeof(ObLobId)); // lob_id
        row_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
        row_objs[1] = ObObj::make_min_obj(); // seq_id set min
        ObRowkey min_row_key(row_objs, 2);

        row_objs[2].reset();
        row_objs[2].set_varchar(lob_id_ptr_, sizeof(ObLobId)); // lob_id
        row_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
        row_objs[3] = ObObj::make_max_obj(); // seq_id set max
        ObRowkey max_row_key(row_objs + 2, 2);

        common::ObNewRange range;
        range.table_id_ = table_id;
        range.start_key_ = min_row_key;
        range.end_key_ = max_row_key;
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();
        scan_param.key_ranges_.reset();
        if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
          LOG_WARN("failed to push key range.", K(ret), K(scan_param), K(range));
        } else {
          ObAccessService *oas = MTL(ObAccessService*);
          if (OB_ISNULL(oas)) {
            ret = OB_ERR_INTERVAL_INVALID;
            LOG_WARN("get access service failed.", K(ret));
          } else if (OB_FAIL(oas->table_scan(scan_param, meta_iter))) {
            LOG_WARN("do table scan falied.", K(ret), K(scan_param));
          }
        }
      }
      param.allocator_->free(buf);
    }
  }
  return ret;
}

int ObPersistentLobApator::get_lob_data(
    ObLobAccessParam &param,
    uint64_t piece_id,
    ObLobPieceInfo& info)
{
  int ret = OB_SUCCESS;
  if (piece_id == ObLobMetaUtil::LOB_META_INLINE_PIECE_ID) {
    // do nothing. read data from lob meta directly
  } else {
    // 1. get tablets
    ObTabletHandle data_tablet;
    ObTabletHandle lob_meta_tablet;
    ObTabletHandle lob_piece_tablet;
    if (OB_FAIL(get_lob_tablets(param, data_tablet, lob_meta_tablet, lob_piece_tablet))) {
      LOG_WARN("failed to get tablets.", K(ret), K(param));
    } else {
      uint64_t tenant_id = param.tenant_id_;
      // 2. prepare tablet scan param
      ObTableScanParam scan_param;
      scan_param.table_param_ = param.piece_tablet_param_;
      scan_param.tablet_id_ = lob_piece_tablet.get_obj()->get_tablet_meta().tablet_id_;
      scan_param.schema_version_ = lob_piece_tablet.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
      const uint64_t table_id = 0;
      bool tmp_scan_backward = param.scan_backward_;
      param.scan_backward_ = false;
      if (OB_FAIL(build_common_scan_param(param, table_id, ObLobPieceUtil::LOB_PIECE_COLUMN_CNT, scan_param))) {
        LOG_WARN("build common scan param failed.", K(ret));
      } else if (OB_FAIL(prepare_table_param(param, scan_param, false))) {
        LOG_WARN("prepare lob meta table param failed.", K(ret));
      } else {
        // set is_get
        scan_param.is_get_ = true;
        // build key range
        void *buf = param.allocator_->alloc(sizeof(ObObj));
        if (OB_ISNULL(buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc range obj failed.", K(ret));
        } else {
          ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
          row_objs[0].reset();
          row_objs[0].set_uint64(piece_id);
          ObRowkey row_key(row_objs, 1);

          common::ObNewRange range;
          range.table_id_ = table_id;
          range.start_key_ = row_key;
          range.end_key_ = row_key;
          range.border_flag_.set_inclusive_start();
          range.border_flag_.set_inclusive_end();

          if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
            LOG_WARN("failed to push key range.", K(ret), K(scan_param), K(range));
          } else {
            ObAccessService *oas = MTL(ObAccessService*);
            common::ObNewRowIterator *iter = nullptr;
            if (OB_ISNULL(oas)) {
              ret = OB_ERR_INTERVAL_INVALID;
              LOG_WARN("get access service failed.", K(ret));
            } else if (OB_FAIL(oas->table_scan(scan_param, iter))) {
              LOG_WARN("do table scan falied.", K(ret), K(scan_param));
            } else {
              oceanbase::common::ObNewRow *row = nullptr;
              ret = iter->get_next_row(row);
              if (OB_FAIL(ret)) {
                LOG_WARN("get next row failed.", K(ret));
              } else if (OB_FAIL(ObLobPieceUtil::transform(row, info))) {
                LOG_WARN("failed to transform row.", K(ret));
              }
            }
            if (OB_NOT_NULL(iter)) {
              iter->reset();
            }
          }
        }
        param.allocator_->free(buf);
      }
      if (OB_SUCC(ret)) {
        param.scan_backward_  = tmp_scan_backward; // recover
      }
    }
  }

  return ret;
}

int ObPersistentLobApator::revert_scan_iter(common::ObNewRowIterator *iter)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret));
  } else if (OB_FAIL(oas->revert_scan_iter(iter))) {
    LOG_WARN("revert scan iterator failed", K(ret));
  }
  return ret;
}

int ObPersistentLobApator::fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id)
{
  int ret = OB_SUCCESS;
  common::ObTabletID lob_meta_tablet_id;
  common::ObTabletID lob_piece_tablet_id;
  if (OB_FAIL(get_lob_tablets_id(param, lob_meta_tablet_id, lob_piece_tablet_id))) {
    LOG_WARN("get lob tablet id failed.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    share::ObTabletAutoincrementService &auto_inc = share::ObTabletAutoincrementService::get_instance();
    if (OB_FAIL(auto_inc.get_autoinc_seq(tenant_id, lob_meta_tablet_id, lob_id))) {
      LOG_WARN("get lob_id fail", K(ret), K(tenant_id), K(lob_meta_tablet_id));
    } else {
      LOG_DEBUG("get lob_id succ", K(lob_id), K(tenant_id), K(lob_meta_tablet_id));
    }
  }
  return ret;
}

int ObPersistentLobApator::prepare_lob_meta_dml(
    ObLobAccessParam& param,
    const uint64_t tenant_id,
    const ObTabletHandle& data_tablet,
    const ObTabletHandle& lob_meta_tablet)
{
  int ret = OB_SUCCESS;
  if (param.dml_base_param_ == nullptr) {
    share::schema::ObTableDMLParam* table_dml_param = nullptr;
    void *buf = param.allocator_->alloc(sizeof(ObDMLBaseParam));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc dml base param", K(ret));
    } else {
      param.dml_base_param_ = new(buf)ObDMLBaseParam();
      buf = param.allocator_->alloc(sizeof(share::schema::ObTableDMLParam));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc table dml param", K(ret));
      } else {
        table_dml_param = new(buf)share::schema::ObTableDMLParam(*param.allocator_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_lob_meta_table_dml(param, tenant_id, table_dml_param,
                                                *param.dml_base_param_, param.column_ids_, data_tablet, lob_meta_tablet))) {
      LOG_WARN("failed to build meta schema", K(ret), K(data_tablet), K(lob_meta_tablet));
    }
  } else {
    if (param.seq_no_st_.is_valid()) {
      if (param.used_seq_cnt_ < param.total_seq_cnt_) {
        param.dml_base_param_->spec_seq_no_ = param.seq_no_st_ + param.used_seq_cnt_;
        param.used_seq_cnt_++;
        LOG_DEBUG("dml lob meta with seq no", K(param.dml_base_param_->spec_seq_no_));
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to get seq no from param.", K(ret), K(param));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid seq no from param.", K(ret), K(param));
    }
  }
  return ret;
}

int ObPersistentLobApator::build_lob_meta_table_dml(
    ObLobAccessParam& param,
    const uint64_t tenant_id,
    ObTableDMLParam* dml_param,
    ObDMLBaseParam& dml_base_param,
    ObSEArray<uint64_t, 6>& column_ids,
    const ObTabletHandle& data_tablet,
    const ObTabletHandle& lob_meta_tablet)
{
  int ret = OB_SUCCESS;
  // dml base
  dml_base_param.timeout_ = param.timeout_;
  dml_base_param.is_total_quantity_log_ = param.is_total_quantity_log_;
  dml_base_param.tz_info_ = NULL;
  dml_base_param.sql_mode_ = SMO_DEFAULT;
  dml_base_param.encrypt_meta_ = &dml_base_param.encrypt_meta_legacy_;
  dml_base_param.snapshot_ = param.snapshot_;
  dml_base_param.check_schema_version_ = false; // lob tablet should not check schema version
  if (param.seq_no_st_.is_valid()) {
    if (param.used_seq_cnt_ < param.total_seq_cnt_) {
      dml_base_param.spec_seq_no_ = param.seq_no_st_ + param.used_seq_cnt_;
      param.used_seq_cnt_++;
      LOG_DEBUG("dml lob meta with seq no", K(dml_base_param.spec_seq_no_));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get seq no from param.", K(ret), K(param));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seq no from param.", K(ret), K(param));
  }

  HEAP_VAR(ObTableSchema, tbl_schema, param.allocator_) {
    ObTableSchema* table_schema = param.meta_table_schema_;

    for (int i = 0; OB_SUCC(ret) && i < ObLobMetaUtil::LOB_META_COLUMN_CNT; ++i) {
      if (OB_FAIL(column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("push column ids failed.", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (table_schema == nullptr) {
      table_schema = &tbl_schema;
      if (OB_FAIL(get_lob_tablet_schema(tenant_id, true, *table_schema, dml_base_param.tenant_schema_version_))) {
        LOG_WARN("failed get lob tablet schema.", K(ret));
      } else {
        dml_base_param.schema_version_ = lob_meta_tablet.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
      }
    } else {
      /**
       * for test current
      */
      dml_base_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
      dml_base_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml_param->convert(table_schema, dml_base_param.tenant_schema_version_, column_ids))) {
      LOG_WARN("failed to convert dml param.", K(ret));
    } else {
      dml_base_param.table_param_ = dml_param;
    }
  }
  return ret;
}

int ObPersistentLobApator::erase_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;

  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;
  // get Access service
  ObAccessService *oas = MTL(ObAccessService*);

  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret), KP(oas));
  } else if (OB_FAIL(get_lob_tablets(param,
                                     data_tablet,
                                     lob_meta_tablet,
                                     lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else if (OB_ISNULL(param.tx_desc_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    if (OB_FAIL(prepare_lob_meta_dml(param, tenant_id, data_tablet, lob_meta_tablet))) {
      LOG_WARN("failed to prepare lob meta dml", K(ret));
    } else {
      // get tx desc
      transaction::ObTxDesc* tx_desc = param.tx_desc_;

      int64_t affected_rows = 0;
      ObObj cell[ObLobMetaUtil::LOB_META_COLUMN_CNT];
      ObNewRow new_row;
      common::ObSingleRowIteratorWrapper single_iter;
      set_lob_meta_row(cell, new_row, in_row);
      single_iter.set_row(&new_row);
      if (OB_FAIL(oas->delete_rows(param.ls_id_,
                                    lob_meta_tablet.get_obj()->get_tablet_meta().tablet_id_,
                                    *tx_desc,
                                    *param.dml_base_param_,
                                    param.column_ids_,
                                    &single_iter,
                                    affected_rows))) {
        LOG_WARN("failed to insert row.", K(ret));
      }
    }
  }
  return ret;
}

int ObPersistentLobApator::erase_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;

  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;

  // get Access service
  ObAccessService *oas = MTL(ObAccessService*);

  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret), KP(oas));
  } else if (OB_FAIL(get_lob_tablets(param,
                                     data_tablet,
                                     lob_meta_tablet,
                                     lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else if (OB_ISNULL(param.tx_desc_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;

    ObDMLBaseParam dml_base_param;
    share::schema::ObTableDMLParam table_dml_param(*param.allocator_);
    ObSEArray<uint64_t, ObLobPieceUtil::LOB_PIECE_COLUMN_CNT> column_ids;

    if (OB_FAIL(build_lob_piece_table_dml(param, tenant_id, table_dml_param, dml_base_param,
                                             column_ids, data_tablet, lob_piece_tablet))) {
      LOG_WARN("failed to build piece schema", K(ret), K(data_tablet), K(lob_piece_tablet));
    } else {

      transaction::ObTxDesc* tx_desc = param.tx_desc_;;

      // construct insert data
      int64_t affected_rows = 0;
      ObObj cell[ObLobPieceUtil::LOB_PIECE_COLUMN_CNT];
      char serialize_buf[32] = {0};
      // make insert iterator
      ObNewRow new_row;

      common::ObSingleRowIteratorWrapper single_iter;
      single_iter.set_row(&new_row);

      if (OB_FAIL(set_lob_piece_row(serialize_buf, 32, cell, new_row, &single_iter, in_row))) {
        LOG_WARN("failed to set insert piece row.", K(ret), K(in_row));
      } else if (OB_FAIL(oas->delete_rows(param.ls_id_,
                                    lob_piece_tablet.get_obj()->get_tablet_meta().tablet_id_,
                                    *tx_desc,
                                    dml_base_param,
                                    column_ids,
                                    &single_iter,
                                    affected_rows))) {
        LOG_WARN("failed to insert row.", K(ret));
      }
    }
  }
  return ret;
}

int ObPersistentLobApator::write_lob_meta(ObLobAccessParam& param, ObLobMetaInfo& in_row)
{
  int ret = OB_SUCCESS;

  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;

  ObAccessService *oas = MTL(ObAccessService*);

  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret), KP(oas));
  } else if (OB_FAIL(get_lob_tablets(param,
                                     data_tablet,
                                     lob_meta_tablet,
                                     lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else if (OB_ISNULL(param.tx_desc_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    if (OB_FAIL(prepare_lob_meta_dml(param, tenant_id, data_tablet, lob_meta_tablet))) {
      LOG_WARN("failed to prepare lob meta dml.", K(ret));
    } else {
      // get tx desc
      transaction::ObTxDesc* tx_desc = param.tx_desc_;;

      int64_t affected_rows = 0;
      ObObj cell[ObLobMetaUtil::LOB_META_COLUMN_CNT];
      ObNewRow new_row;
      common::ObSingleRowIteratorWrapper single_iter;
      set_lob_meta_row(cell, new_row, in_row);
      single_iter.set_row(&new_row);
      if (OB_FAIL(oas->insert_rows(param.ls_id_,
                                    lob_meta_tablet.get_obj()->get_tablet_meta().tablet_id_,
                                    *tx_desc,
                                    *param.dml_base_param_,
                                    param.column_ids_,
                                    &single_iter,
                                    affected_rows))) {
        LOG_WARN("failed to insert row.", K(ret));
      }
    }
  }
  return ret;
}

int ObPersistentLobApator::update_lob_meta(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row)
{
  int ret = OB_SUCCESS;

  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;

  // get Access service
  ObAccessService *oas = MTL(ObAccessService*);

  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret), KP(oas));
  } else if (OB_FAIL(get_lob_tablets(param,
                                     data_tablet,
                                     lob_meta_tablet,
                                     lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else if (OB_ISNULL(param.tx_desc_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    ObSEArray<uint64_t, 6> update_column_ids;

    for (int i = 2; OB_SUCC(ret) && i < ObLobMetaUtil::LOB_META_COLUMN_CNT; ++i) {
      if (OB_FAIL(update_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("push column ids failed.", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_lob_meta_dml(param, tenant_id, data_tablet, lob_meta_tablet))) {
      LOG_WARN("failed to prepare lob meta dml", K(ret));
    } else {
      // get tx desc
      transaction::ObTxDesc* tx_desc = param.tx_desc_;

      int64_t affected_rows = 0;
      ObObj new_row_cell[ObLobMetaUtil::LOB_META_COLUMN_CNT];
      ObNewRow new_tbl_row;
      set_lob_meta_row(new_row_cell, new_tbl_row, new_row);
      ObObj old_row_cell[ObLobMetaUtil::LOB_META_COLUMN_CNT];
      ObNewRow old_tbl_row;
      set_lob_meta_row(old_row_cell, old_tbl_row, old_row);
      ObLobUpdIterator upd_iter(&old_tbl_row, &new_tbl_row);
      if (OB_FAIL(oas->update_rows(param.ls_id_,
                                    lob_meta_tablet.get_obj()->get_tablet_meta().tablet_id_,
                                    *tx_desc,
                                    *param.dml_base_param_,
                                    param.column_ids_,
                                    update_column_ids,
                                    &upd_iter,
                                    affected_rows))) {
        LOG_WARN("failed to insert row.", K(ret));
      }
    }
  }
  return ret;
}


int ObPersistentLobApator::build_lob_piece_table_dml(
    ObLobAccessParam& param,
    const uint64_t tenant_id,
    share::schema::ObTableDMLParam& dml_param,
    ObDMLBaseParam& dml_base_param,
    ObSEArray<uint64_t, ObLobPieceUtil::LOB_PIECE_COLUMN_CNT> &column_ids,
    const ObTabletHandle& data_tablet,
    const ObTabletHandle& lob_piece_tablet)
{
  int ret = OB_SUCCESS;

  // dml base
  dml_base_param.timeout_ = param.timeout_;
  dml_base_param.is_total_quantity_log_ = param.is_total_quantity_log_;
  dml_base_param.tz_info_ = NULL;
  dml_base_param.sql_mode_ = SMO_DEFAULT;
  dml_base_param.encrypt_meta_ = &dml_base_param.encrypt_meta_legacy_;

  HEAP_VAR(ObTableSchema, tbl_schema, param.allocator_) {
    ObTableSchema* table_schema = param.piece_table_schema_;

    for (int i = 0; OB_SUCC(ret) && i < ObLobPieceUtil::LOB_PIECE_COLUMN_CNT; ++i) {
      if (OB_FAIL(column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("push column ids failed.", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (table_schema == nullptr) {
      table_schema = &tbl_schema;
      if (OB_FAIL(get_lob_tablet_schema(tenant_id, false, *table_schema, dml_base_param.tenant_schema_version_))) {
        LOG_WARN("failed get lob tablet schema.", K(ret));
      } else {
        dml_base_param.schema_version_ = lob_piece_tablet.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
      }
    } else {
      /**
       * for test current
      */
      dml_base_param.schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
      dml_base_param.tenant_schema_version_ = share::OB_CORE_SCHEMA_VERSION + 1;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml_param.convert(table_schema, dml_base_param.tenant_schema_version_, column_ids))) {
      LOG_WARN("failed to convert dml param.", K(ret));
    } else {
      dml_base_param.table_param_ = &dml_param;
    }
  }
  return ret;
}

int ObPersistentLobApator::write_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;

  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;

  // get Access service
  ObAccessService *oas = MTL(ObAccessService*);

  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret), KP(oas));
  } else if (OB_FAIL(get_lob_tablets(param,
                                     data_tablet,
                                     lob_meta_tablet,
                                     lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else if (OB_ISNULL(param.tx_desc_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;

    ObDMLBaseParam dml_base_param;
    share::schema::ObTableDMLParam table_dml_param(*param.allocator_);
    ObSEArray<uint64_t, ObLobPieceUtil::LOB_PIECE_COLUMN_CNT> column_ids;

    if (OB_FAIL(build_lob_piece_table_dml(param, tenant_id, table_dml_param, dml_base_param,
                                             column_ids, data_tablet, lob_piece_tablet))) {
      LOG_WARN("failed to build piece schema", K(ret), K(data_tablet), K(lob_piece_tablet));
    } else {
      // get tx desc
      transaction::ObTxDesc* tx_desc = param.tx_desc_;;

      // construct insert data
      int64_t affected_rows = 0;
      ObObj cell[ObLobPieceUtil::LOB_PIECE_COLUMN_CNT];
      char serialize_buf[32] = {0};
      // make insert iterator
      ObNewRow new_row;

      common::ObSingleRowIteratorWrapper single_iter;
      single_iter.set_row(&new_row);

      if (OB_FAIL(set_lob_piece_row(serialize_buf, 32, cell, new_row, &single_iter, in_row))) {
        LOG_WARN("failed to set insert piece row.", K(ret), K(in_row));
      } else if (OB_FAIL(oas->insert_rows(param.ls_id_,
                                    lob_piece_tablet.get_obj()->get_tablet_meta().tablet_id_,
                                    *tx_desc,
                                    dml_base_param,
                                    column_ids,
                                    &single_iter,
                                    affected_rows))) {
        LOG_WARN("failed to insert row.", K(ret));
      }
    }
  }
  return ret;
}

int ObPersistentLobApator::update_lob_piece_tablet(ObLobAccessParam& param, ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;

  // 1. get tablets
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;

  // get Access service
  ObAccessService *oas = MTL(ObAccessService*);

  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("get access service failed.", K(ret), KP(oas));
  } else if (OB_FAIL(get_lob_tablets(param,
                                     data_tablet,
                                     lob_meta_tablet,
                                     lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret), K(param));
  } else if (OB_ISNULL(param.tx_desc_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get tx desc null.", K(ret), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;

    ObDMLBaseParam dml_base_param;
    share::schema::ObTableDMLParam table_dml_param(*param.allocator_);
    ObSEArray<uint64_t, ObLobPieceUtil::LOB_PIECE_COLUMN_CNT> column_ids, update_column_ids;

    for (int i = 1; OB_SUCC(ret) && i < ObLobPieceUtil::LOB_PIECE_COLUMN_CNT; ++i) {
      if (OB_FAIL(update_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("push column ids failed.", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_lob_piece_table_dml(param, tenant_id, table_dml_param, dml_base_param,
                                             column_ids, data_tablet, lob_piece_tablet))) {
      LOG_WARN("failed to build piece schema", K(ret), K(data_tablet), K(lob_piece_tablet));
    } else {
      // get tx desc
      transaction::ObTxDesc* tx_desc = param.tx_desc_;

      // construct insert data
      int64_t affected_rows = 0;
      ObObj cell[ObLobPieceUtil::LOB_PIECE_COLUMN_CNT];
      char serialize_buf[32] = {0};
      // make insert iterator
      ObNewRow new_row;

      common::ObSingleRowIteratorWrapper single_iter;
      single_iter.set_row(&new_row);

      if (OB_FAIL(set_lob_piece_row(serialize_buf, 32, cell, new_row, &single_iter, in_row))) {
        LOG_WARN("failed to set insert piece row.", K(ret), K(in_row));
      } else if (OB_FAIL(oas->update_rows(param.ls_id_,
                                    lob_piece_tablet.get_obj()->get_tablet_meta().tablet_id_,
                                    *tx_desc,
                                    dml_base_param,
                                    column_ids,
                                    update_column_ids,
                                    &single_iter,
                                    affected_rows))) {
        LOG_WARN("failed to insert row.", K(ret));
      }
    }
  }
  return ret;
}

int ObPersistentLobApator::get_lob_tablet_schema(
    uint64_t tenant_id,
    bool is_meta,
    ObTableSchema& schema,
    int64_t &tenant_schema_version)
{
  int ret = OB_SUCCESS;
  ObTenantSchemaService* tenant_service = MTL(ObTenantSchemaService*);
  ObMultiVersionSchemaService * schema_service = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(schema_service = tenant_service->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get multi ver schema service.", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed get schema guard.", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, tenant_schema_version))) {
    LOG_WARN("failed to get tenant_schema_version.", K(ret), K(tenant_id));
  } else if (is_meta && OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_meta_schema(schema))) {
    LOG_WARN("get lob meta schema failed", K(ret));
  } else if (!is_meta && OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_piece_schema(schema))) {
    LOG_WARN("get lob piece schema failed", K(ret));
  }

  return ret;
}

int ObPersistentLobApator::build_common_scan_param(
    const ObLobAccessParam &param,
    const uint64_t table_id,
    uint32_t col_num,
    ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = param.tenant_id_;
  scan_param.ls_id_ = param.ls_id_;

  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                          false, // daily_merge
                          false, // optimize
                          false, // sys scan
                          true, // full_row
                          false, // index_back
                          false, // query_stat
                          ObQueryFlag::MysqlMode, // sql_mode
                          false // read_latest
                        );
  query_flag.disable_cache();
  query_flag.scan_order_ = param.scan_backward_ ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
  scan_param.scan_flag_.flag_ = query_flag.flag_;
  // set column ids
  scan_param.column_ids_.reset();
  for (uint32_t i = 0; OB_SUCC(ret) && i < col_num; i++) {
    if (OB_FAIL(scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("push col id failed.", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
    // table param
    scan_param.index_id_ = table_id; // table id
    scan_param.is_get_ = false;
    // set timeout
    scan_param.timeout_ = param.timeout_;
    // scan_param.virtual_column_exprs_
    scan_param.limit_param_.limit_ = -1;
    scan_param.limit_param_.offset_ = 0;
    // sessions
    scan_param.snapshot_ = param.snapshot_;
    scan_param.sql_mode_ = param.sql_mode_;
    // common set
    scan_param.allocator_ = param.allocator_;
    scan_param.for_update_ = false;
    scan_param.for_update_wait_timeout_ = scan_param.timeout_;
    scan_param.scan_allocator_ = param.allocator_;
    scan_param.frozen_version_ = -1;
    scan_param.force_refresh_lc_ = false;
    scan_param.output_exprs_ = nullptr;
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.op_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
    scan_param.need_scn_ = false;
    scan_param.pd_storage_flag_ = false;
    scan_param.fb_snapshot_ = param.fb_snapshot_;
  }
  return ret;
}

bool ObPersistentLobApator::check_lob_tablet_id(
    const common::ObTabletID &data_tablet_id,
    const common::ObTabletID &lob_meta_tablet_id,
    const common::ObTabletID &lob_piece_tablet_id)
{
  bool bret = false;
  if (OB_UNLIKELY(!lob_meta_tablet_id.is_valid() || lob_meta_tablet_id == data_tablet_id)) {
    bret = true;
  } else if (OB_UNLIKELY(!lob_piece_tablet_id.is_valid() || lob_piece_tablet_id == data_tablet_id)) {
    bret = true;
  } else if (OB_UNLIKELY(lob_meta_tablet_id == lob_piece_tablet_id)) {
    bret = true;
  }
  return bret;
}

int ObPersistentLobApator::get_lob_tablets(
    ObLobAccessParam& param,
    ObTabletHandle &data_tablet,
    ObTabletHandle &lob_meta_tablet,
    ObTabletHandle &lob_piece_tablet)
{
  int ret = OB_SUCCESS;
  ObTabletBindingMdsUserData ddl_data;
  if (OB_FAIL(inner_get_tablet(param, param.tablet_id_, data_tablet))) {
    LOG_WARN("failed to get data tablet", K(ret), K(param.ls_id_), K(param.tablet_id_));
  } else if (OB_FAIL(data_tablet.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet));
  } else {
    const common::ObTabletID &lob_meta_tablet_id = ddl_data.lob_meta_tablet_id_;
    const common::ObTabletID &lob_piece_tablet_id = ddl_data.lob_piece_tablet_id_;
    if (OB_UNLIKELY(check_lob_tablet_id(param.tablet_id_, lob_meta_tablet_id, lob_piece_tablet_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data or lob tablet id.", K(ret), K(param.tablet_id_), K(lob_meta_tablet_id), K(lob_piece_tablet_id));
    } else if (OB_FAIL(inner_get_tablet(param, lob_meta_tablet_id, lob_meta_tablet))) {
      LOG_WARN("get lob meta tablet failed.", K(ret), K(lob_meta_tablet_id));
    } else if (OB_FAIL(inner_get_tablet(param, lob_piece_tablet_id, lob_piece_tablet))) {
      LOG_WARN("get lob meta tablet failed.", K(ret), K(lob_piece_tablet_id));
    }
  }
  return ret;
}

int ObPersistentLobApator::get_lob_tablets_id(
    ObLobAccessParam& param,
    common::ObTabletID &lob_meta_tablet_id,
    common::ObTabletID &lob_piece_tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle data_tablet;
  ObTabletHandle lob_meta_tablet;
  ObTabletHandle lob_piece_tablet;

   if (OB_FAIL(get_lob_tablets(param,
                               data_tablet,
                               lob_meta_tablet,
                               lob_piece_tablet))) {
    LOG_WARN("failed to get tablets.", K(ret));
  } else {
    lob_meta_tablet_id = lob_meta_tablet.get_obj()->get_tablet_meta().tablet_id_;
    lob_piece_tablet_id = lob_piece_tablet.get_obj()->get_tablet_meta().tablet_id_;
  }

  return ret;
}

int ObPersistentLobApator::inner_get_tablet(
    const ObLobAccessParam &param,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(param.ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(tablet_id,
                                                                 handle,
                                                                 param.timeout_,
                                                                 ObMDSGetTabletMode::READ_READABLE_COMMITED,
                                                                 param.snapshot_.core_.version_))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id), K(param));
  }
  return ret;
}

void ObPersistentLobApator::set_lob_meta_row(
    ObObj* cell,
    ObNewRow& new_row,
    ObLobMetaInfo& in_row)
{
  for (int64_t i = 0; i < ObLobMetaUtil::LOB_META_COLUMN_CNT; ++i) {
    cell[i].reset();
    cell[i].set_nop_value();
  }
  cell[ObLobMetaUtil::LOB_ID_COL_ID].set_varchar(reinterpret_cast<char*>(&in_row.lob_id_), sizeof(ObLobId));
  cell[ObLobMetaUtil::LOB_ID_COL_ID].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  cell[ObLobMetaUtil::SEQ_ID_COL_ID].set_varchar(in_row.seq_id_);
  cell[ObLobMetaUtil::SEQ_ID_COL_ID].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  cell[ObLobMetaUtil::BYTE_LEN_COL_ID].set_uint32(in_row.byte_len_);
  cell[ObLobMetaUtil::CHAR_LEN_COL_ID].set_uint32(in_row.char_len_);
  cell[ObLobMetaUtil::PIECE_ID_COL_ID].set_uint64(in_row.piece_id_);

  cell[ObLobMetaUtil::LOB_DATA_COL_ID].set_varchar(in_row.lob_data_);
  cell[ObLobMetaUtil::LOB_DATA_COL_ID].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);

  new_row.assign(cell, ObLobMetaUtil::LOB_META_COLUMN_CNT);
}

int ObPersistentLobApator::set_lob_piece_row(
    char* buf,
    size_t buf_len,
    ObObj* cell,
    ObNewRow& new_row,
    common::ObSingleRowIteratorWrapper* new_row_iter,
    ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ObLobPieceUtil::LOB_PIECE_COLUMN_CNT; ++i) {
    cell[i].reset();
    cell[i].set_nop_value();
  }
  cell[0].set_uint64(in_row.piece_id_);
  cell[1].set_uint32(in_row.len_);

  int64_t pos = 0;
  if (!in_row.macro_id_.is_valid()) {
    LOG_WARN("failed to serialize macro id, macro id invalid", K(ret), K(in_row.macro_id_));
  } else if (OB_FAIL(in_row.macro_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize macro id", K(ret), K(buf_len), K(pos));
  } else {
    cell[2].set_varchar(buf, pos);
    cell[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);

    new_row.assign(cell, ObLobPieceUtil::LOB_PIECE_COLUMN_CNT);
    new_row_iter->set_row(&new_row);
  }

  return ret;
}

int ObLobUpdIterator::get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_row_) || OB_ISNULL(new_row_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get next row failed, null val.", K(ret), K(old_row_), K(new_row_));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (!got_old_row_) {
    row = old_row_;
    got_old_row_ = true;
  } else {
    row = new_row_;
    got_old_row_ = false;
    is_iter_end_ = true;
  }
  return ret;
}

} // storage
} // oceanbase

