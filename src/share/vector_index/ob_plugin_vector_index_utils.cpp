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
#define USING_LOG_PREFIX SERVER
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/ob_vec_index_builder_util.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace share
{

int ObPluginVectorIndexUtils::get_task_read_snapshot(ObLSID &ls_id, SCN &read_version)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  // ObLSWRSHandler::get_ls_weak_read_ts
  storage::ObLSService *ls_svr = MTL(storage::ObLSService*);
  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ls", K(ret), K(ls_id));
  } else {
    read_version = ls->get_ls_wrs_handler()->get_ls_weak_read_ts();
  }

  return ret;
}

int ObPluginVectorIndexUtils::add_key_ranges(uint64_t table_id, ObRowkey& rowkey, storage::ObTableScanParam &scan_param)
{
  INIT_SUCC(ret);
  ObNewRange new_range;
  if (OB_NOT_NULL(scan_param.scan_allocator_)) {
    // LOG_INFO("scan_allocator_before_reuse", K(scan_param.scan_allocator_->used()), K(scan_param.scan_allocator_->total()));
    scan_param.scan_allocator_->reuse();
    // LOG_INFO("scan_allocator_after_reuse", K(scan_param.scan_allocator_->used()), K(scan_param.scan_allocator_->total()));
  }
  if (OB_FAIL(new_range.build_range(table_id, rowkey))) {
    LOG_WARN("failed to build range.", K(ret), K(table_id), K(rowkey));
  } else if (FALSE_IT(scan_param.key_ranges_.reuse())) {
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(new_range))) {
    LOG_WARN("failed to build key ranges.", K(ret), K(table_id), K(rowkey));
  }

  return ret;
}

int ObPluginVectorIndexUtils::iter_table_rescan(storage::ObTableScanParam &scan_param, common::ObNewRowIterator *iter)
{
  INIT_SUCC(ret);
  ObAccessService *tsc_service = MTL(ObAccessService *);

  if (OB_FAIL(tsc_service->reuse_scan_iter(false, iter))) {
    LOG_WARN("failed to reuse scan iter.", K(ret));
  } else if (OB_FAIL(tsc_service->table_rescan(scan_param, iter))) {
    LOG_WARN("failed to rescan iter.", K(ret));
  }

  return ret;
}

int ObPluginVectorIndexUtils::get_extra_info_objs(storage::ObTableScanParam &scan_param,
                                                  ObIAllocator &allocator,
                                                  int64_t extra_column_count,
                                                  blocksstable::ObDatumRow *datum_row,
                                                  ObVecExtraInfoObj *out_extra_info_objs)
{
  INIT_SUCC(ret);
  if (extra_column_count == 0) {
  } else if (OB_ISNULL(out_extra_info_objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(extra_column_count));
  } else {
    const ObIArray<share::schema::ObColumnParam *> *out_col_param =
        scan_param.table_param_->get_read_info().get_columns();
    const ObIArray<int32_t> &out_idxs = scan_param.table_param_->get_output_projector();
    if (OB_ISNULL(out_col_param) || out_idxs.count() != extra_column_count + 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column count not equal.", K(ret), KP(out_col_param), K(out_idxs), K(extra_column_count));
    }
    for (int i = 0; OB_SUCC(ret) && i < extra_column_count; ++i) {
      if (out_idxs.at(i + 1) >= out_col_param->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column count not equal.", K(ret), KP(out_col_param), K(out_idxs), K(i));
      } else {
        ObObjMeta meta_type = out_col_param->at(out_idxs.at(i + 1))->get_meta_type();
        const ObDatum &extra_datum = datum_row->storage_datums_[i + 1];
        if (OB_FALSE_IT(out_extra_info_objs[i].reset())) {
        } else if (OB_FAIL(out_extra_info_objs[i].from_datum(extra_datum, meta_type, &allocator))) {
          LOG_WARN("failed to from obj.", K(ret), K(extra_datum), K(meta_type), K(i));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::read_object_from_data_table_iter(ObObj *&input_obj,
                                                               int32_t data_table_rowkey_count,
                                                               uint64_t table_id,
                                                               storage::ObTableScanParam &scan_param,
                                                               common::ObNewRowIterator *iter,
                                                               schema::ObIndexType type,
                                                               ObIAllocator &allocator,
                                                               ObObj &output_vec_obj,
                                                               int64_t extra_column_count,
                                                               ObVecExtraInfoObj *output_extra_info_objs,
                                                               bool &get_data)
{
  INIT_SUCC(ret);
  ObRowkey rowkey(input_obj, data_table_rowkey_count);

  ObString vector;
  if (OB_FAIL(add_key_ranges(table_id, rowkey, scan_param))) {
    LOG_WARN("failed to set vid id key", K(ret));
  } else if (OB_FAIL(iter_table_rescan(scan_param, iter))) {
    LOG_WARN("failed to recan vid id scan param.", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    storage::ObTableScanIterator *scan_iter = dynamic_cast<storage::ObTableScanIterator *>(iter);

    if (OB_ISNULL(scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast to vid iter.", K(ret));
    } else if (OB_FAIL(scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from next table.", K(ret));
      } else {
        output_vec_obj.reset();
        ret = OB_SUCCESS;
      }
    } else {
      if (datum_row->get_column_count() != 1 + extra_column_count &&  // at least vector col
          datum_row->get_column_count() != 2) {  // vector col and pk_increrment
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(extra_column_count), K(datum_row->get_column_count()));
      } else {
        if (extra_column_count > 0 && OB_FAIL(get_extra_info_objs(scan_param, allocator, extra_column_count, datum_row, output_extra_info_objs))) {
          LOG_WARN("failed to get extra info.", K(ret), K(extra_column_count), K(datum_row->storage_datums_[extra_column_count]));
        } else {
          char *copy_str = nullptr;
          ObString vector = datum_row->storage_datums_[0].get_string();
          int64_t size = vector.length();
          if (size == 0) {
            output_vec_obj.reset();
          } else if (OB_ISNULL(copy_str = static_cast<char *>(allocator.alloc(sizeof(char) * size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocator.", K(ret));
          } else {
            memcpy(copy_str, vector.ptr(), size);
            output_vec_obj.reset();
            output_vec_obj.set_string(ObVarcharType, copy_str, size);
            get_data = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::read_object_from_vid_rowkey_table_iter(ObObj *input_obj,
                                                                     uint64_t table_id,
                                                                     storage::ObTableScanParam &scan_param,
                                                                     common::ObNewRowIterator *iter,
                                                                     schema::ObIndexType type,
                                                                     ObIAllocator &allocator,
                                                                     ObObj *&output_obj,
                                                                     int32_t data_table_rowkey_count)
{
  INIT_SUCC(ret);
  ObRowkey rowkey(input_obj, 1); // vid_rowkey table only has one rowkey column

  if (OB_FAIL(add_key_ranges(table_id, rowkey, scan_param))) {
    LOG_WARN("failed to set vid id key", K(ret));
  } else if (OB_FAIL(iter_table_rescan(scan_param, iter))) {
    LOG_WARN("failed to recan vid id scan param.", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    storage::ObTableScanIterator *scan_iter = dynamic_cast<storage::ObTableScanIterator *>(iter);

    if (OB_ISNULL(scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast to vid iter.", K(ret));
    } else if (OB_FAIL(scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from next table.", K(ret));
      } else {
        // do nothing
        LOG_INFO("vid is removed", K(ret), K(rowkey));
      }
    } else {
      const ObIArray<share::schema::ObColumnParam *> *out_col_param
        = scan_param.table_param_->get_read_info().get_columns();

      if (datum_row->get_column_count() != data_table_rowkey_count + 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_table_rowkey_count; ++i) {
          ObObj tmp_obj;
          output_obj[i].reset();
          ObObjMeta meta_type = out_col_param->at(i + 1)->get_meta_type();
          if (OB_FAIL(datum_row->storage_datums_[i + 1].to_obj(tmp_obj, meta_type))) {
            LOG_WARN("failed to convert datum to obj.", K(ret), K(i), K(datum_row->storage_datums_[i + 1]));
          } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, output_obj[i]))) {
            LOG_WARN("failed to write obj.", K(ret), K(i), K(tmp_obj));
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObPluginVectorIndexUtils::get_extra_column_count(const schema::ObTableParam &table_param,
                                                         schema::ObIndexType type)
{
  int64_t extra_column_count = 0;
  // Note. if DELTA_BUFFER table change table definition, must change this!
  if (type == INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL) {
    extra_column_count = table_param.get_read_info().get_schema_column_count() -
                         ObVecIndexBuilderUtil::OB_VEC_DELTA_BUFFER_TABLE_INDEX_COL_CNT -
                         1;  // index_rowkey_column_cnt + common_col_cnt. The non-primary key columns of
                             // delta_buffer_table and index_id_table are 1.
  } else if (type == INDEX_TYPE_VEC_INDEX_ID_LOCAL) {
    extra_column_count = table_param.get_read_info().get_schema_column_count() -
                         ObVecIndexBuilderUtil::OB_VEC_INDEX_ID_TABLE_INDEX_COL_CNT - 1;
  } else if (type == INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL) {
    extra_column_count =
        table_param.get_read_info().get_schema_column_count() -
        ObVecIndexBuilderUtil::OB_VEC_INDEX_SNAPSHOT_DATA_TABLE_INDEX_COL_CNT -
        3;  // index_rowkey_column_cnt + common_col_cnt ,  The non-primary key columns of snapshot_data is 3
  } else if (type == INDEX_TYPE_IS_NOT) {
    extra_column_count = table_param.get_read_info().get_schema_rowkey_count();
  }
  return extra_column_count;
}

int ObPluginVectorIndexUtils::get_data_table_out_column_id(
  ObSEArray<uint64_t, 4> &vector_column_ids,
  uint64_t incr_index_table_id,
  uint64_t data_table_id,
  uint64_t tenant_id,
  ObPluginVectorIndexAdaptor *adapter)
{
  INIT_SUCC(ret);
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *delta_buffer_schema = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = MTL(schema::ObTenantSchemaService*)->get_schema_service();
  int64_t extra_info_actual_size = 0;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema manager", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, incr_index_table_id, delta_buffer_schema))) {
    LOG_WARN("failed to get table schema by index id.", K(ret), K(tenant_id), K(incr_index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, table_schema))) {
    LOG_WARN("failed to get data table scheam.", K(ret), K(data_table_id));
  } else if (OB_ISNULL(delta_buffer_schema) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid index table schema.", K(ret), KP(delta_buffer_schema), KP(table_schema));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_id(*table_schema, *delta_buffer_schema, vector_column_ids))) {
    LOG_WARN("failed to get vector index column id.", K(ret));
  } else if (vector_column_ids.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector column id count invalid.", K(ret), K(vector_column_ids.count()));
  } else if (OB_FAIL(adapter->get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("failed to get extra info actual size.", K(ret));
  } else if (!adapter->get_is_need_vid()) {
    if (extra_info_actual_size > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info actual size is not 0 for table without pk.", K(ret), K(extra_info_actual_size));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(vector_column_ids))){
      LOG_WARN("failed to get pk increment column id.", K(ret));
    }
  } else if (extra_info_actual_size > 0) {
    adapter->set_extra_column_count(0);
    ObSEArray<uint64_t, 4> extra_column_ids;
    if (OB_FAIL(ObVectorIndexUtil::get_extra_info_column_id(*table_schema, *delta_buffer_schema, extra_column_ids))) {
      LOG_WARN("failed to get extra info column id.", K(ret));
    } else if (extra_column_ids.count() > 0) {
      adapter->set_extra_column_count(extra_column_ids.count());
      for (int i = 0; OB_SUCC(ret) && i < extra_column_ids.count(); ++i) {
        if (OB_FAIL(vector_column_ids.push_back(extra_column_ids.at(i)))) {
          LOG_WARN("failed to push back extra column id.", K(ret), K(extra_column_ids.at(i)));
        }
      }
    }
  }

  return ret;
}


int ObPluginVectorIndexUtils::read_vector_info(ObPluginVectorIndexAdaptor *adapter,
                                               ObIAllocator &allocator,
                                               ObLSID &ls_id,
                                               SCN target_scn,
                                               ObVectorQueryAdaptorResultContext &ada_ctx)
{
  INIT_SUCC(ret);
  uint64_t vid_id_table_table_id = adapter->get_vid_rowkey_table_id();
  uint64_t data_table_table_id = adapter->get_data_table_id();
  schema::ObTableParam vid_table_param(allocator);
  schema::ObTableParam data_table_param(allocator);
  common::ObNewRowIterator *vid_id_iter = nullptr;
  common::ObNewRowIterator *data_iter = nullptr;
  schema::ObIndexType type = INDEX_TYPE_VEC_VID_ROWKEY_LOCAL;
  ObObj *output_vec_obj = nullptr;
  ObVecExtraInfoObj* output_extra_info_obj = nullptr;
  int64_t extra_column_count = 0;
  ObAccessService *tsc_service = MTL(ObAccessService *);

  SMART_VARS_2((storage::ObTableScanParam, vid_id_scan_param),
               (storage::ObTableScanParam, data_scan_param)) {
    ObArenaAllocator vid_id_scan_allocator("VecIdxTaskSC1", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObArenaAllocator data_scan_allocator("VecIdxTaskSC2", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObArenaAllocator batch_temp_allocator("VecIdxTaskSC3", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    uint32_t alloc_size = (ada_ctx.get_count() > ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE)
                          ? ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE
                          : ada_ctx.get_count();
    if (ada_ctx.get_count() == 0) {
      // do noting
    } else if (OB_ISNULL(output_vec_obj = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * alloc_size)))) { // use lots of memory
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc mem.", K(ret));
    } else if (adapter->get_is_need_vid() && OB_FAIL(read_local_tablet(ls_id,
                                        adapter,
                                        target_scn,
                                        type,
                                        allocator,
                                        vid_id_scan_allocator,
                                        vid_id_scan_param,
                                        vid_table_param,
                                        vid_id_iter))) {
      LOG_WARN("failed to read vid id table local tablet.", K(ret));
    } else if (OB_FAIL(read_local_tablet(ls_id,
                                        adapter,
                                        target_scn,
                                        INDEX_TYPE_IS_NOT,
                                        allocator,
                                        data_scan_allocator,
                                        data_scan_param,
                                        data_table_param,
                                        data_iter))) {
      LOG_WARN("failed to read data table local tablet.", K(ret));
    } else if (OB_FALSE_IT(extra_column_count = adapter->get_extra_column_count())) {
    } else if (extra_column_count > 0) {
      if (OB_ISNULL(output_extra_info_obj = static_cast<ObVecExtraInfoObj *>(allocator.alloc(sizeof(ObVecExtraInfoObj) * extra_column_count * alloc_size)))) { // use lots of memory
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc mem.", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (adapter->get_is_need_vid()) {
      bool get_data = false;
      void *buf = nullptr;
      ObObj *obj_ptr =  nullptr;
      int32_t data_table_rowkey_count = vid_table_param.get_output_projector().count() - 1;
      LOG_INFO("data_table_rowkey_count", K(data_table_rowkey_count));
      if (data_table_rowkey_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get data table rowkey count invalid.", K(ret), K(data_table_rowkey_count));
      } else {
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * data_table_rowkey_count))) { // use lots of memory
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc mem.", K(ret), K(data_table_rowkey_count));
        } else {
          obj_ptr = new (buf) ObObj[data_table_rowkey_count];
        }
      }

      for (int64_t j = 0; OB_SUCC(ret) && j < ada_ctx.get_count(); j += ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) {
        batch_temp_allocator.reuse();
        int64_t vec_cnt = ada_ctx.get_vec_cnt();
        for (int64_t i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
          vid_id_scan_param.key_ranges_.pop_back();
          data_scan_param.key_ranges_.pop_back();
          if (OB_FAIL(read_object_from_vid_rowkey_table_iter(&(ada_ctx.get_vids()[i+j]),
                                                  vid_id_table_table_id,
                                                  vid_id_scan_param,
                                                  vid_id_iter,
                                                  type,
                                                  batch_temp_allocator,
                                                  obj_ptr,
                                                  data_table_rowkey_count))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to read obj from 2nd table.", K(ret));
            } else {
              ret = OB_SUCCESS; // read next vid
              output_vec_obj[i].reset();
              if (OB_NOT_NULL(output_extra_info_obj)) {
                for (int64_t extra_idx = 0; OB_SUCC(ret) && extra_idx < extra_column_count; ++extra_idx) {
                  output_extra_info_obj[i * extra_column_count + extra_idx].reset();
                }
              }
            }
          } else if (OB_FAIL(read_object_from_data_table_iter(obj_ptr,
                                                              data_table_rowkey_count,
                                                              data_table_table_id,
                                                              data_scan_param,
                                                              data_iter,
                                                              INDEX_TYPE_IS_NOT,
                                                              batch_temp_allocator,
                                                              output_vec_obj[i],
                                                              extra_column_count,
                                                              OB_NOT_NULL(output_extra_info_obj) ? &(output_extra_info_obj[i * extra_column_count]) : nullptr,
                                                              get_data))) {
            LOG_WARN("failed to read obj from data table.", K(ret));
          }
        }

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }

        if (OB_SUCC(ret)) {
          ada_ctx.set_vectors(output_vec_obj);
          ada_ctx.set_extra_infos(output_extra_info_obj);
          if (OB_FAIL(adapter->complete_delta_buffer_table_data(&ada_ctx))) {
            LOG_WARN("failed to complete delta buffer", KR(ret));
          } else {
            // do nothing, ada_ctx.do_next_batch already called in complete_delta_buffer_table_data
          }
        }
      }
    } else if (!adapter->get_is_need_vid()) {
      bool get_data = false;
      int32_t data_table_rowkey_count = 1;

      for (int64_t j = 0; OB_SUCC(ret) && j < ada_ctx.get_count(); j += ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) {
        batch_temp_allocator.reuse();
        int64_t vec_cnt = ada_ctx.get_vec_cnt();
        for (int64_t i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
          data_scan_param.key_ranges_.pop_back();
          ObObj *input_obj = &(ada_ctx.get_vids()[i+j]);
          input_obj->meta_.set_uint64();
          if (OB_FAIL(read_object_from_data_table_iter(input_obj,
                                                       data_table_rowkey_count,
                                                       data_table_table_id,
                                                       data_scan_param,
                                                       data_iter,
                                                       INDEX_TYPE_IS_NOT,
                                                       batch_temp_allocator,
                                                       output_vec_obj[i],
                                                       extra_column_count,
                                                       nullptr,
                                                       get_data))) {
            LOG_WARN("failed to read obj from data table.", K(ret));
          }
        }

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }

        if (OB_SUCC(ret)) {
          ada_ctx.set_vectors(output_vec_obj);
          if (OB_FAIL(adapter->complete_delta_buffer_table_data(&ada_ctx))) {
            LOG_WARN("failed to complete delta buffer", KR(ret));
          }
        }
      }
    }
    LOG_INFO("memdata sync scan_allocator_usage",
      K(vid_id_scan_allocator.used()), K(vid_id_scan_allocator.total()),
      K(data_scan_allocator.used()), K(data_scan_allocator.total()),
      K(batch_temp_allocator.used()), K(batch_temp_allocator.total()));
  }

  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(vid_id_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(vid_id_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    vid_id_iter = nullptr;
    if (OB_NOT_NULL(data_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert data_iter failed", K(ret));
      }
    }
    vid_id_iter = nullptr;
  }

  return ret;
}
// debug interface, remove later
int ObPluginVectorIndexUtils::test_read_local_data(ObLSID &ls_id,
                                                   ObPluginVectorIndexAdaptor *adapter,
                                                   ObIndexType index_type,
                                                   SCN target_scn,
                                                   ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  storage::ObTableScanParam scan_param;
  schema::ObTableParam table_param(allocator);
  common::ObNewRowIterator *table_iter = nullptr;
  ObAccessService *tsc_service = MTL(ObAccessService *);

  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adapter", K(ret), KPC(adapter));
  } else if (OB_FAIL(read_local_tablet(ls_id,
                                       adapter,
                                       target_scn,
                                       index_type,
                                       allocator,
                                       allocator,
                                       scan_param,
                                       table_param,
                                       table_iter))) {
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(index_type), KPC(adapter));
  } else {
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(table_iter);
    bool read_finish = false;
    int row_cnt = 0;
    while(OB_SUCC(ret) && !read_finish) {
      blocksstable::ObDatumRow* datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("dump local read finished", K(row_cnt), K(index_type));
          read_finish = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("dump local read fail to get next row", KR(ret), K(row_cnt), K(index_type));
        }
      } else if (FALSE_IT(row_cnt++)) {
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("dump local read row is null.", K(ret), K(index_type));
      } else {
        // print for debug
        LOG_INFO("dump local read row", K(row_cnt), K(index_type), KPC(datum_row));
      }
    }
  }
  if (OB_NOT_NULL(table_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(table_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert test table_iter failed", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::try_sync_vbitmap_memdata(ObLSID &ls_id,
                                                       ObPluginVectorIndexAdaptor *adapter,
                                                       SCN &target_scn,
                                                       ObIAllocator &allocator,
                                                       ObVectorQueryAdaptorResultContext &ada_ctx)
{
  int ret = OB_SUCCESS;
  schema::ObIndexType index_type = INDEX_TYPE_VEC_INDEX_ID_LOCAL;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  common::ObNewRowIterator *index_id_iter = nullptr;
  storage::ObTableScanParam vbitmap_scan_param;
  schema::ObTableParam vbitmap_table_param(allocator);

  if (OB_FAIL(read_local_tablet(ls_id,
                                adapter,
                                target_scn,
                                index_type,
                                allocator,
                                allocator,
                                vbitmap_scan_param,
                                vbitmap_table_param,
                                index_id_iter))) { // read_local_tablet 4rd aux index get rowkey, backword
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(index_type), KPC(adapter));
  } else if (OB_FAIL(adapter->check_index_id_table_readnext_status(&ada_ctx, index_id_iter, target_scn))) {
    LOG_WARN("fail to check and sync vbitmap.", KR(ret));
  } // ToDo: may also need to sync vector to incr memdata

  if (OB_NOT_NULL(index_id_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(index_id_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert index_id_iter failed", K(ret));
    }
    index_id_iter = nullptr;
  }

  return ret;
}

int ObPluginVectorIndexUtils::try_sync_snapshot_memdata(ObLSID &ls_id,
                                                        ObPluginVectorIndexAdaptor *&adapter,
                                                        const bool create_new_adp,
                                                        SCN &target_scn,
                                                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  schema::ObIndexType index_type = INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  common::ObNewRowIterator *snapshot_idx_iter = nullptr;
  storage::ObTableScanParam snapshot_scan_param;
  schema::ObTableParam snapshot_table_param(allocator);
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
  int64_t index_count = 0;

  void *adpt_buff = nullptr;
  if (OB_FAIL(read_local_tablet(ls_id,
                                adapter,
                                target_scn,
                                index_type,
                                allocator,
                                allocator,
                                snapshot_scan_param,
                                snapshot_table_param,
                                snapshot_idx_iter))) { // read_local_tablet 5th aux index get rowkey
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(index_type), KPC(new_adapter));
  } else {
    blocksstable::ObDatumRow *row = nullptr;
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(snapshot_idx_iter);
    if (OB_FAIL(table_scan_iter->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row", K(ret), K(row));
    } else if (adapter->get_snapshot_key_prefix().empty() ||
               !row->storage_datums_[0].get_string().prefix_match(adapter->get_snapshot_key_prefix())) {
      ObString key_prefix;
      ObString target_prefix;
      if (OB_ISNULL(vector_index_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, vec_idx_mgr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get vector index ls mgr", KR(ret));
        }
      } else if (OB_ISNULL(vec_idx_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid vector index ls mgr", KR(ret));
      } else {
        if (create_new_adp) {
          adpt_buff = vector_index_service->get_allocator().alloc(sizeof(ObPluginVectorIndexAdaptor));
          if (OB_ISNULL(adpt_buff)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
          } else {
            new_adapter = new(adpt_buff)ObPluginVectorIndexAdaptor(&vector_index_service->get_allocator(), vec_idx_mgr->get_memory_context(), adapter->get_tenant_id());
            new_adapter->set_create_type(adapter->get_create_type());
            if (OB_FAIL(new_adapter->copy_meta_info(*adapter))) {
              LOG_WARN("failed to copy meta info", K(ret));
            } else if (OB_FAIL(new_adapter->init(vec_idx_mgr->get_memory_context(), vec_idx_mgr->get_all_vsag_use_mem()))) {
              LOG_WARN("failed to init adpt.", K(ret));
            } else if (OB_FAIL(new_adapter->set_index_identity(adapter->get_index_identity()))) {
              LOG_WARN("failed to set index identity", K(ret));
            } else {
              adapter = new_adapter;
            }
          }
        } else {
          new_adapter = adapter;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ob_write_string(allocator, row->storage_datums_[0].get_string(), key_prefix))) {
        LOG_WARN("failed to write string", K(ret), K(row->storage_datums_[0].get_string()));
      } else if (OB_FAIL(iter_table_rescan(snapshot_scan_param, table_scan_iter))) {
        LOG_WARN("failed to rescan", K(ret));
      } else {

        ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        ObHNSWDeserializeCallback::CbParam param;
        param.iter_ = snapshot_idx_iter;
        param.allocator_ = &tmp_allocator;

        ObHNSWDeserializeCallback callback(static_cast<void*>(new_adapter));
        ObIStreamBuf::Callback cb = callback;
        // ToDo: concurrency with weakread
        ObVectorIndexSerializer index_seri(tmp_allocator);
        ObVectorIndexMemData *snap_memdata = new_adapter->get_snap_data_();
        // name rule: TabletID_SCN_xxx_data_partn, we need TabletID_SCN
        ObVectorIndexAlgorithmType index_type;
        ObString target_prefix;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(snap_memdata)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("snap memdata is null", K(ret));
        } else {
          TCWLockGuard lock_guard(snap_memdata->mem_data_rwlock_);
          if (OB_FAIL(index_seri.deserialize(snap_memdata->index_, param, cb, MTL_ID()))) {
            LOG_WARN("serialize index failed.", K(ret));
          } else if (OB_FAIL(obvectorutil::immutable_optimize(snap_memdata->index_))) {
            LOG_WARN("fail to index immutable_optimize", K(ret));
          } else if (OB_FALSE_IT(index_type = new_adapter->get_snap_index_type())) {
          } else if (OB_FAIL(get_split_snapshot_prefix(index_type, key_prefix, target_prefix))) {
            LOG_WARN("fail to get split snapshot prefix", K(ret), K(index_type), K(key_prefix));
          } else if (OB_FAIL(new_adapter->set_snapshot_key_prefix(target_prefix))) {
            LOG_WARN("failed to set snapshot key prefix", K(ret), K(index_type), K(target_prefix));
          } else if (OB_FAIL(obvectorutil::get_index_number(snap_memdata->index_, index_count))) {
            ret = OB_ERR_VSAG_RETURN_ERROR;
            LOG_WARN("fail to get incr index number", K(ret));
          } else if (index_count == 0) {
            free_memdata_resource(VIRT_SNAP, snap_memdata, new_adapter->get_allocator(), new_adapter->get_tenant_id());
            //should not release mem_ctx here, create by init_mem, not init_memdata
            //if (OB_NOT_NULL(snap_memdata->mem_ctx_)) {
            //  snap_memdata->mem_ctx_->~ObVsagMemContext();
            //  adapter->get_allocator()->free(snap_memdata->mem_ctx_);
            //  snap_memdata->mem_ctx_ = nullptr;
            //}
            LOG_INFO("memdata sync snapshot index complement no data", K(index_count), K(ls_id), K(index_type), KPC(new_adapter));
          } else { // index_count > 0
            new_adapter->close_snap_data_rb_flag();
            LOG_INFO("memdata sync snapshot index complement data", K(index_count), K(ls_id), K(index_type), KPC(new_adapter));
          }
        }
      }
    }
  }
  // free adapter memory when failed
  if ((OB_FAIL(ret) || index_count == 0) && OB_NOT_NULL(new_adapter) && create_new_adp) {
    LOG_INFO("release new adapter memory in failure", K(ret));
    new_adapter->~ObPluginVectorIndexAdaptor();
    vector_index_service->get_allocator().free(adpt_buff);
    adpt_buff = nullptr;
    new_adapter = nullptr;
    adapter = nullptr;
  }
  if (OB_NOT_NULL(snapshot_idx_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(snapshot_idx_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snapshot_idx_iter failed", K(ret));
    }
    snapshot_idx_iter = nullptr;
  }
  return ret;
}

int ObPluginVectorIndexUtils::refresh_adp_from_table(
    ObLSID &ls_id,
    ObPluginVectorIndexAdaptor *&adapter,
    const bool create_new_adapter,
    SCN target_scn,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adapter", K(ret), KPC(adapter));
  } else if (adapter->get_create_type() != CreateTypeComplete) {
    // skip not complete adapter.
  } else {
    common::ObNewRowIterator *delta_buf_iter = nullptr;
    ObAccessService *tsc_service = MTL(ObAccessService *);
    storage::ObTableScanParam inc_scan_param;
    schema::ObTableParam inc_table_param(allocator);
    int64_t extra_info_actual_size = 0;
    if (OB_FAIL(try_sync_snapshot_memdata(ls_id, adapter, create_new_adapter, target_scn, allocator))) {
      LOG_WARN("failed to refresh mem snapshots without refresh incr", KR(ret));
    } else if (OB_FAIL(read_local_tablet(ls_id,
                                  adapter,
                                  target_scn,
                                  INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
                                  allocator,
                                  allocator,
                                  inc_scan_param,
                                  inc_table_param,
                                  delta_buf_iter))) {
      LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL), KP(adapter));
    } else if (OB_FAIL(adapter->get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("fail to get extra info actual size", K(ret), KPC(adapter));
    } else {
      int64_t extra_info_column_count =
          extra_info_actual_size > 0
              ? ObPluginVectorIndexUtils::get_extra_column_count(inc_table_param,INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL)
              : 0;
      ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, adapter->get_tenant_id());
      ObVectorQueryAdaptorResultContext ada_ctx(adapter->get_tenant_id(), extra_info_column_count, &allocator, &tmp_allocator);
      if (OB_FAIL(adapter->check_delta_buffer_table_readnext_status(&ada_ctx, delta_buf_iter,target_scn))) {
        LOG_WARN("fail to check_delta_buffer_table_readnext_status.", K(ret));
      } else if (OB_FAIL(try_sync_vbitmap_memdata(ls_id, adapter, target_scn, allocator, ada_ctx))) {
        LOG_WARN("failed to sync vbitmap", KR(ret));
      } else if (ada_ctx.get_status() == PVQ_COM_DATA) {
        if (OB_FAIL(read_vector_info(adapter, allocator, ls_id, target_scn, ada_ctx))) {
          LOG_WARN("failed to read vector_info", KR(ret));
        }
      }
    }
    if (OB_NOT_NULL(delta_buf_iter) && OB_NOT_NULL(tsc_service)) {
      int tmp_ret = tsc_service->revert_scan_iter(delta_buf_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert delta_buf_iter failed", K(tmp_ret));
      }
      delta_buf_iter = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_read_scn(bool is_leader, ObLSID &ls_id, SCN &target_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  // ObLSWRSHandler::get_ls_weak_read_ts
  storage::ObLSService *ls_svr = MTL(storage::ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(ls_svr), K(ls_id));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else  if (is_leader && OB_FAIL(target_scn.convert_from_ts(ObTimeUtility::fast_current_time()))) {
    LOG_WARN("failed to convert ts to scn", K(ret));
  } else if (!is_leader && FALSE_IT(target_scn = ls_handle.get_ls()->get_ls_wrs_handler()->get_ls_weak_read_ts())) {
  }
  return ret;
}

int ObPluginVectorIndexUtils::query_need_refresh_memdata(ObPluginVectorIndexAdaptor *adapter, ObLSID &ls_id, bool is_leader)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  SCN target_scn;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(adapter), K(ls_id));
  } else {
    bool need_retry = false;
    common::ObSpinLockGuard ctx_guard(adapter->get_reload_lock());
    if (adapter->get_reload_finish()) {
      need_retry = true;
    } else if (OB_FAIL(get_read_scn(is_leader, ls_id, target_scn))) {
      LOG_WARN("failed to get weak read scn", K(ret), K(is_leader));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::refresh_memdata(ls_id, adapter, target_scn, allocator))) {
      LOG_WARN("fail to refresh adapter", K(ret));
    } else if (OB_FALSE_IT(adapter->set_reload_finish(true))) {
    } else {
      need_retry = true;
    }
    if (OB_SUCC(ret) && need_retry) {
      ret = OB_SCHEMA_EAGAIN; // sql retry
      LOG_WARN("sql retry ret_code to process_adapter_state", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::refresh_memdata(ObLSID &ls_id,
                                              ObPluginVectorIndexAdaptor *adapter,
                                              SCN target_scn,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  // ToDo: remove test interface later
#if 0
  if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_ROWKEY_VID_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_ROWKEY_VID_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_VID_ROWKEY_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_VID_ROWKEY_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_INDEX_ID_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_INDEX_ID_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_IS_NOT, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_IS_NOT));
  }
#endif
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adapter", K(ret), KPC(adapter));
  } else {
    MTL_SWITCH(adapter->get_tenant_id()) {
      ObPluginVectorIndexAdaptor *new_adapter = adapter;
      ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
      ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
      if (OB_ISNULL(vector_index_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, vec_idx_mgr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get vector index ls mgr", KR(ret));
        }
      } else if (OB_ISNULL(vec_idx_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid vector index ls mgr", KR(ret));
      } else if (OB_FAIL(refresh_adp_from_table(ls_id, new_adapter, true, target_scn, allocator))) {
        LOG_WARN("failed to refresh adapter from table", K(ret), KPC(adapter));
      }
      if (adapter != new_adapter && OB_NOT_NULL(new_adapter)) {
        if (OB_SUCC(ret)) {
          RWLock::WLockGuard lock_guard(vec_idx_mgr->get_adapter_map_lock());
          if (OB_FAIL(vec_idx_mgr->replace_old_adapter(new_adapter))) {
            LOG_WARN("failed to replace old adapter", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          LOG_INFO("release new vector index adapter on failed", K(ret), K(new_adapter));
          new_adapter->~ObPluginVectorIndexAdaptor();
          void *adpt_buff  = reinterpret_cast<void*>(new_adapter);
          vector_index_service->get_allocator().free(adpt_buff);
          new_adapter = nullptr;
        }
      }
    }
  }
  return ret;
}

static bool is_non_shared_vec_index_aux_table(schema::ObIndexType type)
{
  bool bret = false;
  bret = (is_vec_delta_buffer_type(type)
          || is_vec_index_id_type(type)
          || is_vec_index_snapshot_data_type(type));
  return bret;
}

int ObPluginVectorIndexUtils::read_local_tablet(ObLSID &ls_id,
                                                ObPluginVectorIndexAdaptor* adapter,
                                                SCN target_scn,
                                                schema::ObIndexType type,
                                                ObIAllocator &allocator,
                                                ObIAllocator &scan_allocator,
                                                ObTableScanParam &scan_param,
                                                ObTableParam &table_param,
                                                common::ObNewRowIterator *&scan_iter,
                                                ObIArray<uint64_t> *out_column_ids,
                                                const bool need_all_columns)
{
  int ret = OB_SUCCESS;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  scan_iter = nullptr;

  // init scan param refer to ObLocalIndexLookupOp::init_scan_param()
  // assign ls_id, tablet_id, tx_snapshot
  // set need_scn_ = true if need ora_rowscn
  ObTabletID tablet_id;
  uint64_t table_id = OB_INVALID_ID;
  ObTabletHandle tablet_handle;
  ObLSHandle ls_handle;

  // INDEX_TYPE_IS_NOT means data tablet
  if (!ls_id.is_valid() || OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls id or adapter", KR(ret), K(ls_id), KPC(adapter));
  } else if (is_vec_delta_buffer_type(type)) {
    tablet_id = adapter->get_inc_tablet_id();
    table_id = adapter->get_inc_table_id();
  } else if (is_vec_index_id_type(type)) {
    tablet_id = adapter->get_vbitmap_tablet_id();
    table_id = adapter->get_vbitmap_table_id();
  } else if (is_vec_index_snapshot_data_type(type)) {
    tablet_id = adapter->get_snap_tablet_id();
    table_id = adapter->get_snapshot_table_id();
  } else if (is_vec_rowkey_vid_type(type)) {
    tablet_id = adapter->get_rowkey_vid_tablet_id();
    table_id = adapter->get_rowkey_vid_table_id();
  } else if (is_vec_vid_rowkey_type(type)) {
    tablet_id = adapter->get_vid_rowkey_tablet_id();
    table_id = adapter->get_vid_rowkey_table_id();
  } else if (type == INDEX_TYPE_IS_NOT) {
    tablet_id = adapter->get_data_tablet_id();
    table_id = adapter->get_data_table_id();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K(type));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("read table tablet", K(ls_id), K(tablet_id), K(table_id), K(type), K(target_scn));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(tablet_id,
                                                                 tablet_handle,
                                                                 0, // timeout
                                                                 ObMDSGetTabletMode::READ_READABLE_COMMITED,
                                                                 target_scn))) {
    LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id), K(type));
  } else {
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    scan_param.schema_version_ = tablet_handle.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
    if (OB_FAIL(init_common_scan_param(scan_param, adapter, target_scn, &allocator, &scan_allocator, type, table_id))) {
      LOG_WARN("fail to init common scan param", KR(ret), KPC(adapter));
    } else if (OB_FAIL(init_table_param(&table_param,
                                        adapter->get_inc_table_id(),
                                        adapter->get_data_table_id(),
                                        table_id,
                                        type,
                                        adapter,
                                        out_column_ids,
                                        need_all_columns))) {
      LOG_WARN("fail to init table param", KR(ret), KPC(adapter));
    } else if (FALSE_IT(scan_param.table_param_ = &table_param)) {
    } else {
      common::ObNewRange range;
      void *buf = nullptr;
      uint32_t col_cnt = 0;
      if (is_non_shared_vec_index_aux_table(type)) {
        if (OB_FAIL(get_non_shared_index_aux_table_rowkey_colum_count(type, col_cnt))) {
          LOG_WARN("fail to get index aux table colum count", KR(ret), K(type));
        } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * col_cnt * 2))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc scan range obj failed.", K(ret));
        } else {
          ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
          for (int i = 0; i < col_cnt; i++) {
            row_objs[i] = ObObj::make_min_obj();
          }
          ObRowkey min_row_key(row_objs, col_cnt);
          for (int j = col_cnt; j < col_cnt * 2; j++) {
            row_objs[j] = ObObj::make_max_obj();
          }
          ObRowkey max_row_key(row_objs + col_cnt, col_cnt);

          range.table_id_ = table_id;
          range.start_key_ = min_row_key;
          range.end_key_ = max_row_key;
          range.border_flag_.set_inclusive_start();
          range.border_flag_.set_inclusive_end();
        }
      } else {
        // vid_rowkey table or data table, get rowkey while complete
        if (OB_FAIL(get_shared_table_rowkey_colum_count(type, adapter->get_tenant_id(), table_id, col_cnt))) {
          LOG_WARN("fail to get index aux table colum count", KR(ret), K(type));
        } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * col_cnt * 2))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc scan range obj failed.", K(ret));
        } else {
          ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
          for (int i = 0; i < col_cnt; i++) {
            row_objs[i] = ObObj::make_min_obj();
          }
          ObRowkey min_row_key(row_objs, col_cnt);
          for (int j = col_cnt; j < col_cnt * 2; j++) {
            row_objs[j] = ObObj::make_max_obj();
          }
          ObRowkey max_row_key(row_objs + col_cnt, col_cnt);

          range.table_id_ = table_id;
          range.start_key_ = min_row_key;
          range.end_key_ = max_row_key;
          range.border_flag_.set_inclusive_start();
          range.border_flag_.set_inclusive_end();
        }
      }

      // need lob helper for read aux table 5?
      scan_param.key_ranges_.reset();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
        LOG_WARN("failed to push key range.", K(ret), K(scan_param), K(range));
      } else {
        ObAccessService *oas = MTL(ObAccessService*);
        if (OB_ISNULL(oas)) {
          ret = OB_ERR_INTERVAL_INVALID;
          LOG_WARN("get access service failed.", K(ret));
        } else if (OB_FAIL(oas->table_scan(scan_param, scan_iter))) {
          LOG_WARN("do table scan falied.", K(ret), K(scan_param));
        }
      }

      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::init_common_scan_param(storage::ObTableScanParam& scan_param,
                                                     ObPluginVectorIndexAdaptor *adapter,
                                                     SCN target_scn,
                                                     ObIAllocator *allocator,
                                                     ObIAllocator *scan_allocator,
                                                     ObIndexType type,
                                                     uint64_t table_id)
{
  // fix validate adapter & allocator
  // refer to ObPersistentLobApator::build_common_scan_param
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag(is_vec_index_id_type(type) ? ObQueryFlag::Reverse : ObQueryFlag::Forward, // scan_order
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
  query_flag.scan_order_ = is_vec_index_id_type(type) ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
  scan_param.scan_flag_.flag_ = query_flag.flag_;
  // set column ids
  scan_param.column_ids_.reset();
  uint32 col_cnt = 0;

  if (is_vec_index(type) || type == INDEX_TYPE_IS_NOT){
    if(OB_ISNULL(adapter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null adapter", KR(ret), K(type));
    } else if (OB_FAIL(get_special_index_aux_table_column_count(type, adapter->get_tenant_id(),
                                                                table_id, col_cnt, scan_param))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index type", KR(ret), K(type));
    }
  }

  if (OB_SUCC(ret)) {
    scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
    // table param
    scan_param.index_id_ = 0;
    scan_param.is_get_ = false;
    // set timeout
    scan_param.timeout_ = INT64_MAX;
    // scan_param.virtual_column_exprs_
    scan_param.limit_param_.limit_ = -1;
    scan_param.limit_param_.offset_ = 0;
    // sessions

    scan_param.snapshot_.init_weak_read(target_scn);

    // never read_latest
    // if(param.read_latest_) {
    //  scan_param.tx_id_ = param.snapshot_.core_.tx_id_;
    // }
    scan_param.sql_mode_ = SMO_DEFAULT;
    // common set
    scan_param.allocator_ = allocator;
    scan_param.for_update_ = false;
    scan_param.for_update_wait_timeout_ = scan_param.timeout_;
    scan_param.scan_allocator_ = scan_allocator;
    scan_param.frozen_version_ = -1;
    scan_param.force_refresh_lc_ = false;
    scan_param.output_exprs_ = nullptr;
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.op_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
    scan_param.need_scn_ = false;
    scan_param.pd_storage_flag_ = false;
    // not flashback
    // scan_param.fb_snapshot_ = param.fb_snapshot_;
  }
  return ret;
}

int ObPluginVectorIndexUtils::init_table_param(ObTableParam *table_param,
                                               uint64_t inc_table_id,
                                               uint64_t data_table_id,
                                               uint64_t table_id,
                                               schema::ObIndexType type,
                                               ObPluginVectorIndexAdaptor *adapter,
                                               ObIArray<uint64_t> *out_column_ids,
                                               const bool need_all_columns)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> column_ids;
  uint64_t tenant_id = 0;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null adapter", KR(ret), K(inc_table_id), K(data_table_id), K(table_id), K(type));
  } else {
    tenant_id = adapter->get_tenant_id();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), KR(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), KR(table_id));
  } else if (is_vec_delta_buffer_type(type)) {
    ObArray<uint64_t> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else if (tmp_column_ids.count() < 3) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", K(tmp_column_ids.count()));
    } else {
      // need [vid][type][vector]
      uint64_t vid_column_id = 0;
      uint64_t type_column_id = 0;
      uint64_t vector_column_id = 0;
      ObSEArray<uint64_t, 4> part_column_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_vid_column()) {
          vid_column_id = col_schema->get_column_id();
        } else if (col_schema->is_hidden_pk_column_id(col_schema->get_column_id())) {
          if (vid_column_id == 0) {  // vid_column_id != 0 means it has been assigned by vid col.
            vid_column_id = col_schema->get_column_id();
          }
        } else if (col_schema->is_vec_hnsw_type_column()) {
          type_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_vector_column()) {
          vector_column_id = col_schema->get_column_id();
        } else if (OB_FAIL(part_column_ids.push_back(col_schema->get_column_id()))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (vid_column_id == 0 || type_column_id == 0 || vector_column_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get valid column id", K(ret), K(vid_column_id), K(type_column_id), K(vector_column_id));
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 2nd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(type_column_id))) {
        LOG_WARN("failed to push 3rd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(vector_column_id))) {
        LOG_WARN("failed to push 4th column id.", K(ret));
      }
      if (OB_SUCC(ret) && need_all_columns) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_column_ids.count(); ++i) {
          if (OB_FAIL(column_ids.push_back(part_column_ids.at(i)))) {
            LOG_WARN("failed to push back column id.", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(out_column_ids)) {
        out_column_ids->assign(column_ids);
      }
      if (OB_SUCC(ret) && OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_index_id_type(type)) {
    // different with other index, refer to ObTscCgService::extract_vec_ir_access_columns
    ObArray<uint64_t> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else if (tmp_column_ids.count() < 4) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", K(tmp_column_ids.count()));
    } else {
      // need [scn][vid][type][vector]
      uint64_t scn_column_id = 0;
      uint64_t vid_column_id = 0;
      uint64_t type_column_id = 0;
      uint64_t vector_column_id = 0;
      ObSEArray<uint64_t, 4> part_column_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_scn_column()) {
          scn_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_vid_column()) {
          vid_column_id = col_schema->get_column_id();
        } else if (col_schema->is_hidden_pk_column_id(col_schema->get_column_id())) {
          if (vid_column_id == 0) {  // vid_column_id != 0 means it has been assigned by vid col.
            vid_column_id = col_schema->get_column_id();
          }
        } else if (col_schema->is_vec_hnsw_type_column()) {
          type_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_vector_column()) {
          vector_column_id = col_schema->get_column_id();
        } else if (OB_FAIL(part_column_ids.push_back(col_schema->get_column_id()))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (scn_column_id == 0 || vid_column_id == 0 || type_column_id == 0 || vector_column_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get valid column id", K(ret), K(scn_column_id), K(vid_column_id), K(type_column_id), K(vector_column_id));
      } else if (OB_FAIL(column_ids.push_back(scn_column_id))) {
        LOG_WARN("failed to push 1st column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 2nd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(type_column_id))) {
        LOG_WARN("failed to push 3rd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(vector_column_id))) {
        LOG_WARN("failed to push 4th column id.", K(ret));
      }
      if (OB_SUCC(ret) && need_all_columns) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_column_ids.count(); ++i) {
          if (OB_FAIL(column_ids.push_back(part_column_ids.at(i)))) {
            LOG_WARN("failed to push back column id.", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(out_column_ids)) {
        out_column_ids->assign(column_ids);
      }
      if (OB_SUCC(ret) && OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_vid_rowkey_type(type)) {
    uint64_t vid_column_id = 0;
    ObSEArray<uint64_t, 4> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      // make sure vid column is the first output column
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count() && vid_column_id == 0; ++i) {
        const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(i);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_vid_column()) {
          vid_column_id = col_schema->get_column_id();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 1st column id.", K(ret));
      } else if (FALSE_IT(tmp_column_ids.reuse())) {
      } else if (OB_FAIL(data_table_schema->get_rowkey_column_ids(tmp_column_ids))) {
        LOG_WARN("failed to get data table rowkey column id", K(ret), KPC(data_table_schema));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        if (OB_FAIL(column_ids.push_back(tmp_column_ids[i]))) {
          LOG_WARN("failed to push column id.", K(ret), K(i), K(tmp_column_ids[i]), K(vid_column_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_index_snapshot_data_type(type)) {
    const ObTableSchema *data_table_schema = NULL;
    ObSEArray<uint64_t, 4> tmp_column_ids;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      uint64_t key_column_id = 0;
      uint64_t lob_data_column_id = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_key_column()) {
          key_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_data_column()) {
          lob_data_column_id = col_schema->get_column_id();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (key_column_id == 0 || lob_data_column_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected snapshot data column ids", K(key_column_id), K(lob_data_column_id));
      } else if (OB_FAIL(column_ids.push_back(key_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(key_column_id));
      } else if (OB_FAIL(column_ids.push_back(lob_data_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(lob_data_column_id));
      } else {
        table_param->get_enable_lob_locator_v2() = true;
        table_param->set_is_vec_index(true);
        if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
          LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
        }
      }
    }
  } else if (is_vec_index(type)) {
    if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (type == INDEX_TYPE_IS_NOT) {
    if (OB_FAIL(get_data_table_out_column_id(column_ids, inc_table_id, table_id, tenant_id, adapter))) {
      LOG_WARN("failed to get vec column id.", K(ret));
    } else {
      if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_WARN("failed to convert table param.", K(ret));
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_non_shared_index_aux_table_colum_count(schema::ObIndexType type, uint32 &col_cnt)
{
  static const uint32 delta_buffer_tab_col_cnt = 3; // vid, type, vector, "ora_rowscn", 3 or 4 columns
  static const uint32 index_id_tab_col_cnt = 4; // scn ,vid, type, vector
  static const uint32 index_snapshot_tab_col_cnt = 2; // key, data

  int ret = OB_SUCCESS;
  col_cnt = 0;
  if (is_vec_delta_buffer_type(type)) {
    col_cnt = delta_buffer_tab_col_cnt;
  } else if (is_vec_index_id_type(type)) {
    col_cnt = index_id_tab_col_cnt;
  } else if (is_vec_index_snapshot_data_type(type)) {
    col_cnt = index_snapshot_tab_col_cnt;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_non_shared_index_aux_table_colum_count", K(type), K(col_cnt)); // remove after debug;
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_special_index_aux_table_column_count(
  schema::ObIndexType type,
  uint64_t tenant_id,
  uint64_t table_id,
  uint32 &col_cnt,
  storage::ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), K(table_id));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_ERROR("fail to get index table all column ids", K(table_schema));
  } else if (OB_FAIL(scan_param.column_ids_.assign(column_ids))) {
    LOG_WARN("failed to assign column ids.", K(ret));
  } else {
    col_cnt = column_ids.count();
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_special_index_aux_table_column_count", K(type), K(col_cnt), K(column_ids), K(tenant_id)); // remove after debug;
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_non_shared_index_aux_table_rowkey_colum_count(schema::ObIndexType type, uint32 &col_cnt)
{
  // only need to do range scan for aux index table 3, 4, 5
  // other tables only needs multi get
  static const uint32 delta_buffer_tab_col_cnt = 2; // rowkey:vid, type, other:vector, "ora_rowscn"
  static const uint32 index_id_tab_col_cnt = 3; // rowkey:scn ,vid, type, other:vector
  static const uint32 index_snapshot_tab_col_cnt = 1; // rowkey:key, other:data

  int ret = OB_SUCCESS;
  col_cnt = 0;
  if (is_vec_delta_buffer_type(type)) {
    col_cnt = delta_buffer_tab_col_cnt;
  } else if (is_vec_index_id_type(type)) {
    col_cnt = index_id_tab_col_cnt;
  } else if (is_vec_index_snapshot_data_type(type)) {
    col_cnt = index_snapshot_tab_col_cnt;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_non_shared_index_aux_table_rowkey_colum_count", K(type), K(col_cnt)); // remove after debug;
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_shared_table_rowkey_colum_count(schema::ObIndexType type,
                                                                  uint64_t tenant_id,
                                                                  uint64_t table_id,
                                                                  uint32 &col_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), KR(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), KR(table_id));
  } else {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    if (OB_FAIL(rowkey_info.get_column_ids(column_ids))) {
      LOG_WARN("get rowkey_info from  table schema faild", KR(ret), KR(table_id), KPC(table_schema));
    } else {
      col_cnt = column_ids.count();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_non_shared_index_aux_table_rowkey_colum_count", K(type), K(col_cnt), K(column_ids)); // remove after debug;
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_vector_index_adapter(ObPluginVectorIndexAdaptor* &adapter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adapter)) {
    // do nothing
  } else {
    if (adapter->dec_ref_and_check_release()) {
      ObIAllocator *allocator = adapter->get_allocator();
      if (OB_ISNULL(allocator)) {
        const int ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "release vector index adapter failed", KPC(adapter));
      } else {
        // OB_LOG(DEBUG, "adatper released", KPC(adapter));
        adapter->~ObPluginVectorIndexAdaptor();
        allocator->free(adapter);
      }
      adapter = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_vector_index_build_helper(ObIvfBuildHelper* &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(helper)) {
    // do nothing
  } else {
    if (helper->dec_ref_and_check_release()) {
      ObIAllocator *allocator = helper->get_allocator();
      if (OB_ISNULL(allocator)) {
        const int ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "release ivf vector index build helper failed", KPC(helper));
      } else {
        helper->~ObIvfBuildHelper();
        allocator->free(helper);
      }
      helper = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_ivf_cache_mgr(ObIvfCacheMgr* &mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    // do nothing
  } else if (mgr->dec_ref_and_check_release()) {
    ObIAllocator &allocator = mgr->get_self_allocator();
    mgr->~ObIvfCacheMgr();
    allocator.free(mgr);
    mgr = nullptr;
  }
  return ret;
}

ObVectorIndexRecordType ObPluginVectorIndexUtils::index_type_to_record_type(schema::ObIndexType type)
{
  ObVectorIndexRecordType record_type = VIRT_MAX;
  if (schema::is_vec_delta_buffer_type(type)) {
    record_type =  VIRT_INC;
  } else if (schema::is_vec_index_id_type(type)) {
    record_type = VIRT_BITMAP;
  } else if (schema::is_vec_index_snapshot_data_type(type)) {
    record_type = VIRT_SNAP;
  }
  return record_type;
}

ObAdapterCreateType ObPluginVectorIndexUtils::index_type_to_create_type(schema::ObIndexType type)
{
  ObAdapterCreateType create_type = CreateTypeMax;
  if (schema::is_vec_delta_buffer_type(type)) {
    create_type =  CreateTypeInc;
  } else if (schema::is_vec_index_id_type(type)) {
    create_type = CreateTypeBitMap;
  } else if (schema::is_vec_index_snapshot_data_type(type)) {
    create_type = CreateTypeSnap;
  }
  return create_type;
}

int ObPluginVectorIndexUtils::get_vector_index_prefix(const ObTableSchema &index_schema,
                                                      ObString &prefix)
{
  int ret = OB_SUCCESS;
  prefix.reset();
  if (!index_schema.is_vec_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, not vector index table", K(ret), K(index_schema));
  } else if (index_schema.is_vec_rowkey_vid_type() || index_schema.is_vec_vid_rowkey_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index type, only support get none share table prefix",
      K(ret), K(index_schema));
  } else {
    ObString tmp_table_name = index_schema.get_table_name();
    const int64_t table_name_len = tmp_table_name.length();

    const char* delta_buffer_table = ObVecIndexBuilderUtil::DELTA_BUFFER_TABLE_NAME_SUFFIX;
    const char* index_id_table = ObVecIndexBuilderUtil::INDEX_ID_TABLE_NAME_SUFFIX;
    const char* index_snapshot_data_table = ObVecIndexBuilderUtil::SNAPSHOT_DATA_TABLE_NAME_SUFFIX;
    int64_t prefix_len = 0;

    if (index_schema.is_vec_delta_buffer_type()) {
      prefix_len = table_name_len - strlen(delta_buffer_table);
    } else if (index_schema.is_vec_index_id_type()) {
      prefix_len = table_name_len - strlen(index_id_table);
    } else if (index_schema.is_vec_index_snapshot_data_type()) {
      prefix_len = table_name_len - strlen(index_snapshot_data_table);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector index type", K(ret), K(index_schema));
    }
    if (OB_SUCC(ret)) {
      prefix.assign_ptr(tmp_table_name.ptr(), prefix_len);
      LOG_INFO("get_index_prefix", K(prefix), K(tmp_table_name));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::erase_ivf_build_helper(ObLSID ls_id, const ObIvfHelperKey &key)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vec_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(vec_index_service->erase_ivf_build_helper(ls_id, key))) {
    LOG_WARN("failed to erase ivf build helper", K(ret), K(ls_id), K(key));
  }
  if (ret == OB_HASH_NOT_EXIST) {
    LOG_WARN("erase ivf build helper, key not exist", K(ret), K(ls_id), K(key));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_split_snapshot_prefix(
    const ObVectorIndexAlgorithmType index_type, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;

  if (index_type == VIAT_HGRAPH) {
    ObString split_item("_hgraph_");
    if (OB_FAIL(split_snapshot_prefix(src, split_item, dst))) {
      LOG_WARN("fail to split snapshot prefix");
    }
  } else if (index_type == VIAT_HNSW || index_type == VIAT_HNSW_SQ || index_type == VIAT_HNSW_BQ) {
    ObString split_item("_hnsw_");
    if (OB_FAIL(split_snapshot_prefix(src, split_item, dst))) {
      LOG_WARN("fail to split snapshot prefix");
    }
  } else if (index_type == VIAT_IPIVF) {
    ObString split_item("_ipivf_");
    if (OB_FAIL(split_snapshot_prefix(src, split_item, dst))) {
      LOG_WARN("fail to split ipivf snapshot prefix");
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index type", K(ret), K(index_type));
  }
  return ret;
}

int ObPluginVectorIndexUtils::split_snapshot_prefix(const ObString &src, const ObString &item, ObString &dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  if (src.empty() || item.empty() || (src.length() < item.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(src), K(item));
  } else {
    const char *str = strstr(src.ptr(), item.ptr());
    if (OB_ISNULL(str)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is not include to src", K(ret), K(src), K(dst));
    } else {
      const int64_t len = str - src.ptr();
      dst.assign_ptr(src.ptr(), static_cast<int32_t>(len));
    }
  }
  return ret;
}

void ObPluginVectorIndexUtils::set_ls_leader_flag(const ObLSID &ls_id, const bool is_leader)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (!ls_id.is_valid() || OB_ISNULL(vector_index_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->acquire_vector_index_mgr(ls_id, index_ls_mgr))) {
    LOG_WARN("fail to acquire vector index mgr", K(ret), K(ls_id));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(index_ls_mgr));
  } else {
    index_ls_mgr->set_ls_leader(is_leader);
    LOG_INFO("success to set ls leader", K(ls_id), K(is_leader));
  }
}

int ObPluginVectorIndexUtils::get_ls_leader_flag(const ObLSID &ls_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, index_ls_mgr))) {
    LOG_WARN("fail to get vector index ls mgr", KR(ret), K(ls_id));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    is_leader = index_ls_mgr->get_ls_leader();
    LOG_TRACE("success to set ls leader", K(ls_id), K(is_leader));
  }
  return ret;
}


int ObPluginVectorIndexUtils::fill_mem_context_detail_info(ObPluginVectorIndexService *service, ObIArray<ObLSTabletPair> &tablet_ids, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> adaptor_ptr_set;
  if (tablet_ids.count() > 0 && OB_FAIL(adaptor_ptr_set.create(tablet_ids.count()))) {
    LOG_WARN("fail to create tablet_id set", KR(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    ObLSID ls_id = tablet_ids.at(i).ls_id_;
    ObTabletID tablet_id = tablet_ids.at(i).tablet_id_;
    ObPluginVectorIndexAdapterGuard adapter_guard;
    if (OB_FAIL(service->get_adapter_inst_guard(ls_id, tablet_id, adapter_guard))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get adapter inst guard", K(ls_id), K(tablet_id), KR(ret));
      }
    } else {
      ObPluginVectorIndexAdaptor *adaptor = adapter_guard.get_adatper();
      if (OB_ISNULL(adaptor)) {
        // do nothing
      } else if (OB_FAIL(adaptor_ptr_set.exist_refactored(reinterpret_cast<int64_t>(adaptor)))) {
        if (ret == OB_HASH_EXIST) {
          // do nothing
          ret = OB_SUCCESS;
        } else if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          if (adaptor != NULL) {
            if (OB_NOT_NULL(adaptor->get_incr_data()) &&
                OB_NOT_NULL(adaptor->get_incr_data()->mem_ctx_) &&
                OB_NOT_NULL(adaptor->get_incr_data()->mem_ctx_->mem_ctx())) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos,", \"vsag_incr_%lu\":%ld", adaptor->get_inc_table_id(), adaptor->get_incr_vsag_mem_hold()))) {
                OB_LOG(WARN, "failed to get vsag incr data mem info", K(ret));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_NOT_NULL(adaptor->get_snap_data_()) &&
                OB_NOT_NULL(adaptor->get_snap_data_()->mem_ctx_) &&
                OB_NOT_NULL(adaptor->get_snap_data_()->mem_ctx_->mem_ctx())) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos,", \"vsag_snap_%lu\":%ld", adaptor->get_snapshot_table_id(), adaptor->get_snap_vsag_mem_hold()))) {
                OB_LOG(WARN, "failed to get vsag snap data mem info", K(ret));
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_NOT_NULL(adaptor->get_vbitmap_data()) &&
                OB_NOT_NULL(adaptor->get_vbitmap_data()->mem_ctx_) &&
                OB_NOT_NULL(adaptor->get_vbitmap_data()->mem_ctx_->mem_ctx())) {
              if (OB_FAIL(databuff_printf(buf, buf_len, pos,", \"vsag_bitmap_%lu\":%lu", adaptor->get_vbitmap_table_id(), adaptor->get_vbitmap_data()->mem_ctx_->hold()))) {
                OB_LOG(WARN, "failed to get vsag bitmap data mem info", K(ret));
              }
            }
          }
          OZ(adaptor_ptr_set.set_refactored(reinterpret_cast<int64_t>(adaptor)));
        } else {
          LOG_WARN("fail to create tablet_id set", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::get_mem_context_detail_info(ObPluginVectorIndexService *service,
                                                          ObIArray<ObLSTabletPair> &complete_tablet_ids,
                                                          ObIArray<ObLSTabletPair> &partial_tablet_ids,
                                                          ObIArray<ObLSTabletPair> &cache_tablet_ids,
                                                          char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_mem_context_detail_info(service, complete_tablet_ids, buf, buf_len, pos))) {
    LOG_WARN("failed to fill complete adaptor detail info", KR(ret));
  } else if (OB_FAIL(fill_mem_context_detail_info(service, partial_tablet_ids, buf, buf_len, pos))) {
    LOG_WARN("failed to fill partial adaptor detail info", KR(ret));
  } else if (OB_FAIL(fill_ivf_mem_context_detail_info(service, cache_tablet_ids, buf, buf_len, pos))) {
    LOG_WARN("failed to fill ivf mem_ctx detail info", KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexUtils::fill_ivf_mem_context_detail_info(ObPluginVectorIndexService *service, ObIArray<ObLSTabletPair> &tablet_ids, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> adaptor_ptr_set;
  if (tablet_ids.count() > 0 && OB_FAIL(adaptor_ptr_set.create(tablet_ids.count()))) {
    LOG_WARN("fail to create tablet_id set", KR(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    ObLSID ls_id = tablet_ids.at(i).ls_id_;
    ObTabletID tablet_id = tablet_ids.at(i).tablet_id_;
    ObIvfCacheMgrGuard cache_mgr_guard;
    if (OB_FAIL(service->acquire_ivf_cache_mgr_guard(ls_id, tablet_id, cache_mgr_guard))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get adapter inst guard", K(ls_id), K(tablet_id), KR(ret));
      }
    } else {
      ObIvfCacheMgr *adaptor = cache_mgr_guard.get_ivf_cache_mgr();
      if (OB_FAIL(ret) || OB_ISNULL(adaptor)) {
        // do nothing
      } else if (OB_FAIL(adaptor_ptr_set.exist_refactored(reinterpret_cast<int64_t>(adaptor)))) {
        if (ret == OB_HASH_EXIST) {
          // do nothing
          ret = OB_SUCCESS;
        } else if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          if (adaptor != NULL) {
            if (OB_FAIL(databuff_printf(buf, buf_len, pos,", \"ivf_cache_%lu\":%ld", adaptor->get_table_id(), adaptor->get_memory_hold()))) {
              OB_LOG(WARN, "failed to get vsag incr data mem info", K(ret));
            }
          }
          OZ(adaptor_ptr_set.set_refactored(reinterpret_cast<int64_t>(adaptor)));
        } else {
          LOG_WARN("fail to create tablet_id set", K(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
