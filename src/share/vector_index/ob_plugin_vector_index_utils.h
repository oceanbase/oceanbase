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
#ifndef OCEANBASE_OBSERVER_OB_MOCK_PLUGIN_VECTOR_INDEX_UTILS_DEFINE_H_
#define OCEANBASE_OBSERVER_OB_MOCK_PLUGIN_VECTOR_INDEX_UTILS_DEFINE_H_
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "common/rowkey/ob_rowkey.h"
#include "src/share/schema/ob_tenant_schema_service.h"
#include "lib/vector/ob_vector_util.h"
#include "ob_vector_kmeans_ctx.h"

namespace oceanbase
{
namespace share
{

class ObVsagLoggerSingleton
{
private:
  ObVsagLoggerSingleton() {}

public:
  ObVsagLoggerSingleton(const ObVsagLoggerSingleton&) = delete;
  ObVsagLoggerSingleton& operator=(const ObVsagLoggerSingleton&) = delete;

  static obvectorutil::ObVsagLogger &getInstance() {
    static obvectorutil::ObVsagLogger instance;
    return instance;
  }

};

class ObPluginVectorIndexUtils
{
public:
  static int get_task_read_snapshot(ObLSID &ls_id, SCN &read_version);
  static int refresh_memdata(ObLSID &ls_id,
                             ObPluginVectorIndexAdaptor *adapter,
                             SCN target_scn,
                             ObIAllocator &allocator);
  static int release_vector_index_adapter(ObPluginVectorIndexAdaptor* &adapter);
  static int release_vector_index_build_helper(ObIvfBuildHelper* &helper);
  static ObVectorIndexRecordType index_type_to_record_type(schema::ObIndexType type);

  static ObAdapterCreateType index_type_to_create_type(schema::ObIndexType type);

  static int get_vector_index_prefix(const ObTableSchema &index_schema, ObString &prefix);
  static int set_vsag_logger() {
    return obvectorutil::init_vasg_logger(&ObVsagLoggerSingleton::getInstance());
  }

  static int add_key_ranges(uint64_t table_id, ObRowkey& rowkey, storage::ObTableScanParam &scan_param);
  static int iter_table_rescan(storage::ObTableScanParam &scan_param, common::ObNewRowIterator *iter);

  static int read_object_from_vid_rowkey_table_iter(ObObj *input_obj,
                                                    uint64_t table_id,
                                                    storage::ObTableScanParam &scan_param,
                                                    common::ObNewRowIterator *iter,
                                                    schema::ObIndexType type,
                                                    ObIAllocator &allocator,
                                                    ObObj *&output_obj,
                                                    int32_t data_table_rowkey_count);
  static int read_object_from_data_table_iter(ObObj *&input_obj,
                                              int32_t data_table_rowkey_count,
                                              uint64_t table_id,
                                              storage::ObTableScanParam &scan_param,
                                              common::ObNewRowIterator *iter,
                                              schema::ObIndexType type,
                                              ObIAllocator &allocator,
                                              ObObj &output_obj,
                                              bool &get_data);
  static int get_vec_column_id (ObSEArray<uint64_t, 4> &vector_column_ids,
                                uint64_t incr_index_table_id,
                                uint64_t data_table_id,
                                uint64_t tenant_id);
  static int read_vector_info(ObPluginVectorIndexAdaptor *adapter,
                              ObIAllocator &allocator,
                              ObLSID &ls_id,
                              SCN target_scn,
                              ObVectorQueryAdaptorResultContext &ada_ctx);

  static int test_read_local_data(ObLSID &ls_id,
                                  ObPluginVectorIndexAdaptor *adapter,
                                  ObIndexType index_type,
                                  SCN target_scn,
                                  ObIAllocator &allocator);

private:
  static int read_local_tablet(ObLSID &ls_id,
                               ObPluginVectorIndexAdaptor* adapter,
                               SCN target_scn,
                               schema::ObIndexType type,
                               ObIAllocator &allocator,
                               ObIAllocator &scan_allocator,
                               ObTableScanParam &scan_param,
                               ObTableParam &table_param,
                               common::ObNewRowIterator *&scan_iter);
  static int init_common_scan_param(storage::ObTableScanParam& scan_param,
                                    ObPluginVectorIndexAdaptor *adapter,
                                    SCN target_scn,
                                    ObIAllocator *allocator,
                                    ObIAllocator *scan_allocator,
                                    schema::ObIndexType type,
                                    uint64_t table_id);
  static int init_table_param(ObTableParam *table_param,
                              uint64_t inc_table_id,
                              uint64_t data_table_id,
                              uint64_t table_id,
                              schema::ObIndexType type,
                              ObPluginVectorIndexAdaptor *adapter);
  static int get_non_shared_index_aux_table_colum_count(schema::ObIndexType type, uint32 &col_cnt);
  static int get_non_shared_index_aux_table_rowkey_colum_count(schema::ObIndexType type, uint32 &col_cnt);
  static int get_special_index_aux_table_column_count(schema::ObIndexType type,
                                                      uint64_t tenant_id,
                                                      uint64_t table_id,
                                                      uint32 &col_cnt,
                                                      storage::ObTableScanParam& scan_param);
  static int get_shared_table_rowkey_colum_count(schema::ObIndexType type,
                                                 uint64_t tenant_id,
                                                 uint64_t table_id,
                                                 uint32 &col_cnt);
  static int try_sync_snapshot_memdata(ObLSID &ls_id,
                                       ObPluginVectorIndexAdaptor *adapter,
                                       SCN &target_scn,
                                       ObIAllocator &allocator,
                                       ObVectorQueryAdaptorResultContext &ada_ctx);
  static int try_sync_vbitmap_memdata(ObLSID &ls_id,
                                      ObPluginVectorIndexAdaptor *adapter,
                                      SCN &target_scn,
                                      ObIAllocator &allocator,
                                      ObVectorQueryAdaptorResultContext &ada_ctx);
};

} // namespace share
} // namespace oceanbase
#endif