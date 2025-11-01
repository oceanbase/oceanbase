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
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

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
  static int refresh_adp_from_table(ObLSID &ls_id,
                                    ObPluginVectorIndexAdaptor *&adapter,
                                    const bool create_new_adapter,
                                    SCN target_scn,
                                    ObIAllocator &allocator);
  static int release_vector_index_adapter(ObPluginVectorIndexAdaptor* &adapter);
  static int release_vector_index_build_helper(ObIvfBuildHelper* &helper);
  static int release_ivf_cache_mgr(ObIvfCacheMgr* &mgr);
  static ObVectorIndexRecordType index_type_to_record_type(schema::ObIndexType type);

  static ObAdapterCreateType index_type_to_create_type(schema::ObIndexType type);

  static int get_vector_index_prefix(const ObTableSchema &index_schema, ObString &prefix);
  static int get_prefix(const ObString &src, const ObString &item, ObString &dst);
  static int get_suffix(const ObString &src, const ObString &item, ObString &dst);
  static int get_key_prefix_scn(const ObString &key_prefix, int64_t &scn);
  static int get_table_key_scn(const ObString &key_str, int64_t &scn);
  static int get_split_snapshot_prefix(const ObVectorIndexAlgorithmType index_type,
                                       const ObString &src,
                                       ObString &dst);
  static int set_vsag_logger() {
    return obvectorutil::init_vasg_logger(&ObVsagLoggerSingleton::getInstance());
  }
  static void set_ls_leader_flag(const ObLSID &ls_id, const bool is_leader);
  static int get_ls_leader_flag(const ObLSID &ls_id, bool &is_leader);
  static int get_read_scn(bool is_leader, ObLSID &ls_id, SCN &target_scn);
  static int query_need_refresh_memdata(ObPluginVectorIndexAdaptor *adapter, ObLSID &ls_id, bool is_leader);
  static int check_snapshot_iter_need_rescan(common::ObNewRowIterator *snapshot_idx_iter, bool &need_rescan, blocksstable::ObDatumRow *&row);

  static int add_key_ranges(uint64_t table_id, ObRowkey& rowkey, storage::ObTableScanParam &scan_param);
  static int add_key_ranges(uint64_t table_id, ObRowkey& start_key, ObRowkey& end_key, storage::ObTableScanParam &scan_param);
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
                                              int64_t extra_info_count,
                                              ObVecExtraInfoObj *output_extra_objs,
                                              bool &get_data);
  static int read_object_from_embedded_table_iter(ObObj *&input_obj,
                                                  int32_t data_table_rowkey_count,
                                                  uint64_t table_id,
                                                  storage::ObTableScanParam &scan_param,
                                                  common::ObNewRowIterator *iter,
                                                  ObIAllocator &allocator,
                                                  ObObj &output_vec_obj,
                                                  int64_t extra_column_count,
                                                  ObVecExtraInfoObj *output_extra_info_objs,
                                                  bool &get_data);
  static int64_t get_extra_column_count(const schema::ObTableParam &table_param, schema::ObIndexType type);
  static int get_extra_column_count(ObPluginVectorIndexAdaptor &adapter, int &column_count);
  static int get_data_table_out_column_id(ObSEArray<uint64_t, 4> &vector_column_ids,
                                          uint64_t incr_index_table_id,
                                          uint64_t data_table_id,
                                          uint64_t tenant_id,
                                          ObPluginVectorIndexAdaptor *adapter);
  static int get_extra_info_objs(storage::ObTableScanParam &scan_param,
                                 ObIAllocator &allocator,
                                 int64_t extra_info_count,
                                 blocksstable::ObDatumRow *datum_row,
                                 ObVecExtraInfoObj *out_extra_info_objs,
                                 int offset = 1);
  static int read_vector_info(ObPluginVectorIndexAdaptor *adapter,
                              ObIAllocator &allocator,
                              ObLSID &ls_id,
                              SCN target_scn,
                              ObVectorQueryAdaptorResultContext &ada_ctx);
  static int read_local_tablet(ObLSID &ls_id,
                               ObPluginVectorIndexAdaptor* adapter,
                               SCN target_scn,
                               schema::ObIndexType type,
                               ObIAllocator &allocator,
                               ObIAllocator &scan_allocator,
                               ObTableScanParam &scan_param,
                               ObTableParam &table_param,
                               common::ObNewRowIterator *&scan_iter,
                               ObIArray<uint64_t> *out_column_ids = nullptr,
                               const bool need_all_columns = false,
                               const bool is_reverse_scan = false,
                               const bool need_ora_scn = false);
  static int get_snap_index_visible_row_iter(ObAccessService *tsc_service,
                                            ObLSID &ls_id,
                                            ObPluginVectorIndexAdaptor *adapter,
                                            SCN &target_scn,
                                            ObIAllocator &allocator,
                                            storage::ObTableScanParam &snapshot_scan_param,
                                            schema::ObTableParam &snapshot_table_param,
                                            common::ObNewRowIterator *&snapshot_idx_iter,
                                            blocksstable::ObDatumRow *&row);
  static int get_snap_index_visible_row_key(ObLSID &ls_id,
                                            ObPluginVectorIndexAdaptor *adapter,
                                            share::SCN &target_scn,
                                            ObIAllocator &allocator,
                                            ObString &row_key);

  static int test_read_local_data(ObLSID &ls_id,
                                  ObPluginVectorIndexAdaptor *adapter,
                                  ObIndexType index_type,
                                  SCN target_scn,
                                  ObIAllocator &allocator);
  static int erase_ivf_build_helper(ObLSID ls_id, const ObIvfHelperKey &key);
  static int get_mem_context_detail_info(ObPluginVectorIndexService *service,
                                         ObIArray<ObLSTabletPair> &complete_tablet_ids,
                                         ObIArray<ObLSTabletPair> &partial_tablet_ids,
                                         ObIArray<ObLSTabletPair> &cache_tablet_ids,
                                         char *buf, int64_t buf_len, int64_t &pos);

private:
  static const int EMBEDDED_TABLE_BASE_COLUMN_CNT = 2;
  static int init_common_scan_param(storage::ObTableScanParam& scan_param,
                                    ObPluginVectorIndexAdaptor *adapter,
                                    SCN target_scn,
                                    ObIAllocator *allocator,
                                    ObIAllocator *scan_allocator,
                                    schema::ObIndexType type,
                                    uint64_t table_id,
                                    const bool is_reverse_scan);
  static int init_table_param(ObTableParam *table_param,
                              uint64_t inc_table_id,
                              uint64_t data_table_id,
                              uint64_t table_id,
                              schema::ObIndexType type,
                              ObPluginVectorIndexAdaptor *adapter,
                              ObIArray<uint64_t> *out_column_ids = nullptr,
                              const bool need_all_columns = false,
                              const bool need_ora_scn = false);
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
                                       ObPluginVectorIndexAdaptor *&adapter,
                                       const bool create_new_adp,
                                       SCN &target_scn,
                                       ObIAllocator &allocator);
  static int try_sync_vbitmap_memdata(ObLSID &ls_id,
                                      ObPluginVectorIndexAdaptor *adapter,
                                      SCN &target_scn,
                                      ObIAllocator &allocator,
                                      ObVectorQueryAdaptorResultContext &ada_ctx);
  static int fill_mem_context_detail_info(ObPluginVectorIndexService *service, ObIArray<ObLSTabletPair> &tablet_ids, char *buf, int64_t buf_len, int64_t &pos);
  static int fill_ivf_mem_context_detail_info(ObPluginVectorIndexService *service, ObIArray<ObLSTabletPair> &tablet_ids, char *buf, int64_t buf_len, int64_t &pos);

};

} // namespace share
} // namespace oceanbase
#endif
