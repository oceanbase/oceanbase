/**
 * Copyright (c) 2025 OceanBase
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
#include <gtest/gtest.h>
#include <iostream>
#define private public
#define protected public

#include "simple_server/vector_index/test_vector_index_utils.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"

namespace oceanbase {
namespace unittest {

using namespace oceanbase::common;
using namespace oceanbase::share;

int TestVectorIndexBase::get_current_scn(share::SCN &current_scn)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);

  current_scn.set_invalid();
  int64_t start_us = ObTimeUtility::fast_current_time();
  const transaction::MonotonicTs stc = transaction::MonotonicTs(start_us);
  transaction::MonotonicTs rts(0);

  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else if (OB_FAIL(txs->get_ts_mgr()->get_gts(tenant_id, stc, NULL, current_scn, rts))) {
    LOG_WARN("get scn from cache.", KR(ret));
  }
  return ret;
}

void TestVectorIndexBase::insert_data(const int64_t idx, const int64_t insert_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  LOGI("start fill data", K(idx));
  std::mt19937 rng;
  rng.seed(idx);
  const int dim = 512;
  const int cnt = insert_cnt;
  std::uniform_real_distribution<> distrib_real;
  for (int i = 0; i < cnt; ++i) {
    std::string insert_sql = "insert into t1(embedding) values('[";
    for (int i = 0; i < dim; ++i) {
      if (i > 0) insert_sql += ",";
      insert_sql += std::to_string(distrib_real(rng));
    }
    insert_sql += "]')";
    // std::cout << "insert_sql: " << insert_sql << std::endl;
    WRITE_SQL_BY_CONN(connection, insert_sql.c_str());
    ASSERT_EQ(1, affected_rows);
  }
  ASSERT_EQ(0, sql_proxy.close(connection, true));
}

void TestVectorIndexBase::search_data(const int64_t idx, const int64_t select_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  // LOGI("start search data", K(idx));
  std::mt19937 rng;
  rng.seed(idx);
  const int dim = 512;
  const int cnt = select_cnt;
  std::uniform_real_distribution<> distrib_real;
  for (int i = 0; i < cnt; ++i) {

    std::string vec = "'[";
    for (int i = 0; i < dim; ++i) {
      if (i > 0) vec += ",";
      vec += std::to_string(distrib_real(rng));
    }
    vec += "]'";
    std::string select_sql =
      "(select c1 val from t1 order by cosine_distance(embedding, " + vec + ") limit 1) minus (select c1 val from t1 order by cosine_distance(embedding, " + vec + ") approx limit 1)";
    // std::string select_sql =
    //   "select c1 val from t1 order by cosine_distance(embedding, " + vec + ") approx limit 10";
    // std::cout << "select_sql: " << select_sql << std::endl;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(connection->execute_read(OB_SYS_TENANT_ID, select_sql.c_str(), res))) {
        LOG_WARN("execute_read fail", K(ret), KCSTRING(select_sql.c_str()));
      } else {
        int64_t val = -1;
        sqlclient::ObMySQLResult *result = res.get_result();
        if (result == nullptr) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("result is null", K(ret), KCSTRING(select_sql.c_str()));
        } else {
          int64_t miss_cnt = 0;
          std::string miss_vids;
          while (OB_SUCC(result->next())) {
            if (OB_FAIL(result->get_int("val", val))) {
              LOG_WARN("select failed", KR(ret), KCSTRING(select_sql.c_str()));
            } else {
              ++miss_cnt;
              if (miss_cnt > 1) miss_vids += ",";
              miss_vids += std::to_string(val);
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          if (OB_SUCC(ret) && miss_cnt > 1) {
            std::cout << "miss_cnt=" << miss_cnt << ", miss_vids=[" << miss_vids << "]" << std::endl;
          }
        }
      }
    }
  }
  ASSERT_EQ(0, sql_proxy.close(connection, true));
}

int TestVectorIndexBase::get_snapshot_metadata(ObPluginVectorIndexAdaptor* adaptor, const ObLSID &ls_id, ObVectorIndexMeta &meta)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  share::SCN current_scn;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;

  if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(const_cast<ObLSID&>(ls_id),
                                adaptor,
                                current_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator,
                                allocator,
                                snap_scan_param,
                                snap_table_param,
                                snap_data_iter))) {
    LOG_WARN("read_local_tablet fail", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (! datum_row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first row of vector index is not meta row", K(ret), KPC(datum_row));
  } else {
    ObString meta_data = datum_row->storage_datums_[1].get_string();
    int64_t pos = 0;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, true, meta_data, nullptr))) {
      LOG_WARN("read real string data fail", K(ret));
    } else if (OB_FAIL(meta.deserialize(meta_data.ptr(), meta_data.length(), pos))) {
      LOG_WARN("deserialize meta fail", K(ret), K(meta_data.length()), K(pos));
    } else {
      LOG_INFO("meta info", K(meta));
    }
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int TestVectorIndexBase::check_segment_meta_row(ObPluginVectorIndexAdaptor *adaptor,
    const ObLSID& ls_id, ObVectorIndexSegmentMeta &seg_meta)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::TEST);
  share::SCN current_scn;
  schema::ObTableParam snap_table_param(allocator);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::open_segment_data_iter(adaptor, allocator, ls_id,
                                adaptor->get_snap_tablet_id(), seg_meta.start_key_, seg_meta.end_key_,
                                current_scn, snap_scan_param,
                                snap_table_param,
                                snap_data_iter))) {
    LOG_WARN("open_segment_data_iter fail", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (datum_row->storage_datums_[0].get_string().compare(seg_meta.start_key_) != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first row of vector index is not meta row", K(ret), KPC(datum_row));
  } else {
    ObString real_data = datum_row->storage_datums_[1].get_string();
    int64_t pos = 0;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, true, real_data, nullptr))) {
      LOG_WARN("read real string data fail", K(ret));
    } else if (OB_FAIL(adaptor->create_snap_segment(seg_meta.index_type_, seg_meta))) {
      LOG_WARN("init seg data fail", K(ret), K(seg_meta));
    } else if (seg_meta.segment_handle_->deserialize_meta(real_data.ptr(), real_data.length(), pos)) {
      LOG_WARN("deserialize meta fail", K(ret), K(real_data.length()), K(pos));
    } else if (OB_ISNULL(seg_meta.segment_handle_->ibitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOGI("segment ibitmap is null", K(ret));
    } else if (OB_ISNULL(seg_meta.segment_handle_->ibitmap_->insert_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOGI("segment insert bitmap is null", K(ret));
    } else if (OB_NOT_NULL(seg_meta.segment_handle_->ibitmap_->delete_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOGI("segment delete bitmap is not null", K(ret));
    } else if (OB_ISNULL(seg_meta.segment_handle_->vbitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOGI("segment ibitmap is null", K(ret));
    } else if (OB_ISNULL(seg_meta.segment_handle_->vbitmap_->insert_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOGI("segment insert bitmap is null", K(ret));
    } else if (OB_ISNULL(seg_meta.segment_handle_->vbitmap_->delete_bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOGI("segment delete bitmap is not null", K(ret));
    } else {
      int64_t ii_cnt = roaring64_bitmap_get_cardinality(seg_meta.segment_handle_->ibitmap_->insert_bitmap_);
      int64_t vi_cnt = roaring64_bitmap_get_cardinality(seg_meta.segment_handle_->vbitmap_->insert_bitmap_);
      int64_t vd_cnt = roaring64_bitmap_get_cardinality(seg_meta.segment_handle_->vbitmap_->delete_bitmap_);
      const bool is_subset = roaring64_bitmap_is_subset(
          seg_meta.segment_handle_->vbitmap_->insert_bitmap_,
          seg_meta.segment_handle_->ibitmap_->insert_bitmap_);

      LOG_INFO("seg_meta info", K(seg_meta), K(ii_cnt), K(vi_cnt), K(vd_cnt), K(is_subset));
      // if (ii_cnt != vi_cnt || vd_cnt !=0 || ! is_subset || vi_cnt != 10000) {
      //   ret = OB_ERR_UNEXPECTED;
      // }
    }
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }

  return ret;
}

int TestVectorIndexBase::check_vector_index_task_finished()
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  if (OB_FAIL(sql_proxy.acquire(connection))) {
    LOG_WARN("acquire fail", K(ret));
  } else {
      while (OB_SUCC(ret)) {
      if (OB_FAIL(
          SSH::select_int64(sql_proxy, "select count(*) val from oceanbase.__all_vector_index_task", task_cnt))) {
        LOG_WARN("select vector index task count fail", K(ret));
      } else if (task_cnt != 0) {
        if (REACH_TIME_INTERVAL(10 * 1000000)) {  // 10s
          LOGI("vector index task not finished: %ld", task_cnt);
        }
         ob_usleep(1000 * 1000);
      } else {
        break;
      }
    }
  }
  return ret;
}

int TestVectorIndexBase::check_vector_index_task_success()
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  if (OB_FAIL(sql_proxy.acquire(connection))) {
    LOG_WARN("acquire fail", K(ret));
  } else if (OB_FAIL(
      SSH::select_int64(sql_proxy, "select count(*) val from oceanbase.__all_vector_index_task_history where ret_code != 0", task_cnt))) {
    LOG_WARN("select vector index task count fail", K(ret));
  } else if (task_cnt != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOGI("there are some vector index task not success: %ld", task_cnt);
  }
  return ret;
}

int TestVectorIndexBase::check_vector_index_task_count(const int64_t expected_cnt)
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  if (OB_FAIL(sql_proxy.acquire(connection))) {
    LOG_WARN("acquire fail", K(ret));
  } else {
      while (OB_SUCC(ret)) {
      if (OB_FAIL(
          SSH::select_int64(sql_proxy, "select count(*) val from oceanbase.__all_vector_index_task_history", task_cnt))) {
        LOG_WARN("select vector index task count fail", K(ret));
      } else if (task_cnt != expected_cnt) {
        if (REACH_TIME_INTERVAL(10 * 1000000)) {  // 10s
          LOGI("vector index task not finished: %ld, expected: %ld", task_cnt, expected_cnt);
        }
         ob_usleep(1000 * 1000);
      } else {
        break;
      }
    }
  }
  return ret;
}

}  // namespace unittest
}  // namespace oceanbase
