/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_ODPS_TABLE_UTILS_H_
#define _OB_ODPS_TABLE_UTILS_H_

#include "share/external_table/ob_external_table_utils.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"

namespace oceanbase
{
namespace share
{

class ObOdpsTableUtils {
public:
  static int resolve_odps_start_step(const ObOdpsScanTask *scan_task,
                                     int64_t &start,
                                     int64_t &step);
  static int make_odps_scan_task(const common::ObString &file_url,
                                 const uint64_t part_id,
                                 const int64_t first_lineno,
                                 const int64_t last_lineno,
                                 const common::ObString &session_id,
                                 const int64_t first_split_idx,
                                 const int64_t last_split_idx,
                                 ObOdpsScanTask &scan_task);

  static ObString get_part_spec(const ObString &file_info)
  {
      return file_info;
  }
  static ObString get_part_spec(const ObExternalFileInfo &file_info)
  {
      return file_info.file_url_;
  }
  static ObString get_part_spec(const sql::ObOptOdpsFile *file)
  {
      return NULL == file ? ObString() : file->file_url_;
  }
  static ObString get_part_spec(const ObExternalTableFileManager::ObExternalFileInfoTmp &file_info)
  {
      return file_info.file_url_;
  }

  // definitions are after the class (template implementation section)
  template <typename PartRecordType>
  static int fetch_odps_all_partitions_size(
      ObSQLSessionInfo &session,
      const ObString &porperty_str,
      const ObIArray<PartRecordType> &partition_strs,
      const uint64_t tenant_id,
      const uint64_t table_ref_id,
      common::ObIAllocator &allocator_for_map_key,
      common::hash::ObHashMap<ObOdpsPartitionKey, int64_t>
          &partition_str_to_file_size /* life time longer than partition_str_to_file_size*/);

  template <typename PartRecordType>
  static int fetch_odps_all_partitions_info(
      ObSQLSessionInfo &session,
      const ObString &porperty_str,
      const ObIArray<PartRecordType> &partition_strs,
      const sql::ObOdpsJniConnector::OdpsFetchType fetch_type,
      const int64_t dop_of_collect_external_table_statistics,
      const uint64_t tenant_id, common::ObIAllocator &allocator,
      ObSEArray<ObString, 20> &partition_names,
      ObSEArray<int64_t, 20> &partition_file_sizes,
      ObSEArray<ObString, 20> &session_ids);
};

// =====================================================================
// 模板实现（体量大，移出类外保持声明区紧凑）
// =====================================================================
  template <typename PartRecordType>
int ObOdpsTableUtils::fetch_odps_all_partitions_size(
      ObSQLSessionInfo &session,
      const ObString &porperty_str,
      const ObIArray<PartRecordType> &partition_strs,
      const uint64_t tenant_id,
      const uint64_t table_ref_id,
      common::ObIAllocator &allocator_for_map_key,
      common::hash::ObHashMap<ObOdpsPartitionKey, int64_t>
          &partition_str_to_file_size /* life time longer than partition_str_to_file_size*/) {
    int ret = OB_SUCCESS;
    int64_t dop_of_collect_external_table_statistics = 1;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid()) &&
        static_cast<int64_t>(
            tenant_config->_dop_of_collect_external_table_statistics) > 0) {
      dop_of_collect_external_table_statistics =
          tenant_config->_dop_of_collect_external_table_statistics;
    } else {
      int64_t default_dop =
          (partition_strs.count() / 4 > 0) ? (partition_strs.count() / 4) : 1;
      double min_cpu;
      double max_cpu;
      if (OB_ISNULL(GCTX.omt_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(
                     GCTX.omt_->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
        LOG_WARN("fail to get tenant cpu", K(ret));
      } else {
        dop_of_collect_external_table_statistics =
            min(default_dop, (int)max_cpu);
      }
    }
    if (OB_SUCC(ret) && !partition_str_to_file_size.created()) {
      if (OB_UNLIKELY(partition_strs.count() == 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition_strs is empty", K(ret));
      } else {
        OZ(partition_str_to_file_size.create(
            partition_strs.count(), ObMemAttr(MTL_ID(), "ODPS_PART_SIZE")));
      }
    }
    ObSEArray<PartRecordType, 20> partition_strs_filter;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_strs.count(); ++i) {
      int64_t file_size = 0;
      const ObString &odps_partition = get_part_spec(partition_strs.at(i));
      if (0 == odps_partition.compare(ObExternalTableUtils::dummy_file_name())) {
        // do nothing
      } else if (OB_SUCC(partition_str_to_file_size.get_refactored(
                   ObOdpsPartitionKey(table_ref_id, odps_partition),
                   file_size))) {
        // do nothing
      } else if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        OZ(partition_strs_filter.push_back(partition_strs.at(i)));
      }
    }
    ObSEArray<ObString, 20> partition_names_ret;
    ObSEArray<int64_t, 20> partition_file_sizes_ret;
    ObSEArray<ObString, 20> session_ids_ret;
    if (OB_SUCC(ret) && partition_strs_filter.count() > 0) {
      OZ(fetch_odps_all_partitions_info(
          session,
          porperty_str, partition_strs_filter,
          sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_SIZE,
          dop_of_collect_external_table_statistics, tenant_id,
          allocator_for_map_key, partition_names_ret, partition_file_sizes_ret,
          session_ids_ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_names_ret.count(); ++i) {
      const ObString &partition_name = partition_names_ret.at(i);
      const int64_t file_size = partition_file_sizes_ret.at(i);
      OZ(partition_str_to_file_size.set_refactored(ObOdpsPartitionKey(table_ref_id, partition_name), file_size));
    }
    return ret;
  }


  template <typename PartRecordType>
int ObOdpsTableUtils::fetch_odps_all_partitions_info(
      ObSQLSessionInfo &session,
      const ObString &porperty_str,
      const ObIArray<PartRecordType> &partition_strs,
      const sql::ObOdpsJniConnector::OdpsFetchType fetch_type,
      const int64_t dop_of_collect_external_table_statistics,
      const uint64_t tenant_id, common::ObIAllocator &allocator,
      ObSEArray<ObString, 20> &partition_names,
      ObSEArray<int64_t, 20> &partition_file_sizes,
      ObSEArray<ObString, 20> &session_ids) {
    int ret = OB_SUCCESS;
    ObSqlString query_sql;

    if (OB_SUCC(ret)) {
      // 构建正确的VALUES查询, 存在limit的时候不查询 timeout_us
      // THIS_WORKER.get_timeout_remain()
      // int64_t remain_timeout =
      //     min(THIS_WORKER.get_timeout_remain(), 10 * 1000 * 1000); // 10s
      int64_t remain_timeout = THIS_WORKER.get_timeout_remain();
      OZ(query_sql.assign_fmt(
          "SELECT/*+ query_timeout(%ld) parallel(%ld) no_rewrite "
          "PQ_SUBQUERY(HASH ALL) */ partition_str, (select "
          "CALC_ODPS_SIZE(partition_str, %d, property_str) file_size from "
          "dual) as file_size FROM ",
          remain_timeout, dop_of_collect_external_table_statistics,
          (fetch_type ==
                   sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_ROW_COUNT
               ? 0
               : 1) /* 获取partition size */));
      OZ(query_sql.append_fmt("(SELECT/*+ no_rewrite*/ * FROM "));
      OZ(query_sql.append_fmt(
          "(VALUES ROW('%.*s')) property_name(property_str), ",
          static_cast<int>(porperty_str.length()), porperty_str.ptr()));
      OZ(query_sql.append("(VALUES "));
      int64_t file_size = 0;
      int64_t file_to_collect = 0;

      for (int64_t i = 0; OB_SUCC(ret) && i < partition_strs.count(); ++i) {
        const ObString &odps_partition = get_part_spec(partition_strs.at(i));
        if (0 ==
            odps_partition.compare(ObExternalTableUtils::dummy_file_name())) {
          // do nothing
        } else {
          // ODPSQz IF NON Partition ROW()
          OZ(query_sql.append_fmt("ROW(\"%.*s\")",
                                  static_cast<int>(odps_partition.length()),
                                  odps_partition.ptr()));
          if (i < partition_strs.count() - 1) {
            OZ(query_sql.append(","));
          }
          file_to_collect += 1;
        }
      }
      OZ(query_sql.append(")  partition_name(partition_str)),"));
      OZ(query_sql.append(
          "(select count(*) from internal.oceanbase.__all_dummy);"));

      if (file_to_collect > 0) {
        ObMySQLTransaction trans;

        CK(OB_NOT_NULL(GCTX.sql_proxy_));
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to assign query sql", KR(ret));
        } else {
          OZ(trans.start(GCTX.sql_proxy_, tenant_id));

          LOG_INFO("odps query_sql", K(query_sql));
          SMART_VAR(ObISQLClient::ReadResult, result)
          {
            if (OB_FAIL(trans.read(result, tenant_id, query_sql.ptr()))) {
              LOG_WARN("failed to read result", KR(ret));
            } else {
              ObMySQLResult *res = NULL;
              int res_ret = OB_SUCCESS;
              if (OB_ISNULL(res = result.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get mysql result failed", KR(ret), K(tenant_id),
                         K(query_sql));
              } else {
                if (fetch_type ==
                    sql::ObOdpsJniConnector::OdpsFetchType::GET_ODPS_TABLE_SIZE) {
                  while (OB_SUCCESS == (res_ret = res->next())) {
                    int64_t file_size = 0;
                    ObString partition_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "partition_str",
                                                partition_str);
                    ObString file_size_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "file_size",
                                                file_size_str);
                    OZ(ob_write_string(allocator, partition_str,
                                       partition_str));
                    // ObString to int64_t
                    if (OB_SUCC(ret)) {
                      bool valid = false;
                      file_size = ObFastAtoi<int64_t>::atoi(
                          file_size_str.ptr(),
                          file_size_str.ptr() + file_size_str.length(), valid);
                      if (!valid) {
                        ret = OB_ERR_DATA_TRUNCATED;
                      }
                    }
                    OZ(partition_names.push_back(partition_str));
                    OZ(partition_file_sizes.push_back(file_size));
                    LOG_TRACE("ODPS get partition info ",
                              K(fetch_type ==
                                        sql::ObOdpsJniConnector::OdpsFetchType::
                                            GET_ODPS_TABLE_ROW_COUNT
                                    ? "row count"
                                    : "size"),
                              K(partition_str), K(file_size));
                  }
                  if (res_ret != OB_ITER_END) {
                    ret = res_ret;
                    LOG_WARN("failed to get next row", KR(ret), K(tenant_id),
                             K(query_sql));
                  }
                } else { // GET_ODPS_TABLE_ROW_COUNT
                  while (OB_SUCCESS == (res_ret = res->next())) {
                    int64_t row_count = 0;
                    ObString partition_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "partition_str",
                                                partition_str);
                    ObString row_count_str;
                    EXTRACT_VARCHAR_FIELD_MYSQL(*res, "file_size",
                                                row_count_str);
                    ObString row_count_str_part = row_count_str.split_on('|');
                    ObString session_id = row_count_str;
                    if (OB_SUCC(ret)) {
                      bool valid = false;
                      row_count = ObFastAtoi<int64_t>::atoi(
                          row_count_str_part.ptr(),
                          row_count_str_part.ptr() +
                              row_count_str_part.length(),
                          valid);
                      if (!valid) {
                        ret = OB_ERR_DATA_TRUNCATED;
                      }
                    }
                    ObString partition_str_cp;
                    ObString session_id_cp;
                    OZ(ob_write_string(allocator, partition_str,
                                       partition_str_cp));
                    OZ(partition_names.push_back(partition_str_cp));
                    OZ(partition_file_sizes.push_back(row_count));
                    OZ(ob_write_string(allocator, session_id, session_id_cp));
                    OZ(session_ids.push_back(session_id_cp));
                    LOG_WARN("ODPS get partition info ", K(partition_str),
                             K(row_count), K(session_id));
                    LOG_TRACE("ODPS get partition info ",
                              K(fetch_type ==
                                        sql::ObOdpsJniConnector::OdpsFetchType::
                                            GET_ODPS_TABLE_ROW_COUNT
                                    ? "row count"
                                    : "size"),
                              K(partition_str), K(row_count), K(session_id));
                  }
                  if (res_ret != OB_ITER_END) {
                    ret = res_ret;
                    LOG_WARN("failed to get next row", KR(ret), K(tenant_id),
                             K(query_sql));
                  }
                }
              }
            }
          }
        }
        OZ(trans.end(true));
        if (trans.is_started()) {
          trans.end(false);
        }
      }
    }
    return ret;
  }

} // namespace share
} // namespace oceanbase
#endif /* _OB_ODPS_TABLE_UTILS_H_ */
