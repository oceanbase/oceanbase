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

#define USING_LOG_PREFIX SHARE

#include "ob_partition_table_proxy.h"
#include "ob_ipartition_table.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_array_iterator.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "ob_replica_filter.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"
#include "common/ob_partition_key.h"
#include "share/ob_time_zone_info_manager.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"

namespace oceanbase {
namespace share {

using namespace common;
using namespace common::sqlclient;

namespace partition_table {

class ColNames {
public:
  int update(const char* name, const char* new_name);
  int append_name_list(common::ObSqlString& sql);

  virtual common::ObIArray<const char*>& get_names() = 0;
};

class PartitionTableColNames : public ColNames {
public:
  PartitionTableColNames();
  virtual ~PartitionTableColNames()
  {}

  virtual common::ObIArray<const char*>& get_names()
  {
    return cols_;
  }

protected:
  static const int64_t MAX_COL_CNT = 256;
  const char* col_names_[MAX_COL_CNT];
  common::ObArrayHelper<const char*> cols_;
};

template <typename T>
class IndexToIndex : public T {
public:
  int64_t index(const int64_t idx) const
  {
    return idx;
  }
};

template <typename T>
class IndexToName : public T {
public:
  const char* index(const int64_t idx) const;
};

template <typename T>
const char* IndexToName<T>::index(const int64_t idx) const
{
  const char* name = NULL;
  if (idx < 0 || idx >= this->cols_.count()) {
    LOG_ERROR("invalid idx", K(idx), "cols", this->cols_);
  } else {
    name = this->cols_.at(idx);
  }
  return name;
}

typedef IndexToName<PartitionTableColNames> KVPartitionTableCol;
typedef IndexToIndex<PartitionTableColNames> NormalPartitionTableCol;

int ColNames::update(const char* name, const char* new_name)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (NULL == name || NULL == new_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid name", K(ret), KP(name), KP(new_name));
  } else {
    FOREACH_CNT_X(n, get_names(), OB_ENTRY_NOT_EXIST == ret)
    {
      if (STRCMP(*n, name) == 0) {
        *n = new_name;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ColNames::append_name_list(common::ObSqlString& sql)
{

  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_names().count(); ++i) {
    if (OB_FAIL(sql.append_fmt(i > 0 ? ", %s" : "%s", get_names().at(i)))) {
      LOG_WARN("append name failed", K(ret), "name", get_names().at(i));
    }
  }
  return ret;
}

PartitionTableColNames::PartitionTableColNames()
{
  static const char* names[] = {
      "gmt_create",
      "gmt_modified",
      "tenant_id",
      "table_id",
      "partition_id",
      "svr_ip",
      "svr_port",
      "sql_port",
      "unit_id",
      "partition_cnt",
      "zone",
      "role",
      "member_list",
      "row_count",
      "data_size",
      "data_version",
      "data_checksum",
      "row_checksum",
      "column_checksum",
      "is_original_leader",
      "is_previous_leader",
      "create_time",
      "rebuild",
      "replica_type",
      "required_size",
      "status",
      "is_restore",
      "partition_checksum",
      "quorum",
      "fail_list",
      "recovery_timestamp",
      "memstore_percent",
      "data_file_id",
  };

  STATIC_ASSERT(sizeof(names) <= sizeof(col_names_), "column name array will overflow");
  MEMCPY(col_names_, names, sizeof(names));
  cols_.init(ARRAYSIZEOF(col_names_), col_names_, ARRAYSIZEOF(names));
}

}  // namespace partition_table
using namespace partition_table;

ObISQLClient& ObPartitionTableProxy::get_sql_client()
{
  return trans_.is_started() ? static_cast<ObISQLClient&>(trans_) : static_cast<ObISQLClient&>(sql_proxy_);
}

int ObPartitionTableProxy::start_trans()
{
  int ret = OB_SUCCESS;
  if (trans_.is_started()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transaction already started");
  } else if (OB_FAIL(trans_.start(&sql_proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  }
  return ret;
}

void ObPartitionTableProxy::set_merge_error_cb(ObIMergeErrorCb* cb)
{
  merge_error_cb_ = cb;
}

int ObPartitionTableProxy::end_trans(const int return_code)
{
  int ret = OB_SUCCESS;
  if (trans_.is_started()) {
    if (OB_FAIL(trans_.end(OB_SUCCESS == return_code))) {
      LOG_WARN("end transaction failed", K(ret), K(return_code));
    }
  }
  return ret;
}

template <typename RESULT, typename COLS>
int ObPartitionTableProxy::construct_partition_replica(RESULT& res, const COLS& cols, ObIAllocator* allocator,
    const bool ignore_row_checksum, ObPartitionReplica& replica, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator && !ignore_row_checksum) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null while ignore_row_checksum is false", K(allocator), K(ignore_row_checksum), K(ret));
  } else {
    ObString zone;
    ObString ip;
    ObString member_list;
    int64_t table_id = 0;
    int64_t port = 0;
    int64_t sql_port = 0;
    int64_t unit_id = 0;
    ObString column_checksum;
    ObString status_str;
    ObString default_status("REPLICA_STATUS_NORMAL");
    int64_t role = replica.role_;
    int64_t row_checksum = 0;
    int64_t is_original_leader = replica.is_original_leader_;
    int64_t is_previous_leader = 0;
    int64_t rebuild = 0;
    int64_t idx = 0;
    int64_t replica_type = 0;
    int64_t tenant_id = 0;
    int64_t create_time = 0;
    ObString fail_list;
    // column select order defined in PartitionTableColNames::PartitionTableColNames
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx + 2), tenant_id);
    {
      ObTimeZoneInfoWrap tz_info_wrap;
      ObTZMapWrap tz_map_wrap;
      OZ(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap));
      tz_info_wrap.set_tz_info_map(tz_map_wrap.get_tz_map());
      GET_COL_IGNORE_NULL(
          res.get_timestamp, cols.index(idx++), tz_info_wrap.get_time_zone_info(), replica.create_time_us_);
      GET_COL_IGNORE_NULL(
          res.get_timestamp, cols.index(idx++), tz_info_wrap.get_time_zone_info(), replica.modify_time_us_);
    }
    idx++;
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), table_id);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica.partition_id_);
    GET_COL_IGNORE_NULL(res.get_varchar, cols.index(idx++), ip);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), port);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), sql_port);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), unit_id);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica.partition_cnt_);
    GET_COL_IGNORE_NULL(res.get_varchar, cols.index(idx++), zone);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), role);
    GET_COL_IGNORE_NULL(res.get_varchar, cols.index(idx++), member_list);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica.row_count_);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica.data_size_);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica.data_version_);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica.data_checksum_);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), row_checksum);
    GET_COL_IGNORE_NULL(res.get_varchar, cols.index(idx++), column_checksum);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), is_original_leader);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), is_previous_leader);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), create_time);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), rebuild);
    GET_COL_IGNORE_NULL(res.get_int, cols.index(idx++), replica_type);
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), replica.required_size_, 0);
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_varchar, cols.index(idx++), status_str, default_status);
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), replica.is_restore_, 0);
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), replica.partition_checksum_, 0);
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), replica.quorum_, OB_INVALID_COUNT);
    GET_COL_IGNORE_NULL(res.get_varchar, cols.index(idx++), fail_list);
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), replica.recovery_timestamp_, 0);
    int64_t memstore_percent = 100;
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), memstore_percent, 100 /* default 100 */);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(replica.set_memstore_percent(memstore_percent))) {
      LOG_WARN("fail to set memstore percent", K(ret));
    }
    GET_COL_IGNORE_NULL_WITH_DEFAULT_VALUE(res.get_int, cols.index(idx++), replica.data_file_id_, 0);

    if (OB_SUCC(ret)) {
      replica.table_id_ = static_cast<uint64_t>(table_id);
      if (OB_FAIL(replica.zone_.assign(zone))) {
        LOG_WARN("replica.zone_ assign failed", K(zone), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      replica.role_ = static_cast<ObRole>(role);
      if (OB_FAIL(ObPartitionReplica::text2member_list(to_cstring(member_list), replica.member_list_))) {
        LOG_WARN("text2member_list failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      replica.reset_fail_list();
      if (need_fetch_faillist) {
        if (OB_FAIL(replica.deep_copy_faillist(fail_list))) {
          LOG_WARN("failed to deep copy str", K(ret), K(fail_list), K(replica));
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObReplicaStatus status;
      char buf[MAX_REPLICA_STATUS_LENGTH];
      int64_t pos = status_str.to_string(buf, MAX_REPLICA_STATUS_LENGTH);
      if (pos != status_str.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to write string to buf", K(ret), K(status_str));
      } else if (OB_FAIL(get_replica_status(buf, status))) {
        LOG_WARN("fail to get replica status", K(ret), K(status_str));
      } else {
        replica.status_ = status;
      }
    }

    if (OB_SUCC(ret)) {
      if (false == replica.server_.set_ip_addr(ip, static_cast<uint32_t>(port))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid server address", K(ip), K(port));
      } else {
        replica.sql_port_ = sql_port;
        replica.unit_id_ = static_cast<uint64_t>(unit_id);
      }
    }
    if (OB_SUCC(ret)) {
      replica.is_original_leader_ = is_original_leader;
      replica.to_leader_time_ = is_previous_leader;
      replica.rebuild_ = rebuild;
      replica.replica_type_ = (ObReplicaType)replica_type;
      if (ignore_row_checksum) {
        // do nothing
      } else {
        replica.row_checksum_.checksum_ = static_cast<uint64_t>(row_checksum);
        if (OB_FAIL(replica.row_checksum_.string2column_checksum(*allocator, to_cstring(column_checksum)))) {
          LOG_WARN("convert column checksum string to column checksum fail", K(ret), K(column_checksum));
        }
      }
    }
  }
  LOG_DEBUG("construct partition replica", K(ret), K(replica));
  return ret;
}

template <typename RESULT, typename COLS>
int ObPartitionTableProxy::construct_partition_info(RESULT& res, const COLS& cols, const bool filter_flag_replica,
    ObPartitionInfo& partition_info, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  ObPartitionReplica replica;
  bool is_checksum_error = false;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", K(ret));
      }
      break;
    }

    is_checksum_error = false;
    const bool ignore_row_checksum = false;
    replica.reuse();
    if (OB_FAIL(construct_partition_replica(
            res, cols, partition_info.get_allocator(), ignore_row_checksum, replica, need_fetch_faillist))) {
      LOG_WARN("construct_partition_replica failed", K(ret));
    } else if (filter_flag_replica && replica.is_flag_replica()) {
      // do nothing
    } else if (OB_FAIL(partition_info.add_replica_ignore_checksum_error(replica, is_checksum_error))) {
      LOG_WARN("partition add replica failed", K(ret), K(replica), K(partition_info));
    } else if (is_checksum_error) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(merge_error_cb_)) {
        // nothing todo
      } else if (OB_SUCCESS != (tmp_ret = merge_error_cb_->submit_merge_error_task())) {
        LOG_WARN("fail to report checksum error", K(tmp_ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(partition_info.update_replica_status())) {
      LOG_WARN("update partition replica status failed", K(ret), K(partition_info));
    }
  }

  return ret;
}

template <typename RESULT, typename COLS>
int ObPartitionTableProxy::construct_partition_infos(RESULT& res, const COLS& cols, const bool filter_flag_replica,
    int64_t& fetch_count, ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  ObPartitionInfo partition_info;
  fetch_count = 0;
  ObPartitionReplica replica;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  partition_info.set_allocator(&allocator);
  bool is_checksum_error = false;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", K(ret));
      }
      break;
    }

    ++fetch_count;
    replica.reuse();
    is_checksum_error = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_partition_replica(
                   res, cols, partition_info.get_allocator(), ignore_row_checksum, replica, need_fetch_faillist))) {
      LOG_WARN("construct_partition_replica failed", K(ret));
    } else if (OB_INVALID_ID == replica.table_id_ || OB_INVALID_INDEX == replica.partition_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_id or partition_id is invalid",
          "table_id",
          replica.table_id_,
          "partition_id",
          replica.partition_id_,
          K(ret));
    } else if (filter_flag_replica && replica.is_flag_replica()) {
      // do nothing
      LOG_DEBUG("filter flag replica", K(ret), K(replica));
    } else if (OB_INVALID_ID == partition_info.get_table_id() ||
               OB_INVALID_INDEX == partition_info.get_partition_id()) {
      partition_info.set_table_id(replica.table_id_);
      partition_info.set_partition_id(replica.partition_id_);
      if (OB_FAIL(partition_info.add_replica_ignore_checksum_error(replica, is_checksum_error))) {
        LOG_WARN("partition add replica failed", K(ret), K(replica), K(partition_info));
      }
    } else if (partition_info.get_table_id() == replica.table_id_ &&
               partition_info.get_partition_id() == replica.partition_id_) {
      if (OB_FAIL(partition_info.add_replica_ignore_checksum_error(replica, is_checksum_error))) {
        LOG_WARN("partition add replica failed", K(ret), K(replica), K(partition_info));
      }
    } else {
      // new partition info
      partition_info.reset_row_checksum();
      if (OB_FAIL(partition_info.update_replica_status())) {
        LOG_WARN("update replica status failed", K(ret), K(partition_info));
      } else if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push_back failed", K(ret));
      } else {
        partition_info.reuse();
        partition_info.set_table_id(replica.table_id_);
        partition_info.set_partition_id(replica.partition_id_);
        if (OB_FAIL(partition_info.add_replica_ignore_checksum_error(replica, is_checksum_error))) {
          LOG_WARN("partition add replica failed", K(partition_info), K(replica), K(ret));
        }
      }
    }
    if (is_checksum_error) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(merge_error_cb_)) {
        // nothing todo
      } else if (OB_SUCCESS != (tmp_ret = merge_error_cb_->submit_merge_error_task())) {
        LOG_WARN("fail to report checksum error", K(tmp_ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // last partition info
    if (OB_INVALID_ID != partition_info.get_table_id() && OB_INVALID_INDEX != partition_info.get_partition_id()) {
      if (OB_FAIL(partition_info.update_replica_status())) {
        LOG_WARN("update replica status failed", K(ret), K(partition_info));
      } else if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename RESULT, typename COLS>
int ObPartitionTableProxy::construct_partition_infos(
    RESULT& res, const COLS& cols, common::ObIAllocator& allocator, common::ObIArray<ObPartitionInfo*>& partition_infos)
{
  int ret = OB_SUCCESS;
  ObPartitionReplica replica;
  bool is_checksum_error = false;
  bool filter_flag_replica = true;
  bool ignore_row_checksum = true;
  bool need_fetch_faillist = false;
  ObPartitionInfo* partition = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(res.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next result failed", K(ret));
      }
      break;
    }
    replica.reuse();
    is_checksum_error = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_partition_replica(
                   res, cols, &allocator, ignore_row_checksum, replica, need_fetch_faillist))) {
      LOG_WARN("construct_partition_replica failed", K(ret));
    } else if (OB_INVALID_ID == replica.table_id_ || OB_INVALID_INDEX == replica.partition_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_id or partition_id is invalid", K(ret), K(replica));
    } else if (filter_flag_replica && replica.is_flag_replica()) {
      LOG_DEBUG("filter flag replica", K(ret), K(replica));
    } else {
      if (OB_ISNULL(partition) || partition->get_table_id() != replica.table_id_ ||
          partition->get_partition_id() != replica.partition_id_) {
        // new partition info
        if (OB_ISNULL(partition)) {
          // skip
        } else if (OB_FAIL(partition->update_replica_status())) {
          LOG_WARN("update replica status failed", K(ret), KPC(partition));
        } else if (OB_FAIL(partition_infos.push_back(partition))) {
          LOG_WARN("fail to push back partition", K(ret), KPC(partition));
        }
        if (FAILEDx(ObPartitionInfo::alloc_new_partition_info(allocator, partition))) {
          LOG_WARN("fail to alloc partition", K(ret), K(replica));
        } else {
          partition->set_table_id(replica.table_id_);
          partition->set_partition_id(replica.partition_id_);
        }
      }
      if (FAILEDx(partition->add_replica_ignore_checksum_error(replica, is_checksum_error))) {
        LOG_WARN("partition add replica failed", KPC(partition), K(replica), K(ret));
      }
    }
    if (is_checksum_error) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(merge_error_cb_)) {
        // nothing todo
      } else if (OB_SUCCESS != (tmp_ret = merge_error_cb_->submit_merge_error_task())) {
        LOG_WARN("fail to report checksum error", K(tmp_ret));
      }
    }
  }
  // last partition info
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(partition)) {
  } else if (OB_FAIL(partition->update_replica_status())) {
    LOG_WARN("update replica status failed", K(ret), KPC(partition));
  } else if (OB_FAIL(partition_infos.push_back(partition))) {
    LOG_WARN("push_back failed", K(ret), KPC(partition));
  }
  return ret;
}

// if replica.unit_id_ is a valid value, try to update unit_id in partition table
// only the leader need to update to_leader_time_.
int ObKVPartitionTableProxy::fill_dml_splicer_for_update(
    const ObPartitionReplica& replica, ObDMLSqlSplicer& dml_splicer)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char member_list[MAX_MEMBER_LIST_LENGTH] = "";
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (false == replica.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), "server", replica.server_);
  } else if (OB_FAIL(ObPartitionReplica::member_list2text(replica.member_list_, member_list, MAX_MEMBER_LIST_LENGTH))) {
    LOG_WARN("member_list2text failed", OBJ_K(replica, member_list), K(ret));
  } else {
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", extract_tenant_id(replica.table_id_))) ||
        OB_FAIL(dml_splicer.add_pk_column("table_id", replica.table_id_)) ||
        OB_FAIL(dml_splicer.add_pk_column(OBJ_K(replica, partition_id))) ||
        OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip)) ||
        OB_FAIL(dml_splicer.add_pk_column("svr_port", replica.server_.get_port())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, sql_port))) ||
        OB_FAIL(dml_splicer.add_column("zone", replica.zone_.ptr())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, partition_cnt))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, role))) ||
        OB_FAIL(dml_splicer.add_column("member_list", member_list)) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, row_count))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_size))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_version))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_checksum))) ||
        OB_FAIL(dml_splicer.add_column("row_checksum", static_cast<int64_t>(replica.row_checksum_.checksum_))) ||
        OB_FAIL(dml_splicer.add_column("replica_type", replica.replica_type_)) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, required_size))) ||
        OB_FAIL(dml_splicer.add_column("status", ob_replica_status_str(replica.status_))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, is_restore))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, partition_checksum))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, quorum))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, rebuild))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, recovery_timestamp))) ||
        OB_FAIL(dml_splicer.add_column("memstore_percent", replica.get_memstore_percent())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_file_id)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (!replica.fail_list_.empty()) {
      if (OB_FAIL(dml_splicer.add_column("fail_list", replica.fail_list_.ptr()))) {
        LOG_WARN("add column failed", K(ret), K(replica));
      }
    }
    if (OB_SUCC(ret) && OB_INVALID_ID != replica.unit_id_) {
      if (OB_FAIL(dml_splicer.add_column("unit_id", replica.unit_id_))) {
        LOG_WARN("add column fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && replica.is_leader_like()) {
      if (OB_FAIL(dml_splicer.add_column("is_previous_leader", replica.to_leader_time_))) {
        LOG_WARN("add column fail", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionTableProxy::handover_partition(
    const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  const int64_t trx_timeout_us = 60L * 1000L * 1000L;  // 60s
  if (!pg_key.is_valid() || !src_addr.is_valid() || !dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(src_addr), K(dest_addr));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(trx_timeout_us))) {
    LOG_WARN("fail to set transaction timeout", K(ret), K(trx_timeout_us));
  } else if (OB_FAIL(start_trans())) {
    LOG_WARN("start trans failed", K(ret), K(pg_key));
  } else {
    const uint64_t table_id = pg_key.get_table_id();
    const int64_t partition_id = pg_key.get_partition_id();
    if (OB_FAIL(remove(table_id, partition_id, src_addr))) {
      LOG_WARN("fail to remove src replica row", K(ret), K(pg_key), K(src_addr));
    } else if (OB_FAIL(update_replica_status(table_id, partition_id, dest_addr, REPLICA_STATUS_NORMAL))) {
      LOG_WARN("fail to update dest replica row", K(ret), K(pg_key), K(dest_addr));
    }
    int trans_ret = end_trans(ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("fail to end trans", K(trans_ret), K(pg_key));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }
  }
  return ret;
}

int ObPartitionTableProxy::replace_partition(
    const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  const ObPGKey pg_key = replica.partition_key();
  const int64_t trx_timeout_us = 60L * 1000L * 1000L;  // 60s
  if (!replica.is_valid() || !src_addr.is_valid() || !dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(replica), K(src_addr), K(dest_addr));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(trx_timeout_us))) {
    LOG_WARN("fail to set transaction timeout", K(ret), K(trx_timeout_us));
  } else if (OB_FAIL(start_trans())) {
    LOG_WARN("start trans failed", K(ret), K(pg_key));
  } else {
    const uint64_t table_id = pg_key.get_table_id();
    const int64_t partition_id = pg_key.get_partition_id();
    if (OB_FAIL(remove(table_id, partition_id, src_addr))) {
      LOG_WARN("fail to remove src replica row", K(ret), K(pg_key), K(src_addr));
    } else if (OB_FAIL(update_replica(replica, true /*replace*/))) {
      LOG_WARN("fail to update dest replica row", K(ret), K(pg_key), K(replica), K(dest_addr));
    }
    int trans_ret = end_trans(ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("fail to end trans", K(trans_ret), K(pg_key));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }
  }
  return ret;
}

int ObPartitionTableProxy::fill_dml_splicer(const ObPartitionReplica& replica, ObDMLSqlSplicer& dml_splicer)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char member_list[MAX_MEMBER_LIST_LENGTH] = "";
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (false == replica.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), "server", replica.server_);
  } else if (OB_FAIL(ObPartitionReplica::member_list2text(replica.member_list_, member_list, MAX_MEMBER_LIST_LENGTH))) {
    LOG_WARN("member_list2text failed", OBJ_K(replica, member_list), K(ret));
  } else {
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", extract_tenant_id(replica.table_id_))) ||
        OB_FAIL(dml_splicer.add_pk_column("table_id", replica.table_id_)) ||
        OB_FAIL(dml_splicer.add_pk_column(OBJ_K(replica, partition_id))) ||
        OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip)) ||
        OB_FAIL(dml_splicer.add_pk_column("svr_port", replica.server_.get_port())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, sql_port))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, unit_id))) ||
        OB_FAIL(dml_splicer.add_column("zone", replica.zone_.ptr())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, partition_cnt))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, role))) ||
        OB_FAIL(dml_splicer.add_column("member_list", member_list)) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, row_count))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_size))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_version))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_checksum))) ||
        OB_FAIL(dml_splicer.add_column("create_time", 0))  // DEPRECATED
        || OB_FAIL(dml_splicer.add_column("is_previous_leader", replica.to_leader_time_)) ||
        OB_FAIL(dml_splicer.add_column("row_checksum", static_cast<int64_t>(replica.row_checksum_.checksum_))) ||
        OB_FAIL(dml_splicer.add_column("column_checksum", "")) ||
        OB_FAIL(dml_splicer.add_column("replica_type", replica.replica_type_)) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, required_size))) ||
        OB_FAIL(dml_splicer.add_column("status", ob_replica_status_str(replica.status_))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, is_restore))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, partition_checksum))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, quorum))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, rebuild))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, recovery_timestamp))) ||
        OB_FAIL(dml_splicer.add_column("memstore_percent", replica.get_memstore_percent())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_file_id)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (!replica.fail_list_.empty()) {
      if (OB_FAIL(dml_splicer.add_column("fail_list", replica.fail_list_.ptr()))) {
        LOG_WARN("add column failed", K(ret), K(replica));
      }
    }
  }
  return ret;
}

int ObKVPartitionTableProxy::fetch_partition_info(const bool lock_replica, const uint64_t table_id,
    const int64_t partition_id, const bool filter_flag_replica, ObPartitionInfo& partition_info,
    const bool need_fetch_faillist, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!core_table_.is_valid() || OB_ALL_ROOT_TABLE_TID != extract_pure_id(table_id) || 0 != partition_id ||
      OB_INVALID_ID != cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cluster_id), K_(core_table), KT(table_id), K(partition_id));
  } else {
    partition_info.set_table_id(table_id);
    partition_info.set_partition_id(partition_id);
    KVPartitionTableCol cols;

    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(lock_replica ? core_table_.load_for_update() : core_table_.load())) {
      LOG_WARN("load core table failed", K(ret));
    } else if (OB_FAIL(construct_partition_info(
                   core_table_, cols, filter_flag_replica, partition_info, need_fetch_faillist))) {
      LOG_WARN("construct partition info failed", K(ret), K_(core_table));
    }
    // restore
    core_table_.set_sql_client(get_sql_client());
  }
  return ret;
}

int ObKVPartitionTableProxy::fetch_by_table_id(const uint64_t tenant_id, const uint64_t table_id,
    const int64_t partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
    ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)

{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  UNUSED(max_fetch_count);
  if (!core_table_.is_valid() || OB_ALL_ROOT_TABLE_TID != extract_pure_id(table_id) || 0 != partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition_info;
    partition_info.set_table_id(table_id);
    partition_info.set_partition_id(partition_id);
    partition_info.set_allocator(&allocator);
    KVPartitionTableCol cols;

    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(core_table_.load())) {
      LOG_WARN("load core table failed", K(ret));
    } else if (OB_FAIL(construct_partition_info(
                   core_table_, cols, filter_flag_replica, partition_info, need_fetch_faillist))) {
      LOG_WARN("construct partition info failed", K(ret), K_(core_table));
    } else if (OB_FAIL(partition_infos.push_back(partition_info))) {
      LOG_WARN("fail to push back partition inffo", K(ret), K(partition_info));
    }
    core_table_.set_sql_client(get_sql_client());
  }
  return ret;
}

int ObKVPartitionTableProxy::fetch_partition_infos(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
    ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum, bool specify_table_id,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  UNUSED(ignore_row_checksum);
  UNUSED(specify_table_id);
  UNUSED(need_fetch_faillist);
  if (!core_table_.is_valid() || OB_SYS_TENANT_ID != tenant_id || OB_INVALID_ID == start_table_id ||
      start_partition_id < 0 || max_fetch_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        K_(core_table),
        K(tenant_id),
        KT(start_table_id),
        K(start_partition_id),
        K(max_fetch_count));
  } else if (start_table_id > combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) ||
             (start_table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) &&
                 start_partition_id >= ObIPartitionTable::ALL_ROOT_TABLE_PARTITION_ID)) {
    // do nothing
  } else {
    const bool lock_replica = false;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID);
    const int64_t partition_id = 0;
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    ObPartitionInfo partition_info;
    partition_info.set_allocator(&allocator);
    if (OB_FAIL(fetch_partition_info(lock_replica, table_id, partition_id, filter_flag_replica, partition_info))) {
      LOG_WARN("fetch_partition_info failed",
          K(lock_replica),
          KT(table_id),
          K(filter_flag_replica),
          K(partition_id),
          K(ret));
    } else {
      if (partition_info.replica_count() > max_fetch_count) {
        // do nothing
      } else {
        partition_info.reset_row_checksum();
        if (OB_FAIL(partition_infos.push_back(partition_info))) {
          LOG_WARN("push_back failed", K(ret));
        } else {
          max_fetch_count -= partition_info.replica_count();
        }
      }
    }
  }
  return ret;
}

int ObKVPartitionTableProxy::fetch_partition_infos_pt(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t start_table_id, const int64_t start_partition_id, int64_t& max_fetch_count,
    ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!core_table_.is_valid() || pt_table_id != combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
      pt_partition_id != ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID || OB_INVALID_ID == start_table_id ||
      start_partition_id < 0 || max_fetch_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        K_(core_table),
        KT(pt_table_id),
        K(pt_partition_id),
        KT(start_table_id),
        K(start_partition_id),
        K(max_fetch_count));
  } else {
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    const bool filter_flag_replica = false;
    bool ignore_row_checksum = true;
    if (OB_FAIL(fetch_partition_infos(tenant_id,
            start_table_id,
            start_partition_id,
            filter_flag_replica,
            max_fetch_count,
            partition_infos,
            ignore_row_checksum,
            need_fetch_faillist))) {
      LOG_WARN("fetch_partition_infos failed",
          K(tenant_id),
          K(start_table_id),
          K(start_partition_id),
          K(filter_flag_replica),
          K(max_fetch_count),
          K(ret));
    }
  }
  return ret;
}

int ObKVPartitionTableProxy::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (1 != keys.count() || OB_INVALID_ID != cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid keys cnt", K(ret), K(cluster_id), "cnt", keys.count());
  } else {
    ObPartitionInfo* partition = NULL;
    const ObPartitionKey& key = keys.at(0);
    bool lock_replica = false;
    bool filter_flag_replica = true;
    bool need_fetch_faillist = false;
    if (OB_FAIL(ObPartitionInfo::alloc_new_partition_info(allocator, partition))) {
      LOG_WARN("fail to alloc partition", K(ret), K(key));
    } else if (OB_FAIL(fetch_partition_info(lock_replica,
                   key.get_table_id(),
                   key.get_partition_id(),
                   filter_flag_replica,
                   *partition,
                   need_fetch_faillist))) {
      LOG_WARN("fail to get partition", K(ret), K(key));
    } else if (OB_FAIL(partitions.push_back(partition))) {
      LOG_WARN("fail to push back partition", K(ret), K(key));
    }
  }
  return ret;
}

int ObKVPartitionTableProxy::update_replica(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  bool no_use = false;
  if (OB_ALL_ROOT_TABLE_TID != extract_pure_id(replica.table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id", K(replica.table_id_), "pure_id", extract_pure_id(replica.table_id_));
  } else {
    const bool filter_flag_replica = false;
    const bool lock_replica = true;
    ObPartitionInfo partition;
    ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
    partition.set_allocator(&allocator);
    if (OB_FAIL(fetch_partition_info(
            lock_replica, replica.table_id_, replica.partition_id_, filter_flag_replica, partition))) {
      LOG_WARN("get partition failed", K(ret), K_(replica.table_id), K_(replica.partition_id), K(filter_flag_replica));
    } else if (replica.is_leader_like()) {
      ObPartitionInfo::ReplicaArray& all_replicas = partition.get_replicas_v2();
      // replicas not in leader member
      for (int64_t i = 0; OB_SUCC(ret) && i < all_replicas.count(); ++i) {
        if (partition.is_leader_like(i) && all_replicas.at(i).server_ != replica.server_) {
          LOG_INFO("update replica role to FOLLOWER", "replica", all_replicas.at(i));
          if (OB_FAIL(set_to_follower_role(replica.table_id_, replica.partition_id_, all_replicas.at(i).server_))) {
            LOG_WARN("update replica role to follower failed", K(ret), "replica", all_replicas.at(i));
          } else {
            LOG_INFO("set privious leader to follower",
                "table_id",
                replica.table_id_,
                "old_leader",
                all_replicas.at(i).server_,
                "new_leader",
                replica.server_);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(update_replica(replica, no_use))) {
    LOG_WARN("fail to update replica", K(ret));
  }
  return ret;
}

int ObKVPartitionTableProxy::update_replica(const ObPartitionReplica& replica, const bool replace)
{
  UNUSED(replace);
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  if (!core_table_.is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), K(replica));
  } else if (!is_trans_started()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update replica should be called in transaction");
  } else if (OB_FAIL(fill_dml_splicer_for_update(replica, dml))) {
    LOG_WARN("fill dml splicer failed", K(ret), K(replica));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_column("modify_time_us", ObTimeUtility::current_time())) ||
        OB_FAIL(dml.add_column("is_original_leader", replica.is_original_leader_))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.replace_row(cells, affected_rows))) {
      LOG_WARN("update failed", K(ret), K(replica));
    } else {
      if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows");
      }
    }
  }

  return ret;
}

int ObKVPartitionTableProxy::batch_report_with_optimization(
    const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  UNUSED(replicas);
  UNUSED(with_role);
  return OB_NOT_SUPPORTED;
}

int ObKVPartitionTableProxy::batch_report_partition_role(
    const common::ObIArray<share::ObPartitionReplica>& pkey_array, const common::ObRole new_role)
{
  UNUSED(pkey_array);
  UNUSED(new_role);
  return OB_NOT_SUPPORTED;
}

int ObKVPartitionTableProxy::set_to_follower_role(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  const ObRole role = FOLLOWER;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id), K(server));
  } else if (!is_trans_started()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update replica role should be called in transaction");
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", server.get_port())) ||
        OB_FAIL(dml.add_column(K(role)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
      LOG_WARN("update role failed", K(ret), K(cells));
    } else {
      if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(affected_rows));
      }
    }
  }

  return ret;
}

int ObKVPartitionTableProxy::implicit_start_trans()
{
  int ret = OB_SUCCESS;
  if (trans_.is_started()) {
    implicit_trans_started_ = false;
  } else if (OB_FAIL(trans_.start(&sql_proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    implicit_trans_started_ = true;
    LOG_INFO("kv partition table start implicit transaction");
  }
  return ret;
}

int ObKVPartitionTableProxy::implicit_end_trans(const int return_code)
{
  int ret = OB_SUCCESS;
  if (trans_.is_started() && implicit_trans_started_) {
    if (OB_FAIL(trans_.end(OB_SUCCESS == return_code))) {
      LOG_WARN("end transaction failed", K(ret), K(return_code));
    } else {
      LOG_INFO("kv partition table implicit end trans success", K(ret), K(return_code));
    }
    implicit_trans_started_ = false;
  }
  return ret;
}

int ObKVPartitionTableProxy::load_for_update()
{
  int ret = OB_SUCCESS;
  if (!core_table_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table));
  } else if (!is_loaded_ && OB_FAIL(core_table_.load_for_update())) {
    LOG_WARN("load for update failed", K(ret));
  } else {
    is_loaded_ = true;
  }
  return ret;
}

int ObKVPartitionTableProxy::remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id), K(server));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else if (OB_FAIL(implicit_start_trans())) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(load_for_update())) {
    LOG_WARN("load for update failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", server.get_port()))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.delete_row(cells, affected_rows))) {
      LOG_WARN("delete replica failed", K(ret), K(cells));
    } else {
      // we may remove partition not exist in partition table, so ignore affected row check
      if (!is_single_row(affected_rows)) {
        LOG_WARN("expected deleted single row", K(affected_rows), K(cells));
      }
    }
  }

  int trans_ret = implicit_end_trans(ret);
  if (OB_SUCCESS != trans_ret) {
    LOG_WARN("end trans failed", K(trans_ret));
    ret = OB_SUCCESS == ret ? trans_ret : ret;
  }
  return ret;
}

int ObKVPartitionTableProxy::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid() ||
      OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id), K(server), K(unit_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else if (OB_FAIL(implicit_start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(load_for_update())) {
    LOG_WARN("load kv table for update failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", server.get_port())) ||
        OB_FAIL(dml.add_column(K(unit_id)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
      LOG_WARN("update unit_id failed", K(ret), K(cells));
    } else {
      if (!is_single_row(affected_rows)) {
        LOG_WARN("expected update single row", K(affected_rows), K(cells));
      }
    }
  }

  int trans_ret = implicit_end_trans(ret);
  if (OB_SUCCESS != trans_ret) {
    LOG_WARN("end trans failed", K(trans_ret));
    ret = OB_SUCCESS == ret ? trans_ret : ret;
  }
  return ret;
}

int ObKVPartitionTableProxy::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id), K(server));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else if (OB_FAIL(implicit_start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(load_for_update())) {
    LOG_WARN("load kv table for update failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", server.get_port())) ||
        OB_FAIL(dml.add_column(K(rebuild)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
      LOG_WARN("update rebuild flag failed", K(ret), K(cells));
    } else {
      if (affected_rows < 0 || affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected row", K(ret), K(affected_rows), K(cells));
      }
    }
  }

  int trans_ret = implicit_end_trans(ret);
  if (OB_SUCCESS != trans_ret) {
    LOG_WARN("end trans failed", K(trans_ret));
    ret = OB_SUCCESS == ret ? trans_ret : ret;
  }
  return ret;
}

int ObKVPartitionTableProxy::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  char* fail_list_str = NULL;
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id), K(server));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else if (NULL == (fail_list_str = static_cast<char*>(allocator.alloc(OB_MAX_FAILLIST_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator fail list", K(ret), K(OB_MAX_FAILLIST_LENGTH));
  } else if (OB_FAIL(ObPartitionReplica::fail_list2text(fail_list, fail_list_str, OB_MAX_FAILLIST_LENGTH))) {
    LOG_WARN("fail tran fail list to string", K(ret));
  } else if (OB_FAIL(implicit_start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(load_for_update())) {
    LOG_WARN("load kv table for update failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", server.get_port())) ||
        OB_FAIL(dml.add_column("fail_list", fail_list_str))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
      LOG_WARN("update rebuild flag failed", K(ret), K(cells));
    } else {
      if (affected_rows < 0 || affected_rows > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected row", K(ret), K(affected_rows), K(cells));
      }
    }
  }

  int trans_ret = implicit_end_trans(ret);
  if (OB_SUCCESS != trans_ret) {
    LOG_WARN("end trans failed", K(trans_ret));
    ret = OB_SUCCESS == ret ? trans_ret : ret;
  }
  return ret;
}

int ObKVPartitionTableProxy::update_replica_status(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const ObReplicaStatus status)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char* status_str = nullptr;
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid() || 0 > status ||
      REPLICA_STATUS_MAX == status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id), K(server), K(status));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else if (OB_ISNULL(status_str = ob_replica_status_str(status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get status str", K(ret), K(status));
  } else if (OB_FAIL(implicit_start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(load_for_update())) {
    LOG_WARN("load kv table for update failed", K(ret));
  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_pk_column("svr_ip", ip)) || OB_FAIL(dml.add_pk_column("svr_port", server.get_port())) ||
        OB_FAIL(dml.add_column("status", status_str))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
      LOG_WARN("update unit_id failed", K(ret), K(cells));
    } else {
      if (!is_single_row(affected_rows)) {
        LOG_WARN("expected update single row", K(affected_rows), K(cells));
      }
    }
  }

  int trans_ret = implicit_end_trans(ret);
  if (OB_SUCCESS != trans_ret) {
    LOG_WARN("end trans failed", K(trans_ret));
    ret = OB_SUCCESS == ret ? trans_ret : ret;
  }
  return ret;
}

int ObKVPartitionTableProxy::set_original_leader(
    const uint64_t table_id, const int64_t partition_id, const bool is_original_leader)
{
  int ret = OB_SUCCESS;
  if (!core_table_.is_valid() || OB_INVALID_ID == table_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K_(core_table), KT(table_id), K(partition_id));
  } else if (OB_FAIL(implicit_start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(load_for_update())) {
    LOG_WARN("load kv table for update failed", K(ret));

  } else {
    int64_t affected_rows = 0;
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    core_table_.set_sql_client(get_sql_client());
    ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
    if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
        OB_FAIL(dml.add_column("is_original_leader", 0))) {
      LOG_WARN("add column failed", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
      LOG_WARN("splice core table cells failed", K(ret));
    } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
      LOG_WARN("update original leader flag failed", K(ret), K(cells));
    } else {
      if (is_original_leader) {
        dml.reset();
        cells.reset();
        affected_rows = 0;
        if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(table_id))) ||
            OB_FAIL(dml.add_pk_column("table_id", table_id)) || OB_FAIL(dml.add_pk_column(K(partition_id))) ||
            OB_FAIL(dml.add_pk_column("role", LEADER)) || OB_FAIL(dml.add_column("is_original_leader", 1))) {
          LOG_WARN("add column failed", K(ret));
        } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
          LOG_WARN("splice core table cells failed", K(ret));
        } else if (OB_FAIL(core_table_.update_row(cells, affected_rows))) {
          LOG_WARN("update original leader flag failed", K(ret), K(cells));
        }
      }
    }
  }

  int trans_ret = implicit_end_trans(ret);
  if (OB_SUCCESS != trans_ret) {
    LOG_WARN("end trans failed", K(trans_ret));
    ret = OB_SUCCESS == ret ? trans_ret : ret;
  }
  return ret;
}

int ObNormalPartitionTableProxy::fetch_partition_info(const bool lock_replica, const uint64_t table_id,
    const int64_t partition_id, const bool filter_flag_replica, ObPartitionInfo& partition_info,
    const bool need_fetch_faillist, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else {
    partition_info.set_table_id(table_id);
    partition_info.set_partition_id(partition_id);

    NormalPartitionTableCol cols;
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result)
    {
      if (OB_FAIL(sql.assign("SELECT *"))) {
        LOG_WARN("assign sql failed", K(ret));
      } else if (OB_FAIL(sql.append_fmt(" FROM %s WHERE tenant_id = %lu AND table_id = %lu AND partition_id = %lu%s"
                                        " ORDER BY tenant_id, table_id, partition_id, svr_ip, svr_port",
                     table_name,
                     extract_tenant_id(table_id),
                     table_id,
                     partition_id,
                     lock_replica ? " FOR UPDATE" : ""))) {
        LOG_WARN("assign sql string failed", K(ret));
      } else if (OB_FAIL(get_sql_client().read(result, cluster_id, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(cluster_id), "sql", sql.ptr());
      } else if (NULL == result.get_result()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed");
      } else if (OB_FAIL(construct_partition_info(
                     *result.get_result(), cols, filter_flag_replica, partition_info, need_fetch_faillist))) {
        LOG_WARN("construct partition info failed", K(ret));
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::fetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
    ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  bool specify_table_id = true;
  bool ignore_row_checksum = true;
  if (OB_FAIL(fetch_partition_infos(tenant_id,
          start_table_id,
          start_partition_id,
          filter_flag_replica,
          max_fetch_count,
          partition_infos,
          ignore_row_checksum,
          specify_table_id,
          need_fetch_faillist))) {
    LOG_WARN("fail to getch partition_infos", K(ret));
  }
  return ret;
}

int ObNormalPartitionTableProxy::fetch_partition_infos(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, const bool filter_flag_replica, int64_t& max_fetch_count,
    ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum, bool specify_table_id,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  ObSqlString specified_tenant_str;
  ObSqlString specified_partition_str;  // empty here
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == start_table_id || start_partition_id < 0 || max_fetch_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id), KT(start_table_id), K(start_partition_id), K(max_fetch_count));
  } else if (OB_FAIL(init_empty_string(specified_partition_str))) {
    LOG_WARN("init_empty_string failed", K(ret));
  } else if (OB_FAIL(specified_tenant_str.append_fmt("tenant_id = %lu", tenant_id))) {
    LOG_WARN("fail to appent fmt", K(ret));
  } else {
    static const int PT_ARRAY_SIZE = 2;
    const char* pt_names[PT_ARRAY_SIZE];
    pt_names[0] = OB_ALL_ROOT_TABLE_TNAME;
    pt_names[1] = OB_ALL_TENANT_META_TABLE_TNAME;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(pt_names); ++i) {
      int64_t fetch_count = 0;
      if (extract_pure_id(start_table_id) > OB_MIN_USER_TABLE_ID && 0 == STRCMP(pt_names[i], OB_ALL_ROOT_TABLE_TNAME)) {
        continue;
      } else if (specify_table_id && is_sys_table(start_table_id) &&
                 0 != STRCMP(pt_names[i], OB_ALL_ROOT_TABLE_TNAME)) {
        // for prefetch_partition_by_table_id:
        // 1. sys table schemas exist on standby cluster, but partitions not exist,
        //    __all_tenant_meta_table cannot by accessed,ignore;
        // 2. sys table schemas and partitions exist on primary cluster,
        //    no need to access __all_tenant_meta_table, ignore.
        continue;
      } else {
        uint64_t sql_tenant_id =
            0 == STRCMP(pt_names[i], OB_ALL_TENANT_META_TABLE_TNAME) ? tenant_id : OB_SYS_TENANT_ID;
        if (OB_FAIL(fetch_partition_infos_impl(pt_names[i],
                start_table_id,
                start_partition_id,
                filter_flag_replica,
                max_fetch_count,
                specified_tenant_str,
                specified_partition_str,
                fetch_count,
                partition_infos,
                sql_tenant_id,
                ignore_row_checksum,
                specify_table_id,
                need_fetch_faillist))) {
          LOG_WARN("fetch partition infos impl failed",
              "pt_name",
              pt_names[i],
              KT(start_table_id),
              K(start_partition_id),
              K(filter_flag_replica),
              K(max_fetch_count),
              K(specified_tenant_str),
              K(specified_partition_str),
              K(fetch_count),
              K(sql_tenant_id),
              K(ret));
        } else {
          max_fetch_count -= fetch_count;
          if (0 == max_fetch_count) {
            if (partition_infos.count() <= 2) {
              LOG_WARN("may max fetch count too small, you may get uncomplete partition",
                  K(max_fetch_count),
                  K(fetch_count),
                  "partition_count",
                  partition_infos.count());
              break;
            } else {
              // last partition info may be not complete, trim it
              partition_infos.pop_back();
              break;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObNormalPartitionTableProxy::fetch_partition_infos_pt(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t start_table_id, const int64_t start_partition_id, int64_t& max_fetch_count,
    common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  int64_t fetch_count = 0;
  ObSqlString specified_tenant_str;  // empty here
  ObSqlString specified_partition_str;
  const bool filter_flag_replica = false;
  uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
  if (OB_INVALID_ID == pt_table_id || pt_partition_id < 0 || OB_INVALID_ID == start_table_id ||
      start_partition_id < 0 || max_fetch_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        K(pt_table_id),
        K(pt_partition_id),
        K(start_table_id),
        K(start_partition_id),
        K(max_fetch_count));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) != pt_table_id &&
             OB_ALL_TENANT_META_TABLE_TID != extract_pure_id(pt_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id", K(pt_table_id), K(ret));
  } else if (OB_FAIL(ObIPartitionTable::partition_table_id_to_name(pt_table_id, table_name))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id", K(pt_table_id), K(ret));
  } else if (NULL == table_name) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name", K(ret), K(pt_table_id));
  } else if (OB_FAIL(specified_tenant_str.assign_fmt("tenant_id >= %lu", extract_tenant_id(start_table_id)))) {
    LOG_WARN("assign_fmt failed", K(ret));
  } else {
    sql_tenant_id =
        (0 == STRCMP(table_name, OB_ALL_TENANT_META_TABLE_TNAME)) ? extract_tenant_id(pt_table_id) : OB_SYS_TENANT_ID;
    // in mysql, non partitioned table don't support syntax "partition (p?)"
    if (OB_ALL_ROOT_TABLE_TID == extract_pure_id(pt_table_id) ||
        OB_ALL_TENANT_META_TABLE_TID == extract_pure_id(pt_table_id)) {
      if (OB_FAIL(init_empty_string(specified_partition_str))) {
        LOG_WARN("init_empty_string failed", K(ret));
      }
    } else {
      if (OB_FAIL(specified_partition_str.assign_fmt("partition (p%ld)", pt_partition_id))) {
        LOG_WARN("assign_fmt failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(fetch_partition_infos_impl(table_name,
                 start_table_id,
                 start_partition_id,
                 filter_flag_replica,
                 max_fetch_count,
                 specified_tenant_str,
                 specified_partition_str,
                 fetch_count,
                 partition_infos,
                 sql_tenant_id,
                 true,
                 need_fetch_faillist))) {
    LOG_WARN("fetch partition infos impl failed",
        K(table_name),
        KT(start_table_id),
        K(start_partition_id),
        K(filter_flag_replica),
        K(max_fetch_count),
        K(specified_tenant_str),
        K(specified_partition_str),
        K(fetch_count),
        K(sql_tenant_id),
        K(ret));
  } else {
    max_fetch_count -= fetch_count;
    if (0 == max_fetch_count && partition_infos.count() > 0) {
      // last partition info may be not complete, trim it
      partition_infos.pop_back();
    }
  }
  return ret;
}

bool ObNormalPartitionTableProxy::ObPartitionKeyCompare::operator()(
    const ObPartitionInfo* left, const common::ObPartitionKey& right)
{
  return left->get_table_id() < right.get_table_id() ||
         (left->get_table_id() == right.get_table_id() && left->get_partition_id() < right.get_partition_id());
}

int ObNormalPartitionTableProxy::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (keys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key cnt", K(ret));
  } else {
    const char* table_name = NULL;
    uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
    if (OB_FAIL(partition_table_name(keys.at(0).get_table_id(), table_name, sql_tenant_id))) {
      LOG_WARN("get partition table name failed", K(ret), "table_id", keys.at(0).get_table_id());
    } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "NULL table_name or invalid sql_tenant_id", K(ret), "table_id", keys.at(0).get_table_id(), K(sql_tenant_id));
    } else {
      NormalPartitionTableCol cols;
      ObSqlString sql;
      SMART_VAR(ObISQLClient::ReadResult, result)
      {
        if (OB_FAIL(sql.append_fmt(" SELECT * FROM %s WHERE (tenant_id, table_id, partition_id) IN (", table_name))) {
          LOG_WARN("fail to assign sql", K(ret));
        } else {
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          uint64_t table_id = OB_INVALID_ID;
          int64_t partition_id = OB_INVALID_ID;
          for (int i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
            const ObPartitionKey& key = keys.at(i);
            tenant_id = key.get_tenant_id();
            table_id = key.get_table_id();
            partition_id = key.get_partition_id();
            if (!key.is_valid()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid key", K(ret), K(key));
            } else if (OB_ALL_CORE_TABLE_TID == extract_pure_id(table_id) ||
                       OB_ALL_ROOT_TABLE_TID == extract_pure_id(table_id)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid key", K(ret), K(key));
            } else if (0 == i) {
            } else if (is_sys_table(table_id) != is_sys_table(keys.at(i - 1).get_table_id())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid batch", K(ret), K(key), K(keys.at(i - 1)));
            } else if (!is_sys_table(table_id) && tenant_id != keys.at(i - 1).get_tenant_id()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid batch", K(ret), K(key), K(keys.at(i - 1)));
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(sql.append_fmt(
                           "%s (%ld, %ld, %ld)", 0 == i ? "" : ",", tenant_id, table_id, partition_id))) {
              LOG_WARN("fail to assign sql", K(ret), K(key));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(sql.append_fmt(" ) ORDER BY tenant_id, table_id, partition_id, svr_ip, svr_port"))) {
          LOG_WARN("assign sql string failed", K(ret));
        } else if (OB_FAIL(get_sql_client().read(result, cluster_id, sql_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", K(ret), K(cluster_id), "sql", sql.ptr());
        } else if (NULL == result.get_result()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed");
        } else if (OB_FAIL(construct_partition_infos(*result.get_result(), cols, allocator, partitions))) {
          LOG_WARN("batch construct partition info failed", K(ret));
        } else if (partitions.count() != keys.count()) {
          LOG_INFO("partitions'cnt not match with keys's, should mock",
              "partition_cnt",
              partitions.count(),
              "key_cnt",
              keys.count());
          ObArray<ObPartitionKey> mock_keys;
          ObPartitionKeyCompare comp;
          for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
            const ObPartitionKey& key = keys.at(i);
            common::ObArray<ObPartitionInfo*>::iterator iter =
                std::lower_bound(partitions.begin(), partitions.end(), key, comp);
            if (iter == partitions.end() || (*iter)->get_table_id() != key.get_table_id() ||
                (*iter)->get_partition_id() != key.get_partition_id()) {
              if (OB_FAIL(mock_keys.push_back(key))) {
                LOG_WARN("fail to push back key", K(ret), K(key));
              }
            }
          }
          if (OB_SUCC(ret) && partitions.count() + mock_keys.count() != keys.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cnt not match",
                K(ret),
                "partition_cnt",
                partitions.count(),
                "mock_key_cnt",
                mock_keys.count(),
                "key_cnt",
                keys.count());
            for (int64_t i = 0; i < partitions.count(); i++) {
              if (OB_ISNULL(partitions.at(i))) {
                LOG_INFO("partition is null", K(i));
              } else {
                LOG_INFO("dump partititon",
                    "table_id",
                    partitions.at(i)->get_table_id(),
                    "partition_id",
                    partitions.at(i)->get_partition_id());
              }
            }
            for (int64_t i = 0; i < mock_keys.count(); i++) {
              LOG_INFO("dump mock key", "key", mock_keys.at(i));
            }
            for (int64_t i = 0; i < keys.count(); i++) {
              LOG_INFO("dump key", "key", keys.at(i));
            }
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < mock_keys.count(); i++) {
            ObPartitionInfo* partition = NULL;
            ObPartitionKey& key = mock_keys.at(i);
            if (OB_FAIL(ObPartitionInfo::alloc_new_partition_info(allocator, partition))) {
              LOG_WARN("fail to alloc partition", K(ret), K(key));
            } else {
              partition->set_table_id(key.get_table_id());
              partition->set_partition_id(key.get_partition_id());
              if (OB_FAIL(partitions.push_back(partition))) {
                LOG_WARN("fail to push back partition", K(ret), KPC(partition));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::fill_dml_splicer_for_update(
    const ObPartitionReplica& replica, ObDMLSqlSplicer& dml_splicer)
{
  int ret = OB_SUCCESS;
  char member_list[MAX_MEMBER_LIST_LENGTH] = "";
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (false == replica.server_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), "server", replica.server_);
  } else if (OB_FAIL(ObPartitionReplica::member_list2text(replica.member_list_, member_list, MAX_MEMBER_LIST_LENGTH))) {
    LOG_WARN("member_list2text failed", OBJ_K(replica, member_list), K(ret));
  } else {
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", extract_tenant_id(replica.table_id_))) ||
        OB_FAIL(dml_splicer.add_pk_column("table_id", replica.table_id_)) ||
        OB_FAIL(dml_splicer.add_pk_column(OBJ_K(replica, partition_id))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, sql_port))) ||
        OB_FAIL(dml_splicer.add_column("zone", replica.zone_.ptr())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, partition_cnt))) ||
        OB_FAIL(dml_splicer.add_column("member_list", member_list)) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, row_count))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_size))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_version))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_checksum))) ||
        OB_FAIL(dml_splicer.add_column("row_checksum", static_cast<int64_t>(replica.row_checksum_.checksum_))) ||
        OB_FAIL(dml_splicer.add_column("column_checksum", "")) ||
        OB_FAIL(dml_splicer.add_column("replica_type", replica.replica_type_)) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, required_size))) ||
        OB_FAIL(dml_splicer.add_column("status", ob_replica_status_str(replica.status_))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, is_restore))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, partition_checksum))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, quorum))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, rebuild))) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, recovery_timestamp))) ||
        OB_FAIL(dml_splicer.add_column("memstore_percent", replica.get_memstore_percent())) ||
        OB_FAIL(dml_splicer.add_column(OBJ_K(replica, data_file_id)))) {
      LOG_WARN("add column failed", K(ret));
    } else if (!replica.fail_list_.empty()) {
      if (OB_FAIL(dml_splicer.add_column("fail_list", replica.fail_list_.ptr()))) {
        LOG_WARN("add column failed", K(ret), K(replica));
      }
    }
    if (OB_SUCC(ret) && replica.is_leader_like()) {
      if (OB_FAIL(dml_splicer.add_column("is_previous_leader", replica.to_leader_time_))) {
        LOG_WARN("add column fail", K(ret));
      }
    }
    if (OB_SUCC(ret) && replica.is_follower()) {
      if (OB_FAIL(dml_splicer.add_pk_column("svr_ip", ip)) ||
          OB_FAIL(dml_splicer.add_pk_column("svr_port", replica.server_.get_port())) ||
          OB_FAIL(dml_splicer.add_column(OBJ_K(replica, role)))) {
        LOG_WARN("fail to add column", K(ret));
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::update_replica(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  ObPTSqlSplicer dml;
  int64_t affected_rows = 0;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (OB_FAIL(partition_table_name(replica.table_id_, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), "table_id", replica.table_id_);
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), "table_id", replica.table_id_, K(sql_tenant_id));
  } else {
    ObNormalPartitionUpdateHelper exec(get_sql_client(), sql_tenant_id);
    bool is_inner_trans = false;
    if (replica.is_leader_like() && !is_trans_started()) {
      is_inner_trans = true;
      if (OB_FAIL(start_trans())) {
        LOG_WARN("start transaction failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_dml_splicer(replica, dml))) {
        LOG_WARN("fill dml splicer failed", K(ret), K(replica));
      } else if (OB_FAIL(exec.exec_insert_update_replica(table_name, dml, affected_rows))) {
        LOG_WARN("fail to exec update", K(ret), K(table_name), K(replica));
      } else if (replica.is_leader_like()) {
        dml.reset();
        if (OB_FAIL(fill_dml_splicer_for_update(replica, dml))) {
          LOG_WARN("fill dml splicer failed", K(ret), K(replica));
        } else if (OB_FAIL(exec.exec_update_leader_replica(replica, table_name, dml, affected_rows))) {
          LOG_WARN("fail to exec update", K(ret), K(table_name), K(replica));
        }
      }
    }
    if (is_inner_trans && is_trans_started()) {
      int trans_ret = end_trans(ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end trans failed", K(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::generate_batch_report_partition_role_sql(const char* table_name,
    const common::ObIArray<share::ObPartitionReplica>& replica_array, const common::ObRole new_role,
    common::ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(nullptr == table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(table_name));
  } else if (OB_UNLIKELY(INVALID_ROLE == new_role)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_role));
  } else if (OB_UNLIKELY(!GCTX.self_addr_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("self addr invalid", KR(ret), "self", GCTX.self_addr_);
  } else if (!GCTX.self_addr_.ip_to_string(ip, sizeof(ip))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to convert ip to string", KR(ret), "self", GCTX.self_addr_);
  } else {
    if (FOLLOWER == new_role) {
      // construct the follower updating clause
      if (OB_FAIL(sql.append_fmt("UPDATE %s SET role=%d WHERE (svr_ip,svr_port) IN (('%s',%d)) "
                                 "AND (tenant_id,table_id,partition_id) IN ",
              table_name,
              FOLLOWER,
              ip,
              GCTX.self_addr_.get_port()))) {
        LOG_WARN("fail to append format", KR(ret));
      }
    } else {
      // construct the leader updating clause
      if (OB_FAIL(sql.append_fmt("UPDATE %s SET role = CASE WHEN "
                                 "(svr_ip='%s' and svr_port=%d) THEN %d ELSE %d END, is_previous_leader = CASE ",
              table_name,
              ip,
              GCTX.self_addr_.get_port(),
              new_role,
              FOLLOWER))) {
        LOG_WARN("fail to append format", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
          const share::ObPartitionReplica& replica = replica_array.at(i);
          if (OB_FAIL(sql.append_fmt(
                  "WHEN (svr_ip='%s' and svr_port=%d AND tenant_id=%ld AND table_id=%ld AND partition_id=%ld) "
                  "THEN %ld %s",
                  ip,
                  GCTX.self_addr_.get_port(),
                  extract_tenant_id(replica.get_table_id()),
                  replica.get_table_id(),
                  replica.get_partition_id(),
                  replica.to_leader_time_,
                  (replica_array.count() - 1 == i
                          ? "ELSE is_previous_leader END WHERE (tenant_id,table_id,partition_id) IN "
                          : "")))) {
            LOG_WARN("fail to append fmt", KR(ret));
          }
        }
      }
    }
    // compensate the select condition for update
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
        const share::ObPartitionReplica& replica = replica_array.at(i);
        if (OB_FAIL(sql.append_fmt("%s(%ld,%ld,%ld)%s",
                (0 == i ? "(" : ","),
                extract_tenant_id(replica.get_table_id()),
                replica.get_table_id(),
                replica.get_partition_id(),
                (replica_array.count() - 1 == i ? ")" : "")))) {
          LOG_WARN("fail to append fmt", KR(ret), K(replica));
        }
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::batch_report_partition_role(
    const common::ObIArray<share::ObPartitionReplica>& replica_array, const common::ObRole new_role)
{
  int ret = OB_SUCCESS;
  const char* table_name = nullptr;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (replica_array.count() <= 0) {
    // bypass
  } else if (OB_UNLIKELY(INVALID_ROLE == new_role)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_role));
  } else if (FALSE_IT(table_id = replica_array.at(0).get_table_id())) {
    // shall never be here
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("fail to get partition table name", KR(ret), K(table_id));
  } else if (OB_UNLIKELY(nullptr == table_name || OB_INVALID_TENANT_ID == sql_tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_name or sql tenant_id unexpected", KR(ret), KP(table_name), K(sql_tenant_id));
  } else if (OB_FAIL(generate_batch_report_partition_role_sql(table_name, replica_array, new_role, sql))) {
    LOG_WARN("fail to generate batch report partition role sql", KR(ret));
  } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql_tenant_id), K(sql));
  }
  return ret;
}

int ObNormalPartitionTableProxy::batch_report_with_optimization(
    const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  ObPTSqlSplicer dml;
  int64_t affected_rows = 0;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (replicas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cnt", K(ret), "cnt", replicas.count());
  } else if (OB_FAIL(partition_table_name(replicas.at(0).table_id_, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(sql_tenant_id));
  } else {
    ObNormalPartitionUpdateHelper exec(get_sql_client(), sql_tenant_id);
    FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
    {
      if (OB_ISNULL(replica)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid replica", K(ret), KPC(replica));
      } else if (OB_ALL_CORE_TABLE_TID == extract_pure_id(replica->table_id_) ||
                 OB_ALL_ROOT_TABLE_TID == extract_pure_id(replica->table_id_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table_id", K(ret), KPC(replica));
      } else if ((replica->is_leader_like() && (with_role || replica->need_force_full_report())) ||
                 replica->is_remove_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("replica status invalid", K(ret), KPC(replica), K(with_role));
      } else if (OB_FAIL(fill_dml_splicer(*replica, dml))) {
        LOG_WARN("fill dml splicer failed", K(ret), KPC(replica));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_batch_insert_update_replica(table_name, with_role, dml, affected_rows))) {
        LOG_WARN("fail to exec update", K(ret), K(table_name));
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::update_replica(const ObPartitionReplica& replica, const bool replace)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica", K(ret), K(replica));
  } else if (OB_FAIL(partition_table_name(replica.table_id_, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), "table_id", replica.table_id_);
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), "table_id", replica.table_id_, K(sql_tenant_id));
  } else if (OB_FAIL(fill_dml_splicer(replica, dml))) {
    LOG_WARN("fill dml splicer failed", K(ret), K(replica));
  } else {
    ObDMLExecHelper exec(get_sql_client(), sql_tenant_id);
    if (OB_FAIL(dml.add_gmt_modified())) {
      LOG_WARN("add gmt modified to dml sql failed", K(ret));
    } else if (OB_FAIL((exec.*(replace ? &ObDMLExecHelper::exec_insert_update : &ObDMLExecHelper::exec_insert_ignore))(
                   table_name, dml, affected_rows))) {
      LOG_WARN("execute update failed", K(ret), K(replace), K(replica));
    } else {
      // only insert on duplicate key update (replace) check affected single row
      if (replace && (is_zero_row(affected_rows) || affected_rows > 2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", K(affected_rows));
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::set_to_follower_role(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  const ObRole role = FOLLOWER;
  const char* table_name = NULL;
  int64_t affected_rows = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else if (OB_FAIL(
                 sql.assign_fmt("UPDATE %s SET role = %d, "
                                "gmt_modified = now(6) WHERE tenant_id = %lu AND table_id = %lu AND partition_id = %ld "
                                "AND svr_ip = '%s' AND svr_port = %d",
                     table_name,
                     role,
                     extract_tenant_id(table_id),
                     table_id,
                     partition_id,
                     ip,
                     server.get_port()))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), "sql", sql.ptr());
  } else {
    if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected_rows", K(affected_rows));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::remove(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  int64_t affected_rows = 0;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObSqlString sql;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret), K(server));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu AND table_id = %lu AND partition_id = %ld"
                                    " AND svr_ip = '%s' AND svr_port = %d",
                 table_name,
                 extract_tenant_id(table_id),
                 table_id,
                 partition_id,
                 ip,
                 server.get_port()))) {
    LOG_WARN("assign sql string failed", K(ret));
  } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), "sql", sql.ptr());
  } else {
    // we may remove partition not exist in partition table, so ignore affected row check
    if (!is_single_row(affected_rows)) {
      LOG_WARN("expected deleted single row", K(affected_rows), K(sql));
    } else {
      LOG_INFO("delete row from partition table", K(sql));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server), K(unit_id));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt(
            "UPDATE %s SET unit_id = %lu WHERE "
            "tenant_id = %lu AND table_id = %lu AND partition_id = %ld AND svr_ip = '%s' AND svr_port = %d",
            table_name,
            unit_id,
            extract_tenant_id(table_id),
            table_id,
            partition_id,
            ip,
            server.get_port()))) {
      LOG_WARN("sql assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (!is_single_row(affected_rows)) {
      LOG_WARN("expected update single row", K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET rebuild = %lu WHERE tenant_id = %lu "
                               "AND table_id = %lu AND partition_id = %ld AND svr_ip = '%s' AND svr_port = %d",
            table_name,
            rebuild ? 1L : 0L,
            extract_tenant_id(table_id),
            table_id,
            partition_id,
            ip,
            server.get_port()))) {
      LOG_WARN("sql assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (affected_rows < 0 || affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affect row", K(ret), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  char* fail_list_str = NULL;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else if (NULL == (fail_list_str = static_cast<char*>(allocator.alloc(OB_MAX_FAILLIST_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator fail list", K(ret), K(OB_MAX_FAILLIST_LENGTH));
  } else if (OB_FAIL(ObPartitionReplica::fail_list2text(fail_list, fail_list_str, OB_MAX_FAILLIST_LENGTH))) {
    LOG_WARN("fail tran fail list to string", K(ret), K(fail_list));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET fail_list = '%s' WHERE tenant_id = %lu"
                               " AND table_id = %lu AND partition_id = %ld AND svr_ip = '%s' AND svr_port = %d",
            table_name,
            fail_list_str,
            extract_tenant_id(table_id),
            table_id,
            partition_id,
            ip,
            server.get_port()))) {
      LOG_WARN("sql assign_fmt failed", K(ret));
    } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (affected_rows < 0 || affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affect row", K(ret), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::update_replica_status(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const ObReplicaStatus status)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  const char* status_str = nullptr;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0 || !server.is_valid() || 0 > status ||
      REPLICA_STATUS_MAX == status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server), K(status));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (false == server.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server ip to string failed", K(ret));
  } else if (OB_ISNULL(status_str = ob_replica_status_str(status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get status str", K(ret), K(status));
  } else {
    ObSqlString sql;
    int64_t affected_rows = 0;
    const char* sql_template = "UPDATE %s SET status = '%s' WHERE tenant_id = %lu "
                               "AND table_id = %lu AND partition_id = %ld AND svr_ip = '%s' AND svr_port = %d";
    if (OB_FAIL(sql.assign_fmt(sql_template,
            table_name,
            status_str,
            extract_tenant_id(table_id),
            table_id,
            partition_id,
            ip,
            server.get_port()))) {
      LOG_WARN("sql assign_fmt failed", K(ret), K(table_id), K(partition_id));
    } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (affected_rows < 0 || affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affect row", K(ret), K(affected_rows), K(sql));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::set_original_leader(
    const uint64_t table_id, const int64_t partition_id, const bool is_original_leader)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id || partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (OB_FAIL(partition_table_name(table_id, table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed", K(ret), K(table_id));
  } else if (NULL == table_name || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(table_id), K(sql_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET is_original_leader = (role = %d AND 1 = %ld), gmt_modified = now(6) "
                                    "WHERE tenant_id = %lu AND table_id = %lu AND partition_id = %ld",
                 table_name,
                 LEADER,
                 static_cast<int64_t>(is_original_leader),
                 extract_tenant_id(table_id),
                 table_id,
                 partition_id))) {
    LOG_WARN("assign sql failed", K(ret));
  } else if (OB_FAIL(get_sql_client().write(sql_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), "sql", sql.ptr());
  } else {
    if (affected_rows <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected_rows", K(affected_rows));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::init_empty_string(ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql_string.assign(""))) {
    LOG_WARN("assign failed", K(ret));
  }
  return ret;
}

int ObNormalPartitionTableProxy::fetch_partition_infos_impl(const char* pt_name, const uint64_t start_table_id,
    const int64_t start_partition_id, const bool filter_flag_replica, const int64_t max_fetch_count,
    const ObSqlString& specified_tenant_str, const ObSqlString& specified_partition_str, int64_t& fetch_count,
    ObIArray<ObPartitionInfo>& partition_infos, uint64_t sql_tenant_id, bool ignore_row_checksum, bool specify_table_id,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (NULL == pt_name ||
      (0 != STRCMP(pt_name, OB_ALL_ROOT_TABLE_TNAME) && 0 != STRCMP(pt_name, OB_ALL_TENANT_META_TABLE_TNAME))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_name", K(ret), K(pt_name), K(ret));
  } else if (OB_INVALID_ID == start_table_id || start_partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", KT(start_table_id), K(start_partition_id), K(ret));
  } else {
    NormalPartitionTableCol cols;
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result)
    {
      fetch_count = 0;
      if (OB_FAIL(sql.assign("SELECT *"))) {
        LOG_WARN("assign sql failed", K(ret));
      } else if (!specify_table_id &&
                 OB_FAIL(sql.append_fmt(" FROM %s %s%sWHERE %s%s"
                                        "((tenant_id, table_id, partition_id) > (%lu, %lu, %ld)) "
                                        "ORDER BY TENANT_ID, TABLE_ID, PARTITION_ID, SVR_IP, SVR_PORT LIMIT %ld",
                     pt_name,
                     specified_partition_str.ptr(),
                     (specified_partition_str.empty() ? "" : " "),
                     (0 == STRCMP(pt_name, OB_ALL_TENANT_META_TABLE_TNAME) ? "" : specified_tenant_str.ptr()),
                     (0 == STRCMP(pt_name, OB_ALL_TENANT_META_TABLE_TNAME) || specified_tenant_str.empty() ? ""
                                                                                                           : " and "),
                     extract_tenant_id(start_table_id),
                     start_table_id,
                     start_partition_id,
                     max_fetch_count))) {
        LOG_WARN("assign sql string failed", K(ret));
      } else if (specify_table_id &&
                 OB_FAIL(sql.append_fmt(" FROM %s %s%sWHERE %s%s"
                                        " table_id = %lu and partition_id >= %ld "
                                        "ORDER BY TENANT_ID, TABLE_ID, PARTITION_ID, SVR_IP, SVR_PORT LIMIT %ld",
                     pt_name,
                     specified_partition_str.ptr(),
                     (specified_partition_str.empty() ? "" : " "),
                     specified_tenant_str.ptr(),
                     (specified_tenant_str.empty() ? "" : " and "),
                     start_table_id,
                     start_partition_id,
                     max_fetch_count))) {
        LOG_WARN("fail to append sql string", K(ret));
      } else if (OB_FAIL(get_sql_client().read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), "sql", sql.ptr());
      } else if (NULL == result.get_result()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed");
      } else if (OB_FAIL(construct_partition_infos(*result.get_result(),
                     cols,
                     filter_flag_replica,
                     fetch_count,
                     partition_infos,
                     ignore_row_checksum,
                     need_fetch_faillist))) {
        LOG_WARN("construct partition info failed", K(ret));
      } else if (fetch_count > max_fetch_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fetch_count bigger than max_fetch_count", K(fetch_count), K(max_fetch_count), K(ret));
      }
      LOG_DEBUG("fetch_partition_infos_impl", K(ret), K(sql), K(sql_tenant_id));
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::partition_table_name(
    const uint64_t table_id, const char*& table_name, uint64_t& sql_tenant_id)
{
  int ret = OB_SUCCESS;
  table_name = NULL;
  sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), KT(table_id));
  } else if (is_sys_table(table_id)) {
    table_name = OB_ALL_ROOT_TABLE_TNAME;
    sql_tenant_id = OB_SYS_TENANT_ID;
  } else {
    table_name = OB_ALL_TENANT_META_TABLE_TNAME;
    sql_tenant_id = extract_tenant_id(table_id);
  }
  return ret;
}

int ObNormalPartitionTableProxy::append_sub_batch_sql_fmt(common::ObSqlString& sql_string, int64_t& start,
    const common::ObIArray<common::ObPartitionKey>& pkeys, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  const int64_t BATCH_CNT = 128;
  bool first = true;
  for (int64_t index = start; OB_SUCC(ret) && index < pkeys.count() && cnt < BATCH_CNT; ++index) {
    const ObPartitionKey& pkey = pkeys.at(index);
    if (pkey.get_tenant_id() != tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tennat_id not match", K(ret), K(tenant_id), "tenant_id_in_key", pkey.get_tenant_id());
    } else if (pkey.get_tenant_id() == common::OB_SYS_TENANT_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys tenant", K(ret), "tenant_id_in_key", pkey.get_tenant_id());
    } else if (is_sys_table(pkey.get_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys table", K(pkey));
    } else if (OB_FAIL(sql_string.append_fmt(
                   "%s(%lu, %ld)", first ? "" : ", ", pkey.get_table_id(), pkey.get_partition_id()))) {
      LOG_WARN("fail to append fmt", K(ret));
    } else {
      first = false;
      ++start;
      ++cnt;
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::multi_fetch_partition_infos(const uint64_t tenant_id,
    const common::ObIArray<common::ObPartitionKey>& pkeys, const bool filter_flag_replicas,
    common::ObIArray<share::ObPartitionInfo>& partition_infos)
{
  int ret = OB_SUCCESS;
  // can only be invoked by user tenant non system table
  partition_infos.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (pkeys.count() <= 0) {
    // do nothing
  } else {
    for (int64_t start = 0; OB_SUCC(ret) && start < pkeys.count(); /*nop*/) {
      const bool ignore_row_checksum = true;
      int64_t fetch_count = 0;
      common::ObSqlString sql_string;
      SMART_VAR(ObISQLClient::ReadResult, result)
      {
        NormalPartitionTableCol cols;
        if (OB_FAIL(sql_string.append_fmt("SELECT * FROM %s WHERE TENANT_ID=%lu AND"
                                          " (table_id, partition_id) IN (",
                OB_ALL_TENANT_META_TABLE_TNAME,
                tenant_id))) {
          LOG_WARN("fail to append format", K(ret));
        } else if (OB_FAIL(append_sub_batch_sql_fmt(sql_string, start, pkeys, tenant_id))) {
          LOG_WARN("fail to append sub batch sql fmt", K(ret));
        } else if (OB_FAIL(sql_string.append_fmt(") ORDER BY TENANT_ID, TABLE_ID, PARTITION_ID, SVR_IP, SVR_PORT"))) {
          LOG_WARN("fail to append", K(ret));
        } else if (OB_FAIL(get_sql_client().read(result, tenant_id, sql_string.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), "sql_string", sql_string.ptr());
        } else if (OB_UNLIKELY(NULL == result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", K(ret));
        } else if (OB_FAIL(construct_partition_infos(*result.get_result(),
                       cols,
                       filter_flag_replicas,
                       fetch_count,
                       partition_infos,
                       ignore_row_checksum))) {
          LOG_WARN("fail to construct partition info", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObNormalPartitionTableProxy::update_fresh_replica_version(const ObAddr& addr, const int64_t sql_port,
    const ObIArray<obrpc::ObCreatePartitionArg>& create_partition_args, const int64_t data_version)
{
  int ret = OB_SUCCESS;
  const char* table_name = NULL;
  const char* status = NULL;
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!addr.is_valid() || sql_port <= 0 || create_partition_args.empty() || data_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        K(addr),
        K(sql_port),
        "create partition args count",
        create_partition_args.count(),
        K(data_version));
  } else if (is_sys_table(create_partition_args.at(0).partition_key_.get_table_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only __all_tenant_meta_table partition supported",
        K(ret),
        "table_id",
        create_partition_args.at(0).partition_key_.get_table_id());
  } else if (false == addr.ip_to_string(ip, sizeof(ip))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert server address to string failed", K(ret), K(addr));
  } else {
    FOREACH_CNT_X(create_partition_arg, create_partition_args, OB_SUCC(ret))
    {
      if (create_partition_arg->partition_key_.get_table_id() !=
          create_partition_args.at(0).partition_key_.get_table_id()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("partition should belong to one table",
            K(ret),
            "table_id",
            create_partition_arg->partition_key_.get_table_id(),
            "first_table_id",
            create_partition_args.at(0).partition_key_.get_table_id());
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (create_partition_args.at(0).split_info_.is_valid()) {
      status = UNMERGED_STATUS;
    } else {
      status = NORMAL_STATUS;
    }
  }

  ObSqlString sql;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(partition_table_name(
                 create_partition_args.at(0).partition_key_.get_table_id(), table_name, sql_tenant_id))) {
    LOG_WARN("get partition table name failed",
        K(ret),
        "table_id",
        create_partition_args.at(0).partition_key_.get_table_id());
  } else if (OB_ISNULL(table_name) || OB_INVALID_TENANT_ID == sql_tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL table_name or invalid sql_tenant_id", K(ret), K(sql_tenant_id));
  } else if (OB_FAIL(
                 sql.assign_fmt("UPDATE /*+ use_plan_cache(none) */ "
                                "%s SET data_version = %ld, sql_port = %ld, replica_type = %ld, status = '%s' WHERE "
                                "tenant_id = %ld AND table_id = %ld AND svr_ip = '%s' AND svr_port = %d "
                                "AND data_version = -1 AND partition_id IN (",
                     table_name,
                     data_version,
                     sql_port,
                     static_cast<int64_t>(create_partition_args.at(0).replica_type_),
                     status,
                     extract_tenant_id(create_partition_args.at(0).partition_key_.get_table_id()),
                     create_partition_args.at(0).partition_key_.get_table_id(),
                     ip,
                     addr.get_port()))) {
    LOG_WARN("assign sql failed", K(ret));
  } else {
    bool append_comma = false;
    FOREACH_CNT_X(create_partition_arg, create_partition_args, OB_SUCC(ret))
    {
      if (OB_FAIL(sql.append_fmt(
              "%s%ld", append_comma ? ", " : "", create_partition_arg->partition_key_.get_partition_id()))) {
        LOG_WARN("append sql failed", K(ret));
      } else {
        append_comma = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("append sql failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_proxy_.write(sql_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (affected_rows < 0 || affected_rows > create_partition_args.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), "arg count", create_partition_args.count());
    }
  }

  return ret;
}

int ObNormalPartitionUpdateHelper::exec_batch_insert_update_replica(
    const char* table_name, const bool with_role, const ObPTSqlSplicer& splicer, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(table_name));
  } else if (OB_FAIL(splicer.splice_batch_insert_update_replica_sql(table_name, with_role, sql))) {
    LOG_WARN("fail to splice update partition sql", K(ret));
  } else if (OB_FAIL(sql_client_.write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObNormalPartitionUpdateHelper::exec_insert_update_replica(
    const char* table_name, const ObPTSqlSplicer& splicer, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(table_name));
  } else if (OB_FAIL(splicer.splice_insert_update_replica_sql(table_name, sql))) {
    LOG_WARN("fail to splice update partition sql", K(ret));
  } else if (OB_FAIL(sql_client_.write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObNormalPartitionUpdateHelper::exec_update_leader_replica(
    const ObPartitionReplica& replica, const char* table_name, const ObPTSqlSplicer& splicer, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(table_name));
  } else if (OB_FAIL(splicer.splice_update_leader_replica_sql(replica, table_name, sql))) {
    LOG_WARN("fail to splice update partition sql", K(ret), K(replica));
  } else if (OB_FAIL(sql_client_.write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret), K(sql));
  }
  return ret;
}

int ObPartitionTableProxyFactory::get_proxy(const uint64_t table_id, ObPartitionTableProxy*& proxy)
{
  int ret = OB_SUCCESS;
  proxy = NULL;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", K(ret), KT(table_id));
  } else {
    proxy = (OB_ALL_ROOT_TABLE_TID == extract_pure_id(table_id)) ? static_cast<ObPartitionTableProxy*>(&kv_proxy_)
                                                                 : static_cast<ObPartitionTableProxy*>(&normal_proxy_);
    if (OB_ISNULL(proxy)) {
    } else {
      proxy->reset();
      proxy->set_merge_error_cb(merge_error_cb_);
      proxy->set_server_config(config_);
    }
  }
  return ret;
}

int ObPartitionTableProxyFactory::get_tenant_proxies(
    const uint64_t tenant_id, ObIArray<ObPartitionTableProxy*>& proxies)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    kv_proxy_.reset();
    normal_proxy_.reset();
    kv_proxy_.set_merge_error_cb(merge_error_cb_);
    normal_proxy_.set_merge_error_cb(merge_error_cb_);
    if (OB_FAIL(proxies.push_back(&kv_proxy_))) {
      LOG_WARN("push_back failed", K(ret));
    } else if (OB_FAIL(proxies.push_back(&normal_proxy_))) {
      LOG_WARN("push_back failed", K(ret));
    }
  } else {
    normal_proxy_.reset();
    normal_proxy_.set_merge_error_cb(merge_error_cb_);
    if (OB_FAIL(proxies.push_back(&normal_proxy_))) {
      LOG_WARN("push_back failed", K(ret));
    }
  }
  return ret;
}

int ObPartitionTableProxyFactory::get_proxy_of_partition_table(
    const uint64_t pt_table_id, ObPartitionTableProxy*& proxy)
{
  int ret = OB_SUCCESS;
  proxy = NULL;
  if (OB_INVALID_ID == pt_table_id || !ObIPartitionTable::is_persistent_partition_table(pt_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id", K(ret), KT(pt_table_id));
  } else {
    proxy = (OB_ALL_CORE_TABLE_TID == extract_pure_id(pt_table_id))
                ? static_cast<ObPartitionTableProxy*>(&kv_proxy_)
                : static_cast<ObPartitionTableProxy*>(&normal_proxy_);
    if (OB_ISNULL(proxy)) {
    } else {
      proxy->reset();
      proxy->set_merge_error_cb(merge_error_cb_);
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
