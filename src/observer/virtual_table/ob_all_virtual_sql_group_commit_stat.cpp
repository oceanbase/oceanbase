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

#include "observer/virtual_table/ob_all_virtual_sql_group_commit_stat.h"
#ifdef OB_HOTSPOT_GROUP_COMMIT
#include "lib/string/ob_sql_string.h"
#include "sql/ob_sql_group_commit_aggregator.h"
#include "share/rc/ob_tenant_base.h"
#endif
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
#ifdef OB_HOTSPOT_GROUP_COMMIT
using namespace oceanbase::sql;
#endif

namespace oceanbase
{
namespace observer
{

#ifdef OB_HOTSPOT_GROUP_COMMIT
static int build_group_commit_sql_text(
    ObIAllocator &allocator,
    const sql::ObGroupSqlKey &sql_key,
    ObString &sql_text)
{
  int ret = OB_SUCCESS;
  common::ObSqlString sql_text_buf;
  if (OB_FAIL(sql_text_buf.append_fmt("sql=%.*s param_types_cnt=%ld param_types=[",
                                      sql_key.ps_sql_key_.ps_sql_.length(),
                                      sql_key.ps_sql_key_.ps_sql_.ptr(),
                                      sql_key.param_types_cnt_))) {
    LOG_WARN("failed to append sql text prefix", K(ret), K(sql_key));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_key.param_types_cnt_; ++i) {
      if (i > 0 && OB_FAIL(sql_text_buf.append(","))) {
        LOG_WARN("failed to append param type separator", K(ret), K(i), K(sql_key));
      } else if (OB_FAIL(sql_text_buf.append_fmt("%d", static_cast<int>(sql_key.param_types_[i])))) {
        LOG_WARN("failed to append param type", K(ret), K(i), K(sql_key));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql_text_buf.append("]"))) {
      LOG_WARN("failed to append param type suffix", K(ret), K(sql_key));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, sql_text_buf.string(), sql_text))) {
    LOG_WARN("failed to deep copy formatted sql text", K(ret), K(sql_key));
  }
  return ret;
}
#endif

void ObAllVirtualSqlGroupCommitStat::reset()
{
  cur_idx_ = 0;
  stat_infos_.reset();
  arena_allocator_.reuse();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualSqlGroupCommitStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    LOG_WARN("execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualSqlGroupCommitStat::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualSqlGroupCommitStat::release_last_tenant()
{
  cur_idx_ = 0;
  stat_infos_.reset();
  arena_allocator_.reuse();
}

#ifdef OB_HOTSPOT_GROUP_COMMIT
// Callback class to collect statistics from sql_map_
class CollectSqlGroupCommitStatCallback
{
public:
  CollectSqlGroupCommitStatCallback(
      ObSEArray<ObSqlGroupCommitStatInfo, 128> &stat_infos,
      ObIAllocator &allocator,
      uint64_t tenant_id)
    : stat_infos_(stat_infos),
      allocator_(allocator),
      tenant_id_(tenant_id)
  {}

  int operator()(const common::hash::HashMapPair<ObGroupSqlKey, ObGroupSqlValue*> &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(entry.second)) {
      ObSqlGroupCommitStatInfo stat_info;
      stat_info.tenant_id_ = tenant_id_;
      if (OB_FAIL(build_group_commit_sql_text(allocator_, entry.first, stat_info.sql_text_))) {
        LOG_WARN("failed to build sql text", K(ret), K(entry.first));
      } else {
        stat_info.sql_hash_ = stat_info.sql_text_.hash();
      }
      stat_info.single_exec_count_ = entry.second->get_single_exec_count();
      stat_info.batch_exec_count_ = entry.second->get_batch_exec_count();
      stat_info.batch_exec_req_cnt_ = entry.second->get_batch_exec_req_cnt();
      stat_info.split_count_ = entry.second->get_split_count();
      stat_info.split_exec_req_cnt_ = entry.second->get_split_exec_req_cnt();
      stat_info.group_value_count_ = entry.second->key_map_.size();

      if (OB_SUCC(ret) && OB_FAIL(stat_infos_.push_back(stat_info))) {
        LOG_WARN("failed to push back stat info", K(ret));
      }
    }
    return ret;
  }

private:
  ObSEArray<ObSqlGroupCommitStatInfo, 128> &stat_infos_;
  ObIAllocator &allocator_;
  uint64_t tenant_id_;
};
#endif

int ObAllVirtualSqlGroupCommitStat::collect_stat_info()
{
  int ret = OB_SUCCESS;
#ifdef OB_HOTSPOT_GROUP_COMMIT
  ObSqlGroupCommitAggregator *aggregator = MTL(ObSqlGroupCommitAggregator*);

  if (OB_ISNULL(aggregator)) {
    LOG_DEBUG("sql group commit aggregator not exist for tenant", K(MTL_ID()));
  } else {
    CollectSqlGroupCommitStatCallback callback(stat_infos_, arena_allocator_, MTL_ID());
    if (OB_FAIL(const_cast<ObGroupSqlMap&>(aggregator->get_sql_map()).foreach_refactored(callback))) {
      LOG_WARN("failed to collect stat info", K(ret));
    }
  }
#endif
  return ret;
}

int ObAllVirtualSqlGroupCommitStat::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  start_to_read_ = true;

  // Collect statistics on first call for this tenant
  if (stat_infos_.count() == 0 && OB_FAIL(collect_stat_info())) {
    LOG_WARN("fail to collect stat info", K(ret));
  } else if (cur_idx_ >= stat_infos_.count()) {
    ret = OB_ITER_END;
  } else {
    ObSqlGroupCommitStatInfo &stat_info = stat_infos_.at(cur_idx_);
    const int64_t col_count = output_column_ids_.count();

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case STAT_COLUMN::SVR_IP:
          if (!GCTX.self_addr().ip_to_string(ipbuf_, common::OB_IP_STR_BUFF)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get ip string", K(ret));
          } else {
            cells[i].set_varchar(ObString::make_string(ipbuf_));
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case STAT_COLUMN::SVR_PORT:
          cells[i].set_int(GCTX.self_addr().get_port());
          break;
        case STAT_COLUMN::TENANT_ID:
          cells[i].set_int(stat_info.tenant_id_);
          break;
        case STAT_COLUMN::SQL_TEXT:
          cells[i].set_varchar(stat_info.sql_text_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case STAT_COLUMN::SQL_HASH:
          cells[i].set_uint(ObUInt64Type, stat_info.sql_hash_);
          break;
        case STAT_COLUMN::SINGLE_EXEC_COUNT:
          cells[i].set_int(stat_info.single_exec_count_);
          break;
        case STAT_COLUMN::BATCH_EXEC_COUNT:
          cells[i].set_int(stat_info.batch_exec_count_);
          break;
        case STAT_COLUMN::BATCH_EXEC_REQ_CNT:
          cells[i].set_int(stat_info.batch_exec_req_cnt_);
          break;
        case STAT_COLUMN::SPLIT_COUNT:
          cells[i].set_int(stat_info.split_count_);
          break;
        case STAT_COLUMN::SPLIT_EXEC_REQ_CNT:
          cells[i].set_int(stat_info.split_exec_req_cnt_);
          break;
        case STAT_COLUMN::GROUP_VALUE_COUNT:
          cells[i].set_int(stat_info.group_value_count_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid column id", K(ret), K(col_id), K(i), K(output_column_ids_));
          break;
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
