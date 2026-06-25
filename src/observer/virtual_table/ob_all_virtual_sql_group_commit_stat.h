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
#ifndef OB_ALL_VIRTUAL_SQL_GROUP_COMMIT_STAT_H_
#define OB_ALL_VIRTUAL_SQL_GROUP_COMMIT_STAT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

// Statistics info for each SQL in group commit
struct ObSqlGroupCommitStatInfo
{
  ObSqlGroupCommitStatInfo()
    : tenant_id_(0),
      sql_text_(),
      sql_hash_(0),
      single_exec_count_(0),
      batch_exec_count_(0),
      batch_exec_req_cnt_(0),
      split_count_(0),
      split_exec_req_cnt_(0),
      group_value_count_(0)
  {}
  void reset()
  {
    tenant_id_ = 0;
    sql_text_.reset();
    sql_hash_ = 0;
    single_exec_count_ = 0;
    batch_exec_count_ = 0;
    batch_exec_req_cnt_ = 0;
    split_count_ = 0;
    split_exec_req_cnt_ = 0;
    group_value_count_ = 0;
  }
  uint64_t tenant_id_;
  common::ObString sql_text_;
  uint64_t sql_hash_;
  int64_t single_exec_count_;
  int64_t batch_exec_count_;
  int64_t batch_exec_req_cnt_;
  int64_t split_count_;
  int64_t split_exec_req_cnt_;
  int64_t group_value_count_;
  TO_STRING_KV(K_(tenant_id), K_(sql_text), K_(sql_hash), K_(single_exec_count),
               K_(batch_exec_count), K_(batch_exec_req_cnt), K_(split_count), K_(split_exec_req_cnt), K_(group_value_count));
};

class ObAllVirtualSqlGroupCommitStat : public common::ObVirtualTableScannerIterator,
                                       public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualSqlGroupCommitStat()
    : ObVirtualTableScannerIterator(),
      cur_idx_(0),
      stat_infos_(),
      arena_allocator_("SqlGrpCmtStat", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  virtual ~ObAllVirtualSqlGroupCommitStat() { reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  enum STAT_COLUMN {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    SQL_TEXT,
    SINGLE_EXEC_COUNT,
    BATCH_EXEC_COUNT,
    BATCH_EXEC_REQ_CNT,
    SPLIT_COUNT,
    SPLIT_EXEC_REQ_CNT,
    GROUP_VALUE_COUNT,
    SQL_HASH
  };
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;
  int collect_stat_info();
private:
  int64_t cur_idx_;
  char ipbuf_[common::OB_IP_STR_BUFF];
  common::ObSEArray<ObSqlGroupCommitStatInfo, 128> stat_infos_;
  common::ObArenaAllocator arena_allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSqlGroupCommitStat);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif // OB_ALL_VIRTUAL_SQL_GROUP_COMMIT_STAT_H_
