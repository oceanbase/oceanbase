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

#define USING_LOG_PREFIX SERVER
#include "ob_table_service.h"
#include "observer/ob_service.h"
#include "ob_table_rpc_processor.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "ob_htable_filter_operator.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/tx_storage/ob_access_service.h"
using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

int ObTableService::init(ObGlobalContext &gctx)
{
  int ret = OB_SUCCESS;
  schema_service_ = gctx.schema_service_;
  if (OB_FAIL(sess_pool_mgr_.init())) {
    LOG_WARN("fail to init tableapi session pool manager", K(ret));
  }
  return ret;
}

int ObTableService::check_htable_query_args(const ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &select_columns = query.get_select_columns();
  int64_t N = select_columns.count();
  if (N != 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("TableQuery with htable_filter should select 4 columns", K(ret), K(N));
  }
  if (OB_SUCC(ret)) {
    if (ObHTableConstants::ROWKEY_CNAME_STR != select_columns.at(0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select K as the first column", K(ret), K(select_columns));
    } else if (ObHTableConstants::CQ_CNAME_STR != select_columns.at(1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select Q as the second column", K(ret), K(select_columns));
    } else if (ObHTableConstants::VERSION_CNAME_STR != select_columns.at(2)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select T as the third column", K(ret), K(select_columns));
    } else if (ObHTableConstants::VALUE_CNAME_STR != select_columns.at(3)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select V as the fourth column", K(ret), K(select_columns));
    }
  }
  if (OB_SUCC(ret)) {
    if (0 != query.get_offset()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable scan should not set Offset and Limit", K(ret), K(query));
    } else if (ObQueryFlag::Forward != query.get_scan_order() && ObQueryFlag::Reverse != query.get_scan_order()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("TableQuery with htable_filter only support forward and reverse scan yet", K(ret));
    }
  }
  return ret;
}

int ObNormalTableQueryResultIterator::get_next_result(table::ObTableQueryResult *&next_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(one_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one_result_ should not be null", K(ret));
  } else if (is_first_result_ || is_query_sync_) {
    if (0 != one_result_->get_property_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("property should be empty", K(ret));
    }
    const ObIArray<ObString> &select_columns = query_->get_select_columns();
    const int64_t N = select_columns.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
      if (OB_FAIL(one_result_->add_property_name(select_columns.at(i)))) {
        LOG_WARN("failed to copy name", K(ret));
      }
    } // end for

    if (is_first_result_) {
      last_row_ = NULL;
      is_first_result_ = false;
    }
  } else {
    one_result_->reset_except_property();
  }

  if (OB_SUCC(ret)) {
    if (NULL != last_row_) {
      if (OB_FAIL(one_result_->add_row(*last_row_))) {
        LOG_WARN("failed to add row, ", K(ret));
      }
      last_row_ = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    next_result = one_result_;
    ObNewRow *row = nullptr;
    while (OB_SUCC(ret) && OB_SUCC(scan_result_->get_next_row(row))) {
      LOG_DEBUG("[yzfdebug] scan result", "row", *row);
      if (OB_FAIL(one_result_->add_row(*row))) {
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          last_row_ = row;
          break;
        } else {
          LOG_WARN("failed to add row", K(ret));
        }
      } else if (one_result_->reach_batch_size_or_result_size(batch_size_, max_result_size_)) {
        NG_TRACE(tag9);
        break;
      } else {
        LOG_DEBUG("[yzfdebug] scan return one row", "row", *row);
      }
    }  // end while
    if (OB_ITER_END == ret) {
      has_more_rows_ = false;
      if (one_result_->get_row_count() > 0) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

bool ObNormalTableQueryResultIterator::has_more_result() const
{
  return has_more_rows_;
}
