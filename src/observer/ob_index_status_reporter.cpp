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

#include "ob_index_status_reporter.h"

#include "lib/hash_func/murmur_hash.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_index_status_table_operator.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace observer {

ObIndexStatusReporter::ObIndexStatusReporter()
    : inited_(false),
      part_key_(),
      self_(),
      index_table_id_(OB_INVALID_ID),
      index_status_(INDEX_STATUS_UNAVAILABLE),
      ret_code_(0),
      sql_proxy_(NULL)
{}

ObIndexStatusReporter::~ObIndexStatusReporter()
{}

int ObIndexStatusReporter::init(const common::ObPartitionKey& part_key, const common::ObAddr& self,
    const uint64_t index_table_id, const share::schema::ObIndexStatus index_status, const int ret_code,
    common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!part_key.is_valid() || !self.is_valid() || OB_INVALID_ID == index_table_id ||
             index_status <= INDEX_STATUS_NOT_FOUND || index_status >= INDEX_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), K(self), K(index_table_id), K(index_status));
  } else {
    part_key_ = part_key;
    self_ = self;
    index_table_id_ = index_table_id;
    index_status_ = index_status;
    ret_code_ = ret_code;
    sql_proxy_ = &sql_proxy;
    inited_ = true;
  }
  return ret;
}

bool ObIndexStatusReporter::is_valid() const
{
  return is_inited() && part_key_.is_valid() && self_.is_valid() && OB_INVALID_ID != index_table_id_ &&
         index_status_ > INDEX_STATUS_NOT_FOUND && index_status_ < INDEX_STATUS_MAX;
}

int64_t ObIndexStatusReporter::hash() const
{
  int64_t val = 0;
  if (!is_valid()) {
    LOG_WARN("invalid argument", "self", *this);
  } else {
    val = part_key_.hash();
    murmurhash(&index_table_id_, sizeof(index_table_id_), val);
  }
  return val;
}

bool ObIndexStatusReporter::operator==(const ObIndexStatusReporter& o) const
{
  bool equal = false;
  if (!is_valid() || !o.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, "other", o);
  } else {
    if (&o == this) {
      equal = true;
    } else {
      equal = (part_key_ == o.part_key_ && index_table_id_ == o.index_table_id_ && self_ == o.self_ &&
               index_status_ == o.index_status_ && ret_code_ == o.ret_code_);
    }
  }
  return equal;
}

int ObIndexStatusReporter::process(const volatile bool& stop)
{
  int ret = OB_EAGAIN;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();
    // infinity retry to success.
    for (int64_t retry_times = 0; OB_SUCCESS != ret && !stop; ++retry_times) {
      if (OB_FAIL(do_process())) {
        LOG_WARN("do report index build status failed",
            K(ret),
            K(retry_times),
            "escape time",
            ObTimeUtility::current_time() - start_time,
            "task",
            *this);

        const static int64_t sleep_step_us = 20 * 1000;  // 20ms
        for (int64_t sleep_usec = RETRY_INTERVAL_US; !stop && sleep_usec > 0; sleep_usec -= sleep_step_us) {
          usleep(static_cast<int32_t>(std::min(sleep_step_us, sleep_usec)));
        }
      }
    }
  }

  return ret;
}

int ObIndexStatusReporter::do_process()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObIndexStatusTableOperator::ObBuildIndexStatus status;
    status.index_status_ = index_status_;
    status.role_ = FOLLOWER;
    status.ret_code_ = ret_code_;
    if (OB_FAIL(ObIndexStatusTableOperator::report_build_index_status(
            index_table_id_, part_key_.get_partition_id(), self_, status, *sql_proxy_))) {
      LOG_WARN("fail to report build index status", K(ret), K(index_table_id_));
    }
  }

  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
