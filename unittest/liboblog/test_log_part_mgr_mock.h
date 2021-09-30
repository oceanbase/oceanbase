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

#include <gtest/gtest.h>
#include "ob_log_part_mgr.h"                        // ObLogPartMgr

using namespace oceanbase::common;
using namespace oceanbase::liboblog;
using namespace oceanbase::transaction;

class MockObLogPartMgr : public IObLogPartMgr
{
public:
  static const int64_t START_TIMESTAMP = 1452763440;
  static const int64_t CUR_SCHEMA_VERSION = 100;

  MockObLogPartMgr(): start_tstamp_(START_TIMESTAMP), cur_schema_version_(CUR_SCHEMA_VERSION)
  {  }

  ~MockObLogPartMgr()
  {  }

  virtual int add_table(const uint64_t table_id,
                        const int64_t start_schema_version,
                        const int64_t start_server_tstamp,
                        const int64_t timeout)
  {
    UNUSED(table_id);
    UNUSED(start_schema_version);
    UNUSED(start_server_tstamp);
    UNUSED(timeout);
    return OB_SUCCESS;
  }

  virtual int drop_table(const uint64_t table_id,
                         const int64_t schema_version_before_drop,
                         const int64_t schema_version_after_drop,
                         const int64_t timeout)
  {
    UNUSED(table_id);
    UNUSED(schema_version_before_drop);
    UNUSED(schema_version_after_drop);
    UNUSED(timeout);
    return OB_SUCCESS;
  }

  virtual int drop_tenant(const uint64_t tenant_id,
                          const int64_t schema_version_before_drop,
                          const int64_t schema_version_after_drop,
                          const int64_t timeout)
  {
    UNUSED(tenant_id);
    UNUSED(schema_version_before_drop);
    UNUSED(schema_version_after_drop);
    UNUSED(timeout);
    return OB_SUCCESS;
  }

  virtual int drop_database(const uint64_t database_id,
                            const int64_t schema_version_before_drop,
                            const int64_t schema_version_after_drop,
                            const int64_t timeout)
  {
    UNUSED(database_id);
    UNUSED(schema_version_before_drop);
    UNUSED(schema_version_after_drop);
    UNUSED(timeout);
    return OB_SUCCESS;
  }

  virtual int add_all_tables(const int64_t schema_version, const int64_t start_tstamp)
  {
    UNUSED(schema_version);
    UNUSED(start_tstamp);
    return OB_SUCCESS;
  }

  virtual int update_schema_version(const int64_t schema_version)
  {
    UNUSED(schema_version);
    return OB_SUCCESS;
  }
  virtual int inc_part_trans_count_on_serving(bool &is_serving,
                                              const ObPartitionKey &key,
                                              const uint64_t prepare_log_id,
                                              const int64_t prepare_log_timestamp,
                                              const int64_t timeout)
  {
    if (prepare_log_timestamp < start_tstamp_) {
      // If the Prepare log timestamp is less than the start timestamp, it must not be served
      is_serving = false;
    } else {
      is_serving = true;
    }

    UNUSED(key);
    UNUSED(prepare_log_id);
    UNUSED(timeout);
    return OB_SUCCESS;
  }

  virtual int dec_part_trans_count(const ObPartitionKey &key)
  {
    UNUSED(key);
    return OB_SUCCESS;
  }
  virtual int update_part_info(const ObPartitionKey &pkey, const uint64_t start_log_id)
  {
    UNUSED(pkey);
    UNUSED(start_log_id);
    return OB_SUCCESS;
  }
  virtual int table_group_match(const char *pattern, bool &is_matched,
                                int fnmatch_flags = FNM_CASEFOLD)
  {
    UNUSED(pattern);
    UNUSED(is_matched);
    UNUSED(fnmatch_flags);
    return OB_SUCCESS;
  }
  virtual int get_table_groups(std::vector<std::string> &table_groups)
  {
    UNUSED(table_groups);
    return OB_SUCCESS;
  }
  virtual int register_part_add_callback(PartAddCallback *callback)
  {
    UNUSED(callback);
    return OB_SUCCESS;
  }
  virtual int register_part_rm_callback(PartRMCallback *callback)
  {
    UNUSED(callback);
    return OB_SUCCESS;
  }
  virtual int register_part_recycle_callback(PartRecycleCallback *callback)
  {
    UNUSED(callback);
    return OB_SUCCESS;
  }
  virtual void print_part_info() {}

private:
  int64_t start_tstamp_;
  int64_t cur_schema_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(MockObLogPartMgr);
};

