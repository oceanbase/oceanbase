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

#ifndef OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_DAG
#define OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_DAG

#include <stdint.h>
#include "lib/worker.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObMdsTableMergeDag : public share::ObIDag
{
public:
  ObMdsTableMergeDag();
  virtual ~ObMdsTableMergeDag() = default;
  ObMdsTableMergeDag(const ObMdsTableMergeDag&) = delete;
  ObMdsTableMergeDag &operator=(const ObMdsTableMergeDag&) = delete;
  const ObMdsTableMergeDagParam& get_param() const { return param_; }
public:
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool ignore_warning() override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(compat_mode), K_(param));
private:
  bool is_inited_;
  lib::Worker::CompatMode compat_mode_;
  ObMdsTableMergeDagParam param_;
};
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_DAG
