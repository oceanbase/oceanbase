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

#ifndef OCEANBASE_STORAGE_OB_I_PARTITION_COMPONENT_FACTORY_
#define OCEANBASE_STORAGE_OB_I_PARTITION_COMPONENT_FACTORY_

namespace oceanbase
{
namespace transaction
{
class ObTransService;
}
namespace memtable
{
class ObIMemtable;
}
namespace storage
{
class ObIPartitionGroup;
class ObMinorFreeze;
class ObIPSFreezeCb;
class ObSSStore;
class ObITable;
class ObReplayStatus;
class ObLS;


class ObIPartitionComponentFactory
{
public:
  // for log stream
  virtual ObLS *get_ls(const uint64_t tenant_id) = 0;

public:
  virtual ~ObIPartitionComponentFactory() {}
  virtual transaction::ObTransService *get_trans_service() = 0;

  virtual void free(ObIPartitionGroup *partition) = 0;
  virtual void free(transaction::ObTransService *txs) = 0;


};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_I_PARTITION_COMPONENT_FACTORY_
