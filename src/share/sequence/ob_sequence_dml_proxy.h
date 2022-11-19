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

#ifndef __OB_SHARE_SEQUENCE_SEQUENCE_DML_PROXY_H__
#define __OB_SHARE_SEQUENCE_SEQUENCE_DML_PROXY_H__

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObMySQLProxy;
class ObMySQLTransaction;
class ObSQLClientRetryWeak;
class ObTimeoutCtx;
namespace number
{
class ObNumber;
}
}
namespace share
{
class ObSequenceOption;
struct SequenceCacheNode;
namespace schema
{
class ObSchemaGetterGuard;
class ObSequenceSchema;
class ObMultiVersionSchemaService;
}
class ObSequenceDMLProxy
{
public:
  ObSequenceDMLProxy();
  virtual ~ObSequenceDMLProxy();
  void init(share::schema::ObMultiVersionSchemaService &schema_service,
            common::ObMySQLProxy &sql_proxy);
  /*
   * 1. select for update, 读取到 sequence 参数
   * 2. 如果是 nocycle，则按照 cache 值为上限尽可能取值填入
   *    next_inclusvie_start, next_inclusvie_end
   *    如果是 cycle， 则也是尽可能取，但如果没有内容可取了，则回环到
   *    起点重新取满一个 cache，并填入
   *    next_inclusvie_start, next_inclusvie_end
   *  3. 更新 sequence_object 表
   */
  int next_batch(const uint64_t tenant_id,
                 const uint64_t sequence_id,
                 const share::ObSequenceOption &option,
                 SequenceCacheNode &cache_range);
  int prefetch_next_batch(
      const uint64_t tenant_id,
      const uint64_t sequence_id,
      const share::ObSequenceOption &option,
      SequenceCacheNode &cache_range);
private:
  /* functions */
  int set_pre_op_timeout(common::ObTimeoutCtx &ctx);
  int init_sequence_value_table(
      common::ObMySQLTransaction &trans,
      common::ObSQLClientRetryWeak &sql_client_retry_weak,
      common::ObIAllocator &allocator,
      uint64_t tenant_id,
      uint64_t sequence_id,
      const ObSequenceOption &option,
      common::number::ObNumber &next_value);

  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObSequenceDMLProxy);
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  bool inited_;
};
}
}
#endif /* __OB_SHARE_SEQUENCE_SEQUENCE_DML_PROXY_H__ */
//// end of header file

