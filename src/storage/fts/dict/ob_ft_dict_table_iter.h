/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_TABLE_ITER_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_TABLE_ITER_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_cache_loader.h"

namespace oceanbase
{
namespace storage
{
class ObFTDictTableIter : public ObIFTDictIterator
{
public:
  ObFTDictTableIter(ObISQLClient::ReadResult &result);
  ~ObFTDictTableIter() override { reset(); }

  // override
public:
  int get_key(ObString &str) override;
  int get_value() override;
  int next() override;

public:
  int init(const ObString &table_name,
           const uint64_t tenant_id,
           const int64_t snapshot_version,
           const bool need_casedown,
           const ObIArray<ObMissingRangeInfo> *partial_ranges = nullptr);

private:
  void reset();

  int append_where_clause(ObSqlString &sql_string, const bool need_casedown,
                          const ObIArray<ObMissingRangeInfo> *partial_ranges);

  bool is_inited_;
  ObISQLClient::ReadResult &res_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_TABLE_ITER_H_
