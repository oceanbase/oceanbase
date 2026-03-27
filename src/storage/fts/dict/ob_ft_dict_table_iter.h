/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_TABLE_ITER_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_TABLE_ITER_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"

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
  int init(const ObString &table_name);

private:
  void reset();

private:
  bool is_inited_;
  ObISQLClient::ReadResult &res_;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_TABLE_ITER_H_
