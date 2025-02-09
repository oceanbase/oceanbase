/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
