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

#ifndef OCEANBASE_SQL_SESSION_ERASE_SESSION_CALLBACK_
#define OCEANBASE_SQL_SESSION_ERASE_SESSION_CALLBACK_
namespace oceanbase {
namespace common {
class ObEndTransReq;
}
namespace sql {
class ObIEraseSessionCallback {
public:
  ObIEraseSessionCallback();
  virtual ~ObIEraseSessionCallback();
  virtual int callback(common::ObEndTransReq& req) = 0;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_SESSION_ERASE_SESSION_CALLBACK_ */
