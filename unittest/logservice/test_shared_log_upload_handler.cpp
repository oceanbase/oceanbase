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
#include "log/ob_shared_log_upload_handler.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace logservice;
namespace unittest
{

TEST(TestLogUploadCtx, test_is_valid)
{
  LogUploadCtx ctx;
  //case 0: origin is invalid
  ASSERT_FALSE(ctx.is_valid());
  //case 1: start_block_id_ > end_block_id_
  ctx.start_block_id_ = 1;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
  ASSERT_FALSE(ctx.is_valid());

  //case 2:!has_file_on_ss_ and start_block_id_ is not 0
  ctx.start_block_id_ = 1;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = 0;
  ASSERT_FALSE(ctx.is_valid());

  //case 3:!has_file_on_ss_ and max_block_id_on_ss_ is not LOG_INVALID_BLOCK_ID
  ctx.start_block_id_ = 0;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = 1;
  ASSERT_TRUE(ctx.is_valid());

   //case 4:has_file_on_ss_ and max_block_id_on_ss_ is  LOG_INVALID_BLOCK_ID
  ctx.start_block_id_ = 1;
  ctx.end_block_id_ = 2;
  ctx.has_file_on_ss_ = true;
  ctx.max_block_id_on_ss_ = 4;
  ASSERT_FALSE(ctx.is_valid());


  //case 6: has_file_on_ss_
  ctx.start_block_id_ = 1;
  ctx.end_block_id_ = 1;
  ctx.has_file_on_ss_ = true;
  ctx.max_block_id_on_ss_ = 0;
  ASSERT_TRUE(ctx.is_valid());

  ctx.end_block_id_ = 3;
  ASSERT_TRUE(ctx.is_valid());

  //case 6: !has_file_on_ss_
  ctx.start_block_id_ = 0;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
  ASSERT_TRUE(ctx.is_valid());
}
TEST(TestLogUploadCtx, after_upload_file)
{
  LogUploadCtx ctx;

  ctx.start_block_id_ = 0;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = 0;

  block_id_t uploaded_block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.after_upload_file(uploaded_block_id));
  uploaded_block_id = 1;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ctx.after_upload_file(uploaded_block_id));
  uploaded_block_id = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ctx.after_upload_file(uploaded_block_id));
  uploaded_block_id = 0;
  ctx.end_block_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ctx.after_upload_file(uploaded_block_id));
  ASSERT_TRUE(ctx.has_file_on_ss_);
  ASSERT_TRUE(ctx.start_block_id_ == 1);
  ASSERT_TRUE(ctx.max_block_id_on_ss_ == 0);

  uploaded_block_id = 1;
  ctx.end_block_id_ = 2;
  ASSERT_EQ(OB_SUCCESS, ctx.after_upload_file(uploaded_block_id));
  ASSERT_TRUE(ctx.has_file_on_ss_);
  ASSERT_TRUE(ctx.start_block_id_ == 2);
  ASSERT_TRUE(ctx.max_block_id_on_ss_ == 1);
}

TEST(TestLogUploadCtx, need_locate_upload_range)
{
  LogUploadCtx ctx;
  ASSERT_TRUE(ctx.need_locate_upload_range());
  //not invalid
  ctx.start_block_id_ = LOG_INVALID_BLOCK_ID;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
  ASSERT_TRUE(ctx.need_locate_upload_range());

  //proposal_id increased
  ctx.start_block_id_ = 0;
  ASSERT_TRUE(ctx.need_locate_upload_range());

  //need check whether end_block_id changed
  ASSERT_TRUE(ctx.need_locate_upload_range());

  ctx.end_block_id_ = 2;
  ASSERT_FALSE(ctx.need_locate_upload_range());

  ctx.start_block_id_ = 2;
  ctx.end_block_id_ = 2;
  ctx.has_file_on_ss_ = true;
  ctx.max_block_id_on_ss_ = 1;
  ASSERT_TRUE(ctx.need_locate_upload_range());
  ctx.end_block_id_ = 3;
  ASSERT_FALSE(ctx.need_locate_upload_range());
}

TEST(TestLogUploadCtx, update)
{
  LogUploadCtx old_ctx;
  LogUploadCtx new_ctx;
  ASSERT_EQ(OB_INVALID_ARGUMENT, old_ctx.update(new_ctx));
  //
  new_ctx.start_block_id_ = 0;
  new_ctx.end_block_id_ = 0;
  new_ctx.has_file_on_ss_ = false;
  new_ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;

  ASSERT_EQ(OB_SUCCESS, old_ctx.update(new_ctx));
  ASSERT_TRUE(old_ctx == new_ctx);
  ASSERT_EQ(OB_SUCCESS, old_ctx.update(new_ctx));
  ASSERT_TRUE(old_ctx == new_ctx);

  ASSERT_EQ(OB_SUCCESS, old_ctx.update(new_ctx));
  ASSERT_TRUE(old_ctx == new_ctx);

  new_ctx.start_block_id_ = 3;
  new_ctx.end_block_id_ = 3;
  new_ctx.has_file_on_ss_ = true;
  new_ctx.max_block_id_on_ss_ = 2;
  ASSERT_EQ(OB_SUCCESS, old_ctx.update(new_ctx));
  ASSERT_TRUE(old_ctx == new_ctx);

  new_ctx.end_block_id_ = 4;
  ASSERT_EQ(OB_SUCCESS, old_ctx.update(new_ctx));
  ASSERT_TRUE(old_ctx == new_ctx);
}

TEST(TestLogUploadCtx, get_next_block_to_upload)
{
  LogUploadCtx ctx;
  block_id_t to_upload_block_id = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ctx.get_next_block_to_upload(to_upload_block_id));
  ctx.start_block_id_ = 0;
  ctx.end_block_id_ = 0;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ctx.get_next_block_to_upload(to_upload_block_id));

  ctx.end_block_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, ctx.get_next_block_to_upload(to_upload_block_id));
  ASSERT_EQ(0, to_upload_block_id);
}

TEST(TestLogSSHandler, basic_function)
{
  ObSharedLogUploadHandler handler;
  LogUploadCtx ctx;
  block_id_t uploaded_block_id = LOG_INVALID_BLOCK_ID;
  //test not init
  ASSERT_EQ(OB_NOT_INIT, handler.get_log_upload_ctx(ctx));
  ASSERT_EQ(OB_NOT_INIT, handler.update_log_upload_ctx(ctx));
  ASSERT_EQ(OB_NOT_INIT, handler.after_upload_file(uploaded_block_id));
  share::ObLSID id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, handler.init(id));
  id = 1;
  ASSERT_EQ(OB_SUCCESS, handler.init(id));
  ASSERT_TRUE(handler.is_inited_);

  //test invalid_argument
  ASSERT_EQ(OB_INVALID_ARGUMENT, handler.update_log_upload_ctx(ctx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, handler.after_upload_file(uploaded_block_id));

  //test OB_SUCCESS
  ASSERT_EQ(OB_SUCCESS, handler.get_log_upload_ctx(ctx));
  ctx.start_block_id_ = 0;
  ctx.end_block_id_ = 1;
  ctx.has_file_on_ss_ = false;
  ctx.max_block_id_on_ss_ = LOG_INVALID_BLOCK_ID;
  ASSERT_EQ(OB_SUCCESS, handler.update_log_upload_ctx(ctx));
  uploaded_block_id = 0;
  ASSERT_EQ(OB_SUCCESS, handler.after_upload_file(uploaded_block_id));
}

}//end of namespace logservice
}//end of namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_ss_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_ss_handler");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
