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

#ifndef OB_TIS620CI_SORT_SCANNER
#define OB_TIS620CI_SORT_SCANNER


class Tis620CiSortScanner
{
private:
  enum {
      INIT_STATE = 1 << 0,
      ISTHAT_STATE = 1 << 1,
      NOT_ISTHAT_STATE = 1 << 2,
      SWAP_STATE = 1 << 3,
      MOVE_STATE = 1 << 4,
      YEILD_STATE = 1<<5,
      END_STATE = 1 << 6,
  };
  enum mysql_version {
      MYSQL5x = 1 << 0,
      MYSQL4 = 1 << 1,
  };
public:

    Tis620CiSortScanner(const unsigned char* str, int len, int version = 1)
                    :state_(1), cur_(0), next_(0), count_(0), move_count_(0), capacity_(len), version_(static_cast<Tis620CiSortScanner::mysql_version>(version)), str_(str) {}
    int get_next_character(unsigned char& next);
    bool has_next();
private:
    void move_next(unsigned char& ch, uint64_t& move_count);
    void adjust_position(int cur, int next);
    uint64_t state_;
    uint64_t cur_;
    uint64_t next_;
    uint64_t count_;
    uint64_t move_count_;
    uint64_t capacity_;
    mysql_version version_;
    const unsigned char* str_;
};

void debug_tis620_sortkey(const unsigned char *str,
                          size_t len,
                          unsigned char *dst,
                          size_t dst_len,
                          int version = 1);//it is a debug function in unittest, not used it
#endif /* OB_TIS620CI_SORT_SCANNER */
