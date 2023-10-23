/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

typedef struct easy_head_t
{
  uint32_t magic_;
  uint32_t len_;
  uint32_t pkt_id_;
  uint32_t reserved_;
} easy_head_t;

#define rk_bswap32  __builtin_bswap32
#if PNIO_ENABLE_CRC
static uint32_t calc_crc(const void* b, size_t len)
{
  return fasthash64(b, len, 0);
}
#endif

#define EASY_HEADER_MAGIC 0xcedbdb01
inline void eh_set(easy_head_t* h, uint32_t len, uint32_t pkt_id)
{
  h->magic_ = EASY_HEADER_MAGIC;
  h->len_ = rk_bswap32(len);
  h->pkt_id_ = rk_bswap32(pkt_id);
  h->reserved_ = 0;
}

inline uint64_t eh_packet_id(const char* b)
{
  return rk_bswap32(((easy_head_t*)b)->pkt_id_);
}

static int64_t eh_decode(char* b, int64_t s)
{
  int64_t bytes = sizeof(easy_head_t);
  if (s >= bytes) {
    easy_head_t* h = (typeof(h))b;
    uint32_t len = rk_bswap32(h->len_);
    const uint32_t max_size = INT32_MAX;
    if (h->magic_ != EASY_HEADER_MAGIC || len > max_size) {
      int err = PNIO_ERROR;
      bytes = -1;
      rk_warn("unexpected packet, magic=%x, len=%x,pkt_id=%x, reserved=%x", h->magic_, h->len_, h->pkt_id_, h->reserved_);
    } else {
      bytes += len;
      PNIO_CRC(assert(s < bytes || h->reserved_ == calc_crc(b + sizeof(easy_head_t), bytes - sizeof(easy_head_t))));
    }
  }
  return bytes;
}

static void eh_copy_msg(str_t* m, uint32_t pkt_id, const char* b, int64_t s)
{
  easy_head_t* h = (typeof(h))m->b;
  m->s = s + sizeof(*h);
  eh_set(h, s, pkt_id);
  if ((uint64_t)(h+1) != (uint64_t)b) {
    memcpy((void*)(h + 1), b, s);
  }
  PNIO_CRC(h->reserved_ = calc_crc(b, s));
}
