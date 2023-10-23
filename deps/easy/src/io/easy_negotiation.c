#include "io/easy_negotiation.h"
#include "io/easy_log.h"
#include "io/easy_socket.h"
#include <sys/socket.h>
#include <sys/ioctl.h>

const uint64_t g_support_eio_maigc[] = {
    0x12567348667799aa,
    0x1237734866785431,
    0x5933893228167181,
    0x6683221dd298cc23,
};

const int g_support_eio_maigc_num = sizeof(g_support_eio_maigc) / sizeof(g_support_eio_maigc[0]);

static int easy_magic_in_support_list(uint64_t magic)
{
    int ret = 0;
    int i = 0;
    for (i = 0; i < g_support_eio_maigc_num; i++) {
        if (magic == g_support_eio_maigc[i]) {
            return 1;
        }
    }

    return ret;
}


extern char *easy_connection_str(easy_connection_t *c);

static int easy_encode_uint64(char *buf, const uint64_t buf_len, int64_t *pos, uint64_t val)
{
  int ret = ((NULL != buf) &&
             ((buf_len - *pos) >= (uint64_t)(sizeof(val)))) ? EASY_OK : EASY_ABORT;

  if (EASY_OK == ret) {
    *(buf + (*pos)++) = (char)(((val) >> 56) & 0xff);
    *(buf + (*pos)++) = (char)(((val) >> 48) & 0xff);
    *(buf + (*pos)++) = (char)(((val) >> 40) & 0xff);
    *(buf + (*pos)++) = (char)(((val) >> 32) & 0xff);
    *(buf + (*pos)++) = (char)(((val) >> 24) & 0xff);
    *(buf + (*pos)++) = (char)(((val) >> 16) & 0xff);
    *(buf + (*pos)++) = (char)(((val) >> 8) & 0xff);
    *(buf + (*pos)++) = (char)((val) & 0xff);
  }

  return ret;
}

static int easy_encode_uint16(char *buf, const uint64_t buf_len, int64_t *pos, uint16_t val)
{
    int ret = ((NULL != buf) &&
               ((buf_len - *pos) >= (uint64_t)(sizeof(val)))) ? EASY_OK : EASY_ABORT;

    if (EASY_OK == ret) {
      *(buf + (*pos)++) = (char)((((val) >> 8)) & 0xff);
      *(buf + (*pos)++) = (char)((val) & 0xff);
    }

    return ret;
}

static int easy_decode_uint64(char *buf, const int64_t data_len, int64_t *pos, uint64_t *val)
{
  int ret = (NULL != buf && data_len - *pos  >= 8) ? EASY_OK : EASY_ERROR;

  if (EASY_OK == ret) {
    *val = (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 56;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 48;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 40;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 32;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 24;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 16;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff)) << 8;
    *val |= (((uint64_t)((*(buf + (*pos)++))) & 0xff));
  }

  return ret;
}

static int easy_decode_uint16(char *buf, const int64_t data_len, int64_t *pos, uint16_t *val)
{
  int ret = (NULL != buf && data_len - *pos  >= 2) ? EASY_OK : EASY_ERROR;

  if (ret == EASY_OK) {
    *val = (uint16_t)(((*(buf + (*pos)++)) & 0xff) << 8);
    *val = (uint16_t)(*val | (*(buf + (*pos)++) & 0xff));
  }

  return ret;
}

static int easy_encode_negotiation_msg(easy_negotiation_msg_t *ne_msg, char *buf, int buf_len, int64_t *encode_len)
{
    int ret = EASY_OK;
    int64_t pos = 0;

    if (NULL == ne_msg || NULL == buf || NULL == encode_len) {
        easy_error_log("easy_encode_negotiation_msg, invalid param!");
        return EASY_ERROR;
    }

    ret = easy_encode_uint64(buf, buf_len, &pos, ne_msg->msg_header.header_magic);
    if (ret != EASY_OK) {
        easy_error_log("send negotiation msg,  encode header magic failed!");
        return ret;
    }

    ret = easy_encode_uint16(buf, buf_len, &pos, ne_msg->msg_header.msg_body_len);
    if (ret != EASY_OK) {
        easy_error_log("send negotiation msg, encode msg body len failed!");
        return ret;
    }

    ret = easy_encode_uint64(buf, buf_len, &pos, ne_msg->msg_body.eio_magic);
    if (ret != EASY_OK) {
        easy_error_log("send negotiation msg, encode eio magic  failed!");
        return ret;
    }

    buf[pos++] = ne_msg->msg_body.io_thread_index;
    *encode_len = pos;

    return ret;
}

static int easy_decode_negotiation_msg(easy_negotiation_msg_t *ne_msg, char *recv_buf, int64_t recv_buf_len, int64_t *decode_len)
{
    int ret = EASY_OK;
    int64_t pos = 0;

    if (NULL == ne_msg || NULL == recv_buf || NULL == decode_len) {
        easy_error_log("easy_decode_negotiation_msg, invalid param!");
        return EASY_ERROR;
    }

    ret = easy_decode_uint64(recv_buf, recv_buf_len, &pos, &(ne_msg->msg_header.header_magic));
    if (ret != EASY_OK) {
        easy_info_log("easy decode header magic failed!ret:%d.", ret);
        return ret;
    }

    ret = easy_decode_uint16(recv_buf, recv_buf_len, &pos, &(ne_msg->msg_header.msg_body_len));
    if (ret != EASY_OK) {
        easy_info_log("decode msg body len failed!ret:%d.", ret);
        return ret;
    }

    ret = easy_decode_uint64(recv_buf, recv_buf_len, &pos, &(ne_msg->msg_body.eio_magic));
    if (ret != EASY_OK) {
        easy_info_log("decode eio magic failed!ret:%d.", ret);
        return ret;
    }

    pos++; //for io thread index
    *decode_len = pos;

    return ret;
}

/*
* easy negotiation packet format
PACKET HEADER:
+------------------------------------------------------------------------+
|         negotiation packet header magic(8B)  | msg body len (2B)
+------------------------------------------------------------------------+

PACKET MSG BODY:
+------------------------------------------------------------------------+
|   io thread corresponding eio magic(8B) |  io thread index (1B)
+------------------------------------------------------------------------+
*/

int net_send_negotiate_message(uint8_t negotiation_enable, int fd, uint64_t magic, int8_t index, uint32_t *conn_has_error)
{
    int ret = EASY_OK;
    int64_t encode_len = 0;
    easy_negotiation_msg_t ne_msg;
    memset(&ne_msg, 0, sizeof(ne_msg));

    ne_msg.msg_header.header_magic = EASY_NEGOTIATION_PACKET_HEADER_MAGIC;
    ne_msg.msg_header.msg_body_len = sizeof(easy_negotiation_msg_body_t);
    ne_msg.msg_body.eio_magic = magic;
    ne_msg.msg_body.io_thread_index = index;

    easy_addr_t addr = easy_inet_getpeername(fd);
    char addr_str[32];
    easy_inet_addr_to_str(&addr, addr_str, 32);
    if (negotiation_enable) {
        if (magic) {
            int send_bytes = 0;
            const int MAX_SEND_LEN = 20;
            char buf[MAX_SEND_LEN];
            memset(buf, 0, sizeof(buf));

            ret = easy_encode_negotiation_msg(&ne_msg, buf, MAX_SEND_LEN, &encode_len);
            if (ret != EASY_OK) {
                easy_error_log("easy encode negotiation msg failed!ret:%d, fd:%d, addr: %s", ret, fd, addr_str);
                return ret;
            }

            if ((ret = easy_socket_error(fd) ) != 0) {
                easy_info_log("retry! socket status is abnormal yet, fd:%d, addr:%s, err:%d.", fd, addr_str, ret);
                if (conn_has_error) *conn_has_error = 1;
                return ret;
            }

            while ((send_bytes = send(fd, buf, encode_len, 0)) < 0 && errno == EINTR);

            if (send_bytes != encode_len) {
                easy_warn_log("send negotiate msg failed. addr: %s, errno:%s", addr_str, strerror(errno));
                return -1;
            } else {
                easy_info_log("eio:%#llx, thread index:%hhu, send:%d bytes,  send negotiation success. addr:%s!", (unsigned long long)ne_msg.msg_body.eio_magic,
                ne_msg.msg_body.io_thread_index, encode_len, addr_str);
            }
        }
    } else {
        easy_info_log("negotiation not enabled!(addr:%s)", addr_str);
    }

    return ret;
}

void net_consume_negotiation_msg(int fd, uint64_t magic)
{
    int ret = EASY_OK;
    const int64_t recv_buf_len = 1 * 1024;
    int rcv_bytes = 0;
    int64_t decode_len = 0;
    easy_negotiation_msg_t ne_msg;
    char recv_buf[recv_buf_len];

    memset(&ne_msg, 0, sizeof(ne_msg));
    memset(recv_buf, 0, sizeof(recv_buf_len));

    while ((rcv_bytes = recv(fd, (char *) recv_buf, sizeof(recv_buf), MSG_PEEK)) < 0 && EINTR == errno);

    if (rcv_bytes <= 0) {
        easy_info_log("no data to read, fd:%d, errno:%d", fd, errno);
        return;
    } else {
        easy_info_log("easy_consume_negotiation_msg, read %d bytes msg!", rcv_bytes);

        ret = easy_decode_negotiation_msg(&ne_msg, recv_buf, rcv_bytes, &decode_len);
        if (ret != EASY_OK) {
            easy_error_log("easy decode negotiation msg failed!ret:%d", ret);
            return;
        }

        if (ne_msg.msg_header.header_magic == EASY_NEGOTIATION_PACKET_HEADER_MAGIC &&
                (ne_msg.msg_body.eio_magic == magic || easy_magic_in_support_list(ne_msg.msg_body.eio_magic))) {
            while ((rcv_bytes = recv(fd, (char *) recv_buf, decode_len, 0)) < 0 && errno == EINTR);
            if (rcv_bytes <= 0) {
                easy_error_log("consume negotiaion buffer failed! fd:%d, errno:%d", fd, errno);
            } else {
                easy_info_log("consume negotiation buffer %d bytes!", rcv_bytes);
            }
        } else {
            easy_info_log("not negotiation msg ! return!");
        }
    }

    return;
}

int easy_send_negotiate_message(easy_connection_t *c)
{
  int ret = EASY_OK;
  easy_io_thread_t *ioth = c->ioth;
  easy_io_t *eio = ioth->eio;
  uint32_t conn_has_error = 0;
  if ((ret = net_send_negotiate_message(eio->negotiation_enable,
                                        c->fd,
                                        eio->magic,
                                        ioth->idx,
                                        &conn_has_error)) == EASY_OK) {
      c->is_negotiated = 1;
  }
  if (conn_has_error) {
      c->conn_has_error = conn_has_error;
  }
  return ret;
}

void easy_consume_negotiation_msg(int fd, easy_io_t *eio)
{
  net_consume_negotiation_msg(fd, eio->magic);
}
