#ifndef EASY_NEGOTIATION_H_
#define EASY_NEGOTIATION_H_

#include "easy_define.h"
#include "io/easy_io_struct.h"

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


EASY_CPP_START

#define EASY_NEGOTIATION_PACKET_HEADER_MAGIC (0x1234567877668833)

#pragma pack(1)
typedef struct easy_negotiation_msg_t easy_negotiation_msg_t;
typedef struct easy_negotiation_msg_header_t easy_negotiation_msg_header_t;
typedef struct easy_negotiation_msg_body_t easy_negotiation_msg_body_t;


struct easy_negotiation_msg_header_t {
    uint64_t header_magic;
    uint16_t msg_body_len;
};

struct easy_negotiation_msg_body_t {
    uint64_t eio_magic;
    uint8_t  io_thread_index;
};

struct easy_negotiation_msg_t {
    easy_negotiation_msg_header_t msg_header;
    easy_negotiation_msg_body_t msg_body;
};

#pragma pack()

#define EASY_NEGOTIATION_PACKET_LEN (sizeof(easy_negotiation_msg_t))

int net_send_negotiate_message(uint8_t negotiation_enable, int fd, uint64_t magic,
                                int8_t index, uint32_t *conn_has_error);
void net_consume_negotiation_msg(int fd, uint64_t magic);
int easy_send_negotiate_message(easy_connection_t *c);
void easy_consume_negotiation_msg(int fd, easy_io_t *eio);

EASY_CPP_END

#endif
