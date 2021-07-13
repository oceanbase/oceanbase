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

#ifndef EASY_TBNET_H_
#define EASY_TBNET_H_

#include <byteswap.h>
#include "util/easy_buf.h"
#include "io/easy_io.h"
#include <arpa/inet.h>
#include <string.h>

#define EASY_TBNET_HEADER_SIZE 16
namespace tbnet {
static easy_addr_t server_id_to_addrv(uint64_t server_id)
{
  struct sockaddr_in address;
  memset(&address, 0, sizeof(struct sockaddr_in));
  address.sin_family = AF_INET;
  address.sin_port = htons(((server_id >> 32) & 0xffff));
  address.sin_addr.s_addr = (server_id & 0xffffffff);
  return *((easy_addr_t*)&address);
}

static uint64_t addrv_to_server_id(easy_addr_t addr)
{
  struct sockaddr_in* address = (struct sockaddr_in*)(&addr);
  uint64_t s = htons(address->sin_port);
  s <<= 32;
  s |= address->sin_addr.s_addr;
  return s;
}

class IServerAdapter;
class Transport;
class DataBuffer {
public:
  DataBuffer(easy_buf_t* pb)
  {
    b = pb;
  }
  DataBuffer(easy_pool_t* p, easy_list_t* pl)
  {
    pool = p;
    l = pl;
  }
  int getDataLen()
  {
    return (b->last - b->pos);
  }
  void drainData(int len)
  {
    b->last += len;
  }
  void writeInt8(uint8_t n)
  {
    b = easy_buf_check_write_space(pool, l, 1);
    *b->last++ = (unsigned char)n;
  }
  void writeInt16(uint16_t n)
  {
    b = easy_buf_check_write_space(pool, l, 2);
    *((uint16_t*)b->last) = bswap_16(n);
    b->last += 2;
  }

  void writeInt32(uint32_t n)
  {
    b = easy_buf_check_write_space(pool, l, 4);
    *((uint32_t*)b->last) = bswap_32(n);
    b->last += 4;
  }

  void writeInt64(uint64_t n)
  {
    b = easy_buf_check_write_space(pool, l, 8);
    *((uint64_t*)b->last) = bswap_64(n);
    b->last += 8;
  }

  void writeBytes(const void* src, int len)
  {
    b = easy_buf_check_write_space(pool, l, len);
    memcpy(b->last, src, len);
    b->last += len;
  }

  void writeString(const char* str)
  {
    int len = (str ? strlen(str) : 0);

    if (len > 0)
      len++;

    b = easy_buf_check_write_space(pool, l, len + sizeof(uint32_t));
    writeInt32(len);

    if (len > 0) {
      memcpy(b->last, str, len);
      b->last += (len);
    }
  }

  void writeString(const std::string& str)
  {
    writeString(str.c_str());
  }

  uint8_t readInt8()
  {
    return ((*b->pos++) & 0xff);
  }

  uint16_t readInt16()
  {
    uint16_t n = bswap_16(*((uint16_t*)b->pos));
    b->pos += 2;
    return n;
  }

  uint32_t readInt32()
  {
    uint32_t n = bswap_32(*((uint32_t*)b->pos));
    b->pos += 4;
    assert(b->pos <= b->last);
    return n;
  }

  uint64_t readInt64()
  {
    uint64_t n = bswap_64(*((uint64_t*)b->pos));
    b->pos += 8;
    assert(b->pos <= b->last);
    return n;
  }

  bool readBytes(void* dst, int len)
  {
    if (b->pos + len > b->last) {
      return false;
    }

    memcpy(dst, b->pos, len);
    b->pos += len;
    assert(b->pos <= b->last);
    return true;
  }
  bool readString(char* str, int len)
  {
    if (str == NULL || b->pos + sizeof(int) > b->last) {
      return false;
    }

    int slen = readInt32();

    if (b->last - b->pos < slen) {
      slen = b->last - b->pos;
    }

    if (len > slen) {
      len = slen;
    }

    if (len > 0) {
      memcpy(str, b->pos, len);
      str[len - 1] = '\0';
    }

    b->pos += slen;
    assert(b->pos <= b->last);
    return true;
  }

private:
  easy_pool_t* pool;
  easy_list_t* l;
  easy_buf_t* b;
  int wl;
};

class PacketHeader {
public:
  uint32_t _chid;
  int _pcode;
  int _dataLen;
};

class Packet {
public:
  Packet()
  {
    memset(&_packetHeader, 0, sizeof(PacketHeader));
  }
  void setChannelId(uint32_t chid)
  {
    _packetHeader._chid = chid;
  }
  uint32_t getChannelId() const
  {
    return _packetHeader._chid;
  }
  void setPCode(int pcode)
  {
    _packetHeader._pcode = pcode;
  }
  int getPCode() const
  {
    return _packetHeader._pcode;
  }
  void setDataLen(int datalen)
  {
    _packetHeader._dataLen = datalen;
  }
  PacketHeader* getPacketHeader()
  {
    return &_packetHeader;
  }
  void setPacketHeader(PacketHeader* header)
  {
    if (header) {
      memcpy(&_packetHeader, header, sizeof(PacketHeader));
    }
  }
  virtual bool isRegularPacket()
  {
    return true;
  }
  void free()
  {}
  virtual ~Packet()
  {}
  virtual bool encode(DataBuffer* output) = 0;
  virtual bool decode(DataBuffer* input, PacketHeader* header) = 0;

public:
  PacketHeader _packetHeader;
  easy_request_t* r;
};

class ControlPacket : public Packet {
public:
  enum { CMD_BAD_PACKET = 1, CMD_TIMEOUT_PACKET, CMD_DISCONN_PACKET };
  ControlPacket(int c) : _command(c)
  {}
  bool isRegularPacket()
  {
    return false;
  }
  int getCommand()
  {
    return _command;
  }
  bool encode(DataBuffer* output)
  {
    return false;
  }
  bool decode(DataBuffer* input, PacketHeader* header)
  {
    return false;
  }

private:
  int _command;
};

class IOComponent {
public:
  IOComponent()
  {}
  ~IOComponent()
  {}
  void setEasyConn(easy_listen_t* l)
  {
    this->l = l;
  }

private:
  easy_listen_t* l;
};

class IPacketFactory {
public:
  IPacketFactory()
  {
    flag = 0x416e4574;
  }
  virtual ~IPacketFactory(){};
  virtual Packet* createPacket(int pcode) = 0;
  void setPacketFlag(int v)
  {
    flag = v;
  }
  int flag;
};

class IPacketStreamer {

public:
  IPacketStreamer()
  {
    _factory = NULL;
  }

  IPacketStreamer(IPacketFactory* factory)
  {
    _factory = factory;
  }

  virtual ~IPacketStreamer()
  {}

  virtual void* decode(easy_message_t* m)
  {
    int flag, chid, pcode, datalen, len;
    Packet* packet;
    uint32_t* iptr;

    // length
    if ((len = m->input->last - m->input->pos) < EASY_TBNET_HEADER_SIZE)
      return NULL;

    // header
    iptr = (uint32_t*)m->input->pos;
    flag = bswap_32(iptr[0]);
    chid = bswap_32(iptr[1]);
    pcode = bswap_32(iptr[2]);
    datalen = bswap_32(iptr[3]);

    if (flag != _factory->flag || datalen < 0 || datalen > 0x4000000) {  // 64M
      easy_error_log("stream error: %x<>%x, dataLen: %d", flag, _factory->flag, datalen);
      m->status = EASY_ERROR;
      return NULL;
    }

    len -= EASY_TBNET_HEADER_SIZE;

    if (len < datalen) {
      m->next_read_len = datalen - len;
      return NULL;
    }

    if ((packet = _factory->createPacket(pcode)) == NULL) {
      m->status = EASY_ERROR;
      return NULL;
    }

    easy_trace_log("chid=%d, pcode=%d, len=%d\n", chid, pcode, datalen);

    // decode class
    m->input->pos += EASY_TBNET_HEADER_SIZE;
    DataBuffer input(m->input);
    packet->setChannelId(chid);
    packet->setPCode(pcode);
    packet->setDataLen(datalen);

    if (packet->decode(&input, &packet->_packetHeader) == false) {
      m->status = EASY_ERROR;
      return NULL;
    }

    return packet;
  }

  virtual int encode(easy_request_t* r, void* data)
  {
    static easy_atomic_t _chid = 0;
    easy_list_t list;
    Packet* packet = (Packet*)data;
    easy_buf_t* b;
    uint32_t *dataLen, len;

    if (r->ms->c->type == EASY_TYPE_CLIENT) {
      packet->_packetHeader._chid = (int)easy_atomic_add_return(&_chid, 1);
    }

    easy_list_init(&list);
    DataBuffer output(r->ms->pool, &list);

    output.writeInt32(_factory->flag);
    output.writeInt32(packet->_packetHeader._chid);
    output.writeInt32(packet->_packetHeader._pcode);
    output.writeInt32(0);
    b = easy_list_get_last(&list, easy_buf_t, node);
    dataLen = (uint32_t*)(b->last - 4);

    if (packet->encode(&output) == false) {
      return EASY_ERROR;
    }

    len = 0;
    easy_list_for_each_entry(b, &list, node)
    {
      len += (b->last - b->pos);
    }
    len -= EASY_TBNET_HEADER_SIZE;

    (*dataLen) = bswap_32(len);
    easy_trace_log("chid=%d, pcode=%d, len=%d\n", packet->_packetHeader._chid, packet->_packetHeader._pcode, len);
    easy_request_addbuf_list(r, &list);

    return EASY_OK;
  }

protected:
  IPacketFactory* _factory;
};

class IPacketHandler {
public:
  enum HPRetCode {
    KEEP_CHANNEL = 0,    // easy_ok
    CLOSE_CHANNEL = -1,  // easy_error
    FREE_CHANNEL = -2    // easy_abort
  };
  IPacketHandler()
  {
    memset(&client_handler, 0, sizeof(easy_io_handler_pt));
    client_handler.user_data2 = (void*)this;
    client_handler.decode = on_decode;
    client_handler.encode = on_encode;
    client_handler.get_packet_id = get_packet_id;
    client_handler.process = process;
    client_handler.on_disconnect = on_disconnect;
  }
  easy_io_handler_pt* getHandler()
  {
    return &client_handler;
  }
  static void* on_decode(easy_message_t* m)
  {
    IPacketHandler* x = (IPacketHandler*)m->c->handler->user_data2;
    return x->client_streamer->decode(m);
  }
  static int on_encode(easy_request_t* r, void* data)
  {
    IPacketHandler* x = (IPacketHandler*)r->ms->c->handler->user_data2;
    return x->client_streamer->encode(r, data);
  }
  static uint64_t get_packet_id(easy_connection_t* c, void* data)
  {
    Packet* packet = (Packet*)data;
    return packet->_packetHeader._chid;
  }
  static int process(easy_request_t* r)
  {
    IPacketHandler* x = (IPacketHandler*)r->ms->c->handler->user_data2;
    Packet* packet = (Packet*)r->ipacket;

    if (packet == NULL) {  // timeout
      ControlPacket cpacket(ControlPacket::CMD_TIMEOUT_PACKET);
      cpacket.r = r;
      x->handlePacket(&cpacket, r->args);
    } else {
      packet->r = r;
      x->handlePacket(packet, r->args);
    }

    easy_session_destroy(r->ms);
    return EASY_OK;
  }
  static int on_disconnect(easy_connection_t* c)
  {
    IPacketHandler* x = (IPacketHandler*)c->handler->user_data2;
    ControlPacket cpacket(ControlPacket::CMD_DISCONN_PACKET);
    x->handlePacket(&cpacket, NULL);
    return EASY_OK;
  }
  void setPacketStreamer(IPacketStreamer* s)
  {
    client_streamer = s;
  }
  virtual ~IPacketHandler()
  {}
  virtual HPRetCode handlePacket(Packet* packet, void* args) = 0;

protected:
  IPacketStreamer* client_streamer;
  easy_io_handler_pt client_handler;
};

class PacketQueue {
public:
  PacketQueue(easy_message_t* pm)
  {
    m = pm;
  }
  int size()
  {
    return m->request_list_count;
  }
  easy_message_t* m;
};

class Connection {

public:
  Connection()
  {}
  Connection(easy_connection_t* c, easy_request_t* r)
  {
    this->addr = c->addr;
    this->c = c;
    this->r = r;
  }

  void setAddress(easy_addr_t addr)
  {
    this->addr = addr;
  }

  ~Connection()
  {}

  bool postPacket(Packet* packet, IPacketHandler* packetHandler = NULL, void* args = NULL, bool noblocking = true)
  {
    if (c == NULL) {
      easy_session_t* s = easy_session_create(0);
      s->r.opacket = packet;
      easy_session_set_args(s, args);

      if (easy_io_dispatch(addr, s) != EASY_OK) {
        easy_session_destroy(s);
        return false;
      } else {
        return true;
      }
    } else {
      r->opacket = packet;
      return true;
    }
  }

  bool isConnectState()
  {
    return (c && c->status == EASY_OK);
  }

  /**
   * serverId
   */
  uint64_t getServerId()
  {
    return addrv_to_server_id(addr);
  }

  uint64_t getPeerId()
  {
    return 0;
  }

  /**
   * localPort
   */
  int getLocalPort()
  {
    return htons(((addr.addr >> 32) & 0xffff));
  }

protected:
  easy_addr_t addr;
  easy_connection_t* c;
  easy_request_t* r;
};

class IServerAdapter {
public:
  IServerAdapter()
  {
    memset(&server_handler, 0, sizeof(easy_io_handler_pt));
    server_handler.user_data2 = (void*)this;
    server_handler.decode = on_decode;
    server_handler.encode = on_encode;
    server_handler.get_packet_id = get_packet_id;
    server_handler.process = process;
    server_handler.cleanup = cleanup;
  }
  easy_io_handler_pt* getHandler()
  {
    return &server_handler;
  }
  static void* on_decode(easy_message_t* m)
  {
    IServerAdapter* x = (IServerAdapter*)m->c->handler->user_data2;
    return x->server_streamer->decode(m);
  }
  static int on_encode(easy_request_t* r, void* data)
  {
    IServerAdapter* x = (IServerAdapter*)r->ms->c->handler->user_data2;
    return x->server_streamer->encode(r, data);
  }
  static uint64_t get_packet_id(easy_connection_t* c, void* data)
  {
    Packet* packet = (Packet*)data;
    return packet->_packetHeader._chid;
  }
  static int process(easy_request_t* r)
  {
    IServerAdapter* x = (IServerAdapter*)r->ms->c->handler->user_data2;
    Packet* packet = (Packet*)r->ipacket;
    Connection c(r->ms->c, r);
    x->handlePacket(&c, packet);
    return (r->opacket ? EASY_OK : EASY_AGAIN);
  }
  static int cleanup(easy_request_t* r, void* ipacket)
  {
    if (r) {
      if (r->ipacket)
        delete (Packet*)r->ipacket;
    } else if (ipacket) {
      delete (Packet*)ipacket;
    }

    return EASY_OK;
  }
  static int batch_process(easy_message_t* m)
  {
    IServerAdapter* x = (IServerAdapter*)m->c->handler->user_data2;
    PacketQueue pq(m);
    Connection c(m->c, NULL);
    x->handleBatchPacket(&c, pq);
    return EASY_OK;
  }
  void setBatchPushPacket(bool batch)
  {
    if (batch)
      server_handler.batch_process = batch_process;
  }
  void setPacketStreamer(IPacketStreamer* s)
  {
    server_streamer = s;
  }
  virtual ~IServerAdapter()
  {}
  virtual IPacketHandler::HPRetCode handlePacket(Connection* connection, Packet* packet) = 0;
  virtual bool handleBatchPacket(Connection* connection, PacketQueue& packetQueue)
  {
    return false;
  }

protected:
  IPacketStreamer* server_streamer;
  easy_io_handler_pt server_handler;
};

class DefaultPacketStreamer : public IPacketStreamer {

public:
  DefaultPacketStreamer()
  {}

  DefaultPacketStreamer(IPacketFactory* factory)
  {
    setPacketFactory(factory);
  }

  ~DefaultPacketStreamer()
  {}

  void setPacketFactory(IPacketFactory* factory)
  {
    _factory = factory;
  }
};

class Transport {

public:
  Transport()
  {
    _eio = easy_eio_create(NULL, 1);
  }

  ~Transport()
  {
    destroy();
  }

  bool start()
  {
    return (easy_eio_start(_eio) == EASY_OK);
  }

  bool stop()
  {
    return (easy_eio_stop(_eio) == EASY_OK);
  }

  bool wait()
  {
    return (easy_eio_wait(_eio) == EASY_OK);
  }

  IOComponent* listen(const char* spec, IPacketStreamer* streamer, IServerAdapter* serverAdapter)
  {
    easy_listen_t* l;
    IOComponent* ioc;
    serverAdapter->setPacketStreamer(streamer);
    l = easy_connection_add_listen(_eio, spec, 0, serverAdapter->getHandler());

    if (l == NULL)
      return NULL;

    ioc = (IOComponent*)easy_pool_calloc(_eio->pool, sizeof(IOComponent));
    ioc->setEasyConn(l);
    return ioc;
  }

  Connection* connect(const char* spec, IPacketStreamer* streamer, IPacketHandler* handler, int timeout = 0)
  {
    Connection* c1;

    handler->setPacketStreamer(streamer);
    easy_addr_t address = easy_inet_str_to_addr(spec, 0);

    if (easy_connection_connect(_eio, address, handler->getHandler(), timeout, NULL, 0) != EASY_OK)
      return NULL;

    c1 = new Connection();
    c1->setAddress(address);
    return c1;
  }

  void destroy()
  {
    if (_eio) {
      easy_eio_destroy(_eio);
      _eio = NULL;
    }
  }

  easy_io_t* geteio()
  {
    return _eio;
  }

private:
  easy_io_t* _eio;
};

class IPacketQueueHandler {
public:
  virtual ~IPacketQueueHandler()
  {}
  virtual bool handlePacketQueue(Packet* packet, void* args) = 0;
};

class PacketQueueThread {
public:
  PacketQueueThread()
  {
    _threadPool = NULL;
  }

  ~PacketQueueThread()
  {}

  void setThreadParameter(easy_io_t* eio, int threadCount, IPacketQueueHandler* handler)
  {
    _handler = handler;
    _threadPool = easy_thread_pool_create(eio, threadCount, requestProcessHandler, this);
  }

  static int requestProcessHandler(easy_request_t* r, void* args)
  {
    PacketQueueThread* x = (PacketQueueThread*)args;
    x->_handler->handlePacketQueue((Packet*)r->ipacket, r->args);
    return EASY_OK;
  }

  void stop()
  {}

  void start()
  {}

  void wait()
  {}

  bool push(Packet* packet, int maxQueueLen = 0, bool block = true)
  {
    return (easy_thread_pool_push(_threadPool, packet->r, (int64_t)(long)packet) == EASY_OK);
  }
  int pushQueue(PacketQueue& queue)
  {
    return easy_thread_pool_push_message(_threadPool, queue.m, (int64_t)(long)queue.m);
  }

private:
  IPacketQueueHandler* _handler;
  easy_thread_pool_t* _threadPool;
};

class ConnectionManager {
public:
  ConnectionManager(Transport* transport, IPacketStreamer* streamer, IPacketHandler* packetHandler)
  {
    _transport = transport;
    _packetHandler = packetHandler;
    _packetHandler->setPacketStreamer(streamer);

    // create client
    _cl = easy_client_create(_transport->geteio(), _packetHandler->getHandler());
  }

  ~ConnectionManager()
  {}

  void setDefaultQueueTimeout(int s, int timeout)
  {
    _timeout = timeout;
  }
  void setDefaultQueueLimit(int a, int b)
  {}

  void setDefaultPacketHandler(IPacketHandler* packetHandler)
  {
    _packetHandler = packetHandler;
  }

  bool sendPacket(uint64_t serverId, Packet* packet, IPacketHandler* packetHandler = NULL, void* args = NULL,
      bool noblocking = true)
  {
    easy_connection_t* c;
    easy_io_handler_pt* handler;
    handler = packetHandler ? packetHandler->getHandler() : _packetHandler->getHandler();
    easy_addr_t addrv = server_id_to_addrv(serverId);
    c = easy_client_get_conn(_transport->geteio(), _cl, addrv, handler, _timeout);

    if (c == NULL)
      return false;

    easy_session_t* s = easy_session_create(0);
    s->r.opacket = packet;
    easy_session_set_args(s, args);

    if (easy_session_dispatch(c, s) != EASY_OK) {
      easy_session_destroy(s);
      return false;
    } else {
      return true;
    }
  }

  Connection* getConnection(uint64_t serverId)
  {
    easy_connection_t* c;
    Connection* c1;
    easy_addr_t addrv = server_id_to_addrv(serverId);
    easy_io_handler_pt* handler = _packetHandler->getHandler();
    c = easy_client_get_conn(_transport->geteio(), _cl, addrv, handler, _timeout);

    if (c == NULL)
      return NULL;

    c1 = (Connection*)easy_pool_calloc(c->pool, sizeof(Connection));
    c1->setEasyConn(c);
    return c1;
  }

  static bool isAlive(uint64_t serverId)
  {
    return true;
  }

private:
  Transport* _transport;
  IPacketHandler* _packetHandler;
  easy_client_t* _cl;
  int _timeout;
};

}  // namespace tbnet

#endif
