// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

	//因为用到了 memcpy 函数，涉及到内存字节的拷贝。
	//当整数以小端模式存储时，即高字节存放在高位，低字节存放在低位，可以直接拷贝
	//而当以大端模式存储时，高字节在低位，低字节在高位，若直接拷贝，则字符串中高字节存放的就是整数中的低字节，反之。会造成乱序。
	//故当以大端模式存储时，需要做运算，算出低字节的值，放到低地址，算出高字节的值，放到高地址。
void EncodeFixed32(char* buf, uint32_t value) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
  memcpy(buf, &value, sizeof(value));
#else
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
#endif
}

	//同 EncodeFixed32 原理，见上注释
void EncodeFixed64(char* buf, uint64_t value) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
  memcpy(buf, &value, sizeof(value));
#else
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
#endif
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

/*
	整数的变长编码，以节省。通常一个整数在计算机中占 4 个字节，但其中可能有很多浪费。
	小于256 的正整数应该只需要1个字节就可以表示。
	小于 256 * 256 - 1 的正整数只需要 2 个字节就可以表示。。。

	编码方式是：将字节的第一位用作标记位，后面的 7 位用作数据存储位。
	每一个字节的第一位若为 0 ，则表示后面一个字节属于编码。否则不属于。
	
	编码得到的 char *，是一个字节序列。编码结束的标志是最后一个个字节的第一位为 0.

	变长编码的精髓在于字节使用了一位标志位指示编码是继续还是停止。

	如 131 (10000011) 编码为 10000011  00000001
	第一个字节是 131 的低 7 位的编码,第二字节是剩余位的编码。
	第一字节的左边的 1 表示编码并末终结于该字节，后面还有字节。右边的两个 1 是数据
	第二个字节的最左边1位为 0 表示编码结束，最右边的 1 表示数据。这个 1 是高位上的 1.

	变长编码和固定编码各有好处。
	固定编码的空间利用率高于变长编码，这是因为变长编码在每个字节的第一位
	都使用了一位用于符号位。
	如在固定编码中 131 只占用一个字节，而在变长编码中占用两个字节。

	变长编码的好处是，面对一个编码结果，不需要其他信息就可以恢复出原整数。而因定编码却不能。因为固定编码
	无法知道编码是否结束了。可能你会反驳，说，我们只需要用一个字符串（以 \0 结尾）作为固定编码的输出，便可
	知道编码是否结束。对这个反驳进行反驳的一个直接解答是，\0 也就是 0，而在固定编码中，任意一个字节都有可能
	是 0；如，对整数 10000000 00000000 00000001	进行固定编码，中间一个字节的结果就为 0，如果用一个字符串来
	存储这个结果，则会发生“提前截断错”，导致错误的结果

	变长编码还有一个额外的好处，变长编码的结果刚好可以用带 \0 的字符串来存储，这是因为编码的结果结束于最后一个
	字节，其首位为 0，也即，若字符串(\0)结束，则编码一定结束。
*/
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);//ptr - buf 是 buf 字符串的长度
}

/*
	相当于把 slice 压缩。原来 slice 存储了字符串 data 和其长度 size。
	现在把 size 变长编码，后面附上 data 一起放在 string 里面
*/
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

/*
	计算一个正整数的变长编码的字节数
*/
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

/************************************************************************/
/* 
	将32位整数的变长编码的结果解析出来
*/
/************************************************************************/
const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();	//因业务的需求，p 的后面加上了某个后缀，现需要删除
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;
  p = GetVarint32Ptr(p, limit, &len);
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len);
  return p + len;
}

bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}
