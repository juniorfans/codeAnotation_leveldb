// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted	//加 5 是因为32位整数只有四个字节，故变长编码
										//始终小于 5 个字节
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
/************************************************************************/
/* 
	将词的长度进行变长编码，将此结果作为词头与词合并返回。
	这相当于是对 Slice 的编码
*/
/************************************************************************/
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }	//lzh: 使用 table 去构造 iter_

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

/************************************************************************/
/* 
	keysize+8 | key | sequence_num | type | valuesize | value
	
	[  var32  ]     [        fix64        ][   var32  ]
*/
/************************************************************************/
void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);	//将 internal_key_size 的变长编码加入 buf，返回 buf 紧贴的后续字节地址
  memcpy(p, key.data(), key_size);					//将 key 拷入 buf 后
  
p += key_size;									//调整 buf 紧贴的后续字节的地址
  EncodeFixed64(p, (s << 8) | type);				//将 SequenceNumber s 和 type 连接起来(type 当成一个字节)进行固定长度编码，一并拷入 buf 后续
  p += 8;											//调整 buf 紧贴的后续字节的地址,64位，8 个字节
  p = EncodeVarint32(p, val_size);					//将 value 的长度进行变长编码拷入 buf 后续，返回 buf 紧贴的后续字节地址
  memcpy(p, value.data(), val_size);				//将 value 拷入 buf 后续
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);

  //lzh: 首个不小于 memkey 的节点
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]	//lzh: 此处应该是 char[0,klength-8)
    //    tag      uint64			//lzh: 此处是 char[klength-8,klength)
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.

    const char* entry = iter.key();//lzh: skiplist 里面存储的 key，它将所有的信息都存储进去，包括 sequencenum , type
    uint32_t key_length;

	//lzh: 前面几个字节(最多四个)存储着 key 的变长编码，它的值被放入 key_length
	//lzh: key_length 指示了 entry 后面紧接着存放的 userkey 的长度
	//lzh: key_ptr 是 key_length 变长编码的结束(exclusive)，也即 userkey 的起始位置
	const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);	
	
    if (comparator_.comparator.user_comparator()->Compare(
			//lzh: 取出 userkey. 此处存储的应该是 InternalKey, 格式为 user_key|sequence|type, 后面 8 位是非数据部分
            Slice(key_ptr, key_length - 8),	//key_length 是 internal_key_size，最后面的 8 个字节是 sequence key 加上 type 的8 字节固长编码，
											//减 8 个字节 后是 key 的大小
            key.user_key()) == 0) {
      // Correct user key

	  //lzh: 固定编码 sequence|type
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);

      switch (static_cast<ValueType>(tag & 0xff)) //取最后一字节的值
	  {
        case kTypeValue: {
			//lzh: key_ptr + key_length 即是 tag 结束的地址，也即是 value 长度和 value 开始的位置
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}
