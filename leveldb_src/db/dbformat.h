﻿// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_FORMAT_H_
#define STORAGE_LEVELDB_DB_FORMAT_H_

#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
//lzh: 第 0 层文件最多4个文件，到达此限制则开始 compaction
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
//lzh: 第 0 层文件到达了 8 个文件时，则开始限制写入的速度
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
//lzh: 第 0 层文件到达了 12 个文件时开始暂停写入
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
//lzh: 在将 memtable 写到磁盘上时，并非一定会写到第 0 层，因为这可能导致
//lzh: 第 0 层文件与第 1 层文件有较大的重叠，增加后面 compaction 负担，
//lzh: 所以可以跳过一些层次，直接写到更高层次的文件中。
//lzh: kMaxMemCompactLevel 就指示了，从 memtable 写入到最高什么层次
static const int kMaxMemCompactLevel = 2;

}

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
//最大值只占 7 个字节是因为要留下一个字节作为 type ，它们组成固长的 8 字节编码
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1);

//lzh: 将用户的 key 结合数据库版本(每次对数据库写入/删除都会更新版本)和数据类型一起作为内部使用的 key
struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
//返回 internal key 里面的 user key
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);	//internal_key.size() - 8 等于 key 的 size
}

//从 internal key 里面解析出 type
inline ValueType ExtractValueType(const Slice& internal_key) {
  assert(internal_key.size() >= 8);	//internal_key 的 size = key size  + type size(8)
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<ValueType>(c);
}

//lzh: [增加代码] 从 internal key 里面解析出 sequence number
inline uint64_t ExtractSequenceNumber(const Slice& internal_key) {
	assert(internal_key.size() >= 8);	//internal_key 的 size = key size  + type size(8)
	const size_t n = internal_key.size();
	uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
	return num >> 8;
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { }
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
// lzh: 与 ParsedInternalKey 本质上是一个东西，只不过 InternalKey 是将 ParsedInternalKey 
// 的三个成员按规则编码成一个字符串后的东西
class InternalKey {
 private:
  std::string rep_;
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  uint64_t sequence_number() const { return ExtractSequenceNumber(rep_); }

  ValueType value_type() const{return ExtractValueType(rep_); }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }
};

inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

/************************************************************************/
/* 
	lzh: internal_key 的格式为 user_key|sequence|type
*/
/************************************************************************/
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);	//lzh: 最后 8 个字节(64位)是 sequence + type
  unsigned char c = num & 0xff;	//lzh: 最后 1 个字节(8位)是 type
  result->sequence = num >> 8;	//lzh: 前面 7 个字节(56位)是 sequence
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);	//除了最后 8  个字段外，前面都是 user_key
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
/************************************************************************/
/* 
	lzh: start_, kstart_, end_ 这三个是用于索引的地址,

	start						kstart													  end
	|							|														  |
	userkey_len (varint32)		userkey_data (userkey_len)		SequnceNumber拼接ValueType

对memtable 进行lookup时使用 [start,end], 对sstable lookup时使用[kstart, end]。
*/
/************************************************************************/
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  ~LookupKey();

  //lzh: 返回 [start_, end_), 即整个数据 userkey_len, userkey_data, SequnceNumber拼接ValueType
  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  //lzh: 返回 [kstart_, end_), 即 userkey_data, SequnceNumber拼接ValueType. 与 InternalKey 的定义是一样的
  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  //lzh: 返回 [kstart_, end_-8), 即 userkey_data. 与 user key 的定义是一样的
  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;	//lzh: exclusive
  char space_[200];      // Avoid allocation for short keys

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}

#endif  // STORAGE_LEVELDB_DB_FORMAT_H_
