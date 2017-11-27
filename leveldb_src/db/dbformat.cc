// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

/************************************************************************/
/* 
	由 ParsedInternalKey 生成 InternalKey
	与 ParseInternalKey 是逆操作

	ParsedInternalKey 即是数据结构，由 user_key, sequence, type 组成
	InternalKey 是一个字符串，构成方式是 user_key|sequence|type
*/
/************************************************************************/
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += user_key.ToString();
  result += buf;
  return result;
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

/************************************************************************/
/* 
	比较 InternalKey:
	1.按 user_key 正序比较
	2.若 1 相等则再按 sequence number 逆序比较
	3.若 2 也相等则再按 type 的逆序比较
*/
/************************************************************************/
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);	// 取 akey 的最后 8 个字节
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8); // 取 akey 的最后 8 个字节
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  //lzh: 寻找比 tmp 大但是比 user_limit 小的字符串, 结果放入 tmp 中. 此函数的作用是缩小 key 的长度
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (user_comparator_->Compare(*start, tmp) < 0) {
    // User key has become larger.  Tack on the earliest possible
    // number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become larger.  Tack on the earliest possible
    // number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

/************************************************************************/
/* 
	lzh:
	往 dst 中依次压入: 

	索引位置			数据
	start_		--->	user_key 的长度+8 的变长编码
	kstart_		--->	user_key 的数据
						s左移八位接上 kTypeValue (s << 8 | kTypeValue)
	end_		--->	末尾位置(exclusive)
*/
/************************************************************************/
LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // A conservative estimate	//保守估计
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  dst = EncodeVarint32(dst, usize + 8);//注意返回的 dst 是经过偏移的
  kstart_ = dst;
  memcpy(dst, user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

}
