// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

/************************************************************************/
/* 
	lzh: 加入 (key, value) 到 block 中 ->
		1. 一个 block 包含多个 entry
		2. 一个 entry 包含一个 (key, value), 但是为了压缩, 后面的 entry 与前面的 entry 采用 diff 方式记录.
		3. 具体而言: leveldb对key的存储进行前缀压缩，每个entry中会记录key与前一个key前缀相同的字节（shared_bytes）以及自己独有的字节（unshared_bytes）
			读取时，对block进行遍历，每个key根据前一个key以及shared_bytes/unshared_bytes可以构造出来
		4. 按 block 内的 entry 完全采用 diff 方式记录, 则像链表一样: 要随机地解析任意一个 entry 都需要从第一个开始, 所以优化为: 每 block_restart_interval 
			个 entry 采用 diff 方式记录.
		
		5. 由于 Add 进来的 key 是按递增顺序加入的, 所以后一个 key 与前一个有较高的重叠概率

		6. block 最后的五个字节(1字节表示是否压缩后4字节为 crc 校验码)是在完成一个 block 时写入的. 在 table_build.cc 中

	lzh: 结构图详见本工程的资源文件: sst_and_block.bmp
*/
/************************************************************************/
void BlockBuilder::Add(const Slice& key, const Slice& value) {
	//lzh: last_key  即是上一个 key
  Slice last_key_piece(last_key_);
  assert(!finished_);

  //lzh: counter_ 一旦为 block_restart_interval 将会重新开始新一轮, 被设置为 0
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;

  //lzh: 当前处于一个 diff 记录中
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());

	//lzh: last_key_piece 指上一个 entry 的 key. 计算当前的 key 与它有多长的重叠
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
	  //lzh: 完成一个区间的 diff 记录, 记录当前一轮的 entry 的大小
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  //lzh: buffer_ 的格式为: 相同的字节数 | 不同的字节数 | value 字节数 | key 中不同的内容 | value 内容
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  //lzh: 下面两步计算完全是为了验证 last_key_ 是否和 key 一样
  last_key_.resize(shared);	//lzh: last_key_ 此时的值是上一个 key, 而 last_key_ 的前 shared 个字节与 key 的前 shared 个字节是一样的
  last_key_.append(key.data() + shared, non_shared);	//lzh: 将 key 的后面 non_shared 字节附到 last_key_ 后面, 
  assert(Slice(last_key_) == key);		//lzh: last_key 将与 key 一样
  counter_++;
}

}
