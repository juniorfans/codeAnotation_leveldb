// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include <stdio.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/logging.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;          // Either Finish() or Abandon() has been called.

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //

  // Invariant: r->pending_index_entry is true only if data_block is empty.
  //lzh: 当且仅当 data_block 是空时 pending_index_entry 是 true
  bool pending_index_entry;

  //lzh: 用于处理加入到 index block
  BlockHandle pending_handle;  // Handle to add to index block	

  std::string compressed_output;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;

  //lzh: 注意因为  r->last_key 可能是 r->num_entries 为零时通过 FindShortestSeparator 计算出来的, 所以要判断 r->num_entries
  if (r->num_entries > 0) {
	  //lzh: 以递增的顺序加入
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  //lzh: pending_index_entry 的意思是: 当前块 r->data_block 满了, 已经 Flush 掉, 当前需要写入那个块的 index 数据
  //lzh: 最开始 pending_index_entry 是 false, 执行完 Flush 之后 pending_index_entry 才会是 true
  if (r->pending_index_entry) {
    assert(r->data_block.empty());

	//lzh: last_key 是上一个 key. 若重新开始一个 block 则 last_key 的值并非是上一个 block 的最后一个 key
	//lzh: 而是寻找到一个位于区间 (last_key, key) 之间的最小值(长度必小于 last_key), 将此值加入 index_block. 
	//lzh: 这样做的目的在于减少了 index_block 中 key 的长度

    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;

	//lzh: pending_handle 中的 offset/size 会在 WriteBlock 中被更改. 
	//lzh: handle_encoding 中即是当前块 offset&size 信息
    r->pending_handle.EncodeTo(&handle_encoding);

	//lzh: 将 (k, v) 写入 index_block, k=r->last_key, v=handle_encoding
	//lzh: 这样做的好处是, 搜索一个 key 时立即就能从 index_block 中根据二分查找法找到小于 key 的最大 k,
	//lzh: k 对应的 handle_encoding 就指示了 key 所在的块的 offset 和 size.
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();

  //lzh: 当前块 datablock 的大小到达阈值, 执行 flush. flush 函数中会设置下次需要写入此块的 index block
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;

  //lzh: 当前块还没有执行 Flush, 此块的 pending_index_entry 必然还是 false
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;	//lzh: 写入块完成后, 后续需要收集此块的 index 数据(在 TableBuilder::Add 做)
    r->status = r->file->Flush();
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }

  //lzh: 设置当前块的 index 数据. 
  //lzh: 首个 block 的 offset 为 0, size 为 block_contents.size(), 注意 size 不含 trailer 大小
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());

  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {

	  //lzh: 每个block后面都会有5个字节的trailer。
	  //lzh: 第 1 个字节表示 block 内的数据是否采用了压缩, 后面 4个字节是 block 数据的 crc 校验码. 参见 资源文件 sst_and_block.bmp 文件
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;	//下一个 block 的 offset 应该是上个块的 offset 加上上个块的大小和 trailer 大小
    }
  }
  r->compressed_output.clear();
  block->Reset();
}

Status TableBuilder::status() const {
  return rep_->status;
}

/************************************************************************/
/* 
	lzh: Add 和 Flush 会依次完成所有的 data_block 文件写入, index 信息收集
	接下来 Finish 会依次写入每个 data_block 对应的 meta_balock

	1. meta_block：每个data_block对应一个meta_block ，保存data_block中的key size/value size/kv counts之类的统计信息，当前版本未实现
	2. metaindex_block: 保存meta_block的索引信息。当前版本未实现。
	3. index_block: 每个 data_block 的 offset/size 都会写入到这个 index_block 中
	4. footer: 文件末尾的固定长度的数据。保存着metaindex_block和index_block的索引信息, 为达到固定的长度，添加padding_bytes。最后有8个字节的magic校验
*/
/************************************************************************/
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;
  BlockHandle metaindex_block_handle;
  BlockHandle index_block_handle;
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    // TODO(postrelease): Add stats and other meta blocks
	//lzh: 没有对 meta_index_block 调用过 Add  函数，则说明这个 block 中没有 entry
	//lzh: 此处写入一个空的 block, 只含有 restarts 和 num_of_restarts 及 trailer. 
	//lzh: 前者是 0 表示了 entry 大小为 0, 后者是 0 表示了有 0 个 restart 点
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }
  if (ok()) {
	  //lzh: 最后一个处理的块的 index 还没有加入到 index_block 里，现在处理
    if (r->pending_index_entry) {

		//lzh: 最后一个块, 找到大于 r->last_key 的最小值. 目的同 FindShortestSeparator
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }

	//lzh: 接下来写入 index_block
    WriteBlock(&r->index_block, &index_block_handle);
  }
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;

	//lzh: 压入 metaindex_block_handle 和 index_block_handle (对齐后再加入 magic)
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}
