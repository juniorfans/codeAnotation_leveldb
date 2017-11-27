// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static const int kTargetFileSize = 2 * 1048576;

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static const int64_t kMaxGrandParentOverlapBytes = 10 * kTargetFileSize;

static double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = 10 * 1048576.0;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(int level) {
  return kTargetFileSize;  // We could vary per level to reduce number of files?
}

namespace {
std::string IntSetToString(const std::set<uint64_t>& s) {
  std::string result = "{";
  for (std::set<uint64_t>::const_iterator it = s.begin();
       it != s.end();
       ++it) {
    result += (result.size() > 1) ? "," : "";
    result += NumberToString(*it);
  }
  result += "}";
  return result;
}
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

/************************************************************************/
/* 
	lzh: 使用二分法找出 files(正序排列) 中首个 largest >= key 的 file
	注意边界条件，举例说明
	若 files[0].min > key 则返回 0, 逻辑上是正确的
	若 files[0].largest < key 则返回 files.size()，表示没有找到.
	因此该函数的返回值这样去描述：
	返回找到 files 中首个 largest 大于 key 的 index. 若不存在则返回 files.size()
	
*/
/************************************************************************/
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}


/************************************************************************/
/* 
	lzh: 判断 files 的 key 有没有落在区间: [smallest_user_key, largest_user_key]
	lzh: (a, b) 与 (x, y) 相交的判定条件是：y>=a && b>=x
*/
/************************************************************************/
bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& files,
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  // Find the earliest possible internal key for smallest_user_key
  InternalKey small(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);

  //lzh: 从 files 中找到首个 largest_key 大于 small 的 file
  //lzh: 注意当 files 的最大 key 小于 small 时，index 的值为 files.size(). 后面会判断这一结果为不相交
  const uint32_t index = FindFile(icmp, files, small.Encode());

  return ((index < files.size()) &&
          icmp.user_comparator()->Compare(
              largest_user_key, files[index]->smallest.user_key()) >= 0);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// If "*iter" points at a value or deletion for user_key, store
// either the value, or a NotFound error and return true.
// Else return false.
static bool GetValue(Iterator* iter, const Slice& user_key,
                     std::string* value,
                     Status* s) {
  if (!iter->Valid()) {
    return false;
  }
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(iter->key(), &parsed_key)) {
    *s = Status::Corruption("corrupted key for ", user_key);
    return true;
  }
  if (parsed_key.user_key != user_key) {
    return false;
  }
  switch (parsed_key.type) {
    case kTypeDeletion:
      *s = Status::NotFound(Slice());  // Use an empty error message for speed
      break;
    case kTypeValue: {
      Slice v = iter->value();
      value->assign(v.data(), v.size());
      break;
    }
  }
  return true;
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files);
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
          tmp.push_back(f);
        }
      }
      if (tmp.empty()) continue;

      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      files = &tmp[0];
      num_files = tmp.size();
    } else {
      // Binary search to find earliest index whose largest key >= ikey.
      uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
      if (index >= num_files) {
        files = NULL;
        num_files = 0;
      } else {
        tmp2 = files[index];
        if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
          // All of "tmp2" is past any data for user_key
          files = NULL;
          num_files = 0;
        } else {
          files = &tmp2;
          num_files = 1;
        }
      }
    }

    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != NULL && stats->seek_file == NULL) {
        // We have had more than one seek for this read.  Charge the 1st file.
        stats->seek_file = last_file_read;
        stats->seek_file_level = last_file_read_level;
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Iterator* iter = vset_->table_cache_->NewIterator(
          options,
          f->number,
          f->file_size);
      iter->Seek(ikey);
      const bool done = GetValue(iter, user_key, value, &s);
      if (!iter->status().ok()) {
        s = iter->status();
        delete iter;
        return s;
      } else {
        delete iter;
        if (done) {
          return s;
        }
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice& smallest_user_key,
                             const Slice& largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, files_[level],
                               smallest_user_key,
                               largest_user_key);
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("['");
      AppendEscapedStringTo(&r, files[i]->smallest.Encode());
      r.append("' .. '");
      AppendEscapedStringTo(&r, files[i]->largest.Encode());
      r.append("']\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
    for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    EscapeString(prev_end.Encode()).c_str(),
                    EscapeString(this_begin.Encode()).c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}


/************************************************************************/
/* 
	lzh: 在currentversion上应用指定的VersionEdit，生成新的MANIFEST信息，保存到磁盘上，并用作current version

*/
/************************************************************************/
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
	//lzh: 1.edit设置log number等4个计数器
	//lzh: 保证 edit 的 log number 是比较大的那个，否则就是致命错误。
	//lzh: 保证 edit 的 log number 小于next file number，否则就是致命错误
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  //lzh: 2.创建一个新的Version v，并把新的edit变动保存到v中。
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }

  //lzh: 3.计算 v 的执行 compaction 的最佳 level
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  //lzh: 如果MANIFEST文件指针不存在，就创建并初始化一个新的MANIFEST文件。
  //lzh: 这只会发生在第一次打开数据库时。这个MANIFEST文件保存了current version的快照
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
	  //lzh: 这里不需要unlock *mu因为我们只会在第一次调用LogAndApply时才走到这里(打开数据库时).  
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  //lzh: 向MANIFEST写入一条新的log，记录current version的信息。
  //lzh: 在文件写操作时unlock锁，写入完成后，再重新lock，以防止浪费在长时间的IO操作上。
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);	//lzh: append到MANIFEST log中
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
	//lzh: 如果刚才创建了一个MANIFEST文件，通过写一个指向它的CURRENT文件  
	//lzh: 安装它；不需要再次检查MANIFEST是否出错，因为如果出错后面会删除它 
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  //lzh: 安装这个 version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;

	/************************************************************************/
	/* 
		lzh: 循环读取一个一个的 record
	*/
	/************************************************************************/
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;

	  /************************************************************************/
	  /* 
		record 解析放于 edit 成员中, 如 has_comparator_, comparator_ 等
	  */
	  /************************************************************************/
      s = edit.DecodeFrom(record);

      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + "does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
  }

  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}


/************************************************************************/
/* 
	lzh: 把currentversion保存到*log中，信息包括comparator名字、compaction点和各级sstable文件，函数逻辑很直观
*/
/************************************************************************/
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

/************************************************************************/
/* 
lzh: 各个 level 上均有一个 versions 列表, 各个 version 有多个文件，取它们的 id 
*/
/************************************************************************/
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
	//lzh: dummy_versions_ is double linked, 所以下面的代码看起来有点奇怪
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      GetOverlappingInputs(level+1, f->smallest, f->largest, &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void VersionSet::GetOverlappingInputs(
    int level,
    const InternalKey& begin,
    const InternalKey& end,
    std::vector<FileMetaData*>* inputs) {
  inputs->clear();
  Slice user_begin = begin.user_key();
  Slice user_end = end.user_key();
  const Comparator* user_cmp = icmp_.user_comparator();
  for (size_t i = 0; i < current_->files_[level].size(); i++) {
    FileMetaData* f = current_->files_[level][i];
    if (user_cmp->Compare(f->largest.user_key(), user_begin) < 0 ||
        user_cmp->Compare(f->smallest.user_key(), user_end) > 0) {
      // Either completely before or after range; skip it
    } else {
      inputs->push_back(f);
    }
  }
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  // lzh: 非第 0 层文件总是有两个文件集合要 compact. c[0], c[1]
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
		//lzh: level 层要 compact的是 c[0], level+1 层的是 c[1]. c->level()+which 即是要 compact 的层数
      if (c->level() + which == 0) {	//lzh: 要 compact 的是第 0 层.
        const std::vector<FileMetaData*>& files = c->inputs_[which];	
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(
              options, files[i]->number, files[i]->file_size);
        }
      } else {	//lzh: 非第 0 层
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if (size_compaction) {
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    c = new Compaction(level);

    // Pick the first file that comes after compact_pointer_[level]
	//lzh: 选出首个最大值大于 compact_pointer[level] 的 sst 文件
	//lzh: http://catkang.github.io/2017/02/03/leveldb-version.html Version中会记录每层上次Compaction结束后的最大Key值compact_pointer_，
	//lzh: 下一次触发自动Compaction会从这个Key开始。容量触发的优先级高于下面将要提到的Seek触发。
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||

		  //lzh: 当前文件的 largest 大于上次 level 层 compact 之后的最大值
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    GetOverlappingInputs(0, smallest, largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}


/************************************************************************/
/* 
	lzh: 此函数被两处调用: PickCompaction 和 CompactRange

	c->inputs_[0] 简写为 c[0], c->inputs_[1] 简写为 c[1]
	
	1. PickCompaction 中 level 上的一个待 compact 的文件与第 0 层文件相交的文件得到 c[0]

	2. 由上面的 c[0] 与 level+1 层相交的文件得到 c[1], c[0],c[1] 范围是 (all_start, all_limit)

	3. 若 c[1] 不为空则继续扩展: level 层与 (c[0] 并 c[1]) 相交的文件集合得到 expanded0, 转到 4. 否则转到 6.

	4. 若 expanded0 的大小大于 c[0] 则继续扩展: level+1 层与 expanded0 相交的文件集合得到的 expanded1. 否则转到 6.

	5. 若 expanded1 的大小大于 c[1] 则扩展至此, c[0]=expanded0, c[1]=expanded1, c[0] 范围是 (smallest, largest), c[0]并c[1] 范围是 (all_start, all_limit). 否则转到 6.

	6. 计算 c[0]并c[1] 与 level+2 相交文件集合作为 c->grandparents_. 计算 level 层下一次 compact 的起始位置是 largest, 即本次compact 的上界c[0].largest

	
*/
/************************************************************************/
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  GetOverlappingInputs(level+1, smallest, largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    GetOverlappingInputs(level, all_start, all_limit, &expanded0);
    if (expanded0.size() > c->inputs_[0].size()) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      GetOverlappingInputs(level+1, new_start, new_limit, &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d to %d+%d\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            int(expanded0.size()),
            int(expanded1.size()));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    GetOverlappingInputs(level + 2, all_start, all_limit, &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        EscapeString(smallest.Encode()).c_str(),
        EscapeString(largest.Encode()).c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey& begin,
    const InternalKey& end) {
  std::vector<FileMetaData*> inputs;
  GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return NULL;
  }

  Compaction* c = new Compaction(level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(level)),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
	//lzh: 避免当前层 level 与 level+2 层有很多键值重叠的 move，否则，继此次 move 
	//之后，level+1 层后续将会需要一次很高耗费的 move
  return (num_input_files(0) == 1 &&
          num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <= kMaxGrandParentOverlapBytes);
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

/************************************************************************/
/* 
	lzh: 判断 user_key 是否只出现在 base level 中(level 或 level+1 层)
*/
/************************************************************************/
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {

	  //lzh: 第 1v1 层的文件 files
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];

    for (; level_ptrs_[lvl] < files.size(); ) {

		//lzh: level_ptrs_[lvl] 表示第 lvl 层的文件在 files 中的开始位置
      FileMetaData* f = files[level_ptrs_[lvl]];

      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
			//lzh: user_key 位于 f 中, 而 f 的层次是 lvl(>= level_+2), 即 user_key 不在 base level 中
          return false;
        }
        break;
      }

	  //lzh: 当前的文件是 f=files[level_ptrs_[lvl]] , 当 user_key > f->largest 时, 说明
	  //lzh: 参与 compation 的 lvl 层首个文件应该再往后面移动, 即 level_ptrs_[lvl]++;
      level_ptrs_[lvl]++;
    }
  }
  return true;
}


/************************************************************************/
/* 
	lzh: 
		函数的使用场景是:	在处理 internal_key 之前是否要停止构建当前 compaction 的输出. (将已经构建好的输出到 sst, 再新起一个构建)
		实现的细节是:		level+2 层的文件中, 所有 key 都比 internal_key 小(即 largest 比它小)的文件的 size 之和
								达到了一个阈值, 则函数返回 true. 否则 false
		
		函数的作用是:		将与 level+2 层相交较多的 internal_key 尽可能地放入同一个文件. 这可以减少 compaction 后生成的 level+1 文件
								与 level+2 层文件的相交程度.
							
		举例说明, 依次传入: a, b, c, d, e, f, g, h, i, j, 注意它们是递增的顺序, 本函数的返回值和函数调用完之后 grandparent_index_ 的值如下表:

			ShouldStopBefore	|	grandparent_index_		|	含义
			------------------------------------------------------------------------------------------------------------------
		a		false						0					a <= f[0].largest
		b		false						1					f[0].largest < b <= f[1].largest
		c		true						2					f[1].largest < c <= f[2].largest, c 与文件 f[0]和f[1] 的交集程度大于阈值, 从它开始的 key 应该新起一个 sst 存放
		d		false						2					c < d <= f[2].largest
		e		true						3					f[2].largest < e <= f[3].largest, e 与文件 f[2] 的交集程度大于阈值
		f		true						4					f[3].largest < f <= f[4].largest, f 与文件 f[3] 的交集程度大于阈值
		g		false						4					g <= f[4].largest
		h		false						5					f[4].largest < h <= f[5].largest
		i		false						5					i <= f[5].largest
		j		true						6					i > f[5].largest, j 与文件 f[4]和f[5] 的交集程度大于阈值
		
		上面的例子, 导致的结果是:
		a, b		---> 1.sst, 与 f[0] 交集较小
		c, d		---> 2.sst, 与 f[0],f[1] 相交程度大于阈值
		e			---> 3.sst, 与 f[2] 相交程度大于阈值
		f, g, h, i	---> 4.sst, 与 f[3] 相交程度大于阈值
		j			---> 5.sst, 与 f[4],f[5] 相交程度大于阈值

		效果很明显, compaction 完成后 1 个 level+1 层的文件与 level+2 层文件相交的总大小是 kMaxGrandParentOverlapBytes.
		如果 level+2 层文件较小时, 可以与多个 level+2 文件相交, 若较大则只与一个 level+2 文件相交
		见工程资源图: sst_compaction_should_stop_before

	其它的细节:
		1.首次调用此函数时 seen_key_ 是 false , 也即一定返回 false, 第二次或之后调用时就一直是 true 了, 根据实际情况返回.

		2.grandparent_index_ 表示在 gradparents_ 中遍历的位置, 每次遍历完之后此值会增加. 

		3.kMaxGrandParentOverlapBytes 控制着 compaction 完成后每个 level+1 层文件与 level+2 层文件相交的总文件大小.

		4.overlapped_bytes_ 指的是当前 compaction 过程处于当前构建中的那一个文件(level+1层), 与 level+2 层相交的那些文件, 其总大小.
			需要注意的是, 每个新开始构建的文件, overlapped_bytes_ 会归 0.

*/
/************************************************************************/
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;

  while (grandparent_index_ < grandparents_.size() && 
			icmp->Compare(internal_key,grandparents_[grandparent_index_]->largest.Encode()) > 0) 
  {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > kMaxGrandParentOverlapBytes) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}
