// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().

	 /************************************************************************/
	 /* 
	 执行完 Prev 后迭代器的saved_key 即是迭代器当前的 user_key，而指针指示的位置是当前 saved_key 节点的上一个节点(Prev 方向)。
	 这一点不同于 Next，Next 的指针所指位置的 user_key 即是迭代器当前的 user_key。
	 造成这个不同的关键原因是:
	 Internalkey 排序规则是 Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序
	 那么相同 user_key 的更新的版本会排在前面(Prev 方向), 所以要找到当前 user_key 的前面一个有效 user_key`, 必须要
	 遍历把 user_key` 的所有节点都遍历完, 才会得知这个 user_key` 是否没有被删除, 或是否有更新版本的值. 
	 */
	 /************************************************************************/

  enum Direction {
    kForward,
    kReverse
  };

  DBIter(const std::string* dbname, Env* env,
         const Comparator* cmp, Iterator* iter, SequenceNumber s)
      : dbname_(dbname),
        env_(env),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false) {
  }
  virtual ~DBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  //lzh: [增加代码]
  Slice internal_key() const {
	  assert(valid_);
	  return (direction_ == kForward) ? iter_->key() : saved_key_;
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  void FindNextUserEntry(bool skipping, std::string* skip);

 private:
  
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  const std::string* const dbname_;
  Env* const env_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;

  Status status_;

  //lzh: 当前遍历方向是 kReverse 时, 即是当前的 key
  std::string saved_key_;     // == current key when direction_==kReverse

  //lzh: 当前遍历方向是 kReverse 时, 即是当前的值(最原始的 value)
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

/************************************************************************/
/* 
	lzh:
	Next 函数的需求是要定位到下一个有效元素位置。
	若当前遍历方向是 kReverse ，设 saved_key_ 对应的用户键是 user_key, 则当前迭代器位置指在了最左边(Prev 方向)
	的那个 user_key 的左边一个位置(此位置有可能是 deletion, 这会在下一次遍历中处理)(Prev 函数的注释中有详细说明).
	显然 Next 要定位到后面紧接着的有效元素的位置，这就需要跳过 user_key 更旧的版本或失效的版本。
	这在 FindNextUserEntry 会处理.
*/
/************************************************************************/
void DBIter::Next() {
  assert(valid_);

  //lzh: 当前函数 Next 需要向后遍历(kForward), 如果当前的遍历方向是 kReverse, 则需要更改为 kForward
  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();//lzh: 注意，leveldb 中所有迭代器失效位置都在最未尾的后面一个位置，当 iter 失效时设置为 First 位置，
						   //lzh: 这是因为上一次遍历的方向是 kReverse，即向 Prev 方向遍历直到 invalid，所以此处 SeekToFirst 是正确的
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  // Temporarily use saved_key_ as storage for key to skip.
  std::string* skip = &saved_key_;
  SaveKey(ExtractUserKey(iter_->key()), skip);	//lzh: 注意，此处设置了 iter 的 user_key 到 saved_key_ 里面
  
  //lzh: 跳过 iter_key().user_key_ 更旧的版本和 deleteType 版本
  FindNextUserEntry(true, skip);
}


/************************************************************************/
/* 
lzh: 该函数耦合了两个功能: 
	1. 遍历过程中遇到了某个 user_key 的最新版本是 deletion, 则后面跳过此 user_key 的所有节点。此过程是递归的，即若存在连续这样的 user_key 则全部会被跳过。 -- (skipping 为 true|false 都有此功能)
	2. 跳过 user_key 等于函数参数指定的 skip 的旧版本 -- skipping 为 true 时有此功能

	总的来说，skipping 为 true 指示了是否需要向后跳过 skip 指示的 user_key 及已被 delete 的节点。 为 false 指示了只向后跳过最新版本为 delete 的节点直到遇到有效的节点。

	Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序所以同 user_key 较新的记录在前面被遍历出, 如果先遇到了一个 deleteType 的 InternalKey, 则后面同 user_key 的InternalKey 要被删除.
	
	传入的参数 skip 可能是 Internalkey, 但此时 skipping 是 false，不影响逻辑(见本文件中 Seek 函数中的调用).个人相信这是原作者的笔误.

	最后注意，如果我们设置 skipping 为 true 且指定 skip 是一个很大的 user_key，则由于两个功能会同时生效，若存在某个 user_key 的最新版本是 deletion 则 skip 会被覆盖
*/
/************************************************************************/
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
	//lzh: sequence_ 是传入的最新的版本号，只处理小于此版本的数据
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
			//lzh: 所有位于此 deletion 后面的相同 user_key 都被跳过
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
				  //lzh: 此处的判断是 <= ，而不仅仅是 =，参考 Seek(const Slice& target) 函数中的注释
          } else {
			  //lzh: 如果 skipping=false 或遍历到的 user_key 大于 skip, 表示已经找到了 NextUserEntry ，直接返回
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();	//lzh: 执行跳过
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}


/************************************************************************/
/* 
	lzh: 与 Next 相对, 功能是找到仅仅比当前 user_key 小的有效的 user_key` 节点
		执行完 Prev 后迭代器的指针指示的位置是当前 saved_key 节点的上一个节点. 这一点不同于 Next.
		造成这个不同的关键原因是:
			Internalkey 排序规则是 Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序
		那么相同 user_key 的更新的版本会排在前面(Prev 方向), 所以要找到当前 user_key 的前面一个有效 user_key`, 必须要
		遍历把 user_key` 的所有节点都遍历完, 才会得知这个 user_key` 是否没有被删除, 或是否有更新版本的值. 
*/
/************************************************************************/
void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false

	//lzh: 保存当前的 user_key 到 saved_key_, 等此函数调用完毕, 即成为“上一个 user_key”
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    //lzh: 见函数上的注释: 只有向 Prev 方向遍历直到遇到不同的 user_key 才知道 saved_key 是不是有效的
	while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {
			//lzh: 已经跳完了所有相同 user_key 的节点, 可以停止
        break;
      }
    }
    direction_ = kReverse;
  }

  //lzh: 上面的循环已经使迭代器指向了首个小于当前 user_key 的节点, 现在调用 FindPrevUserEntry 是为了跳过
  //lzh; 失效的节点
  FindPrevUserEntry();
}

/************************************************************************/
/* 
	lzh: 本函数耦合两个功能:
	1. 向前移动当前迭代器的位置, 使其指示到更小的一个 user_key 的节点.(此节点可能是 deletion)
	2. saved_key_ 需要指示出之前一个有效的 user_key`. (暗示着, 如果之前一个节点是 deletion, 则需要继续往前遍历).
		此过程是递归的, 如果往前移动的过程中, 遇到的 user_key 的最新版本总是 deletion 则始终需要往前移动.
	
	设当前迭代器指示着 user_key` 的某个节点, 往前遍历遇到它的最新版本, 但是类型是 deletion, 则遍历到的所有 user_key` 节点
	都不再是有效的, 所以需要继续往前遍历. 这也是函数里面这段判断代码的意义: 
		if ((value_type != kTypeDeletion) &&	//上一节点的类型不能是 deletion
			user_comparator_->Compare(ikey.user_key, saved_key_) < 0)	//遇到了更小的 user_key
	举例说明:
	设 user_key: B < A, 且有如下操作流程: A.add	-->	A.add	-->	A.delete	-->	B.add --> B.delete, 则迭代器依次遍历的顺序是:(简单地以链表形式去表示)
	(提示: Internal key 的排序规则: user_key 正序比较 --> sequence number 逆序 --> type 的逆序):

	1(B.delete)	-->	2(B.add)	-->	3(A.delete)	-->	4(A.add)	-->	5(A.add)

	若当前迭代器位置指向 5, 则 FindPrevUserEntry 依次遍历的节点是 4, 3, 2, 1
	遍历到 2 时, 由于之前节点的类型是 deletion 则不应该 break, 继续遍历到 1, 再因为 !valid() 返回. 
	
	以下细节需要注意:
		1. 正常情况下函数结束时迭代器指向的位置的后一个节点不可能是 deletion, 当且仅当整个迭代器遍历到了尽头.
		2. 函数结束时迭代器所指位置可能是一个 deletion节点(这种情况下,形成这个局面的原因是做了空的 delete).举例如下：
			0(A.add)	-->	1(A.delete)	-->	2(B.add)
			当前迭代器指向 2 时, 往前遍历, 则到 1 节点时会 break. 此时迭代器会指在 1，且 saved_key_ 会保存着 2 节点的值.
			仔细观察上面的数据, 操作序列是: 增加B, 删除A, 增加A. 实际上，不管之前的数据(user_key) 有没有 A，“删除A”这个操作
			是无意义的.
	
*/
/************************************************************************/
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
				//lzh: value_type 是上一次遍历到的节点的类型, 
				//lzh: 当且仅当上一节点不是 deletion（也即上一个 user_key 的最新版本不是 deletion） 且当前节点是一个新的 user_key 时跳出
				//lzh: 注意当前节点有可能是 deletion
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
		//lzh: 注意, 在此之前 value_type 指的是上一次遍历到的 Internalkey 的 type
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  //lzh: 注意上面的 break 时，value_type!=kTypeDeletion，此时 value_type 并未被重新赋值
  //lzh: 所以此分支表示整个迭代器遍历完了，但是首个节点的 value_type 是 kTypeDeletion，所以置迭代器状态为 invalid
  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

/************************************************************************/
/* 
	lzh: Seek 调用 FindNextUserEntry 传入 skipping=false, 这点和 SeekToFirst一样, 
		但是和 Next 不一样, Next 传入的是 true.
		原因如下: 
		考虑 Seek 和 SeekToFirst 的功能, 我们需要定位到的位置: 
			当它是 deletion, 表明当前 user_key 最新的记录是被删除了, 所以应该继续往后面 seek(即 skip); 
			当它是 valueType 时, 即使它是一个旧版本的值, 也不应该跳过, 因为有可能调用者就是想定位到旧版本.
		
		考虑 Next 的功能, 我们需要定位到当前版本 user_key 的下一个有效可访问位置. 若下个位置是 deletion 显然也应该跳过, 
		若是 valueType, 且 user_key 与上一个一致, 但版本较旧, 为了得到 “下一个有效可访问位置”, 我们也需要跳过.
*/
/************************************************************************/
void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);

  //lzh: 执行 iter_->Seek(saved_key_) 后，有可能 iter_ 指向的是一个 user_key 大于 target 的位置(即 target 不存在)，
  //lzh: 此时 iter_.user_key > saved_key.user_kty，FindNextUserEntry 中的判断 <= 就有意义了，而不仅仅是 ==

  if (iter_->Valid()) {

	//lzh: 此时传入的 saved_key_ 是 Internalkey, 仅此一处。个人认为可能是笔误。但调用完 FindNextUserEntry 函数后, saved_key 被清空
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
	  //lzh: 当且仅当 iter_key() 已经是一个 deletion 时跳过它后面相同 user_key.
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();

  ParsedInternalKey ikey;
  ParseKey(&ikey);

  FindPrevUserEntry();
}

}  // anonymous namespace

// lzh: 此处的 internal_iter 即是 MergingIterator
Iterator* NewDBIterator(
    const std::string* dbname,
    Env* env,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    const SequenceNumber& sequence) {
  return new DBIter(dbname, env, user_key_comparator, internal_iter, sequence);
}

Slice getInternalKey(Iterator* dbIter){
	DBIter *it = (DBIter*) dbIter;
	return it->internal_key();
}

void findNextUserEntry(Iterator* dbIter, bool skipping, std::string* skip){
	((DBIter*)dbIter)->FindNextUserEntry(skipping, skip);
}

}
