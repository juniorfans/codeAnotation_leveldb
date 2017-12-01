// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

/************************************************************************/
/* 
	lzh: MergingIterator 对外提供 Prev, Next, Seek 接口, 
		对内的实现是管理着一堆 IteratorWrapper, 通过它们的 Prev, Next, Seek 实现
		那些功能
*/
/************************************************************************/
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(NULL),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  /************************************************************************/
  /* 
	lzh: 迭代器 seek 到 first 位置: 需要将那一堆 WrapperIterator 全部 seek 到 first 位置
	并且通过比较这 n_ 个迭代器 first 位置的值, 找到最小的迭代器, 让 current_ 指向它
  */
  /************************************************************************/
  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  //lzh: 同 SeektoFirst
  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  /************************************************************************/
  /* 
	lzh: 在各个迭代器中 seek 到 target 的位置, 然后比较这 n_ 个位置的值, 
	找到最小的, 让 current_ 指向那个迭代器
  */
  /************************************************************************/
  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  /************************************************************************/
  /* 
	lzh: 分两种情况
		1.	若上一次遍历的方向是 kForward, 即与本函数需要的遍历方向
			一致, 则只需要将 迭代器 current_ 下一个元素 next 与其它迭代器各自指
			的位置的值进行比较, 找到最小的, 即是整组迭代器的 Next.
		2.	若上一次遍历的方向是 kReverse, 与本次要遍历方向相反, kReverse 遍历
			时, 要往前找到最大的元素, 这样就会造成遍历结束后, 所有的迭代器都会指在
			较大的位置, 这与 kForward 的逻辑明显不一样. 解决方法是对于非 current_ 迭代器
			seek 到各自 current_.key 对应的位置. 再对所有的迭代器各自所指位置值找到最小的, 即是
			整组迭代器的 Next.
  */
  /************************************************************************/
  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  /************************************************************************/
  /* 
  lzh: 与 Next 类似, 分两种情况
	  1.	若上一次遍历的方向是 kReverse, 即与本函数需要的遍历方向
	  一致, 则只需要将 迭代器 current_ 下一个元素 prev 与其它迭代器各自指
	  的位置的值进行比较, 找到最大的, 即是整组迭代器的 Prev.
	  2.	若上一次遍历的方向是 kForward, 与本次要遍历方向相反, kForward 遍历
	  时, 要往后找到最小的元素, 这样就会造成遍历结束后, 所有的迭代器都会指在
	  较小的位置, 这与 kReverse 的逻辑明显不一样. 解决方法是对于非 current_ 迭代器
	  seek 到各自 current_.key 对应的位置. 再对所有的迭代器各自所指位置值找到最大的, 即是
	  整组迭代器的 Next.
  */
  /************************************************************************/
  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;

  //lzh: 共计 n_ 个 IteratorWrapper 对象, children_[i] 指第 i 个 IteratorWrapper 对象
  IteratorWrapper* children_;	
  int n_;

  //lzh: 指向当前正在被使用的那个 IteratorWrapper 对象的指针
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

/************************************************************************/
/* 
	lzh: 定位到最小的
*/
/************************************************************************/
void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = NULL;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == NULL) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

/************************************************************************/
/* 
	lzh: 定位到最大的
*/
/************************************************************************/
void MergingIterator::FindLargest() {
  IteratorWrapper* largest = NULL;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == NULL) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}
