// Author: Alex Thomson (thomson@cs.yale.edu)
//
// Mutex is currently a thin wrapper around pthread_mutex_t, but this may
// change in the future, so please use this Mutex class rather than
// pthread_mutex_t for critical sections in Calvin.
//
// Standard usage idiom:
//
//    Mutex m;                // create a new mutex
//
//    void DoSomething() {
//      Lock l(&m);           // creating a 'Lock' object locks the mutex
//      <do stuff>
//    }                       // when the Lock object goes out of scope and is
//                            // deallocated, the mutex is automatically
//                            // unlocked
//

#ifndef CALVIN_COMMON_MUTEX_H_
#define CALVIN_COMMON_MUTEX_H_

//#define LOCK_DEBUG

#include <pthread.h>
#include <glog/logging.h>
#include "assert.h"

class Mutex {
 public:
  // Mutexes come into the world unlocked.
  Mutex() {
    pthread_mutex_init(&mutex_, NULL);
  }

 private:
  friend class Lock;
  // Actual pthread mutex wrapped by Mutex class.
  pthread_mutex_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);
};

class Lock {
 public:
  explicit Lock(Mutex* mutex) : mutex_(mutex) {
    pthread_mutex_lock(&mutex_->mutex_);
  }
  ~Lock() {
    pthread_mutex_unlock(&mutex_->mutex_);
  }

 private:
  Mutex* mutex_;

  // DISALLOW_DEFAULT_CONSTRUCTOR
  Lock();

  // DISALLOW_COPY_AND_ASSIGN
  Lock(const Lock&);
  Lock& operator=(const Lock&);
};

class MutexRW {
 public:
  // Mutexes come into the world unlocked.
  MutexRW() {
    pthread_rwlock_init(&mutex_, NULL);
  }

 private:
  friend class ReadLock;
  friend class WriteLock;
  // Actual pthread rwlock wrapped by MutexRW class.
  pthread_rwlock_t mutex_;


  // DISALLOW_COPY_AND_ASSIGN
  MutexRW(const MutexRW&);
  MutexRW& operator=(const MutexRW&);
};

class ReadLock {
public:
    explicit ReadLock(){
        mutex_ = nullptr;
    }
  explicit ReadLock(MutexRW* mutex, bool acquire_lock = true, bool debug = false) : mutex_(mutex) {
      if(acquire_lock) {
          pthread_rwlock_rdlock(&mutex_->mutex_);
          unlocked_flag = false;
      } else
      {
          unlocked_flag = true;
      }
  }

  void init(MutexRW* mutex, bool acquire_lock = true){
      assert(mutex_ == nullptr);
      CHECK(init_flag == false);
      init_flag = true;
        mutex_ = mutex;
        if(acquire_lock) {
          pthread_rwlock_rdlock(&mutex_->mutex_);
          unlocked_flag = false;
        } else
        {
          unlocked_flag = true;
        }
  }

  void clear()
  {
      init_flag = false;
      if(!unlocked_flag)
        {
            pthread_rwlock_unlock(&mutex_->mutex_);
        }
        unlocked_flag = false;
        mutex_ = nullptr;
  }

  void unlock(){
      if (!unlocked_flag) {
          unlocked_flag = true;
          pthread_rwlock_unlock(&mutex_->mutex_);
      }
  }
  void lock(){
      if (unlocked_flag) {
          pthread_rwlock_rdlock(&mutex_->mutex_);
          unlocked_flag = false;
      }
  }
  ~ReadLock() {
      if (!unlocked_flag) {
          pthread_rwlock_unlock(&mutex_->mutex_);
      }
  }

 private:
    MutexRW* mutex_;
    bool init_flag;
    bool unlocked_flag;


    // DISALLOW_DEFAULT_CONSTRUCTOR
  //ReadLock();

  // DISALLOW_COPY_AND_ASSIGN
    ReadLock(const ReadLock&);
    ReadLock& operator=(const ReadLock&);
};

class WriteLock {
public:
    explicit WriteLock(){
        mutex_ = nullptr;
        init_flag = false;
    }
  explicit WriteLock(MutexRW* mutex, bool acquire_lock = true, bool debug = false) : mutex_(mutex) {
        init_flag = true;
        if(acquire_lock) {
            pthread_rwlock_wrlock(&mutex_->mutex_);
            unlocked_flag = false;
    } else
    {
        unlocked_flag = true;
    }
  }
    void init(MutexRW* mutex, bool acquire_lock = true){
        assert(mutex_ == nullptr);
        CHECK(init_flag == false);
        init_flag = true;
        mutex_ = mutex;
        if(acquire_lock) {
            pthread_rwlock_wrlock(&mutex_->mutex_);
            unlocked_flag = false;
        } else
        {
            unlocked_flag = true;
        }
    }

    void clear()
    {
        init_flag = false;
        if(mutex_ == nullptr)
        {
            return;
        }
        if(!unlocked_flag)
        {
            pthread_rwlock_unlock(&mutex_->mutex_);
        }
        unlocked_flag = false;
        mutex_ = nullptr;
    }

    void unlock(){
        assert(init_flag == true);
        if (!unlocked_flag) {
            unlocked_flag = true;
            pthread_rwlock_unlock(&mutex_->mutex_);
        }
    }
    void lock(){
        assert(init_flag == true);
        if (unlocked_flag) {
            pthread_rwlock_wrlock(&mutex_->mutex_);
            unlocked_flag = false;
        }
    }
  ~WriteLock() {
      if(!unlocked_flag && init_flag) {
          pthread_rwlock_unlock(&mutex_->mutex_);
      }
  }

 private:
  MutexRW* mutex_;

    bool unlocked_flag;
    bool init_flag;

    // DISALLOW_DEFAULT_CONSTRUCTOR
  //WriteLock();

  // DISALLOW_COPY_AND_ASSIGN
  WriteLock(const WriteLock&);
  WriteLock& operator=(const WriteLock&);
};

#endif  // CALVIN_COMMON_MUTEX_H_

