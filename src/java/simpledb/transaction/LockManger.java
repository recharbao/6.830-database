package simpledb.transaction;

import simpledb.common.Permissions;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManger {
    private Map<Integer, Lock> _pageLock;
    private Map<Integer, Set<TransactionId>> _pageLockRTid;
    private Map<Integer, TransactionId> _pageLockWTid;

    private static LockManger lockManger = new LockManger();

    public static LockManger getLockManger() {
        return lockManger;
    }

    public LockManger() {
        _pageLock = new HashMap<>();
        _pageLockRTid = new HashMap<>();
        _pageLockWTid = new HashMap<>();
    }

//    public boolean isHoldLock(int pageHashCode, TransactionId tid) {
//
//        List<Boolean> result = new ArrayList<>();
//        _pageLockRTid.forEach((a, v) -> {
//            v.stream().forEach(b->{
//                if (b.equals(tid) && a.equals(pageHashCode)) {
//                    result.add(new Boolean(true));
//                }
//            });
//        });
//
//        _pageLockWTid.forEach((a, v) -> {
//            if (v.equals(tid) && a.equals(pageHashCode)) {
//                result.add(new Boolean(true));
//            }
//        });
//
//
//        return result.get(0);
//    }

    public synchronized void acquirePageLock(int pageHashCode, Permissions perm, TransactionId tid) {
        if (perm.equals(Permissions.READ_WRITE)) {
//            System.out.println("READ_WRITE");
//            System.out.println("pageHashCode = " + pageHashCode);

            if (_pageLock.containsKey(pageHashCode)) {
                if ((!_pageLockWTid.containsKey(pageHashCode)) && !(_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid))) {
                    //System.out.println("read_write included before !");
                    _pageLock.get(pageHashCode).lock();
                    // System.out.println("read_write included !");
                }
            }else {
                Lock lock = new ReentrantLock();
                lock.lock();
                // System.out.println("read_write not included !");
                _pageLock.put(pageHashCode, lock);
            }

            _pageLockWTid.put(pageHashCode, tid);

        }else if (perm.equals(Permissions.READ_ONLY)) {
//            System.out.println("READ_ONLY");
//            System.out.println("pageHashCode = " + pageHashCode);

            if (_pageLock.containsKey(pageHashCode)) {
                // System.out.println("read_only include before!");
                // System.out.println("_pageLockRTid.get(pageHashCode).size() = " + _pageLockRTid.get(pageHashCode).size());

                if ((!_pageLockWTid.containsKey(pageHashCode) || (_pageLockWTid.containsKey(pageHashCode) && !_pageLockWTid.get(pageHashCode).equals(tid))) && (!_pageLockRTid.containsKey(pageHashCode) || _pageLockRTid.get(pageHashCode).size() == 0)) {
                    // System.out.println("_pageLockRTid.get(pageHashCode).size() = " + _pageLockRTid.get(pageHashCode).size() + "_pageLock.get(pageHashCode).trylock();" + _pageLock.get(pageHashCode).tryLock());
                    _pageLock.get(pageHashCode).lock();
                }
                // System.out.println("read_only include after!");
            }else {
                Lock lock = new ReentrantLock();
                lock.lock();
                // System.out.println("read_only not included !");
                _pageLock.put(pageHashCode, lock);
            }

            if (!_pageLockRTid.containsKey(pageHashCode)) {
                _pageLockRTid.put(pageHashCode, new HashSet<>());
            }

            Set<TransactionId> set = _pageLockRTid.get(pageHashCode);
            if (!set.contains(tid)) {
                set.add(tid);
            }

            _pageLockRTid.put(pageHashCode, set);
        }
    }

    public synchronized void releasePageLock(int pageHashCode, TransactionId tid) {
        if (_pageLockWTid.containsKey(pageHashCode)) {
            _pageLockWTid.remove(pageHashCode);
//            System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&" +
//                    "&&&&&&&&&&&&&&&&&&&&&&&&&_pageLockWTid = " + _pageLockRTid.containsKey(pageHashCode) + "   " + pageHashCode);
            _pageLock.get(pageHashCode).unlock();
        } else if (_pageLockRTid.get(pageHashCode).contains(tid)) {
            _pageLockRTid.get(pageHashCode).remove(tid);
            if (_pageLockRTid.get(pageHashCode).size() == 0) {
                _pageLock.get(pageHashCode).unlock();
            }
        }
    }
}
