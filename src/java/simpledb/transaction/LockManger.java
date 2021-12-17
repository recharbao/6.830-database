package simpledb.transaction;

import simpledb.common.Permissions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManger {
    private ConcurrentMap<Integer, Lock> _pageLock;
    private ConcurrentMap<Integer, Set<TransactionId>> _pageLockRTid;
    private ConcurrentMap<Integer, TransactionId> _pageLockWTid;

    private static int acquireCount = 0;
    private static int releaseCount = 0;

    private static LockManger lockManger = new LockManger();

    public static LockManger getLockManger() {
        return lockManger;
    }

    public LockManger() {
        _pageLock = new ConcurrentHashMap<>();
        _pageLockRTid = new ConcurrentHashMap<>();
        _pageLockWTid = new ConcurrentHashMap<>();
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

    public void acquirePageLock(int pageHashCode, Permissions perm, TransactionId tid) {
        // System.out.println("acquire !" + (++acquireCount));

        if (perm.equals(Permissions.READ_WRITE)) {
//            System.out.println("READ_WRITE");
//            System.out.println("pageHashCode = " + pageHashCode);

            if (_pageLock.containsKey(pageHashCode)) {
                // System.out.println((_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid)));
                //if ((!_pageLockWTid.containsKey(pageHashCode)) && !(_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid))) {
                // System.out.println(_pageLockWTid.get(pageHashCode) + " " + _pageLockRTid.get(pageHashCode).equals(tid));
                if((!_pageLockWTid.containsKey(pageHashCode) || (_pageLockWTid.containsKey(pageHashCode) && !tid.equals(_pageLockWTid.get(pageHashCode)))) && !(_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid))) {
                    //System.out.println("read_write included before !");
                    _pageLock.get(pageHashCode).lock();
                    //System.out.println("read_write included !");
                }
            }else {
                Lock lock = new ReentrantLock();
                // System.out.println("read_write not included !");
                _pageLock.put(pageHashCode, lock);
                _pageLock.get(pageHashCode).lock();
            }
//
//            if((_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).equals(tid))) {
//                _pageLockRTid.get(pageHashCode).remove(tid);
//            }
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
                // System.out.println("read_only not included !");
                _pageLock.put(pageHashCode, lock);
                _pageLock.get(pageHashCode).lock();
            }

            if (!_pageLockRTid.containsKey(pageHashCode)) {
                _pageLockRTid.put(pageHashCode, new HashSet<>());
            }

            Set<TransactionId> set = _pageLockRTid.get(pageHashCode);
            if (!set.contains(tid) && (!_pageLockWTid.containsKey(pageHashCode) || (_pageLockWTid.containsKey(pageHashCode) && !_pageLockWTid.get(pageHashCode).equals(tid)))) {
                set.add(tid);
            }

            _pageLockRTid.put(pageHashCode, set);
        }
    }

    public void releasePageLock(int pageHashCode, TransactionId tid) {
        // System.out.println("release !" + (++releaseCount));
        if (_pageLockWTid.containsKey(pageHashCode)) {
            _pageLockWTid.remove(pageHashCode);
//            System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&" +
//                    "&&&&&&&&&&&&&&&&&&&&&&&&&_pageLockWTid = " + _pageLockRTid.containsKey(pageHashCode) + "   " + pageHashCode);
            _pageLock.get(pageHashCode).unlock();
        } else if (_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid)) {
            _pageLockRTid.get(pageHashCode).remove(tid);
            if (_pageLockRTid.get(pageHashCode).size() == 0) {
                _pageLock.get(pageHashCode).unlock();
            }
        }
    }

//
//    public synchronized void releaseAll(TransactionId tid) {
////        _pageLock.entrySet().stream()
////                .map(entry->entry.getKey())
////                .forEach(a-> releasePageLock(a, tid));
//
//
//    }
}
