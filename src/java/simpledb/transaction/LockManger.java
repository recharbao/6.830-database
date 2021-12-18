package simpledb.transaction;


import simpledb.common.Permissions;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class LockManger {
    private ConcurrentMap<Integer, Lock> _pageLock;
    private ConcurrentMap<Integer, Set<TransactionId>> _pageLockRTid;
    private ConcurrentMap<Integer, TransactionId> _pageLockWTid;

    private ConcurrentMap<Integer, Set<TransactionId>> _request_page_tids;
    private ConcurrentMap<TransactionId, Set<Integer>> _request_tid_pages;
    private static LockManger lockManger = new LockManger();

    public static LockManger getLockManger() {
        return lockManger;
    }

    public LockManger() {
        _pageLock = new ConcurrentHashMap<>();
        _pageLockRTid = new ConcurrentHashMap<>();
        _pageLockWTid = new ConcurrentHashMap<>();
        _request_page_tids = new ConcurrentHashMap<>();
        _request_tid_pages = new ConcurrentHashMap<>();
    }


    public void acquirePageLock(int pageHashCode, Permissions perm, TransactionId tid) {
        if (_request_page_tids.containsKey(pageHashCode)) {
            if (!_request_page_tids.get(pageHashCode).contains(tid)) {
                _request_page_tids.get(pageHashCode).add(tid);
            }
        } else {
            Set<TransactionId> set = new HashSet<>();
            set.add(tid);
            _request_page_tids.put(pageHashCode, set);
        }

        if (_request_tid_pages.containsKey(tid)) {
            if (!_request_tid_pages.get(tid).contains(pageHashCode)) {
                _request_tid_pages.get(tid).add(pageHashCode);
            }
        } else {
            Set<Integer> set = new HashSet<>();
            set.add(pageHashCode);
            _request_tid_pages.put(tid, set);
        }

        if (perm.equals(Permissions.READ_WRITE)) {
            if (_pageLock.containsKey(pageHashCode)) {
                if ((!_pageLockWTid.containsKey(pageHashCode) || (_pageLockWTid.containsKey(pageHashCode) && !tid.equals(_pageLockWTid.get(pageHashCode)))) && !(_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid))) {
                    _pageLock.get(pageHashCode).lock();
                }
            } else {
                Lock lock = new ReentrantLock();
                _pageLock.put(pageHashCode, lock);
                _pageLock.get(pageHashCode).lock();
            }
            _pageLockWTid.put(pageHashCode, tid);

        } else if (perm.equals(Permissions.READ_ONLY)) {
            if (_pageLock.containsKey(pageHashCode)) {
                if ((!_pageLockWTid.containsKey(pageHashCode) || (_pageLockWTid.containsKey(pageHashCode) && !_pageLockWTid.get(pageHashCode).equals(tid))) && (!_pageLockRTid.containsKey(pageHashCode) || _pageLockRTid.get(pageHashCode).size() == 0)) {
                    _pageLock.get(pageHashCode).lock();
                }
            } else {
                Lock lock = new ReentrantLock();
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

        _request_tid_pages.get(tid).remove(pageHashCode);
        _request_page_tids.get(pageHashCode).remove(tid);
    }

    public void releasePageLock(int pageHashCode, TransactionId tid) {
        if (_pageLockWTid.containsKey(pageHashCode)) {
            _pageLockWTid.remove(pageHashCode);
            _pageLock.get(pageHashCode).unlock();
        } else if (_pageLockRTid.containsKey(pageHashCode) && _pageLockRTid.get(pageHashCode).contains(tid)) {
            _pageLockRTid.get(pageHashCode).remove(tid);
            if (_pageLockRTid.get(pageHashCode).size() == 0) {
                _pageLock.get(pageHashCode).unlock();
            }
        }
    }

    public synchronized boolean detectDeadLock(TransactionId tid, int pageHashCode) {

        List<Integer> keepPages = _pageLockRTid.entrySet().stream()
                .filter(e -> e.getValue().contains(tid))
                .map(a -> a.getKey()).collect(Collectors.toList());


        keepPages.addAll(_pageLockWTid.entrySet().stream()
                .filter(e -> e.getValue().equals(tid)).map(a -> a.getKey())
                .collect(Collectors.toList()));
        
        Set<Integer> requestPages = new HashSet<>();

        requestPages.add(pageHashCode);
        boolean res = requestPagesToHoldTidToCheckTargetTidPages(requestPages, keepPages, tid);
        return res;
    }

    private boolean requestPagesToHoldTidToCheckTargetTidPages(Set<Integer> requestPages, List<Integer> keepPages, TransactionId tid) {

        Set<TransactionId> hold_tids = new HashSet<>();
        requestPages.stream().filter(d -> _pageLockRTid.containsKey(d))
                .map(e -> _pageLockRTid.get(e))
                .filter(f->f != null)
                .forEach(a-> hold_tids.addAll(a));
        requestPages.stream().filter(d->_pageLockWTid.containsKey(d))
                .map(e -> _pageLockWTid.get(e))
                .filter(f->f != null)
                .forEach(a->hold_tids.add(a));

        Set<Integer> hold_tids_request_pages = new HashSet<>();
        hold_tids.stream().filter(d->_request_tid_pages.containsKey(d))
                .map(a -> _request_tid_pages.get(a))
                .forEach(c->hold_tids_request_pages.addAll(c));

        List<Integer> hold_tids_request_pages_list = hold_tids_request_pages.stream().collect(Collectors.toList());

        for (int i = 0; i < hold_tids_request_pages_list.size(); i++) {
            for (int j = 0; j < keepPages.size(); j++) {
                if (hold_tids_request_pages_list.get(i).equals(keepPages.get(j))) {
                    return true;
                }
            }
        }

        if (hold_tids_request_pages_list.size() == 0) {
            return false;
        }
        return requestPagesToHoldTidToCheckTargetTidPages(hold_tids_request_pages, keepPages, tid);
    }
}
