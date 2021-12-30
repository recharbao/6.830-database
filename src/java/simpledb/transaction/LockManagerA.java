package simpledb.transaction;

import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class LockManagerA {

    private ConcurrentMap<Integer, Lock> _pageLock;
    private DetectDeadLock _detectDeadLock;


    public LockManagerA() {
        _pageLock = new ConcurrentHashMap<>();
        _detectDeadLock = new DetectDeadLock();
    }

    private static LockManagerA lockManagerA = new LockManagerA();

    public static LockManagerA getLockManagerA() {
        return lockManagerA;
    }

    public void acquireReadLock(Integer page, simpledb.transaction.TransactionId tid) throws TransactionAbortedException {

        // System.out.println(Thread.currentThread() + " request readlock " + "page : " + page + " tid : " + tid);
        makePageLock(page);
        _pageLock.get(page).lock(true, tid);
        // System.out.println(Thread.currentThread() + " acquire readlock " + "page : " + page + " tid : " + tid);

    }

    public void acquireWriteLock(Integer page, simpledb.transaction.TransactionId tid) throws TransactionAbortedException {
        // System.out.println(Thread.currentThread() + " request writelock " + "page : " + page + " tid : " + tid);
        makePageLock(page);
        int lockStatus = _pageLock.get(page).lock(false, tid);
        synchronized (new Object()) {
            if (lockStatus == 1) {
                _pageLock.get(page).getAcquireLockTids().put(tid, true);
                _pageLock.get(page).lock(tid);
                _pageLock.get(page).changeStatus(false, tid);
                _detectDeadLock.deTidRequestPages(tid, page);
                _detectDeadLock.addPageHoldTids(tid, page);
            } else if (lockStatus == 2) {
                while(true) {
                    try {
                        Thread.sleep(10);
                    } catch (Exception e) {
                        //TODO: handle exception
                    }

                    if (_pageLock.get(page).judge()) {
                        break;
                    }
                }

                _pageLock.get(page).changeStatus(false, tid);
                _detectDeadLock.deTidRequestPages(tid, page);
                _detectDeadLock.addPageHoldTids(tid, page);
            }
        }
        // System.out.println(Thread.currentThread() + " acquire writelock " + "page : " + page + " tid : " + tid);
    }

    public synchronized void releaseLock(Integer page, simpledb.transaction.TransactionId tid) {
        _pageLock.get(page).unLock(tid);
    }

    private synchronized void makePageLock(Integer page) {
        if (!_pageLock.containsKey(page)) {
            Lock lock = new Lock(page, _detectDeadLock);
            _pageLock.put(page, lock);
        }
    }
}



class DetectDeadLock {

    private ConcurrentMap<TransactionId, Set<Integer> > _tidRequestPages;
    private ConcurrentMap<Integer, Set<TransactionId> > _pageHoldTids;

    public DetectDeadLock() {
        _tidRequestPages = new ConcurrentHashMap<>();
        _pageHoldTids = new ConcurrentHashMap<>();
    }

    public void addTidRequestPages(TransactionId tid, Integer page) {
        if (!_tidRequestPages.containsKey(tid)) {
            Set<Integer> set = new HashSet<>();
            set.add(page);
            _tidRequestPages.put(tid, set);
        }else {
            if (!_tidRequestPages.get(tid).contains(page)) {
                _tidRequestPages.get(tid).add(page);
            }
        }
    }


    public void deTidRequestPages(TransactionId tid, Integer page) {
        if (_tidRequestPages.containsKey(tid)) {
            _tidRequestPages.get(tid).remove(page);
        }
    }

    public void addPageHoldTids(TransactionId tid, Integer page) {
        if (_pageHoldTids.containsKey(page)) {
            _pageHoldTids.get(page).add(tid);
        }else {
            Set<TransactionId> set = new HashSet<>();
            set.add(tid);
            _pageHoldTids.put(page, set);
        }
    }

    public void dePageHoldTids(TransactionId tid, Integer page) {
        if (_pageHoldTids.containsKey(page)) {
            _pageHoldTids.get(page).remove(tid);
        }
    }

    public boolean isDeadLock(Integer page, TransactionId tid) {
        Set<Integer> nowTidHoldPages = new HashSet<>();
        _pageHoldTids.entrySet().stream()
                         .filter(a->a.getValue().contains(tid))
                         .map(b->b.getKey())
                         .forEach(c->nowTidHoldPages.add(c));

        Set<Integer> TidrequestPages = new HashSet<>();
        Set<TransactionId> tids = new HashSet<>();
        if (_pageHoldTids.containsKey(page)) {
            tids.addAll(_pageHoldTids.get(page));
        }

        tids.remove(tid);
        tids.stream()
                .forEach(a->{
                    if (_tidRequestPages.containsKey(a)) {
                        TidrequestPages.addAll(_tidRequestPages.get(a));
                    }
                });

        return bfs(TidrequestPages, nowTidHoldPages);
    }

    private boolean bfs(Set<Integer> TidsrequestPages, Set<Integer> nowTidHoldPages) {
        while (true) {
            for(Integer rt : TidsrequestPages) {
                for(Integer ntp : nowTidHoldPages) {
                    if (rt.equals(ntp)) {
                        return true;
                    }
                }
            }


            Set<TransactionId> tids = new HashSet<>();
            TidsrequestPages.stream()
                    .forEach(a->{
                        if (_pageHoldTids.containsKey(a)) {
                            tids.addAll(_pageHoldTids.get(a));
                        }
                    });

            Set<Integer> TidsrequestPages2 = new HashSet<>();
            tids.stream()
                    .forEach(a->{
                        if (_tidRequestPages.containsKey(a)) {
                            TidsrequestPages2.addAll(_tidRequestPages.get(a));
                        }
                    });
            TidsrequestPages = TidsrequestPages2;

            if (TidsrequestPages.size() == 0) {
                break;
            }
        }
        return false;
    }

}


class waitClock {
    private volatile Set<TransactionId> tids = new HashSet<>();
    public void lock(TransactionId tid) {
        tids.add(tid);
        while(tids.size() > 1) {
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void unlock(TransactionId tid) {
        tids.remove(tid);
    }
}


class Lock extends waitClock {
    private volatile boolean _isReadStage = true;
    private AtomicBoolean _isLock = new AtomicBoolean(false);
    private ConcurrentMap<TransactionId, Boolean> _acquireLockTids = new ConcurrentHashMap();
    private Integer _page;
    DetectDeadLock _detectDeadLock;


    public ConcurrentMap<TransactionId, Boolean> getAcquireLockTids() {
        return _acquireLockTids;
    }

    public Lock(Integer page, DetectDeadLock detectDeadLock) {
        _page = page;
        _detectDeadLock = detectDeadLock;
    }

    public boolean judge() {
        long res = _acquireLockTids.entrySet().stream()
                        .filter(a->a.getValue().equals(false))
                        .count();
        if (res > 0) {
            return false ;
        } else {
            return true && _isReadStage;
        }
    }

    public synchronized int lock(boolean isReadStage, TransactionId tid) throws TransactionAbortedException {
        if (_detectDeadLock.isDeadLock(_page, tid)) {
            throw new TransactionAbortedException();
        }
        // System.out.println(Thread.currentThread() + " allow access lock " + "page : " + _page + " tid : " + tid);
        _detectDeadLock.addTidRequestPages(tid, _page);
        if (_isLock.compareAndSet(false, true)) {
            super.lock(tid);
            changeStatus(isReadStage, tid);
        }else {
            if (_acquireLockTids.containsKey(tid)) {
                if (_isReadStage && !isReadStage) {
                    // System.out.println(Thread.currentThread() + " acquire upgrade " + "page : " + _page + " tid : " + tid);
                    if (!_acquireLockTids.get(tid)) {
                        return 1;
                    }else {
                        return 2;
                    }
                } else {

                    _detectDeadLock.deTidRequestPages(tid, _page);
                    _detectDeadLock.addPageHoldTids(tid, _page);
                    return 0;
                }
            }else if (_isReadStage && isReadStage) {
                _acquireLockTids.put(tid, false);
            }else {
                super.lock(tid);
                changeStatus(isReadStage, tid);
            }
        }

        _detectDeadLock.deTidRequestPages(tid, _page);
        _detectDeadLock.addPageHoldTids(tid, _page);
        return 0;
    }

    public synchronized void changeStatus(boolean isReadStage, TransactionId tid) {
        _isReadStage = isReadStage;
        _acquireLockTids.put(tid, true);
        _isLock.set(true);
    }

    public void unLock(TransactionId tid) {
        Boolean isRealLock = _acquireLockTids.remove(tid);
        if (isRealLock != null && isRealLock) {
            _isLock.set(false);
            super.unlock(tid);
        }
        _detectDeadLock.dePageHoldTids(tid, _page);
    }
}
