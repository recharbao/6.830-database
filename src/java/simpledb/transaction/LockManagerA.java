package simpledb.transaction;

import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


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

        System.out.println(Thread.currentThread() + " request readlock " + "page : " + page + " tid : " + tid);
        makePageLock(page);
        _pageLock.get(page).lock(true, tid);
        System.out.println(Thread.currentThread() + " acquire readlock " + "page : " + page + " tid : " + tid);

    }

    public void acquireWriteLock(Integer page, simpledb.transaction.TransactionId tid) throws TransactionAbortedException {
        System.out.println(Thread.currentThread() + " request writelock " + "page : " + page + " tid : " + tid);
        makePageLock(page);
        boolean lockStatus = _pageLock.get(page).lock(false, tid);
        synchronized (new Object()) {
            if (lockStatus) {
                _pageLock.get(page).lock();
                _pageLock.get(page).changeStatus(false, tid);
            }
        }
        System.out.println(Thread.currentThread() + " acquire writelock " + "page : " + page + " tid : " + tid);
    }

    public void releaseLock(Integer page, simpledb.transaction.TransactionId tid) {
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


class Lock extends ReentrantLock {
    private volatile boolean _isReadStage = true;
    private AtomicBoolean _isLock = new AtomicBoolean(false);
    private ConcurrentMap<TransactionId, Boolean> _acquireLockTids = new ConcurrentHashMap();
    private Integer _page;
    DetectDeadLock _detectDeadLock;

    public Lock(Integer page, DetectDeadLock detectDeadLock) {
        _page = page;
        _detectDeadLock = detectDeadLock;
    }

    public synchronized boolean lock(boolean isReadStage, TransactionId tid) throws TransactionAbortedException {
        System.out.println(Thread.currentThread() + " enter1");
        if (_detectDeadLock.isDeadLock(_page, tid)) {
            System.out.println(Thread.currentThread() + " enter2");
            throw new TransactionAbortedException();
        }
        System.out.println(Thread.currentThread() + " allow access lock " + "page : " + _page + " tid : " + tid);
        _detectDeadLock.addTidRequestPages(tid, _page);
        if (_isLock.compareAndSet(false, true)) {
            super.lock();
            changeStatus(isReadStage, tid);
        }else {
            if (_acquireLockTids.containsKey(tid)) {
                if (_isReadStage && !isReadStage) {
                    System.out.println(Thread.currentThread() + " acquire upgrade " + "page : " + _page + " tid : " + tid);
                    if (!_acquireLockTids.get(tid)) {
                        //super.lock();
                        return true;
                    }
                    changeStatus(isReadStage, tid);
                } else {
                    return false;
                }
            }else if (_isReadStage && isReadStage) {
                _acquireLockTids.put(tid, false);
            }else {
                super.lock();
                changeStatus(isReadStage, tid);
            }
        }

        _detectDeadLock.deTidRequestPages(tid, _page);
        _detectDeadLock.addPageHoldTids(tid, _page);
        return false;
    }

    public synchronized void changeStatus(boolean isReadStage, TransactionId tid) {
        _isReadStage = isReadStage;
        _acquireLockTids.put(tid, true);
        _isLock.set(true);
    }

    public synchronized void unLock(TransactionId tid) {
        Boolean isRealLock = _acquireLockTids.remove(tid);
        if (isRealLock != null && isRealLock) {
            _isLock.set(false);
            super.unlock();
        }
        _detectDeadLock.dePageHoldTids(tid, _page);
    }
}
