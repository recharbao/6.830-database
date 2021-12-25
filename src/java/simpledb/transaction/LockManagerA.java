package simpledb.transaction;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import simpledb.common.DbException;

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
        System.out.println("before ReadLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        if (_detectDeadLock.isDeadLock(page, tid)) {
            throw new TransactionAbortedException();
        }
        System.out.println("detect ReadLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        _detectDeadLock.addTidRequestPages(tid, page);
        makePageLock(tid, true, page);
        _pageLock.get(page).lock(true, tid);
        System.out.println("after ReadLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        _detectDeadLock.deTidRequestPages(tid, page);
        _detectDeadLock.addPageHoldTids(tid, page);
    }

    public void acquireWriteLock(Integer page, simpledb.transaction.TransactionId tid) throws TransactionAbortedException {
        System.out.println("before WriteLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        if (_detectDeadLock.isDeadLock(page, tid)) {
            throw new TransactionAbortedException();
        }
        System.out.println("detect WriteLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        _detectDeadLock.addTidRequestPages(tid, page);
        makePageLock(tid, false, page);
        _pageLock.get(page).lock(false, tid);
        System.out.println("after WriteLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        _detectDeadLock.deTidRequestPages(tid, page);
        _detectDeadLock.addPageHoldTids(tid, page);
    }

    public void releaseLock(Integer page, simpledb.transaction.TransactionId tid) {
        System.out.println("before unLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page + " ");
        _pageLock.get(page).unLock(tid);
        System.out.println("after unLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page + " ");
        _detectDeadLock.dePageHoldTids(tid, page);
    }

    private synchronized void makePageLock(TransactionId tid, boolean isReadStage, Integer page) {
        if (!_pageLock.containsKey(page)) {
            System.out.println("not include !");
            Lock lock = new Lock();
            _pageLock.put(page, lock);
//            lock.lock(isReadStage, tid);
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

    public synchronized void addTidRequestPages(TransactionId tid, Integer page) {
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


    public synchronized void deTidRequestPages(TransactionId tid, Integer page) {
        if (_tidRequestPages.containsKey(tid)) {
            _tidRequestPages.get(tid).remove(page);
        }
    }

    public synchronized void addPageHoldTids(TransactionId tid, Integer page) {
        if (_pageHoldTids.containsKey(page)) {
            _pageHoldTids.get(page).add(tid);
        }else {
            Set<TransactionId> set = new HashSet<>();
            set.add(tid);
            _pageHoldTids.put(page, set);
        }
    }

    public synchronized void dePageHoldTids(TransactionId tid, Integer page) {
        if (_pageHoldTids.containsKey(page)) {
            _pageHoldTids.get(page).remove(tid);
        }
    }


    public synchronized boolean isDeadLock(Integer page, TransactionId tid) {
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

        tids.stream()
                .forEach(a->{
                    if (_tidRequestPages.containsKey(a)) {
                        TidrequestPages.addAll(_tidRequestPages.get(a));
                    }
                });

        // if (nowTidHoldPages.contains(page)) {
        //     return false;
        // }
        return bfs(TidrequestPages, nowTidHoldPages);
    }

    private boolean bfs(Set<Integer> TidsrequestPages, Set<Integer> nowTidHoldPages) {
        while (true) {

            System.out.println("Thread : " + Thread.currentThread() + "bfs ==========================================================");
            System.out.println("Thread : " + Thread.currentThread() + "TidsrequestPages");
            TidsrequestPages.stream()
                            .forEach(a->{
                                System.out.print(a);
                            });
                            System.out.println();
            System.out.println("Thread : " + Thread.currentThread() + "nowTidHoldPages");
            nowTidHoldPages.stream()
                            .forEach(a->{
                                System.out.print(a);
                            });
            System.out.println();

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
    private volatile AtomicInteger acquireCount = new AtomicInteger(0);
    private volatile AtomicInteger releaseCount = new AtomicInteger(0);
    private volatile boolean _isReadStage = true;
    private AtomicBoolean _isLock = new AtomicBoolean(false);
    private ConcurrentMap<TransactionId, Boolean> _acquireLockTids = new ConcurrentHashMap();

    public void lock(boolean isReadStage, TransactionId tid) {
//         try {
//             Thread.sleep((int)(Math.random() * 1000));
//         } catch (Exception e) {
//             //TODO: handle exception
//         }
        System.out.println("Thread : " + Thread.currentThread() + " isLock : " + _isLock);
        System.out.println("lock====" + "Thread : " + Thread.currentThread());

        
        if (_isLock.compareAndSet(false, true)) {
            System.out.println("before locked !");
            super.lock();
            acquireCount.getAndIncrement();
            System.out.println("locked !");
            _isReadStage = isReadStage;
            _acquireLockTids.put(tid, true);
        }else {
            if (_acquireLockTids.containsKey(tid)) {
                //_acquireLockTids.stream().forEach(a->System.out.print(a + " "));
                System.out.println();
                if (_isReadStage && !isReadStage) {
                    System.out.println("***********" +  "  Thread : " + Thread.currentThread() + " tid " + tid);
                    //super.unlock();
                    //if (_acquireLockTids.size() > 1) {
                        //_acquireLockTids.stream().forEach(a->System.out.print(a + " "));
                        //System.out.println();
                        //unLock(tid);
                        //lock(false, tid);
                        //super.lock();
//                        while (true) {
//                            long  count = _acquireLockTids.entrySet().stream()
//                                        .filter(a->a.getValue() == true)
//                                        .count();
//                            if (count == 0) {
//                                break;
//                        }
//                    }

                    //System.out.println("***********?");
//                    _isReadStage = false;
//                    _isLock.set(true);

                    // synchronized (this) {
                    //     if (_acquireLockTids.get(tid)) {
                    //         super.unlock();
                    //         _isLock.set(false);
                    //     }
                    // }

                    if (!_acquireLockTids.get(tid)) {
                        System.out.println("Thread : " + Thread.currentThread() + "wait lock !" + "3333333333333333333333333333333333333333333333333333333333333333333");
                        super.lock();
                        acquireCount.getAndIncrement();
                        if (_acquireLockTids.size() == 1) {
                            super.unlock();
                        }
                    }

                    if (_acquireLockTids.size() > 1) {
                        super.lock();
                    }

                    _isReadStage = false;
                    _acquireLockTids.put(tid, true);
                    _isLock.set(true);
                } else {
                    return;
                }
            }else if (_isReadStage && isReadStage) {
                _acquireLockTids.put(tid, false);
            }else if (!_isReadStage && isReadStage) {
                super.lock();
                acquireCount.getAndIncrement();
                _isReadStage = true;
                _acquireLockTids.put(tid, true);
                _isLock.set(true);
            }else if (!_isReadStage && !isReadStage) {
                super.lock();
                acquireCount.getAndIncrement();
                _isReadStage = false;
                _acquireLockTids.put(tid, true);
                _isLock.set(true);
            }else if (_isReadStage && !isReadStage) {
                super.lock();
                acquireCount.getAndIncrement();
                _isReadStage = false;
                _acquireLockTids.put(tid, true);
                _isLock.set(true);
            }
        }
    }

    public void unLock(TransactionId tid) {
        System.out.println("unlock !" + "  Thread : " + Thread.currentThread() + " " + _acquireLockTids.size() + " " + tid);
        
        _acquireLockTids.entrySet().stream()
                        .forEach(a->{
                            System.out.print(a + "  ");
                        });
        System.out.println();
        Boolean ac = _acquireLockTids.remove(tid);
        System.out.println(ac);
        System.out.println("Thread : " + Thread.currentThread() +  "  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + "  releaseCount : " + releaseCount + "  acquireCount : " + acquireCount);
        if (_acquireLockTids.size() == 0) {
            System.out.println("acsize = " + _acquireLockTids.size());
            _isLock.set(false);
            super.unlock();
            releaseCount.getAndDecrement();
            System.out.println("unlock !" + "  Thread : " + Thread.currentThread());
            //randSleep();
        }
    }
}
