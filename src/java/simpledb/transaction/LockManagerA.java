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
        _pageLock.get(page).unLock(tid);
        System.out.println("unLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page + " ");
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

        Set<Integer> TidsrequestPages = new HashSet<>();
        TidsrequestPages.add(page);
        if (nowTidHoldPages.contains(page)) {
            return false;
        }
        return bfs(TidsrequestPages, nowTidHoldPages);
    }

    private boolean bfs(Set<Integer> TidsrequestPages, Set<Integer> nowTidHoldPages) {
        while (true) {
            for(Integer rt : TidsrequestPages) {
                for(Integer ntp : nowTidHoldPages) {
                    if (rt == ntp) {
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
                        _acquireLockTids.remove(tid);
                        super.lock();
                    }
                    // _acquireLockTids.put(tid, true);
                    _isReadStage = false;
                    _isLock.set(true);
                } else {
                    return;
                }
            }else if (_isReadStage && isReadStage) {
                _acquireLockTids.put(tid, false);
            }else if (!_isReadStage && isReadStage) {
                super.lock();
                _isReadStage = true;
                _acquireLockTids.put(tid, true);
                _isLock.set(true);
            }else if (!_isReadStage && !isReadStage) {
                super.lock();
                _isReadStage = false;
                _acquireLockTids.put(tid, true);
                _isLock.set(true);
            }else if (_isReadStage && !isReadStage) {
                super.lock();
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
        if (ac != null && _acquireLockTids.size() == 0) {
            System.out.println("acsize = " + _acquireLockTids.size());
            _isLock.set(false);
            
            super.unlock();
            // try {
            //     Thread.sleep((int)(Math.random() * 1000));
            // } catch (Exception e) {
            //     //TODO: handle exception
            // }
            System.out.println("unlock !" + "  Thread : " + Thread.currentThread());
            //randSleep();
        }
    }
}
