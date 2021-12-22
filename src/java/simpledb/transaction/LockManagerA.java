package simpledb.transaction;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
        if (_detectDeadLock.isDeadLock(page, tid)) {
            throw new TransactionAbortedException();
        }
        _detectDeadLock.addTidRequestPages(tid, page);
        makePageLock(tid, true, page);
        _detectDeadLock.deTidRequestPages(tid, page);
        _detectDeadLock.addPageHoldTids(tid, page);
    }

    public void acquireWriteLock(Integer page, simpledb.transaction.TransactionId tid) throws TransactionAbortedException {
        if (_detectDeadLock.isDeadLock(page, tid)) {
            throw new TransactionAbortedException();
        }
        _detectDeadLock.addTidRequestPages(tid, page);
        makePageLock(tid, false, page);
        _detectDeadLock.deTidRequestPages(tid, page);
        _detectDeadLock.addPageHoldTids(tid, page);
    }

    public void releaseLock(Integer page, simpledb.transaction.TransactionId tid) {
        _pageLock.get(page).unLock(tid);
        _detectDeadLock.dePageHoldTids(tid, page);
    }

    private void makePageLock(TransactionId tid, boolean isReadStage, Integer page) {
        if (_pageLock.containsKey(page)) {
            _pageLock.get(page).lock(isReadStage, tid);
        }else {
            Lock lock = new Lock();
            lock.lock(isReadStage, tid);
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

        Set<Integer> requestTids = _tidRequestPages.get(tid);
        return bfs(requestTids, nowTidHoldPages);
    }

    private boolean bfs(Set<Integer> requestTids, Set<Integer> nowTidHoldPages) {
        for(Integer rt : requestTids) {
            for(Integer ntp : nowTidHoldPages) {
                if (rt == ntp) {
                    return true;
                }
            }
        }

        Set<TransactionId> tids = new HashSet<>();
        requestTids.stream()
                    .forEach(a->tids.addAll(_pageHoldTids.get(a)));

        Set<Integer> request2Tids = new HashSet<>();
        tids.stream()
            .forEach(a->request2Tids.addAll(_tidRequestPages.get(a)));

        if (request2Tids.size() == 0) {
            return false;
        }

        Set<Boolean> result = new HashSet<>();
        request2Tids.stream().forEach(a->{
            result.add(bfs(requestTids, nowTidHoldPages));
        });

        if (result.contains(true)) {
            return true;
        }

        return false;
    }

}



class Lock extends ReentrantLock {
    private boolean _isReadStage = true;
    private boolean _isLock = false;
    private Set<TransactionId> _acquireLockTids = new HashSet<>();

    public void lock(boolean isReadStage, TransactionId tid) {
        //randSleep();
        if (!_isLock) {
            _isLock = true;
            super.lock();
            _isReadStage = isReadStage;
            _acquireLockTids.add(tid);
        }else {
            if (_acquireLockTids.contains(tid)) {
                return;
            }else if (_isReadStage && isReadStage) {
                _acquireLockTids.add(tid);
            }else if (!_isReadStage && isReadStage) {
                super.lock();
                _isReadStage = true;
                _acquireLockTids.add(tid);
                _isLock = true;
            }else if (!_isReadStage && !isReadStage) {
                super.lock();
                _isReadStage = false;
                _acquireLockTids.add(tid);
                _isLock = true;
            }else if (_isReadStage && !isReadStage) {
                super.lock();
                _isReadStage = false;
                _acquireLockTids.add(tid);
                _isLock = true;
            }
        }
    }

    public void unLock(TransactionId tid) {
        _acquireLockTids.remove(tid);
        if (_acquireLockTids.size() == 0) {
            super.unlock();
            //randSleep();
            _isLock = false;
        }
    }

//    private void randSleep() {
//        try {
//            System.out.println(Math.random());
//            Thread.sleep(Math.random());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
