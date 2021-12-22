package simpledb.transaction;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class LockManagerA {

    private ConcurrentMap<Integer, Lock> _pageLock;
    private ConcurrentMap<simpledb.transaction.TransactionId, Set<Integer>> _tidTakeInPages;


    public LockManagerA() {
        _pageLock = new ConcurrentHashMap<>();
        _tidTakeInPages = new ConcurrentHashMap<>();
    }

    private static LockManagerA lockManagerA = new LockManagerA();

    public static LockManagerA getLockManagerA() {
        return lockManagerA;
    }

    public void acquireReadLock(Integer page, simpledb.transaction.TransactionId tid) {
        makePageLock(tid, true, page);
    }

    public void acquireWriteLock(Integer page, simpledb.transaction.TransactionId tid) {
        makePageLock(tid, false, page);
    }

    public void releaseLock(Integer page, simpledb.transaction.TransactionId tid) {
        _pageLock.get(page).unLock(tid);
    }

    private void makePageLock(simpledb.transaction.TransactionId tid, boolean isReadStage, Integer page) {
        if (_pageLock.containsKey(page)) {
            _pageLock.get(page).lock(isReadStage, tid);
        }else {
            Lock lock = new Lock();
            lock.lock(isReadStage, tid);
            _pageLock.put(page, lock);
        }
    }
}


class Lock extends ReentrantLock {
    private boolean _isReadStage = true;
    private boolean _isLock = false;
    private Set<simpledb.transaction.TransactionId> _acquireLockTids = new HashSet<>();

    public void lock(boolean isReadStage, simpledb.transaction.TransactionId tid) {
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

    public void unLock(simpledb.transaction.TransactionId tid) {
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
