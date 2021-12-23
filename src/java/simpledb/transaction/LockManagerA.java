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
        System.out.println("ReadLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        _detectDeadLock.deTidRequestPages(tid, page);
        _detectDeadLock.addPageHoldTids(tid, page);
    }

    public void acquireWriteLock(Integer page, simpledb.transaction.TransactionId tid) throws TransactionAbortedException {
        if (_detectDeadLock.isDeadLock(page, tid)) {
            throw new TransactionAbortedException();
        }
        _detectDeadLock.addTidRequestPages(tid, page);
        System.out.println("WriteLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        makePageLock(tid, false, page);
        System.out.println("WriteLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page);
        _detectDeadLock.deTidRequestPages(tid, page);
        _detectDeadLock.addPageHoldTids(tid, page);
    }

    public void releaseLock(Integer page, simpledb.transaction.TransactionId tid) {
        _pageLock.get(page).unLock(tid);
        System.out.println("unLock !" + "  Thread : " + Thread.currentThread() + "  " + tid + "  " + page + " ");
        _detectDeadLock.dePageHoldTids(tid, page);
    }

    private void makePageLock(TransactionId tid, boolean isReadStage, Integer page) {
        if (_pageLock.containsKey(page)) {
            _pageLock.get(page).lock(isReadStage, tid);
        }else {
            Lock lock = new Lock();
            _pageLock.put(page, lock);
            lock.lock(isReadStage, tid);
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
    private volatile boolean _isLock = false;
    private Set<TransactionId> _acquireLockTids = new HashSet<>();

    public void lock(boolean isReadStage, TransactionId tid) {
        // randSleep();
        // try {
        //     Thread.sleep((int)(Math.random() * 1000));
        // } catch (Exception e) {
        //     //TODO: handle exception
        // }
        System.out.println("Thread : " + Thread.currentThread() + " isLock : " + _isLock);
        System.out.println("lock====" + "Thread : " + Thread.currentThread());
        if (!_isLock) {
            _isLock = true;
            super.lock();
            System.out.println("locked !");
            _isReadStage = isReadStage;
            _acquireLockTids.add(tid);
        }else {
            if (_acquireLockTids.contains(tid)) {
                if (_isReadStage && !isReadStage) {
                    System.out.println("***********" +  "  Thread : " + Thread.currentThread() + " tid " + tid);
                    //super.unlock();
                    if (_acquireLockTids.size() > 1) {
                        super.lock();
                    }
                    //System.out.println("***********?");
                    _isReadStage = false;
                    _isLock = true;
                } else {
                    return;
                }
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
        System.out.println("unlock !" + "  Thread : " + Thread.currentThread() + _acquireLockTids.size());
        if (_acquireLockTids.remove(tid) && _acquireLockTids.size() == 0) {
            System.out.println("acsize = " + _acquireLockTids.size());
            super.unlock();
            // try {
            //     Thread.sleep((int)(Math.random() * 1000));
            // } catch (Exception e) {
            //     //TODO: handle exception
            // }
            System.out.println("unlock !" +   "  Thread : " + Thread.currentThread() );
            //randSleep();
            _isLock = false;
        }
    }


}
