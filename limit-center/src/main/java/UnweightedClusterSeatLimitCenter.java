import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 限流中心（cluster）
 * @author Chen768959
 * @date 2023/5/15
 */
@Slf4j
public class UnweightedClusterSeatLimitCenter implements SeatLimitCenter, Destroy {
    // 负载空闲时，消费线程循环等待时间
    private static final int CONSUMER_FREE_WAIT_TIME = 3000;
    // 负载空闲时，监控超时线程循环等待时间
    private static final int MONITOR_FREE_WAIT_TIME = 10000;
    // 限流中心 redis前缀（用以区分redis中 限流中心与其余业务）
    private static final String REDIS_PRE_STR = "LimitCenter-";
    // 限流中心-线程key redis前缀（用以区分redis中 相同限流中心各进程之间的相同线程id）
    private final String REDIS_KEY_PRE_STR;

    // 限流中心名称，相同name会以集群为单位限流
    private final String centerName;
    // 最大支持的集群并发线程数
    private final int maxWorkReqNum;
    // 允许线程占有并发资源的最大时长（既：调用seating后，如果超过“此时限”后，还未调用outSeat释放资源，则触发监控告警）
    private final long maxSeatMills;
    // 等待队列长度限制
    private final int queueLimit;
    // 排队最大等待时间
    private final long maxWaitMills;
    private final RedisTemplate redisTemplate;

    // 排队等待超时的线程信息，key：线程id，value：线程信息
    private final Map<String, ThreadOb> waitOverTimeMap = new ConcurrentHashMap<>();
    // 线程排队等待队列
    private final Deque<ThreadOb> concurrentLinkedDeque = new ConcurrentLinkedDeque<>();
    // ConcurrentLinkedDeque队列获取size代价较大，通过此原子int单独维护队列当前大小。
    private AtomicInteger currentDequeSize = new AtomicInteger();
    private final ExecutorService consumerExecutorService;
    private final ExecutorService monitorExecutorService;

    private volatile boolean destroy = false;

    public UnweightedClusterSeatLimitCenter(String centerName, int maxWorkReqNum, long maxSeatMills, int queueLimit, long maxWaitMills, RedisTemplate redisTemplate){
        if (redisTemplate == null){
            throw new NullPointerException("RedisTemplate is NULL");
        }
        this.centerName = centerName;
        this.maxWorkReqNum = maxWorkReqNum;
        this.maxWaitMills = maxWaitMills;
        this.queueLimit = queueLimit + maxWorkReqNum;
        this.maxSeatMills = maxSeatMills;
        this.redisTemplate = redisTemplate;
        String shortUuid = getShortUuid();
        this.REDIS_KEY_PRE_STR = shortUuid+"-";

        // 启动消费任务
        this.consumerExecutorService = startConsumerTask();

        // 启动监控任务
        this.monitorExecutorService = startMonitorTask();

        log.info("SeatLimitCenter# start success. "+ centerName
                + "...REDIS_KEY_PRE_STR:" + REDIS_KEY_PRE_STR
                + "...maxWorkReqNum:" + maxWorkReqNum
                + "...maxSeatMills:" + maxSeatMills
                + "...queueLimit:" + queueLimit
                + "...maxWaitMills:" + maxWaitMills);
    }

    /**
     * 判断能否消费等待线程出来"占座"，
     * 可以则消费线程并占座，
     * 增加当前占座线程计数。
     * @author Chen768959
     * @date 2023/5/15 16:52
     * @return void
     */
    private ExecutorService startConsumerTask() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while ( ! destroy ){
                    if (currentDequeSize.get() == 0){
                        try {
                            Thread.sleep(CONSUMER_FREE_WAIT_TIME);
                            continue;
                        } catch (InterruptedException e) {
                            log.error("ConsumerTask error,centerName: "+centerName, e);
                        }
                    }

                    int currentWorkNum = 0;
                    try{
                        currentWorkNum = getCurrentWorkNum();
                    }catch (Exception e){
                        log.error("ConsumerTask getCurrentWorkNum error,centerName: "+centerName, e);
                        continue;
                    }

                    if (currentWorkNum < maxWorkReqNum){
                        ThreadOb threadOb = concurrentLinkedDeque.poll();
                        if (threadOb != null){
                            if (trySeating(threadOb.getThreadId())){
                                log.info("SeatLimitCenter# consume success,centerName: "+centerName+"...queryId:" + threadOb.getThreadInfo() + "...threadId:" + threadOb.getThreadId());
                                signalThread(threadOb);
                                currentDequeSize.decrementAndGet();
                            }else {
                                concurrentLinkedDeque.offerFirst(threadOb);
                            }
                        }
                    }else {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error("ConsumerTask error,centerName: "+centerName, e);
                        }
                    }
                }
            }
        });
        return executorService;
    }

    /**
     * 1、监控队列头部线程是否等待超时，
     * 超时则消费后设置超时标志并唤起线程。
     * 2、监控已占座线程是否占座超时，
     * 超时则告警且离座。
     * @author Chen768959
     * @date 2023/5/15 16:53
     * @return void
     */
    private ExecutorService startMonitorTask() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while ( ! destroy ){
                    int currentWorkNum = getCurrentWorkNum();
                    if (currentDequeSize.get() == 0 && currentWorkNum < maxWorkReqNum ){
                        try {
                            Thread.sleep(MONITOR_FREE_WAIT_TIME);
                            continue;
                        } catch (InterruptedException e) {
                            log.error("ConsumerTask error,centerName: "+centerName, e);
                        }
                    }
                    long concurrentTime = System.currentTimeMillis();

                    // 监控线程是否等待超时
                    ThreadOb firstThreadOb = concurrentLinkedDeque.peekFirst();
                    if (firstThreadOb != null){
                        long timing = concurrentTime - firstThreadOb.getStartWaitTime();
                        if (timing >= maxWaitMills){
                            ThreadOb pollThreadOb = concurrentLinkedDeque.poll();
                            if (pollThreadOb != null){
                                if (pollThreadOb == firstThreadOb){
                                    currentDequeSize.decrementAndGet();
                                    setTimeout(pollThreadOb);
                                    signalThread(pollThreadOb);
                                    log.error("SeatLimitCenter# 线程等待超时,centerName: "+centerName+"...threadId: "+pollThreadOb.getThreadId()+"...threadInfo: "+pollThreadOb.getThreadInfo());
                                }else {
                                    concurrentLinkedDeque.offerFirst(pollThreadOb);
                                }
                            }
                        }
                    }

                    // 监控线程是否占座超时
                    try {
                        List<ThreadOb> currentWorkList = getCurrentWorkList();
                        currentWorkList.forEach(threadOb ->{
                            if ((concurrentTime - threadOb.getStartWorkTime()) >= maxSeatMills){
                                outSeat(threadOb.getThreadId());
                                log.error("SeatLimitCenter# 线程占座超时,centerName: "+centerName+"...threadId: "+threadOb.getThreadId()+"...threadWorkStartTime: "+threadOb.getStartWorkTime());
                            }
                        });
                    }catch (Exception e){
                        log.error("outSeat error,centerName: "+centerName, e);
                    }
                }
            }
        });
        return executorService;
    }

    @Override
    public boolean seating(String queryId) {
        if (destroy){
            throw new IllegalStateException("数据源已变更，请重试");
        }

        String threadId = REDIS_KEY_PRE_STR + Thread.currentThread().getId();
        log.info("SeatLimitCenter# start seat, centerName: "+centerName+"...queryId:" + queryId + "...threadId:" + threadId);

        // 判断是否需要排队阻塞，不需要则直接增加计数
        if ( !needQueueNow() && trySeating(threadId)){
            log.info("SeatLimitCenter# success seat, centerName: "+centerName+"...queryId:" + queryId + "...threadId:" + threadId);
            return true;
        }

        try {
            Lock lock = new ReentrantLock();
            Condition condition = lock.newCondition();
            putQueueWait(threadId, lock, condition, queryId); // 排队阻塞
        }catch (Exception e){
            log.error("SeatLimitCenter# putQueueWait error,centerName: "+centerName,e);
            return false;
        }

        boolean timeout = checkWaitTimeoutAndClearRecord(threadId);
        if (!timeout){
            log.info("SeatLimitCenter# success seat (awaken), centerName: "+centerName+"...queryId:" + queryId + "...threadId:" + threadId);
        }
        return !timeout;
    }

    @Override
    public void outSeat() {
        outSeat(REDIS_KEY_PRE_STR + Thread.currentThread().getId());
    }

    /**
     * 当前是否需要排队
     * @author Chen768959
     * @date 2023/5/15 18:09
     * @return boolean true：需要排队
     */
    private boolean needQueueNow() {
        return (currentDequeSize.get() != 0 || getCurrentWorkNum() >= maxWorkReqNum);
    }

    /**
     * 增加全局占座计数，并占座
     * @author Chen768959
     * @date 2023/5/15 18:09
     * @return boolean true：占座成功
     */
    private boolean trySeating(String threadId) {
        int currentWorkNum = addAndGetCurrentWorkNum(threadId);
        if (currentWorkNum > maxWorkReqNum){
            reduceCurrentWorkNum(threadId);
            return false;
        }

        return true;
    }

    /**
     * 排队且阻塞等待
     * @param threadId
     * @param lock
     * @param condition
     * @author Chen768959
     * @date 2023/5/15 18:10
     * @return void
     */
    private void putQueueWait(String threadId, Lock lock, Condition condition, String threadInfo) throws IllegalStateException,InterruptedException {
        int currentDequeSizeAdd = currentDequeSize.incrementAndGet();
        if (currentDequeSizeAdd > queueLimit){
            currentDequeSize.decrementAndGet();
            throw new IllegalStateException("当前等待队列达到上限");
        }

        ThreadOb threadOb = new ThreadOb(threadId);
        threadOb.setLock(lock);
        threadOb.setCondition(condition);
        threadOb.setStartWaitTime(System.currentTimeMillis());
        threadOb.setThreadInfo(threadInfo);
        try {
            lock.lock();
            concurrentLinkedDeque.offerLast(threadOb);
            log.info("SeatLimitCenter# putQueueWait,centerName: "+centerName+"...queryId:" + threadInfo + "...threadId:" + threadId);
            condition.await();
        }finally {
            lock.unlock();
        }
    }

    /**
     * 检查指定线程是否存在等待超时的情况。
     * 如果存在超时，则返回true，且会清除记录
     * @param threadId
     * @author Chen768959
     * @date 2023/5/15 18:12
     * @return boolean true：有超时情况
     */
    private boolean checkWaitTimeoutAndClearRecord(String threadId) {
        ThreadOb threadOb = waitOverTimeMap.get(threadId);
        if (threadOb != null){
            waitOverTimeMap.remove(threadOb);
            return true;
        }
        return false;
    }

    /**
     * 将指定线程“离座”，减少全局占座计数
     * @param id
     * @author Chen768959
     * @date 2023/5/12 18:16
     * @return void
     */
    private void outSeat(String id) {
        log.info("SeatLimitCenter# outSeat,centerName: "+centerName+"... threadId:" + id);
        reduceCurrentWorkNum(id);
    }

    private void signalThread(ThreadOb threadOb) {
        threadOb.getLock().lock();
        threadOb.getCondition().signal();
        threadOb.getLock().unlock();
    }

    /**
     * 设置线程超时
     * @param pollThreadOb
     * @author Chen768959
     * @date 2023/5/15 15:58
     * @return void
     */
    private void setTimeout(ThreadOb pollThreadOb) {
        waitOverTimeMap.put(pollThreadOb.getThreadId(), pollThreadOb);
    }

    private int addAndGetCurrentWorkNum(String threadId) {
        redisTemplate.opsForHash().put(REDIS_PRE_STR+centerName, threadId, System.currentTimeMillis());
        return getCurrentWorkNum();
    }

    private void reduceCurrentWorkNum(String threadId) {
        redisTemplate.opsForHash().delete(REDIS_PRE_STR+centerName, threadId);
    }

    private int getCurrentWorkNum() {
        return redisTemplate.opsForHash().size(REDIS_PRE_STR+centerName).intValue();
    }

    private List<ThreadOb> getCurrentWorkList() {
        List<ThreadOb> threadObList = new ArrayList<>();
        Map<String, Long> entries = (Map<String, Long>) redisTemplate.opsForHash().entries(REDIS_PRE_STR + centerName);
        entries.forEach((threadId,startWorkTime)->{
            if (threadId!=null && startWorkTime!=null){
                ThreadOb threadOb = new ThreadOb(threadId);
                threadOb.setStartWorkTime(startWorkTime);
                threadObList.add(threadOb);
            }
        });
        return threadObList;
    }

    @Override
    public void destroy(){
        log.info("SeatLimitCenter# destroy start, centerName: "+centerName);
        destroy = true;

        // 关闭所有线程池
        consumerExecutorService.shutdown();
        monitorExecutorService.shutdown();

        // 释放所有等待线程
        ThreadOb pollThreadOb = null;
        while ((pollThreadOb = concurrentLinkedDeque.poll()) != null){
            currentDequeSize.decrementAndGet();
            setTimeout(pollThreadOb);
            signalThread(pollThreadOb);
            log.info("SeatLimitCenter# destroying release Thread, centerName: "+centerName+"...threadId: "+pollThreadOb.getThreadId()+"...threadInfo: "+pollThreadOb.getThreadInfo());
        }

        // 所有正在执行线程出座
        getOwnCenterWorkList(getCurrentWorkList()).forEach(currentOwnWork -> {
            outSeat(currentOwnWork.getThreadId());
        });
    }

    private List<ThreadOb> getOwnCenterWorkList(List<ThreadOb> currentWorkList) {
        return currentWorkList.stream().filter(threadOb -> {
            return threadOb.getThreadId().startsWith(REDIS_KEY_PRE_STR);
        }).collect(Collectors.toList());
    }

    @Override
    protected void finalize() throws Throwable {
        log.info("SeatLimitCenter# destroy end, centerName: "+centerName);
        super.finalize();
    }

    @Data
    private class ThreadOb{
        private final String threadId;
        private Lock lock;
        private Condition condition;
        private long startWaitTime;
        private long startWorkTime;
        private String threadInfo;

        public ThreadOb(String threadId){
            this.threadId = threadId;
        }

        @Override
        public boolean equals(Object anObject) {
            if (this == anObject) {
                return true;
            }
            if (anObject instanceof ThreadOb) {
                ThreadOb anotherThreadOb = (ThreadOb)anObject;
                if (anotherThreadOb.getThreadId() == threadId){
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return threadId.hashCode();
        }
    }


    private static final String[] CHARS = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
            "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6", "7", "8",
            "9", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
            "U", "V", "W", "X", "Y", "Z" };

    private String getShortUuid() {
        StringBuffer shortBuffer = new StringBuffer();
        String uuid = UUID.randomUUID().toString().replace("-", "");
        for (int i = 0; i < 8; i++) {
            String str = uuid.substring(i * 4, i * 4 + 4);
            int x = Integer.parseInt(str, 16);
            shortBuffer.append(CHARS[x % 0x3E]);
        }
        return shortBuffer.toString();

    }
}
