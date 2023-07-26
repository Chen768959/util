import org.springframework.data.redis.core.RedisTemplate;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author Chen768959
 * @date 2023/5/17
 */
public class SeatLimitCenterFactory {
    /**
     * 创建“无权重、针对集群、排队阻塞”式的限流中心
     * @param centerName 限流中心名称，相同name会以集群为单位限流
     * @param maxWorkReqNum 最大支持的集群并发线程数
     * @param maxSeatMills 允许线程占有并发资源的最大时长（既：调用seating后，如果超过“此时限”后，还未调用outSeat释放资源，则触发监控告警）
     * @param queueLimit 等待队列长度限制
     * @param maxWaitMills 排队最大等待时间
     * @param redisTemplate
     * @author Chen768959
     * @date 2023/5/18 10:43
     * @return SeatLimitCenter
     */
    public static SeatLimitCenter newUnweightedClusterLimitCenter(String centerName, int maxWorkReqNum, long maxSeatMills, int queueLimit, long maxWaitMills, RedisTemplate redisTemplate){
        return createLimitCenter(new UnweightedClusterSeatLimitCenter(centerName, maxWorkReqNum, maxSeatMills, queueLimit, maxWaitMills, redisTemplate));
    }

    private static <T extends SeatLimitCenter & Destroy> SeatLimitCenter createLimitCenter(T seatLimitCenter) {
        if (seatLimitCenter == null){
            throw new IllegalArgumentException("seatLimitCenter is null");
        }

        InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Object result =method.invoke(seatLimitCenter,args);
                return result;
            }

            @Override
            protected void finalize() throws Throwable {
                seatLimitCenter.destroy();
                super.finalize();
            }
        };

        return (SeatLimitCenter) Proxy.newProxyInstance(SeatLimitCenter.class.getClassLoader(), seatLimitCenter.getClass().getInterfaces(), handler);
    }
}
