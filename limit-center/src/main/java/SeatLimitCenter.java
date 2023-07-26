/**
 *
 * @author Chen768959
 * @date 2023/5/15
 */
public interface SeatLimitCenter {
    /**
     * 使当前线程“占座”，如果“无座”则此时会排队。
     * （注意，当前线程如果占座“成功”，则需有对应的逻辑确保其“离座”，否则会造成线程长时间占用并发资源，直至被监控杀死并告警）
     * @param queryId
     * @author Chen768959
     * @date 2023/5/15 18:04
     * @return boolean true表示占座成功，当前线程可继续执行。false表示失败，当前系统以达到最大上限，建议终止当前线程的执行。
     */
    boolean seating(String queryId);

    /**
     * 使当前线程"离座"。
     * 释放出当前限流中心的并发数资源。以便其他线程可“占座”。
     * @author Chen768959
     * @date 2023/5/15 20:28
     * @return void
     */
    void outSeat();
}
