/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.BatchOptions;
import org.redisson.api.BatchResult;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.client.protocol.decoder.MapValueDecoder;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.connection.MasterSlaveEntry;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * Base class for implementing distributed locks
 *
 * @author Danila Varatyntsev
 * @author Nikita Koksharov
 */
public abstract class RedissonBaseLock extends RedissonExpirable implements RLock {

    public static class ExpirationEntry {

        private final Map<Long, Integer> threadIds = new LinkedHashMap<>();
        private volatile Timeout timeout;

        public ExpirationEntry() {
            super();
        }

        public synchronized void addThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                counter = 1;
            } else {
                counter++;
            }
            threadIds.put(threadId, counter);
        }
        public synchronized boolean hasNoThreads() {
            return threadIds.isEmpty();
        }
        public synchronized Long getFirstThreadId() {
            if (threadIds.isEmpty()) {
                return null;
            }
            return threadIds.keySet().iterator().next();
        }
        public synchronized void removeThreadId(long threadId) {
            Integer counter = threadIds.get(threadId);
            if (counter == null) {
                return;
            }
            counter--;
            if (counter == 0) {
                threadIds.remove(threadId);
            } else {
                threadIds.put(threadId, counter);
            }
        }


        public void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }
        public Timeout getTimeout() {
            return timeout;
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseLock.class);

    private static final ConcurrentMap<String, ExpirationEntry> EXPIRATION_RENEWAL_MAP = new ConcurrentHashMap<>();
    protected long internalLockLeaseTime;

    final String id;
    final String entryName;

    final CommandAsyncExecutor commandExecutor;

    public RedissonBaseLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.id = commandExecutor.getConnectionManager().getId();
        this.internalLockLeaseTime = commandExecutor.getConnectionManager().getCfg().getLockWatchdogTimeout();
        this.entryName = id + ":" + name;
    }

    protected String getEntryName() {
        return entryName;
    }

    protected String getLockName(long threadId) {
        return id + ":" + threadId;
    }

    private void renewExpiration() {
        ExpirationEntry ee = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (ee == null) {
            return;
        }
        
        Timeout task = commandExecutor.getConnectionManager().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                ExpirationEntry ent = EXPIRATION_RENEWAL_MAP.get(getEntryName());
                if (ent == null) {
                    return;
                }
                Long threadId = ent.getFirstThreadId();
                if (threadId == null) {
                    return;
                }
                
                RFuture<Boolean> future = renewExpirationAsync(threadId);
                future.onComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't update lock " + getRawName() + " expiration", e);
                        EXPIRATION_RENEWAL_MAP.remove(getEntryName());
                        return;
                    }
                    
                    if (res) {
                        // reschedule itself
                        renewExpiration();
                    } else {
                        cancelExpirationRenewal(null);
                    }
                });
            }
        }, internalLockLeaseTime / 3, TimeUnit.MILLISECONDS);
        
        ee.setTimeout(task);
    }
    
    protected void scheduleExpirationRenewal(long threadId) {
        ExpirationEntry entry = new ExpirationEntry();
        ExpirationEntry oldEntry = EXPIRATION_RENEWAL_MAP.putIfAbsent(getEntryName(), entry);
        if (oldEntry != null) {
            oldEntry.addThreadId(threadId);
        } else {
            entry.addThreadId(threadId);
            renewExpiration();
        }
    }
    //实现续锁逻辑。
    /**
     * 大家都知道，如果负责储存这个分布式锁的Redisson节点宕机以后，
     * 而且这个锁正好处于锁住的状态时，这个锁会出现锁死的状态。
     * 为了避免这种情况的发生，Redisson内部提供了一个监控锁的看门狗，
     * 它的作用是在Redisson实例被关闭前，不断的延长锁的有效期。
     * 默认情况下，看门狗的检查锁的超时时间是30秒钟，也可以通过修改 Config.lockWatchdogTimeout 来另行指定。
     *
     *
     *
     * 在使用 RedissonLock#lock() 方法，我们要求持续持有锁，直到手动释放。
     * 但是实际上，我们有一个隐藏条件，如果 Java 进程挂掉时，需要自动释放。
     * 那么，如果实现 RedissonLock#lock() 时，设置过期 Redis 为无限大，或者不过期都不合适。
     * 那么 RedissonLock 是怎么实现的呢？
     * RedissonLock 先获得一个 internalLockLeaseTime 的分布式锁，然后每 internalLockLeaseTime / 3 时间，
     * 定时调用 #renewExpirationAsync(long threadId) 方法，进行续租。这样，在 Java 进程异常 Crash 掉后，
     * 能够保证最多 internalLockLeaseTime 时间后，分布式锁自动释放。
     *
     * 略骚略巧妙~不过为了实现这样的功能，RedissonLock 的整体逻辑，又复杂了一丢丢。
     * @param threadId
     * @return
     */
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +// 情况一，如果持有锁，则重新设置过期时间为 ARGV[1] internalLockLeaseTime ，并返回 1 续租成功。
                        "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                        "return 1; " +
                        "end; " +
                        "return 0;",
                Collections.singletonList(getRawName()),
                internalLockLeaseTime, getLockName(threadId));
    }

    protected void cancelExpirationRenewal(Long threadId) {
        ExpirationEntry task = EXPIRATION_RENEWAL_MAP.get(getEntryName());
        if (task == null) {
            return;
        }
        
        if (threadId != null) {
            task.removeThreadId(threadId);
        }

        if (threadId == null || task.hasNoThreads()) {
            Timeout timeout = task.getTimeout();
            if (timeout != null) {
                timeout.cancel();
            }
            EXPIRATION_RENEWAL_MAP.remove(getEntryName());
        }
    }

    protected <T> RFuture<T> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object... params) {
        CommandBatchService executorService = createCommandBatchService();
        RFuture<T> result = executorService.evalWriteAsync(key, codec, evalCommandType, script, keys, params);
        if (commandExecutor instanceof CommandBatchService) {
            return result;
        }

        RPromise<T> r = new RedissonPromise<>();
        RFuture<BatchResult<?>> future = executorService.executeAsync();
        future.onComplete((res, ex) -> {
            if (ex != null) {
                r.tryFailure(ex);
                return;
            }

            r.trySuccess(result.getNow());
        });
        return r;
    }

    private CommandBatchService createCommandBatchService() {
        if (commandExecutor instanceof CommandBatchService) {
            return (CommandBatchService) commandExecutor;
        }

        MasterSlaveEntry entry = commandExecutor.getConnectionManager().getEntry(getRawName());
        BatchOptions options = BatchOptions.defaults()
                                .syncSlaves(entry.getAvailableSlaves(), 1, TimeUnit.SECONDS);

        return new CommandBatchService(commandExecutor, options);
    }

    protected void acquireFailed(long waitTime, TimeUnit unit, long threadId) {
        get(acquireFailedAsync(waitTime, unit, threadId));
    }
    
    protected RFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        return RedissonPromise.newSucceededFuture(null);
    }

    @Override
    public Condition newCondition() {
        // TODO implement
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocked() {
        return isExists();
    }
    
    @Override
    public RFuture<Boolean> isLockedAsync() {
        return isExistsAsync();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return isHeldByThread(Thread.currentThread().getId());
    }

    @Override
    public boolean isHeldByThread(long threadId) {
        RFuture<Boolean> future = commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.HEXISTS, getRawName(), getLockName(threadId));
        return get(future);
    }

    private static final RedisCommand<Integer> HGET = new RedisCommand<Integer>("HGET", new MapValueDecoder(), new IntegerReplayConvertor(0));
    
    public RFuture<Integer> getHoldCountAsync() {
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, HGET, getRawName(), getLockName(Thread.currentThread().getId()));
    }
    
    @Override
    public int getHoldCount() {
        return get(getHoldCountAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return forceUnlockAsync();
    }

    @Override
    public RFuture<Void> unlockAsync() {
        long threadId = Thread.currentThread().getId();
        return unlockAsync(threadId);
    }

    @Override
    public RFuture<Void> unlockAsync(long threadId) {
        RPromise<Void> result = new RedissonPromise<>();
        RFuture<Boolean> future = unlockInnerAsync(threadId);

        future.onComplete((opStatus, e) -> {
            cancelExpirationRenewal(threadId);

            if (e != null) {
                result.tryFailure(e);
                return;
            }

            if (opStatus == null) {
                IllegalMonitorStateException cause = new IllegalMonitorStateException("attempt to unlock lock, not locked by current thread by node id: "
                        + id + " thread-id: " + threadId);
                result.tryFailure(cause);
                return;
            }

            result.trySuccess(null);
        });

        return result;
    }

    protected abstract RFuture<Boolean> unlockInnerAsync(long threadId);
}
