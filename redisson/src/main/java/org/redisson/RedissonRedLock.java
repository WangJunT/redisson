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

import java.util.List;

import org.redisson.api.RLock;

/**
 * RedLock locking algorithm implementation for multiple locks. 
 * It manages all locks as one.
 * 
 * @see <a href="http://redis.io/topics/distlock">http://redis.io/topics/distlock</a>
 *
 * @author Nikita Koksharov
 *
 */

/**
 * 在 Redis 的分布式环境中，我们假设有 N 个 Redis master 。这些节点完全互相独立，不存在主从复制或者其他集群协调机制。之前我们已经描述了在 Redis 单实例下怎么安全地获取和释放锁。我们确保将在每 N 个实例上使用此方法获取和释放锁。在这个样例中，我们假设有 5 个Redis master 节点，这是一个比较合理的设置，所以我们需要在 5 台机器上面或者 5 台虚拟机上面运行这些实例，这样保证他们不会同时都宕掉。
 *
 * 为了取到锁，客户端应该执行以下操作:
 *
 * 1、获取当前 Unix 时间，以毫秒为单位。
 * 2、依次尝试从 N 个实例，使用相同的 key 和随机值获取锁。在步骤 2 ，当向 Redis 设置锁时，客户端应该设置一个网络连接和响应超时时间，这个超时时间应该小于锁的失效时间。例如你的锁自动失效时间为 10 秒，则超时时间应该在 5-50 毫秒之间。这样可以避免服务器端 Redis 已经挂掉的情况下，客户端还在死死地等待响应结果。如果服务器端没有在规定时间内响应，客户端应该尽快尝试另外一个 Redis 实例。
 * 3、客户端使用当前时间减去开始获取锁时间（步骤 1 记录的时间）就得到获取锁使用的时间。当且仅当从大多数（这里是 3 个节点）的 Redis 节点都取到锁，并且使用的时间小于锁失效时间时，锁才算获取成功。
 * 4、如果取到了锁，key 的真正有效时间等于有效时间减去获取锁所使用的时间（步骤 3 计算的结果）。
 * 5、如果因为某些原因，获取锁失败（没有在至少 N/2 + 1 个 Redis 实例取到锁或者取锁时间已经超过了有效时间），客户端应该在所有的 Redis 实例上进行解锁（即便某些Redis实例根本就没有加锁成功）。
 * 释放锁：
 *
 * 1、释放锁比较简单，向所有的 Redis 实例发送释放锁命令即可，不用关心之前有没有从Redis实例成功获取到锁.
 */
public class RedissonRedLock extends RedissonMultiLock {

    /**
     * Creates instance with multiple {@link RLock} objects.
     * Each RLock object could be created by own Redisson instance.
     *
     * @param locks - array of locks
     */
    public RedissonRedLock(RLock... locks) {
        super(locks);
    }

    @Override
    protected int failedLocksLimit() {
        return locks.size() - minLocksAmount(locks);
    }
    
    protected int minLocksAmount(final List<RLock> locks) {
        return locks.size()/2 + 1;
    }

    @Override
    protected long calcLockWaitTime(long remainTime) {
        return Math.max(remainTime / locks.size(), 1);
    }
    
    @Override
    public void unlock() {
        unlockInner(locks);
    }

}
