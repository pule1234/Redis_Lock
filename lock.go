package redis_lock

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/pule1234/redis_lock/utils"
	"sync/atomic"
	"time"
)

const RedisLockKeyPrefix = "REDIS_LOCK_PREFIX_"

var ErrLockAcquiredByOthers = errors.New("lock is acquired by others")

var ErrNil = redis.ErrNil

func IsRetryableErr(err error) bool {
	return errors.Is(err, ErrLockAcquiredByOthers)
}

// 基于redis实现的分布式锁，不可重入，但保证了对称性
type RedisLock struct {
	LockOptions
	Key    string
	token  string
	client LockClient

	// 看门狗运作标识
	runningDog int32
	// 停止看门狗
	stopDog context.CancelFunc
}

func NewRedisLock(key string, client LockClient, opts ...LockOption) *RedisLock {
	r := RedisLock{
		Key:    key,
		token:  utils.GetProcessAndGoroutineIDStr(),
		client: client,
	}

	for _, opt := range opts {
		opt(&r.LockOptions)
	}

	repairLock(&r.LockOptions)
	return &r
}

// Lock 加锁
func (r *RedisLock) Lock(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			return
		}
		// 加锁成功的情况下，会启动看门狗
		// 该锁本身是不可重入的，所以不会出现同一把锁下看门狗重复启动的情况
		r.watchDog(ctx)
	}()

	// 不管是不是阻塞模式, 都要先获取一把锁
	err = r.tryLock(ctx)
	if err == nil {
		// 加锁成功，直接返回
		return nil
	}

	// 非阻塞模式下加锁失败直接返回错误
	if !r.isBlock {
		return err
	}

	// 判断错误是否可以允许重试， 不可允许的类型则直接返回错误
	if !IsRetryableErr(err) {
		return err
	}

	// 基于阻塞模式持续轮询取锁
	err = r.blockingLock(ctx)
	return
}

func (r *RedisLock) tryLock(ctx context.Context) error {
	// 首先查询锁是否属于自己
	reply, err := r.client.SetNEX(ctx, r.getLockKey(), r.token, r.expireSeconds)
	if err != nil {
		return err
	}

	if reply != 1 {
		return fmt.Errorf("reply: %d, err: %w", reply, ErrLockAcquiredByOthers)
	}

	return nil
}

func (r *RedisLock) watchDog(ctx context.Context) {
	// 非看门狗模式
	if !r.watchDogMode {
		return
	}

	// 确保之前启动的看门狗已经正常回收
	for !atomic.CompareAndSwapInt32(&r.runningDog, 0, 1) {
	}

	// 启动看门狗
	ctx, r.stopDog = context.WithCancel(ctx)
	go func() {
		defer func() {
			atomic.StoreInt32(&r.runningDog, 0)
		}()
		r.runWatchDog(ctx)
	}()
}

func (r *RedisLock) runWatchDog(ctx context.Context) {
	ticker := time.NewTicker(WatchDogWorkStepSeconds * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 看门狗机制负责在用户未显示解锁时，持续未分布式锁进行续期
		// 通过lua脚本，延期之前会确保锁仍然属于自己
		// 未避免因为网络延迟而导致的锁被提前释放的问题， 看门狗续约时间需要把锁的过期时长额外增加5s
		_ = r.DelayExpire(ctx, WatchDogWorkStepSeconds+5)
	}
}

// 更新锁的过期时间， 基于lua脚本实现操作的原子性
func (r *RedisLock) DelayExpire(ctx context.Context, expireSeconds int64) error {
	keysAndArgs := []interface{}{r.getLockKey(), r.token, expireSeconds}
	reply, err := r.client.Eval(ctx, LuaCheckAndExpireDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not expire lock without ownership of lock")
	}

	return nil
}

func (r *RedisLock) blockingLock(ctx context.Context) error {
	// 阻塞模式等锁时间上限
	timeoutCh := time.After(time.Duration(r.blockWaitingSeconds) * time.Second)
	// 轮询tikcer 每隔50ms尝试取锁一次
	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		//ctx 终止了
		case <-ctx.Done():
			return fmt.Errorf("lock failed, ctx timeout, err: %w", ctx.Err())
		case <-timeoutCh:
			return fmt.Errorf("block waiting time out, err: %w", ErrLockAcquiredByOthers)
		default:
		}

		// 尝试取锁
		err := r.tryLock(ctx)
		if err != nil {
			// 加锁成功
			return nil
		}
		// 不可重试类型的错误，直接返回
		if !IsRetryableErr(err) {
			return err
		}
	}

	return nil
}

// 解锁， 在redis中删除该键值对，并且停止看门狗
func (r *RedisLock) Unlock(ctx context.Context) error {
	defer func() {
		// 停止看门狗
		if r.stopDog != nil {
			r.stopDog()
		}
	}()

	keysAndArgs := []interface{}{r.getLockKey(), r.token}

	reply, err := r.client.Eval(ctx, LuaCheckAndDeleteDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not unlock without ownership of lock")
	}

	return nil
}

func (r *RedisLock) getLockKey() string {
	return RedisLockKeyPrefix + r.Key
}
