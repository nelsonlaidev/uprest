import assert from 'node:assert/strict'
import { Ratelimit } from '@upstash/ratelimit'
import { Redis } from '@upstash/redis'

const url = process.env.UPREST_TEST_URL
const token = process.env.UPREST_TEST_TOKEN

assert.ok(url, 'UPREST_TEST_URL is required')
assert.ok(token, 'UPREST_TEST_TOKEN is required')

const redis = new Redis({
  url,
  token,
  retry: false,
  enableTelemetry: false,
})

const prefix: string = `uprest:sdk:${Date.now()}:${process.pid}`
const valueKey = `${prefix}:value`
const countKey = `${prefix}:count`
const hashKey = `${prefix}:hash`
const listKey = `${prefix}:list`
const transactionKey = `${prefix}:transaction`
const subscriptionChannel = `${prefix}:subscription`

try {
  assert.equal(await redis.set(valueKey, 'hello'), 'OK')
  assert.equal(await redis.get(valueKey), 'hello')

  assert.equal(await redis.incr(countKey), 1)
  assert.equal(await redis.expire(countKey, 60), 1)

  assert.equal(await redis.hset(hashKey, { field: 'world' }), 1)
  assert.deepEqual(await redis.hgetall(hashKey), { field: 'world' })

  assert.equal(await redis.lpush(listKey, 'OK'), 1)
  assert.deepEqual(await redis.lrange(listKey, 0, -1), ['OK'])

  const pipeline = redis.pipeline()

  pipeline.set(valueKey, 'updated')
  pipeline.get(valueKey)

  assert.deepEqual(await pipeline.exec(), ['OK', 'updated'])

  const transaction = redis.multi()

  transaction.set(transactionKey, 0)
  transaction.incr(transactionKey)
  transaction.get(transactionKey)

  assert.deepEqual(await transaction.exec(), ['OK', 1, 1])

  const subscriber = redis.subscribe<{ source: string; value: number }>(subscriptionChannel)

  try {
    const message = await new Promise<{ channel: string; message: { source: string; value: number } }>(
      (resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Timed out waiting for subscription message')), 5_000)

        subscriber.on('error', (error) => {
          clearTimeout(timeout)
          reject(error)
        })

        subscriber.on('subscribe', (count) => {
          try {
            assert.equal(count, 1)
          } catch (error) {
            clearTimeout(timeout)
            reject(error)
            return
          }

          void redis.publish(subscriptionChannel, { source: 'sdk', value: 1 }).catch((error) => {
            clearTimeout(timeout)
            reject(error)
          })
        })

        subscriber.on('message', (event) => {
          clearTimeout(timeout)
          resolve(event)
        })
      },
    )

    assert.deepEqual(message, {
      channel: subscriptionChannel,
      message: { source: 'sdk', value: 1 },
    })
  } finally {
    await subscriber.unsubscribe()
  }

  const algorithms = [
    ['fixed-window', Ratelimit.fixedWindow(1, '1 m')],
    ['sliding-window', Ratelimit.slidingWindow(1, '1 m')],
    ['token-bucket', Ratelimit.tokenBucket(1, '1 m', 1)],
  ] as const

  for (const [name, limiter] of algorithms) {
    const identifier = `${prefix}:${name}:identifier`
    const ratelimit = new Ratelimit({
      redis,
      limiter,
      prefix: `${prefix}:${name}`,
      ephemeralCache: false,
      enableTelemetry: false,
    })

    try {
      assert.equal((await ratelimit.limit(identifier)).success, true)
      assert.equal((await ratelimit.limit(identifier)).success, false)
    } finally {
      await ratelimit.resetUsedTokens(identifier)
    }
  }

  console.log('@upstash/redis and @upstash/ratelimit smoke tests passed')
} finally {
  await redis.del(valueKey, countKey, hashKey, listKey, transactionKey)
}
