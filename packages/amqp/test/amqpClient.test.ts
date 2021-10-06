import * as A from "@effect-ts/core/Collections/Immutable/Array"
import * as Set from "@effect-ts/core/Collections/Immutable/Set"
import * as T from "@effect-ts/core/Effect"
import * as M from "@effect-ts/core/Effect/Managed"
import * as S from "@effect-ts/core/Effect/Stream"
import * as Equal from "@effect-ts/core/Equal"
import { pipe } from "@effect-ts/core/Function"
import * as TE from "@effect-ts/jest/Test"
import type { StartedTestContainer } from "testcontainers"
import { GenericContainer } from "testcontainers"

import * as Channel from "../src/Channel"
import * as Client from "../src/Client"
import * as ExchangeType from "../src/ExchangeType"
import * as TestUtils from "./test-utils"

describe("AMQP Client", () => {
  jest.setTimeout(30000)

  let container: StartedTestContainer

  beforeAll(async () => {
    container = await new GenericContainer("rabbitmq:latest")
      .withExposedPorts(5672)
      .start()
  })

  afterAll(async () => {
    await container.stop()
  })

  const { it } = TE.runtime()

  it("should use consume to deliver messages", () =>
    T.gen(function* (_) {
      const testSuffix = yield* _(TestUtils.generateId())
      const exchangeName = `exchange-${testSuffix}`
      const queueName = `queue-${testSuffix}`

      const message1 = yield* _(TestUtils.generateId())
      const message2 = yield* _(TestUtils.generateId())
      const messages = Set.fromArray(Equal.string)([message1, message2])

      const host = container.getHost()
      const port = container.getMappedPort(5672)
      const uri = `amqp://guest:guest@${host}:${port}`

      const computed = yield* _(
        pipe(
          Client.connect(uri),
          M.chain(Client.createChannel),
          M.use((channel) =>
            pipe(
              T.tuple(
                Channel.queueDeclare_(channel, queueName),
                Channel.exchangeDeclare_(channel, exchangeName, ExchangeType.fanout),
                Channel.queueBind_(channel, queueName, exchangeName, "myroutingkey"),
                Channel.publish_(channel, exchangeName, Buffer.from(message1)),
                Channel.publish_(channel, exchangeName, Buffer.from(message2))
              ),
              T.zipRight(
                pipe(
                  Channel.consume_(channel, queueName, "test"),
                  S.take(2),
                  S.runCollect,
                  T.tap((records) =>
                    pipe(
                      T.fromOption(A.last(records)),
                      T.map((record) => record.fields.deliveryTag),
                      T.chain((tag) => Channel.ack_(channel, tag))
                    )
                  ),
                  T.map(A.map((_) => _.content.toString("utf-8")))
                )
              ),
              T.tap(() =>
                pipe(
                  Channel.queueDelete_(channel, queueName),
                  T.zipRight(Channel.exchangeDelete_(channel, exchangeName))
                )
              )
            )
          )
        )
      )

      expect(computed).toEqual(A.from(messages.values()))
    }))

  it("should publish messages with high concurrency", () =>
    T.gen(function* (_) {
      const testSuffix = `ClientTest-${TestUtils.generateId()}`
      const exchangeName = `exchange-${testSuffix}`
      const queueName = `queue-${testSuffix}`

      const numMessages = 10000
      const messages = A.uniq(Equal.string)(
        A.makeBy_(numMessages, (i) => `${i} ${TestUtils.generateId()}`)
      )

      const host = container.getHost()
      const port = container.getMappedPort(5672)
      const uri = `amqp://guest:guest@${host}:${port}`

      const computed = yield* _(
        pipe(
          Client.connect(uri),
          M.chain(Client.createChannel),
          M.use((channel) =>
            pipe(
              T.tuple(
                Channel.queueDeclare_(channel, queueName),
                Channel.exchangeDeclare_(channel, exchangeName, ExchangeType.fanout),
                Channel.queueBind_(channel, queueName, exchangeName, "myroutingkey"),
                T.collectAllPar(
                  A.map_(messages, (message) =>
                    Channel.publish_(channel, exchangeName, Buffer.from(message))
                  )
                )
              ),
              T.zipRight(
                pipe(
                  Channel.consume_(channel, queueName, "test"),
                  S.take(numMessages),
                  S.runCollect,
                  T.tap((records) =>
                    pipe(
                      T.fromOption(A.last(records)),
                      T.map((record) => record.fields.deliveryTag),
                      T.chain((tag) => Channel.ack_(channel, tag))
                    )
                  ),
                  T.map(A.map((_) => _.content.toString("utf-8")))
                )
              ),
              T.tap(() =>
                pipe(
                  Channel.queueDelete_(channel, queueName),
                  T.zipRight(Channel.exchangeDelete_(channel, exchangeName))
                )
              )
            )
          )
        )
      )

      expect(computed).toEqual(messages)
    }))
})
