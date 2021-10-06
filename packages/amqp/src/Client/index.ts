// ets_tracing: off

import type { Array } from "@effect-ts/core/Collections/Immutable/Array"
import type { Chunk } from "@effect-ts/core/Collections/Immutable/Chunk"
import * as C from "@effect-ts/core/Collections/Immutable/Chunk"
import * as Map from "@effect-ts/core/Collections/Immutable/Map"
import * as T from "@effect-ts/core/Effect"
import type { Managed } from "@effect-ts/core/Effect/Managed"
import * as M from "@effect-ts/core/Effect/Managed"
import type * as Ref from "@effect-ts/core/Effect/Ref"
import type { Semaphore } from "@effect-ts/core/Effect/Semaphore"
import * as Sem from "@effect-ts/core/Effect/Semaphore"
import * as S from "@effect-ts/core/Effect/Stream"
import * as O from "@effect-ts/core/Option"
import * as Ord from "@effect-ts/core/Ord"
import type {
  Channel as RChannel,
  Connection,
  Message,
  Options,
  Replies
} from "amqplib/callback_api"
import { connect as connectInternal } from "amqplib/callback_api"

import type { ExchangeType } from "../ExchangeType"
import { represent } from "../ExchangeType"

// -----------------------------------------------------------------------------
// Model
// -----------------------------------------------------------------------------

export class Channel {
  constructor(readonly channel: RChannel, readonly access: Semaphore) {}
}

export interface SocketOptions {
  /**
   * The client CA certificate.
   */
  readonly cert: Buffer
  /**
   * The client private key.
   */
  readonly key: Buffer
  /**
   * The passphrase for the key.
   */
  readonly passphrase: string
  /**
   * Array of trusted CA certificates.
   */
  readonly ca: Array<Buffer>
}

// -----------------------------------------------------------------------------
// Constructors
// -----------------------------------------------------------------------------

/**
 * Creates a `Connection` to the RabbitMQ server.
 */
export function connect(
  url: string | Options.Connect,
  socketOptions: SocketOptions | {} = {}
): Managed<unknown, Error, Connection> {
  return T.toManagedRelease_(
    T.effectAsync((cb) => {
      connectInternal(url, socketOptions, (err, conn) => {
        if (err) {
          cb(T.fail(err))
        } else {
          cb(T.succeed(conn))
        }
      })
    }),
    (conn) =>
      T.effectAsync((cb) => {
        conn.close((err) => {
          if (err) {
            cb(T.die(`Encountered error while closing the connection: ${err}`))
          } else {
            cb(T.unit)
          }
        })
      })
  )
}

export function createChannel(
  connection: Connection
): Managed<unknown, Error, Channel> {
  return M.gen(function* (_) {
    const channel = yield* _(
      T.effectAsync<unknown, Error, RChannel>((cb) => {
        connection.createChannel((err, ch) => {
          if (err) {
            cb(T.fail(err))
          } else {
            cb(T.succeed(ch))
          }
        })
      })
    )

    const permit = yield* _(Sem.makeSemaphore(1))

    return yield* _(
      M.make_(
        T.succeed(new Channel(channel, permit)),
        withChannel((_) =>
          T.effectAsync((cb) => {
            _.close((err) => {
              if (err) {
                cb(T.die(`Encountered error while closing Channel: ${err}`))
              } else {
                cb(T.unit)
              }
            })
          })
        )
      )
    )
  })
}

// -----------------------------------------------------------------------------
// Combinators
// -----------------------------------------------------------------------------

export function queueDeclare_(
  self: Channel,
  queue: string,
  durable = false,
  exclusive = false,
  autoDelete = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
): T.IO<Error, string> {
  return withChannel_(self, (_) =>
    T.map_(
      T.effectAsync<unknown, Error, Replies.AssertQueue>((cb) =>
        _.assertQueue(
          queue,
          {
            durable,
            exclusive,
            autoDelete,
            arguments: Object.fromEntries(args.entries())
          },
          (err, ok) => {
            if (err) {
              cb(T.fail(err))
            } else {
              cb(T.succeed(ok))
            }
          }
        )
      ),
      (_) => _.queue
    )
  )
}

/**
 * Declare a queue.
 *
 * @ets_data_first queueDeclare_
 * @param queue The name of the queue. If left empty, a random queue name will
 * be used.
 * @param durable Set to `true` if we are declaring a durable queue (i.e., the
 * queue will survive a server restart).
 * @param exclusive Whether or not the queue is exclusive to this connection.
 * @param autoDelete Set to `true` if we are declaring an autodelete queue
 * (i.e., the server will delete it when no longer in use).
 * @param args The arguments to provide when creating the queue.
 * @return The name of the created queue.
 */
export function queueDeclare(
  queue: string,
  durable = false,
  exclusive = false,
  autoDelete = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
) {
  return (self: Channel): T.IO<Error, string> =>
    queueDeclare_(self, queue, durable, exclusive, autoDelete, args)
}

export function queueDelete_(
  self: Channel,
  queue: string,
  ifUnused = false,
  ifEmpty = false
): T.IO<Error, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      T.effectAsync((cb) =>
        _.deleteQueue(queue, { ifUnused, ifEmpty }, (err, ok) => {
          if (err) {
            cb(T.fail(err))
          } else {
            cb(T.succeed(ok))
          }
        })
      )
    )
  )
}

/**
 * Delete a queue.
 *
 * @ets_data_first queueDelete_
 * @param queue The name of the queue.
 * @param ifUnused Set to `true` if the queue should be deleted (only if not
 * in use).
 * @param ifEmpty Set to `true` if the queue should be deleted (only if empty).
 */
export function queueDelete(queue: string, ifUnused = false, ifEmpty = false) {
  return (self: Channel): T.IO<Error, void> =>
    queueDelete_(self, queue, ifUnused, ifEmpty)
}

export function exchangeDeclare_(
  self: Channel,
  exchange: string,
  type: ExchangeType,
  durable = false,
  autoDelete = false,
  internal = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
): T.IO<Error, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      T.effectAsync((cb) =>
        _.assertExchange(
          exchange,
          represent(type),
          {
            durable,
            autoDelete,
            internal,
            arguments: Object.entries(args.entries())
          },
          (err, ok) => {
            if (err) {
              cb(T.fail(err))
            } else {
              cb(T.succeed(ok))
            }
          }
        )
      )
    )
  )
}

/**
 * @ets_data_first exchangeDeclare_
 */
export function exchangeDeclare(
  exchange: string,
  type: ExchangeType,
  durable = false,
  autoDelete = false,
  internal = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
) {
  return (self: Channel): T.IO<Error, void> =>
    exchangeDeclare_(self, exchange, type, durable, autoDelete, internal, args)
}

export function exchangeDelete_(
  self: Channel,
  exchange: string,
  ifUnused = false
): T.IO<Error, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      T.effectAsync((cb) =>
        _.deleteExchange(exchange, { ifUnused }, (err, ok) => {
          if (err) {
            cb(T.fail(err))
          } else {
            cb(T.succeed(ok))
          }
        })
      )
    )
  )
}

/**
 * @ets_data_first exchangeDelete_
 */
export function exchangeDelete(exchange: string, ifUnused = false) {
  return (self: Channel): T.IO<Error, void> => exchangeDelete_(self, exchange, ifUnused)
}

export function queueBind_(
  self: Channel,
  queue: string,
  exchange: string,
  routingKey: string,
  args: Map.Map<String, Ref.Ref<any>> = Map.empty
): T.IO<Error, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      T.effectAsync((cb) =>
        _.bindQueue(
          queue,
          exchange,
          routingKey,
          {
            arguments: Object.entries(args.entries())
          },
          (err, ok) => {
            if (err) {
              cb(T.fail(err))
            } else {
              cb(T.succeed(ok))
            }
          }
        )
      )
    )
  )
}

/**
 * @ets_data_first queueBind_
 */
export function queueBind(
  queue: string,
  exchange: string,
  routingKey: string,
  args: Map.Map<String, Ref.Ref<any>> = Map.empty
) {
  return (self: Channel): T.IO<Error, void> =>
    queueBind_(self, queue, exchange, routingKey, args)
}

export function basicQos_(
  self: Channel,
  count: number,
  global = false
): T.IO<Error, void> {
  return T.asUnit(
    withChannel_(self, (_) => T.succeedWith(() => _.prefetch(count, global)))
  )
}

/**
 * @ets_data_first basicQos_
 */
export function basicQos(count: number, global = false) {
  return (self: Channel): T.IO<Error, void> => basicQos_(self, count, global)
}

export function consume_(
  self: Channel,
  queue: string,
  consumerTag: string,
  autoAck = false
): S.IO<Error, Message> {
  return S.ensuring_(
    S.effectAsyncM((offer) =>
      withChannel_(self, (_) =>
        T.succeedWith(() => {
          _.consume(
            queue,
            (msg) => {
              if (msg) {
                offer(T.succeed(C.single(msg)))
              } else {
                offer(T.fail(O.none))
              }
            },
            {
              consumerTag,
              noAck: autoAck
            }
          )
        })
      )
    ),
    T.ignore(
      withChannel_(self, (_) =>
        T.effectAsync((cb) => {
          _.cancel(consumerTag, (err, ok) => {
            if (err) {
              cb(T.fail(err))
            } else {
              cb(T.succeed(ok))
            }
          })
        })
      )
    )
  )
}

/**
 * @ets_data_first consume_
 */
export function consume(queue: string, consumerTag: string, autoAck = false) {
  return (self: Channel): S.IO<Error, Message> =>
    consume_(self, queue, consumerTag, autoAck)
}

export function ack_(
  self: Channel,
  deliveryTag: number,
  multiple = false
): T.IO<Error, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      // The internal method here only calls `message.fileds.deliveryTag`
      T.succeedWith(() => _.ack({ fields: { deliveryTag } } as any, multiple))
    )
  )
}

/**
 * @ets_data_first ack_
 */
export function ack(deliveryTag: number, multiple = false) {
  return (self: Channel): T.IO<Error, void> => ack_(self, deliveryTag, multiple)
}

export function ackMany_(
  self: Channel,
  deliveryTags: Chunk<number>
): T.IO<Error, void> {
  if (C.isEmpty(deliveryTags)) {
    return T.die("Bug, received empty Chunk of deliveryTags")
  }
  return ack_(
    self,
    C.reduce_(deliveryTags, C.unsafeHead(deliveryTags), Ord.max(Ord.number)),
    true
  )
}

/**
 * @ets_data_first ackMany_
 */
export function ackMany(deliveryTags: Chunk<number>) {
  return (self: Channel): T.IO<Error, void> => ackMany_(self, deliveryTags)
}

export function nack_(
  self: Channel,
  deliveryTag: number,
  requeue = false,
  multiple = false
): T.IO<Error, void> {
  return withChannel_(self, (_) =>
    // The internal method here only calls `message.fileds.deliveryTag`
    T.succeedWith(() => _.nack({ fields: { deliveryTag } } as any, multiple, requeue))
  )
}

/**
 * @ets_data_first = nack_
 */
export function nack(deliveryTag: number, requeue = false, multiple = false) {
  return (self: Channel): T.IO<Error, void> =>
    nack_(self, deliveryTag, requeue, multiple)
}

export function nackMany_(
  self: Channel,
  deliveryTags: Chunk<number>,
  requeue = false
): T.IO<Error, void> {
  if (C.isEmpty(deliveryTags)) {
    return T.die("Bug, received empty Chunk of deliveryTags")
  }
  return nack_(
    self,
    C.reduce_(deliveryTags, C.unsafeHead(deliveryTags), Ord.max(Ord.number)),
    requeue,
    true
  )
}

/**
 * @ets_data_first nackMany_
 */
export function nackMany(deliveryTags: Chunk<number>, requeue = false) {
  return (self: Channel): T.IO<Error, void> => nackMany_(self, deliveryTags, requeue)
}

export function publish_(
  self: Channel,
  exchange: string,
  body: Buffer,
  routingKey = "",
  mandatory = false,
  props: Omit<Options.Publish, "mandatory"> = {}
) {
  return withChannel_(self, (_) =>
    T.succeedWith(() => _.publish(exchange, routingKey, body, { mandatory, ...props }))
  )
}

/**
 * @ets_data_first publish_
 */
export function publish(
  exchange: string,
  body: Buffer,
  routingKey = "",
  mandatory = false,
  props: Omit<Options.Publish, "mandatory"> = {}
) {
  return (self: Channel): T.IO<Error, void> =>
    publish_(self, exchange, body, routingKey, mandatory, props)
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

function withChannel_<R, E, A>(
  self: Channel,
  f: (channel: RChannel) => T.Effect<R, E, A>
): T.Effect<R, E, A> {
  return Sem.withPermit_(f(self.channel), self.access)
}

function withChannel<R, E, A>(f: (channel: RChannel) => T.Effect<R, E, A>) {
  return (self: Channel): T.Effect<R, E, A> => withChannel_(self, f)
}
