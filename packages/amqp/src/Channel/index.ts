// ets_tracing: off

import type { Chunk } from "@effect-ts/core/Collections/Immutable/Chunk"
import * as C from "@effect-ts/core/Collections/Immutable/Chunk"
import * as Map from "@effect-ts/core/Collections/Immutable/Map"
import * as T from "@effect-ts/core/Effect"
import type * as Ref from "@effect-ts/core/Effect/Ref"
import type { Semaphore } from "@effect-ts/core/Effect/Semaphore"
import { withPermit_ } from "@effect-ts/core/Effect/Semaphore"
import * as S from "@effect-ts/core/Effect/Stream"
import * as O from "@effect-ts/core/Option"
import * as Ord from "@effect-ts/core/Ord"
import type {
  Channel as AmqpChannel,
  Message as AmqpMessage,
  Options,
  Replies
} from "amqplib/callback_api"

import type { ChannelError } from "../ChannelError"
import * as ChannelErrors from "../ChannelError"
import type { ExchangeType } from "../ExchangeType"
import { represent } from "../ExchangeType"

// -----------------------------------------------------------------------------
// Model
// -----------------------------------------------------------------------------

/**
 * Represents access to a RabbitMQ channel.
 */
export class Channel {
  constructor(readonly channel: AmqpChannel, readonly access: Semaphore) {}
}

// -----------------------------------------------------------------------------
// Combinators
// -----------------------------------------------------------------------------

/**
 * Declare a queue.
 *
 * @param self The RabbitMQ channel to use to declare the queue.
 * @param queue The name of the queue.
 * @param durable Whether or not we are declaring a durable queue (i.e., the
 * queue will survive a server restart).
 * @param exclusive Whether or not the queue is exclusive to this connection.
 * @param autoDelete Whether or not we are declaring an autodelete queue (i.e.,
 * the server will delete the queue when it is no longer in use).
 * @param args The arguments to provide when creating the queue.
 */
export function queueDeclare_(
  self: Channel,
  queue: string,
  durable = false,
  exclusive = false,
  autoDelete = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
): T.IO<ChannelError, Replies.AssertQueue> {
  return withChannel_(self, (_) =>
    T.effectAsync((cb) =>
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
            cb(
              T.fail(
                new ChannelErrors.QueueDeclarationError({
                  message: err.message
                })
              )
            )
          } else {
            cb(T.succeed(ok))
          }
        }
      )
    )
  )
}

/**
 * Declare a queue.
 *
 * @ets_data_first queueDeclare_
 * @param queue The name of the queue.
 * @param durable Whether or not we are declaring a durable queue (i.e., the
 * queue will survive a server restart).
 * @param exclusive Whether or not the queue is exclusive to this connection.
 * @param autoDelete Whether or not we are declaring an autodelete queue (i.e.,
 * the server will delete the queue when it is no longer in use).
 * @param args The arguments to provide when creating the queue.
 */
export function queueDeclare(
  queue: string,
  durable = false,
  exclusive = false,
  autoDelete = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
) {
  return (self: Channel): T.IO<ChannelError, Replies.AssertQueue> =>
    queueDeclare_(self, queue, durable, exclusive, autoDelete, args)
}

/**
 * Deletes a queue.
 *
 * @param self The RabbitMQ channel to use to delete the queue.
 * @param queue The name of the queue.
 * @param ifUnused Whether the queue should be deleted (only if not in use).
 * @param ifEmpty Whether the queue should be deleted (only if empty).
 */
export function queueDelete_(
  self: Channel,
  queue: string,
  ifUnused = false,
  ifEmpty = false
): T.IO<ChannelError, Replies.DeleteQueue> {
  return withChannel_(self, (_) =>
    T.effectAsync((cb) =>
      _.deleteQueue(queue, { ifUnused, ifEmpty }, (err, ok) => {
        if (err) {
          cb(T.fail(new ChannelErrors.QueueDeletionError({ message: err.message })))
        } else {
          cb(T.succeed(ok))
        }
      })
    )
  )
}

/**
 * Delete a queue.
 *
 * @ets_data_first queueDelete_
 * @param queue The name of the queue.
 * @param ifUnused Whether the queue should be deleted (only if not in use).
 * @param ifEmpty Whether the queue should be deleted (only if empty).
 */
export function queueDelete(queue: string, ifUnused = false, ifEmpty = false) {
  return (self: Channel): T.IO<ChannelError, Replies.DeleteQueue> =>
    queueDelete_(self, queue, ifUnused, ifEmpty)
}

/**
 * Declare an exchange.
 *
 * @param self The RabbitMQ channel to use to declare the exchange.
 * @param exchange The name of the exchange.
 * @param type The exchange type.
 * @param durable Whether or not we are declaring a durable exchange (i.e., the
 * exchange will survive a server restart).
 * @param autoDelete Whether or not we are declaring an autodelete exchange
 * (i.e., the server will delete the exchange when it is no longer in use).
 * @param internal Whether or not the exchange is internal (i.e., the exchange
 * cannot be directly published to by a client).
 * @param args Other properties (construction arguments) for the exchange.
 */
export function exchangeDeclare_(
  self: Channel,
  exchange: string,
  type: ExchangeType,
  durable = false,
  autoDelete = false,
  internal = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
): T.IO<ChannelError, Replies.AssertExchange> {
  return withChannel_(self, (_) =>
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
            cb(
              T.fail(
                new ChannelErrors.ExchangeDeclarationError({ message: err.message })
              )
            )
          } else {
            cb(T.succeed(ok))
          }
        }
      )
    )
  )
}

/**
 * Declare an exchange.
 *
 * @ets_data_first exchangeDeclare_
 * @param exchange The name of the exchange.
 * @param type The exchange type.
 * @param durable Whether or not we are declaring a durable exchange (i.e., the
 * exchange will survive a server restart).
 * @param autoDelete Whether or not we are declaring an autodelete exchange
 * (i.e., the server will delete the exchange when it is no longer in use).
 * @param internal Whether or not the exchange is internal (i.e., the exchange
 * cannot be directly published to by a client).
 * @param args Other properties (construction arguments) for the exchange.
 */
export function exchangeDeclare(
  exchange: string,
  type: ExchangeType,
  durable = false,
  autoDelete = false,
  internal = false,
  args: Map.Map<string, Ref.Ref<any>> = Map.empty
) {
  return (self: Channel): T.IO<ChannelError, Replies.AssertExchange> =>
    exchangeDeclare_(self, exchange, type, durable, autoDelete, internal, args)
}

/**
 * Delete an exchange.
 *
 * @param self The RabbitMQ channel to use to delete the exchange.
 * @param exchange The name of the exchange.
 * @param ifUnused Whether or not the exchange should only be deleted if it is
 * not currently in use.
 */
export function exchangeDelete_(
  self: Channel,
  exchange: string,
  ifUnused = false
): T.IO<ChannelError, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      T.effectAsync((cb) =>
        _.deleteExchange(exchange, { ifUnused }, (err, ok) => {
          if (err) {
            cb(
              T.fail(new ChannelErrors.ExchangeDeletionError({ message: err.message }))
            )
          } else {
            cb(T.succeed(ok))
          }
        })
      )
    )
  )
}

/**
 * Delete an exchange.
 *
 * @ets_data_first exchangeDelete_
 * @param exchange The name of the exchange.
 * @param ifUnused Whether or not the exchange should only be deleted if it is
 * not currently in use.
 */
export function exchangeDelete(exchange: string, ifUnused = false) {
  return (self: Channel): T.IO<ChannelError, void> =>
    exchangeDelete_(self, exchange, ifUnused)
}

/**
 * Bind a queue to an exchange.
 *
 * @param self The RabbitMQ channel to use to bind the queue to the exchange.
 * @param queue The name of the queue.
 * @param exchange The name of the exchange.
 * @param routingKey The routing key to use for the binding.
 * @param args Other properties (binding parameters).
 */
export function queueBind_(
  self: Channel,
  queue: string,
  exchange: string,
  routingKey: string,
  args: Map.Map<String, Ref.Ref<any>> = Map.empty
): T.IO<ChannelError, void> {
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
              cb(T.fail(new ChannelErrors.QueueBindError({ message: err.message })))
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
 * Bind a queue to an exchange.
 *
 * @ets_data_first queueBind_
 * @param queue The name of the queue.
 * @param exchange The name of the exchange.
 * @param routingKey The routing key to use for the binding.
 * @param args Other properties (binding parameters).
 */
export function queueBind(
  queue: string,
  exchange: string,
  routingKey: string,
  args: Map.Map<String, Ref.Ref<any>> = Map.empty
) {
  return (self: Channel): T.IO<ChannelError, void> =>
    queueBind_(self, queue, exchange, routingKey, args)
}

/**
 * Request a specific prefetch count "quality of service" settings for a
 * channel.
 *
 * Note that the prefetch count must be between 0 and 65535.
 *
 * @param self The channel to request a prefetch count for.
 * @param count The maximum number of messages that the server will deliver.
 * @param global Whether or not the settings should be applied to the entire
 * channel, rather than to each consumer.
 */
export function basicQos_(
  self: Channel,
  count: number,
  global = false
): T.IO<ChannelError, void> {
  return T.asUnit(
    withChannel_(self, (_) => T.succeedWith(() => _.prefetch(count, global)))
  )
}

/**
 * Request a specific prefetch count "quality of service" settings for a
 * channel.
 *
 * Note that the prefetch count must be between 0 and 65535.
 *
 * @ets_data_first basicQos_
 * @param count The maximum number of messages that the server will deliver.
 * @param global Whether or not the settings should be applied to the entire
 * channel, rather than to each consumer.
 */
export function basicQos(count: number, global = false) {
  return (self: Channel): T.IO<ChannelError, void> => basicQos_(self, count, global)
}

/**
 * Start a non-nolocal, non-exclusive consumer.
 *
 * @param self The RabbitMQ channel to use to consume the queue.
 * @param queue The name of the queue.
 * @param consumerTag A client-generated consumer tag to establish context.
 * @param autoAck `true` if the server should consider messages acknowledged
 * once delivered; `false` if the server should expect explicit acknowledgements.
 */
export function consume_(
  self: Channel,
  queue: string,
  consumerTag: string,
  autoAck = false
): S.IO<ChannelError, AmqpMessage> {
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
            },
            (err) => {
              if (err) {
                offer(
                  T.fail(
                    O.some(
                      new ChannelErrors.QueueConsumeError({ message: err.message })
                    )
                  )
                )
              }
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
              cb(
                T.fail(
                  new ChannelErrors.QueueConsumeCancelError({ message: err.message })
                )
              )
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
 * Start a non-nolocal, non-exclusive consumer.
 *
 * @ets_data_first consume_
 * @param queue The name of the queue.
 * @param consumerTag A client-generated consumer tag to establish context.
 * @param autoAck `true` if the server should consider messages acknowledged
 * once delivered; `false` if the server should expect explicit acknowledgements.
 */
export function consume(queue: string, consumerTag: string, autoAck = false) {
  return (self: Channel): S.IO<ChannelError, AmqpMessage> =>
    consume_(self, queue, consumerTag, autoAck)
}

/**
 * Acknowledge receipt of a message.
 *
 * @param self The RabbitMQ channel to use to acknowledge delivery of a message.
 * @param deliveryTag The tag from the received message.
 * @param multiple `true` to acknowledge all messages up to and including the
 * supplied delivery tag; `false` to acknowledge just the supplied delivery tag.
 */
export function ack_(
  self: Channel,
  deliveryTag: number,
  multiple = false
): T.IO<ChannelError, void> {
  return T.asUnit(
    withChannel_(self, (_) =>
      // The internal method here only calls `message.fileds.deliveryTag`
      T.succeedWith(() => _.ack({ fields: { deliveryTag } } as any, multiple))
    )
  )
}

/**
 * Acknowledge receipt of a message.
 *
 * @ets_data_first ack_
 * @param deliveryTag The tag from the received message.
 * @param multiple `true` to acknowledge all messages up to and including the
 * supplied delivery tag; `false` to acknowledge just the supplied delivery tag.
 */
export function ack(deliveryTag: number, multiple = false) {
  return (self: Channel): T.IO<ChannelError, void> => ack_(self, deliveryTag, multiple)
}

/**
 * Acknowledge receipt of multiple messages.
 *
 * @param self The RabbitMQ channel to use to acknowledge delivery of messages.
 * @param deliveryTags The tags from the received messages.
 */
export function ackMany_(
  self: Channel,
  deliveryTags: Chunk<number>
): T.IO<ChannelError, void> {
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
 * Acknowledge receipt of multiple messages.
 *
 * @ets_data_first ackMany_
 * @param deliveryTags The tags from the received messages.
 */
export function ackMany(deliveryTags: Chunk<number>) {
  return (self: Channel): T.IO<ChannelError, void> => ackMany_(self, deliveryTags)
}

/**
 * Reject receipt of a message.
 *
 * @param self The RabbitMQ channel to use to reject delivery of a message.
 * @param deliveryTag The tag from the received message.
 * @param requeue Whether or not the rejected message should be requeued rather
 * than discarded/dead-lettered.
 * @param multiple `true` to reject all messages up to and including the
 * supplied delivery tag; `false` to reject just the supplied delivery tag.
 */
export function nack_(
  self: Channel,
  deliveryTag: number,
  requeue = false,
  multiple = false
): T.IO<ChannelError, void> {
  return withChannel_(self, (_) =>
    // The internal method here only calls `message.fileds.deliveryTag`
    T.succeedWith(() => _.nack({ fields: { deliveryTag } } as any, multiple, requeue))
  )
}

/**
 * Reject receipt of a message.
 *
 * @ets_data_first nack_
 * @param deliveryTag The tag from the received message.
 * @param requeue Whether or not the rejected message should be requeued rather
 * than discarded/dead-lettered.
 * @param multiple `true` to reject all messages up to and including the
 * supplied delivery tag; `false` to reject just the supplied delivery tag.
 */
export function nack(deliveryTag: number, requeue = false, multiple = false) {
  return (self: Channel): T.IO<ChannelError, void> =>
    nack_(self, deliveryTag, requeue, multiple)
}

/**
 * Reject receipt of multiple messages.
 *
 * @param self The RabbitMQ channel to use to reject delivery of the messages.
 * @param deliveryTags The tags from the received messages.
 * @param requeue Whether or not the rejected messages should be requeued rather
 * than discarded/dead-lettered.
 */
export function nackMany_(
  self: Channel,
  deliveryTags: Chunk<number>,
  requeue = false
): T.IO<ChannelError, void> {
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
 * Reject receipt of multiple messages.
 *
 * @ets_data_first nackMany_
 * @param deliveryTags The tags from the received messages.
 * @param requeue Whether or not the rejected messages should be requeued rather
 * than discarded/dead-lettered.
 */
export function nackMany(deliveryTags: Chunk<number>, requeue = false) {
  return (self: Channel): T.IO<ChannelError, void> =>
    nackMany_(self, deliveryTags, requeue)
}

/**
 * Publishes a message to an exchange.
 *
 * @param self The RabbitMQ channel to use to publish the message.
 * @param exchange The name of the exchange.
 * @param body The message body.
 * @param routingKey The routing key.
 * @param mandatory Whether or not to set the `mandatory` flag.
 * @param props Other properties for the message (i.e., routing headers, etc).
 */
export function publish_(
  self: Channel,
  exchange: string,
  body: Buffer,
  routingKey = "",
  mandatory = false,
  props: Omit<Options.Publish, "mandatory"> = {}
): T.IO<ChannelError, boolean> {
  return withChannel_(self, (_) =>
    T.succeedWith(() => _.publish(exchange, routingKey, body, { mandatory, ...props }))
  )
}

/**
 * Publishes a message to an exchange.
 *
 * @ets_data_first publish_
 * @param exchange The name of the exchange.
 * @param body The message body.
 * @param routingKey The routing key.
 * @param mandatory Whether or not to set the `mandatory` flag.
 * @param props Other properties for the message (i.e., routing headers, etc).
 */
export function publish(
  exchange: string,
  body: Buffer,
  routingKey = "",
  mandatory = false,
  props: Omit<Options.Publish, "mandatory"> = {}
) {
  return (self: Channel): T.IO<ChannelError, boolean> =>
    publish_(self, exchange, body, routingKey, mandatory, props)
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

/**
 * Safely executes an action using a RabbitMQ channel.
 *
 * @param self The channel to use.
 * @param f The action that should be performed.
 */
export function withChannel_<R, E, A>(
  self: Channel,
  f: (channel: AmqpChannel) => T.Effect<R, E, A>
): T.Effect<R, E, A> {
  return withPermit_(f(self.channel), self.access)
}

/**
 * Safely executes an action using a RabbitMQ channel.
 *
 * @ets_data_first withChannel_
 * @param f The action that should be performed.
 */
export function withChannel<R, E, A>(f: (channel: AmqpChannel) => T.Effect<R, E, A>) {
  return (self: Channel): T.Effect<R, E, A> => withChannel_(self, f)
}
