// ets_tracing: off

import type { Array } from "@effect-ts/core/Collections/Immutable/Array"
import * as T from "@effect-ts/core/Effect"
import type { Managed } from "@effect-ts/core/Effect/Managed"
import * as M from "@effect-ts/core/Effect/Managed"
import * as Sem from "@effect-ts/core/Effect/Semaphore"
import type { Channel as RChannel, Connection, Options } from "amqplib/callback_api"
import { connect as connectInternal } from "amqplib/callback_api"

import { Channel, withChannel } from "../Channel"

// -----------------------------------------------------------------------------
// Model
// -----------------------------------------------------------------------------

export interface ClientSocketOptions {
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
  socketOptions: ClientSocketOptions | {} = {}
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
