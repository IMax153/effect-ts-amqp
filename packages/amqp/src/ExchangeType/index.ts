// ets_tracing: off

import { Tagged } from "@effect-ts/core/Case"

// -----------------------------------------------------------------------------
// Model
// -----------------------------------------------------------------------------

export type ExchangeType = Direct | Fanout | Topic | Headers

export class Direct extends Tagged("Direct")<{}> {}

export class Fanout extends Tagged("Fanout")<{}> {}

export class Topic extends Tagged("Topic")<{}> {}

export class Headers extends Tagged("Headers")<{}> {}

// -----------------------------------------------------------------------------
// Constructors
// -----------------------------------------------------------------------------

export const direct: ExchangeType = new Direct()

export const fanout: ExchangeType = new Fanout()

export const topic: ExchangeType = new Topic()

export const headers: ExchangeType = new Headers()

// -----------------------------------------------------------------------------
// Destructors
// -----------------------------------------------------------------------------

export function represent(exchangeType: ExchangeType): string {
  return exchangeType._tag.toLowerCase()
}
