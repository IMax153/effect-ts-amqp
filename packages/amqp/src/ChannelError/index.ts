// ets_tracing: off

import { Tagged } from "@effect-ts/core/Case"

// -----------------------------------------------------------------------------
// Model
// -----------------------------------------------------------------------------

/**
 * Represents errors that can occur when manipulating a RabbitMQ `Channel`.
 */
export type ChannelError =
  | ExchangeDeclarationError
  | ExchangeDeletionError
  | QueueBindError
  | QueueConsumeError
  | QueueConsumeCancelError
  | QueueDeclarationError
  | QueueDeletionError

/**
 * Represents an error that occurred during declaration of an exchange.
 */
export class ExchangeDeclarationError extends Tagged("ExchangeDeclarationError")<{
  readonly message: string
}> {}

/**
 * Represents an error that occurred during deletion of an exchange.
 */
export class ExchangeDeletionError extends Tagged("QueueDeletionError")<{
  readonly message: string
}> {}

/**
 * Represents an error that occurred while binding a queue to an exchange.
 */
export class QueueBindError extends Tagged("QueueBindError")<{
  readonly message: string
}> {}

/**
 * Represents an error that occurred while consuming a queue.
 */
export class QueueConsumeError extends Tagged("QueueConsumeError")<{
  readonly message: string
}> {}

/**
 * Represents an error that occurred while cancelling consumption of a queue.
 */
export class QueueConsumeCancelError extends Tagged("QueueConsumeCancelError")<{
  readonly message: string
}> {}

/**
 * Represents an error that occurred during declaration of a queue.
 */
export class QueueDeclarationError extends Tagged("QueueDeclarationError")<{
  readonly message: string
}> {}

/**
 * Represents an error that occurred during deletion of a queue.
 */
export class QueueDeletionError extends Tagged("QueueDeletionError")<{
  readonly message: string
}> {}
