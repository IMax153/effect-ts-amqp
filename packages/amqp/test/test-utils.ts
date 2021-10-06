import * as T from "@effect-ts/core/Effect"
import * as crypto from "crypto"

function dec2hex(dec: number): string {
  return dec.toString(16).padStart(2, "0")
}

export function generateId(len = 20): T.IO<Error, string> {
  const arr = new Uint8Array((len || 40) / 2)
  return T.effectAsync((cb) => {
    crypto.randomFill(arr, (err, buff) => {
      if (err) {
        cb(T.fail(err))
      }
      cb(T.succeed(Array.from(buff, dec2hex).join("")))
    })
  })
}
