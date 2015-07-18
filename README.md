# pitstop [![Build Status][1]][2]

Message-deferring application with a pluggable backend.

Pitstop is designed to take messages, store them, and re-emit them later as desired. It can handle deferred and/or recurring messages. It's designed to work well with [qb](https://github.com/Rafflecopter/clj-qb).

[![Clojars Project](http://clojars.org/com.rafflecopter/pitstop/latest-version.svg)](http://clojars.org/com.rafflecopter/pitstop)

## Usage

```clojure
(ns your-namespace-here
  (:require [pitstop.core :as p]
            [clj-time.core :as t]
            [qb.util :refer (ack-success nack-error)]
            [clojure.core.async :refer (go-loop <! close!)]))

(def config {:type :mongo ...}) ; See below for config options
(def instance (p/init! config))

;; Listen for emitted messages
(let [{:keys [data stop]} (p/listen instance)]
  (go-loop []
    (let [{:keys [ack msg]} (<! data)]
      (try (handle-msg msg)
           ;; Notify pitstop of successful processing
           (ack-success ack)
        (catch Exception e
          ;; Notify pitstop of an error in processing
          (nack-error ack (.getMessage e)))))
    (recur))

  ;; At some point, you can stop the listener by closing the stop channel
  ;; Some implementations take a bit to close, so you should wait
  ;; until the data channel is closed to exit gracefully.
  (close! stop))

;; Defer a message it for 15 minutes
(p/store! instance {:msg "object"} (t/plus (t/now) (t/minutes 15)))

;; Defer a message with a known ID
;; Messages without ID's will have one when emitted
(p/store! instance {:id "abc" :foo :bar} some-time)
;; Now update it using store! (:id must match)
(p/store! instance {:id "abc" :foo :update-bar} some-updated-time)

;; Recur a message every hour for a day
(let [start-time (t/now)
      end-time (t/plus (t/now) (t/days 1))
      interval (t/hours 1)]
  (p/store! instance {:id "def" :foo :baz} start-time end-time interval))

;; Remove a message before its been emitted (or stopped recurring)
;; It must have a known ID
(p/remove! instance "id-val")
```

## Storages

Currently, the only backend storage is mongo.

### Mongo

The mongo backend storage works by storing messages a single collection. Each message's `:id` is abstracted into the mongo `_id`, or a UUID v4 is attached. The time when the message should next be emitted is stored on the object. When it is time to emit the message, a listener loop will lock the message for a substantial period of time (say 15 minutes), during which, it will try to process the message. If it fails, the lock will expire and it will again be emitted. Upon a successful operation, the task is either removed, or the ready time is updated (for recurring messages).

Configuration

- `:type => :mongo`
- `:coll` Collection name
- `:loop-time` Average time between loops to retrieve messages. Defaults to 1 minute. Use `clj-time` intervals.
- `:lock-time` Time to lock messages. Defaults to 15 minutes. Use `clj-time` intervals.
- Mongo connection stuff:
  + `:host` and `:port` or `:hosts` (a list of `["host:port"]`)
  + `:dbname`
  + Optional `:user` and `:pass`

## License

See [LICENSE](https://github.com/Rafflecopter/pitstop/blob/master/LICENSE) file

[1]: https://travis-ci.org/Rafflecopter/pitstop.png?branch=master
[2]: http://travis-ci.org/Rafflecopter/pitstop
