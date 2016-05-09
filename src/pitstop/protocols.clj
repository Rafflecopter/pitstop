(ns pitstop.protocols)

(defprotocol Storage
  (listen [instance]
    "Start a listener.
    Return a map of {:data chan :stop chan}
    Data channel is channel of items: {:ack ack-chan :msg msg} to be sent
    Upon stop channel closing, listening should cease and channel closed.")
  (store! [instance msg when]
          [instance msg start end every]
    "Store a new _or_ updated deferred or deferred-recurring message
    Deferred messages with just the when argument will be deferred
     emitted on the listen channel at time when.
    Recurring messages will be emitted on a listen channel every interval
     starting at start and ending at end.
    Messages may contain {:id id} which should be respected for updates.
    Returns a ack channel.")
  (remove! [instance id]
    "Remove a deferred or recurring message, denoted by a message's id
    Return a ack channel."))
