(ns pitstop.storage.core)

(defmulti init!
  "Initialize using a config object.
  Return an instance object"
  :type)

(defmulti listen-for-msgs!
  "Start a listener.
  Arguments  are {:inst instance-obj :stopper chan}
  Return a channel of {:success chan :msg msg} to be sent
  Success channel will be closed on success
  Upon stopper channel closing, listening should cease and channel closed"
  #(get-in % [:inst :type]))

(defmulti store-msg!
  "Store a new _or_ updated (via {:id id}) deferred message
  Arguments are {:inst instance-obj :msg msg :when joda/DateTime}"
  #(get-in % [:inst :type]))

(defmulti remove-msg!
  "Remove a deferred message
  Arguments are {:inst instance-obj :id id}"
  #(get-in % [:inst :type]))