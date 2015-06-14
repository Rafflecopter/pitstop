(ns pitstop.messaging.core)

(defmulti init!
  "Initialize using a config object.
  Return an instance object which `(= (:type cfg) (:type instance))`"
  :type)

(defmulti send!
  "Send a message on the messaging platform.
  Arguments are {:inst instance-obj :msg msg}
  Returns a channel that should be closed on success."
  #(get-in % [:inst :type]))