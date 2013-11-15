open Protocol
open Program

(* Data structure for id storage *)
type id_type = M | R
let ids : (worker_id, id_type) Hashtbl.t = Hashtbl.create 10
let mutex : Mutex.t = Mutex.create ()

(* Synchronized add for a hash table *)
let add (k : worker_id) (v : id_type) : unit =
  Mutex.lock mutex;
  Hashtbl.add ids k v;
  Mutex.unlock mutex

(* Synchronized check on if an id is valid *)
let is_valid (k : worker_id) (expected : id_type) : bool =
  Mutex.lock mutex;
  let b : bool = Hashtbl.mem ids k && Hashtbl.find ids k = expected in
  Mutex.unlock mutex; b

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  let get_connection_status v = 
    match v with
      (* builds, saves id and returns it*)
      | InitMapper source -> begin match build source with
        | (Some id, str) -> add id M;
                            send_response client (Mapper (Some id, str))
        | (None, str) -> send_response client (Mapper (None, str))
        end
      (* builds, saves id and returns it*)
      | InitReducer source -> begin match build source with
        | (Some id, str) -> add id R;
                            send_response client (Reducer (Some id, str))                                
        | (None, str) -> send_response client (Reducer (None, str))
        end
      | MapRequest (id, k, v) ->
        (* Validate the id *)
        begin if is_valid id M then
          (* Execute the request *)
          try match run id (k, v) with
          | None -> send_response client (RuntimeError (id, "failure"))
          | Some result -> send_response client (MapResults (id, result))
          with e -> send_response client (RuntimeError (id, "failure"))
        else send_response client (InvalidWorker id)
        end
      | ReduceRequest (id, k, v) ->
        (* Validate the id *)
        begin if is_valid id R then
          (* Execute the request *) 
          try match run id (k, v) with
          | None -> send_response client (RuntimeError (id, "failure"))
          | Some result -> send_response client (ReduceResults (id, result))
          with e -> send_response client (RuntimeError (id, "failure"))
        else send_response client (InvalidWorker id)
        end in
  match Connection.input client with
    | Some v -> if get_connection_status v then handle_request client else ()
    | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

