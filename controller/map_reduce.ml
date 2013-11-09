open Util
open Worker_manager
open Thread_pool

type 'a status =
  | Standby of 'a list * string * string
  | Attempt of 'a list * string * string
  | Success of (string * string) list
  | Failure

let set (tbl : ('a, 'b) Hashtbl.t) (m : Mutex.t) (k : 'a) (v : 'b) : unit =
  Mutex.lock m;
  Hashtbl.replace tbl k v;
  Mutex.unlock m

let get (tbl : ('a, 'b) Hashtbl.t) (m : Mutex.t) (k : 'a) : 'b =
  Mutex.lock m;
  let v : 'b = Hashtbl.find tbl k in
  Mutex.unlock m; v

let map kv_pairs map_filename : (string * string) list =
  let tbl : (int, mapper status) Hashtbl.t = Hashtbl.create 10 in
  let mutex : Mutex.t = Mutex.create () in
  let p : pool = create 100 in
  let m : mapper worker_manager = initialize_mappers map_filename in
  let job (id : int) (x : mapper list) (k : string) (v : string) () : unit =
    let map_worker : mapper = pop_worker m in
    match map map_worker k v with
      | None   -> begin
        if List.length x > 5 then (set tbl mutex id (Failure);
          List.iter (fun w -> push_worker m w) (map_worker::x))
        else set tbl mutex id (Standby ((map_worker::x), k, v))
      end
      | Some l -> begin
        set tbl mutex id (Success l);
        push_worker m map_worker
      end in
  let check (f, t : bool * int list) (id : int) : bool * int list =
    match get tbl mutex id with
      | Standby (x, k, v) -> begin
        set tbl mutex id (Attempt (x, k, v));
        add_work (job id x k v) p;
        (false, id::t)
      end
      | Attempt (x, k, v) -> (false, id::t)
      | Success l         -> (f, t)
      | Failure           -> (f, t) in
  let rec loop (tasks : int list) : unit =
    Thread.delay 0.1;
    let (flag, newtasks) = List.fold_left check (true, []) tasks in
    if flag then () else loop (List.rev newtasks) in
  List.iteri (fun i (k, v) -> Hashtbl.add tbl i (Standby ([], k, v))) kv_pairs;
  loop (List.mapi (fun id x -> id) kv_pairs);
  clean_up_workers m;
  destroy p;
  let append_output (id : int) (s : mapper status) a =
    match s with
    | Success l -> l@a
    | _         -> a in
  Hashtbl.fold append_output tbl []

(* Combines key value pairs with the same key *)
let combine kv_pairs : (string * string list) list =
  let tbl : (string, string list) Hashtbl.t = Hashtbl.create 10 in
  (* Appends new value v to the previous list of values at the key k *)
  let add (k, v : string * string) : unit =
    if Hashtbl.mem tbl k
    then Hashtbl.replace tbl k (v::(Hashtbl.find tbl k))
    else Hashtbl.add tbl k [v] in
  List.iter add kv_pairs;
  Hashtbl.fold (fun k v a -> (k, v)::a) tbl []

let reduce kvs_pairs reduce_filename : (string * string list) list = failwith "potato"
  (*let tbl : (int, reducer status) Hashtbl.t = Hashtbl.create 10 in
  let mutex : Mutex.t = Mutex.create () in
  let p : pool = create 100 in
  let r : reducer worker_manager = initialize_reducers reduce_filename in
  let job (id : int) (x : reducer list) (k : string) (v : string) () : unit =
    let reduce_worker : reducer = pop_worker r in
    match reduce reduce_worker k v with
      | None   -> begin
        if List.length x > 5 then (set tbl mutex id (Failure);
          List.iter (fun w -> push_worker r w) (reduce_worker::x))
        else set tbl mutex id (Standby ((reduce_worker::x), k, v))
      end
      | Some l -> begin
        set tbl mutex id (Success l);
        push_worker r reduce_worker
      end in*)

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

