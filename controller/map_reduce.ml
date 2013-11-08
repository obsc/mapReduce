open Util
open Worker_manager
open Thread_pool

type status =
  | Unstarted
  | Attempt of int
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
  let tasks : (int * (string * string)) list =
    List.mapi (fun id x -> (id, x)) kv_pairs in
  let tbl : (int, status) Hashtbl.t = Hashtbl.create 10 in
  let p : pool = create 100 in
  let m : mapper worker_manager = initialize_mappers map_filename in
  let rec loop (t : (int * (string * string)) list) : unit =
    loop t in
  List.iter (fun (id, kv) -> Hashtbl.add tbl id Unstarted) tasks;
  loop tasks;
  clean_up_workers m;
  destroy p;
  let append_output (id : int) (s : status) a = match s with
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

let reduce kvs_pairs reduce_filename : (string * string list) list =
  failwith "The only thing necessary for evil to triumph is for good men to do nothing"

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

