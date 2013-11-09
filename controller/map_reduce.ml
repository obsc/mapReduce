open Util
open Worker_manager
open Thread_pool

(* Status of a map job *)
type map_status =
  | MStandby of mapper list * string * string
  | MAttempt of mapper list * string * string
  | MSuccess of (string * string) list
  | MFailure

(* Status of a reduce task *)
type reduce_status =
  | RStandby of reducer list * string * string list
  | RAttempt of reducer list * string * string list
  | RSuccess of string * string list
  | RFailure

(* Synchronized setter for a hash table *)
let set (tbl : ('a, 'b) Hashtbl.t) (m : Mutex.t) (k : 'a) (v : 'b) : unit =
  Mutex.lock m;
  Hashtbl.replace tbl k v;
  Mutex.unlock m

(* Synchronized getter for a hash table *)
let get (tbl : ('a, 'b) Hashtbl.t) (m : Mutex.t) (k : 'a) : 'b =
  Mutex.lock m;
  let v : 'b = Hashtbl.find tbl k in
  Mutex.unlock m; v

(* Maps all kv_pairs concurrently by assigning tasks *)
let map kv_pairs map_filename : (string * string) list =
  (* Constructs a hashtable from job id to status *)
  (* Also creates a mutex lock, thread pool and initializes mappers *)
  let tbl : (int, map_status) Hashtbl.t = Hashtbl.create 10 in
  let mutex : Mutex.t = Mutex.create () in
  let p : pool = create 100 in
  let m : mapper worker_manager = initialize_mappers map_filename in
  (* A single job, executed in its own thread
   * Pops a worker, uses it to map, and then handles its output
   * Remove worker if task has failed, but if a task has failed more than
   * 5 times, then throw away the task and recover those workers *)
  let job (id : int) (x : mapper list) (k : string) (v : string) () =
    let map_worker : mapper = pop_worker m in
    match map map_worker k v with
      | None   -> begin
        if List.length x >= 5 then (set tbl mutex id (MFailure);
          List.iter (fun w -> push_worker m w) (map_worker::x))
        else set tbl mutex id (MStandby ((map_worker::x), k, v))
      end
      | Some l -> begin
        set tbl mutex id (MSuccess l);
        push_worker m map_worker
      end in
  (* Removes completed tasks and queues up standby tasks *)
  let check (f, t : bool * int list) (id : int) : bool * int list =
    match get tbl mutex id with
      | MStandby (x, k, v) -> begin
        set tbl mutex id (MAttempt (x, k, v));
        add_work (job id x k v) p;
        (false, id::t)
      end
      | MAttempt (x, k, v) -> (false, id::t)
      | MSuccess l         -> (f, t)
      | MFailure           -> (f, t) in
  (* Loops through all tasks and ends when all tasks are completed *)
  let rec loop (tasks : int list) : unit =
    Thread.delay 0.1;
    let (flag, newtasks) = List.fold_left check (true, []) tasks in
    if flag then () else loop (List.rev newtasks) in
  (* Populates hashtable with all Standby states and starts looping *)
  List.iteri (fun i (k, v) -> Hashtbl.add tbl i (MStandby ([], k, v))) kv_pairs;
  loop (List.mapi (fun id x -> id) kv_pairs);
  clean_up_workers m;
  destroy p;
  (* Generates output by combining all successful jobs *)
  let append_output (id : int) (s : map_status) a =
    match s with
    | MSuccess l -> l@a
    | _          -> a in
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

(* Reduces all kvs_pairs concurrently by assigning tasks *)
let reduce kvs_pairs reduce_filename : (string * string list) list =
  (* Constructs a hashtable from job id to status *)
  (* Also creates a mutex lock, thread pool and initializes reducers *)
  let tbl : (int, reduce_status) Hashtbl.t = Hashtbl.create 10 in
  let mutex : Mutex.t = Mutex.create () in
  let p : pool = create 100 in
  let r : reducer worker_manager = initialize_reducers reduce_filename in
  (* A single job, executed in its own thread
   * Pops a worker, uses it to map, and then handles its output
   * Remove worker if task has failed, but if a task has failed more than
   * 5 times, then throw away the task and recover those workers *)
  let job (id : int) (x : reducer list) (k : string) (v : string list) () =
    let reduce_worker : reducer = pop_worker r in
    match reduce reduce_worker k v with
      | None   -> begin
        if List.length x >= 5 then (set tbl mutex id (RFailure);
          List.iter (fun w -> push_worker r w) (reduce_worker::x))
        else set tbl mutex id (RStandby ((reduce_worker::x), k, v))
      end
      | Some l -> begin
        set tbl mutex id (RSuccess (k, l));
        push_worker r reduce_worker
      end in
  (* Removes completed tasks and queues up standby tasks *)
  let check (f, t : bool * int list) (id : int) : bool * int list =
    match get tbl mutex id with
      | RStandby (x, k, v) -> begin
        set tbl mutex id (RAttempt (x, k, v));
        add_work (job id x k v) p;
        (false, id::t)
      end
      | RAttempt (x, k, v) -> (false, id::t)
      | RSuccess (k, vs)   -> (f, t)
      | RFailure           -> (f, t) in
  (* Loops through all tasks and ends when all tasks are completed *)
  let rec loop (tasks : int list) : unit =
    Thread.delay 0.1;
    let (flag, newtasks) = List.fold_left check (true, []) tasks in
    if flag then () else loop (List.rev newtasks) in
  (* Populates hashtable with all Standby states and starts looping *)
  List.iteri (fun i (k, v)-> Hashtbl.add tbl i (RStandby ([], k, v))) kvs_pairs;
  loop (List.mapi (fun id x -> id) kvs_pairs);
  clean_up_workers r;
  destroy p;
  (* Generates output by combining all successful jobs *)
  let append_output (id : int) (s : reduce_status) a =
    match s with
    | RSuccess (k, vs) -> (k, vs)::a
    | _                -> a in
  Hashtbl.fold append_output tbl []

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

