open Util
open Worker_manager
open Thread_pool

(* TODO implement these *)
let map kv_pairs map_filename : (string * string) list = 
  failwith "Go back whence you came! Trouble the soul of my Mother no more!"

let combine kv_pairs : (string * string list) list = 
  let tbl : (string, string list) Hashtbl.t = Hashtbl.create 10 in
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

