module Make (Ord : Map.OrderedType) = struct

include Map.Make(Ord)

(* Complete the dependent type specifications in this file.  A dependent type
 * may make use of any functions that you have already given dependent types
 * for.  For example, the type for "mem" below depends on "bindings", which is
 * defined before it.
 *)

(** Map.bindings **************************************************************)

let rec is_sorted lst = match lst with
  | []       -> true
  | [x]      -> true
  | x::y::tl -> (Ord.compare x y < 0) && is_sorted (y::tl)

let bindings_spec lst = is_sorted (List.map fst lst)

(* Dependent type for bindings:
 *
 * val bindings : ('a, 'b) t -> (lst : ('a * 'b) list where bindings_spec lst)
 *)
(* Dependent type for bindings:
 *
 * val bindings : 'a t -> (lst : (Ord.t * 'a) list where bindings_spec lst)
 *)

(** Map.mem *******************************************************************)

let mem_spec k table b =
  b = List.exists (fun (k',_) -> k = k') (bindings table)

(* Dependent type for mem: 
 *
 * val mem : (table : ('a,'b) t)
 *        -> (k : 'a)
 *        -> (b : bool where mem_spec table k b)
 *)
(* Dependent type for mem: 
 *
 * val mem : (k : Ord.t)
 *        -> (table : 'a t)
 *        -> (b : bool where mem_spec k table b)
 *)

(** Map.empty *****************************************************************)

let empty_spec table = (List.length (bindings table)) = 0

(* Dependent type for empty: 
 *
 * val empty : (table : 'a t where empty_spec table)
 *)

(** Map.find ******************************************************************)

let find_spec k table v =
  try (List.assoc k (bindings table)) = v
  with Not_found -> true

(* if not mem k table then true
  else (List.assoc k (bindings table)) = v *)

(* Dependent type for find: 
 *
 * val find : (k : Ord.t)
           -> (table : 'a t)
           -> (v : 'a where find_spec k table v)
 *)


(** Map.add *******************************************************************)

(* returns true if every member of l1 is a member of l2 *)
let subset l1 l2 =
  List.for_all (fun x -> List.mem x l2) l1

(* returns true if l1 and l2 contain the same elements *)
let eqset l1 l2 =
  subset l1 l2 && subset l2 l1

(* returns all bindings of table, except for the binding to k *)
let bindings_without k table =
  List.filter (fun (k',_) -> k <> k') (bindings table)

(* specification for add *)
let add_spec k v table result =
  eqset (bindings_without k table) (bindings_without k result)
  && mem  k result
  && find k result = v

(* Dependent type for add: 
 *
 * val add : (table : ('a,'b) t)
 *        -> (k:'a)
 *        -> (v:'b)
 *        -> (result : ('a,'b) t where add_spec table k v result)
 *)

(* Dependent type for add: 
 *
 * val add : (k: Ord.t)
 *        -> (v: 'a)
 *        -> (table : 'a t)
 *        -> (result : 'a t where add_spec k v table result)
 *)


(** Map.remove ****************************************************************)

let remove_spec k table result = 
  eqset (bindings_without k table) (bindings result)
  && not (mem k result)

(* Dependent type for remove: 
 *
 * val remove : (k: Ord.t)
 *           -> (table : 'a t)
 *           -> (result : 'a t where remove_spec k table result)
 *)

(** Map.equal *****************************************************************)

let kvsubset cmp l1 l2 =
  let is_in k v l = try List.assoc k l = v with Not_found -> false in
  List.for_all (fun (k, v) -> is_in k v l2) l1

let kvequal cmp l1 l2 =
  kvsubset cmp l1 l2 && kvsubset cmp l2 l1

let equal_spec cmp table1 table2 b =
  b = kvequal cmp (bindings table1) (bindings table2)

(* Dependent type for equal: 
 * val equal : (cmp : 'a -> 'a -> bool)
            -> (table1 : 'a t)
            -> (table2 : 'a t)
            -> (b : bool where equal_spec cmp table1 table2 b)
 *)

end

(*
** vim: ts=2 sw=2 ai et
*)
