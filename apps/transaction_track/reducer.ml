open Int64;;

let (key, values) = Program.get_input() in
let get_val (v : string) : (int * int) * bool * int64 = Util.unmarshal v in
let trans : ((int * int) * bool * int64) list = List.map get_val values in
(* Compares two timestamps *)
let cmp (t_b1, t_t1 : int * int) (t_b2, t_t2 : int * int) : bool =
  if t_b1 > t_b2 then true
  else if t_b1 = t_b2 && t_t1 >= t_t2 then true
  else false in
(* Checks to see if a value is an input (clear) *)
let check_clear t1 (t2, a, x) = if not a && cmp t2 t1 then t2 else t1 in
(* Last timestamp with a clear *)
let last : int * int = List.fold_left check_clear (0, 0) trans in
(* Add values after last timestamp *)
let add_if acc (t, a, amount) : int64 =
  if a && cmp t last then add acc amount else acc in
Program.set_output [to_string (List.fold_left add_if 0L trans)]
