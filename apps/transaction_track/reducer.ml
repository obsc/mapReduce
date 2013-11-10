open Int64;;

let (key, values) = Program.get_input() in
let get_val (v : string) : (int * int) * bool * int64 = Util.unmarshal v in
let trans : ((int * int) * bool * int64) list = List.map get_val values in
let rev_cmp ((t_b1, t_t1), x1, y1) ((t_b2, t_t2), x2, y2) : int =
  if t_b1 < t_b2 then 1
  else if t_b1 > t_b2 then -1
  else if t_t1 < t_t2 then 1
  else if t_t1 > t_t2 then -1
  else 0 in
let rec get_sum trans acc : int64 =
  match trans with
  | ((t_b, t_t), a, amount)::t -> begin
    if a then get_sum t (add acc amount)
    else acc
  end
  | []   -> acc in
Program.set_output [to_string (get_sum (List.sort rev_cmp trans))]
