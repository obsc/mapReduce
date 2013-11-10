open Int64;;

let (key, values) = Program.get_input() in
let trans = List.map (fun d -> Util.unmarshal d) values in
let helper (reset, acc_val) (time, add, val) = 
	if time > reset && add then (reset, add acc_val val)
	else if time > reset then (time, zero)
	else (reset, acc_val) in
let (reset, sum) = List.fold_left (helper) (0,zero) trans in
Program.set_output [to_string sum]
