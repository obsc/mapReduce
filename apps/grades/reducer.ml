let (key, values) = Program.get_input() in
let sorted = List.sort compare (List.map float_of_string values) in
(* Gets median index of list *)
let med = 
  let mid = List.length sorted / 2 in
  (* odd case *)
  if List.length sorted mod 2 = 1 then List.nth sorted mid
  (* even case *)
  else ((List.nth sorted (mid - 1)) +. (List.nth sorted mid)) /. 2. in
Program.set_output [string_of_float med]
