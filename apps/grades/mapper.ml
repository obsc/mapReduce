let (key, value) = Program.get_input() in
let is_float (g : string) : bool =
    try ignore (float_of_string g); true
    with e -> false in
let helper acc k = 
  match Util.split_spaces k with
  | course::grade::etc -> if is_float grade then (course, grade)::acc else acc
  | _ -> acc in
Program.set_output (List.fold_left helper [] (Util.split_to_class_lst value))