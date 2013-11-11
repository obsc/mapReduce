let (key, value) = Program.get_input() in
let helper acc k = 
  match Util.split_spaces k with
  | course::grade::etc -> (course, grade)::acc
  | _ -> acc in
Program.set_output (List.fold_left helper [] (Util.split_to_class_lst value))