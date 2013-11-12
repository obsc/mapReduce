open Util;;
open Plane;;

let scale_vector scale (v1, v2) = 
	(s_times v1 scale, s_times v2 scale) in
	
(* Calculates the force on body 1 due to body 2*)
let force (m1, l1, v1) (m2,l2,v2) : vector =
	let num = s_times (s_times cBIG_G m2) m1 in
	let denom = s_times (distance l1 l2) (distance l1 l2) in
	let mag = s_divide num denom in
	scale_vector mag (unit_vector l1 l2) in

let (key, value) = Program.get_input() in
let helper acc k = 
  match Util.split_spaces k with
  | course::grade::etc -> (course, grade)::acc
  | _ -> acc in
Program.set_output (List.fold_left helper [] (Util.split_to_class_lst value))