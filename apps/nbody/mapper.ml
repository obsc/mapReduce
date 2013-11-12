open Util;;
open Plane;;

let (key, value) = Program.get_input() in

let (id1, body1, id2, body2) = unmarshal value in

let scale_vector scale (v1, v2) = 
	(s_times v1 scale, s_times v2 scale) in
	
(* Calculates the force on body 1 due to body 2*)
let force (m1, l1, v1) (m2,l2,v2) : vector =
	let num = s_times (s_times cBIG_G m2) m1 in
	let denom = s_times (distance l1 l2) (distance l1 l2) in
	let mag = s_divide num denom in
	scale_vector mag (unit_vector l1 l2) in

let accel (m1, l1, v1) body2 = 
	scale_vector (s_divide 1. m1) (force body1 body2) in

Program.set_output ((id1,accel body1 body2)::[(id2, accel body2 body1)])