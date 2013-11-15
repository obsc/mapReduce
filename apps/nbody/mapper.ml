open Util;;
open Plane;;

let (key, value) = Program.get_input() in

(* The value read in is a tuple of informations*)
let (id1, body1 : string * body) = unmarshal key in
let (id2, body2 : string * body) = unmarshal value in

let scale_vector scale (v1, v2) = 
    (s_times v1 scale, s_times v2 scale) in
    
(* Calculates the acceleration of one body due to the other*)
let accel (m1, l1, v1) (m2, l2, v2) : vector =
    let num = s_times cBIG_G m2 in
    let denom = s_times (distance l1 l2) (distance l1 l2) in
    let mag = s_divide num denom in
    scale_vector mag (unit_vector l1 l2) in

let a1 = (marshal (id1, body1), marshal (accel body1 body2)) in
let a2 = (marshal (id2, body2), marshal (accel body2 body1)) in

Program.set_output ([a1;a2])