open Util;;
open Plane;;

let (key, values) = Program.get_input() in
let (mass, pos, velocity) : (scalar * point * vector) = unmarshal key in
let get_val (v : string) : vector = unmarshal v in
let accel : vector = Plane.sum () (List.map get_val values) in

(* calculate the new vectors of the body *)
let new_pos : point = 
	match (pos, velocity, accel) with
	| ((x,y),(v_x,v_y),(a_x,a_y)) -> (x+v_x+(0.5*a_x), y+v_y+(0.5*a_y)) in
let new_vel : vector = 
	match (velocity,accel) with
	| ((v_x,v_y),(a_x,a_y)) -> (v_x + a_x, v_y + a_y) in
Program.set_output [marshal (mass, pos, velocity)]
