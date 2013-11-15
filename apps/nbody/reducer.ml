open Util;;
open Plane;;

let (key, values) = Program.get_input() in
let (_,body : string * body) = unmarshal key in
let (mass, pos, velocity) = body in
let get_val (v : string) : vector = unmarshal v in
let accel : vector = Plane.sum () (List.map get_val values) in

(* calculate the new position of the body *)
let new_pos : point = 
    match (pos, velocity, accel) with
    | ((x,y),(v_x,v_y),(a_x,a_y)) -> 
        let half a = s_times 0.5 a in 
        (s_plus (s_plus x v_x) (half a_x), s_plus (s_plus y v_y) (half a_y)) in
(* calculate the new velocities of the body *)
let new_vel : vector =
    match (velocity,accel) with
    | ((v_x,v_y),(a_x,a_y)) -> (s_plus v_x a_x, s_plus v_y a_y) in
Program.set_output [marshal (mass, new_pos, new_vel)]
