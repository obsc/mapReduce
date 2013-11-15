open Util

(* Create a transcript of body positions for `steps` time steps *)
let make_transcript (bodies : (string * body) list) (steps : int) : string = 

  (* Recursive call *)
  let rec exec_step (bodies : (string * body) list) (steps : int) (acc : string) : string = 
    (* Base case *)
    if steps = 0 then acc
    else 
      (* Find the new accelerations*)
      (*
      let pair_one acc next cur =
        match (next, cur) with
        | (body1, body2) -> if id1 = id2 then acc
                                      else let pairing = (id1, body1, id2, body2) in
                                           (steps, Util.marshal pairing)::acc *)
      let pair acc next = acc in 

      let kv_pairs = List.fold_left (pair) [] bodies in
      let result = Map_reduce.map_reduce "nbody" "mapper" "reducer" kv_pairs in

      let split_marshal acc next = 
        match next with 
        | (id, body) -> begin
          match body with
          | [] -> acc
          | h::t -> (id, unmarshal h)::acc
        end in
      let new_bodies = List.fold_left (split_marshal) [] result in

      exec_step (new_bodies) (steps - 1) (acc^(string_of_bodies new_bodies)) in
  exec_step bodies steps (string_of_bodies bodies)


let simulation_of_string = function
  | "binary_star" -> Simulations.binary_star
  | "diamond" -> Simulations.diamond
  | "orbit" -> Simulations.orbit
  | "swarm" -> Simulations.swarm
  | "system" -> Simulations.system
  | "terrible_situation" -> Simulations.terrible_situation
  | "zardoz" -> Simulations.zardoz
  | _ -> failwith "Invalid simulation name. Check `shared/simulations.ml`"

let main (args : string array) : unit = 
  if Array.length args < 3 then 
    print_endline "Usage: nbody <simulation> [<outfile>]
  <simulation> is the name of a simulation from shared/simulations.ml
  Results will be written to [<outfile>] or stdout."
  else begin
    let (num_bodies_str, bodies) = simulation_of_string args.(2) in
    let transcript = make_transcript bodies 60 in
    let out_channel = 
      if Array.length args > 3 then open_out args.(3) else stdout in
    output_string out_channel (num_bodies_str ^ "\n" ^ transcript);
    close_out out_channel end
