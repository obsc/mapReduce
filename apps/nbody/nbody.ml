open Util

(* Create a transcript of body positions for `steps` time steps *)
let make_transcript (bodies : (string * body) list) (steps : int) : string = 
  (* Recursive call *)
  let rec execute_step bodies steps acc = 
    (* Base case *)
    if steps = 0 then acc
    else 
    (* Find the new accelerations*)
    let pair_one acc next cur =
      match (next, cur) with
      | (id1, body1, id2, body2) -> if id1 = id2 then acc
                                    else let pairing = (id1, body1, id2, body2) in
                                         (steps, Util.marshal pairing)::acc
    let pair acc next = 
      acc@(List.fold_left (pair_one next) [] bodies) in
    let kv_pairs = List.fold_left (pair) [] bodies in
      Map_reduce.map_reduce "nbody" "mapper" "reducer" kv_pairs in
    (* For each body, update values and append *)

  execute_step bodies steps ""


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
