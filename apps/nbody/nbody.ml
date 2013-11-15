open Util;;

(* Create a transcript of body positions for `steps` time steps *)
let make_transcript (bodies : (string * body) list) (steps : int) : string =
  (* Recursive call *)
  let rec exec_step bodies steps (acc : string) : string =
    (* Base case *)
    if steps = 0 then acc else
    (* Calculate pairs of bodies *)
    let rec pair bodies acc = match bodies with
      | []   -> acc
      | h::t -> pair t (List.fold_left (fun a x -> (h, x)::a) acc t) in
    (* Marshals pairs of bodies *)
    let kv_pairs : (string * string) list =
      List.rev_map (fun (a,b) -> (marshal a, marshal b)) (pair bodies []) in
    (* Executes map_reduce *)
    let result = Map_reduce.map_reduce "nbody" "mapper" "reducer" kv_pairs in
    (* Unmarshal and get next value *)
    let split_marshal (a, b : string * string list) : string * body =
      let (id, prev : string * body) = unmarshal a in
      match b with
        | []   -> failwith "invalid input"
        | h::t -> begin
          let next : body = unmarshal h in (id, next)
        end in
    (* Unmarshals all output *)
    let new_bodies : (string * body) list = List.rev_map split_marshal result in
    print_endline (string_of_int steps);
    exec_step (new_bodies) (steps - 1) (acc^(string_of_bodies new_bodies)) in
  exec_step bodies steps (string_of_bodies bodies) in

let simulation_of_string = function
  | "binary_star" -> Simulations.binary_star
  | "diamond" -> Simulations.diamond
  | "orbit" -> Simulations.orbit
  | "swarm" -> Simulations.swarm
  | "system" -> Simulations.system
  | "terrible_situation" -> Simulations.terrible_situation
  | "zardoz" -> Simulations.zardoz
  | _ -> failwith "Invalid simulation name. Check `shared/simulations.ml`" in

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
    close_out out_channel end in

main Sys.argv