open Int64;;

let ios = int_of_string in
let (skey, value) = Program.get_input() in
let key = ios skey in
(* Split a single block into many transactions *)
let parse_block (block : string list) : (string * string) list =
  match block with
  | []              -> []
  | numtrans::trans -> begin
    let tot : int = ios numtrans in
    (* Map in/out to ((int * int) * bool * int64) for time, add/wipe, amount *)
    let rec add_inout cur trans n_in n_out acc =
      (* Wipe in coins*)
      if n_in > 0 then begin
        match trans with
        | id::t -> let nval = Util.marshal ((key, cur), false, zero) in
          add_inout cur t (n_in - 1) n_out ((id,nval)::acc)
        | _     -> (trans, acc)
      end
      (* Add to out coins *)
      else if n_out > 0 then begin
        match trans with
        | id::v::t -> let nval = Util.marshal ((key, cur), true, of_string v) in
          add_inout cur t 0 (n_out - 1) ((id, nval)::acc)
        | _        -> (trans, acc)
      end
      else (trans, acc) in
    (* Parse an entire transaction *)
    let rec parse_trans cur trans acc = 
      if cur >= tot then acc
      else begin
        match trans with
        | n_in::n_out::t ->
          let (rem, vals) = add_inout cur t (ios n_in) (ios n_out) acc in
          parse_trans (cur + 1) rem vals
        | _              -> acc
      end in
    parse_trans 0 trans []
  end in
Program.set_output (parse_block (Util.split_spaces value))

