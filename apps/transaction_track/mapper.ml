open Int64;;

let (skey, value) = Program.get_input() in
let key = int_of_string skey in
(* Split a single block into many transactions *)
let parse_block block = 
	(* Map inputs/outputs to (int * bool * int64) for time, add/wipe, amount *)
	let rec add_inout trans n_in n_out acc =
		(* Wipe in coins*)
		if n_in > 0 then
			begin
				match trans with
				| [] -> acc
				| id::t -> let nval = Util.marshal (key, false, zero) in
									 add_inout t (numin - 1) numout ((id,nval)::acc)
			end
		(* Add to out coins *)
		else if n_out > 0 then
			begin
				match trans with
				| id::v::t -> let nval = Util.marshal (key, true, of_string v) in
											add_inout t 0 (numout - 1) ((id, nval)::acc)
				| _ -> acc
			end
		else (trans, acc) in
	(* Parse an entire transaction *)
	let rec parse_trans numtrans trans acc = 
		if numtrans = 0 then acc
		else begin
			match trans with
			| n_in::n_out::t -> let (rem, vals) = add_inout trans numin numout acc in
													parse_trans (numtrans - 1) rem vals
			| _ -> acc
		end
	match block with 
	| [] -> []
	| numtrans::trans -> parse_trans (int_of_string numtrans) trans [] in 
Program.set_output (parse_block (Util.split_spaces value))

