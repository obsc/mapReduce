open Util;;

let (key, values) = Program.get_input() in
let get_val (v : string) : vector = Util.unmarshal v in
let accels : vector list = List.map get_val values in
Program.set_output [s_to_string (sum () accels)]
