open Util;;

let main (args : string array) : unit =
  if Array.length args < 3 then
    print_endline "Usage: grades <filename>"
  else
    let filename = args.(2) in
    let students = load_grades filename in
    let get_in d = (string_of_int d.id_num, d.course_grades) in
    let kv_pairs = List.rev_map get_in students in
    let reduced = 
      Map_reduce.map_reduce "grades" "mapper" "reducer" kv_pairs in
    print_reduced_courses reduced
in

main Sys.argv
