open Protocol
open Program

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  match Connection.input client with
    Some v ->
      begin
        match v with
        | InitMapper source -> send_response client (Mapper (build source))
        | InitReducer source -> send_response client (Reducer (build source))
        | MapRequest (id, k, v) -> 
          (* Validate the id *)
          (* Execute the request *) 
          begin
            match run id v with
            | None -> send_response client (RuntimeError (id, "fail"))
            | Some result -> send_response client (MapResults (id, result))
          end
        | ReduceRequest (id, k, v) -> 
          (* Validate the id *)
          (* Execute the request *) 
          begin
            match run id v with
            | None -> send_response client (RuntimeError (id, "fail"))
            | Some result -> send_response client (ReduceResults (id, result))
          end
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

