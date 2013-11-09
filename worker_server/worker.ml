open Protocol
open Program

type id_type = Map | Reduce
let ids : (worker_id, id_type) Hashtbl.t = Hashtbl.create 10

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  let get_connection_status v = 
    match v with
    | InitMapper source -> 
      begin
      match build source with
      | (Some id, str) -> Hashtbl.add ids id Map;
                          send_response client (Mapper (Some id, str))
      | (None, str) -> send_response client (Mapper (None, str))
      end
    | InitReducer source -> 
      begin
      match build source with
      | (Some id, str) -> Hashtbl.add ids id Reduce;
                          send_response client (Mapper (Some id, str))                                
      | (None, str) -> send_response client (Mapper (None, str))
      end
    | MapRequest (id, k, v) -> 
      (* Validate the id *)
      begin if (Hashtbl.mem ids id) && (Hashtbl.find ids id = Map) then
        (* Execute the request *) 
        begin
        match run id v with
        | None -> send_response client (RuntimeError (id, "fail"))
        | Some result -> send_response client (MapResults (id, result))
        end
        else send_response client (InvalidWorker id)
      end
    | ReduceRequest (id, k, v) -> 
      (* Validate the id *)
      begin if (Hashtbl.mem ids id) && (Hashtbl.find ids id = Reduce) then
        (* Execute the request *) 
        begin
        match run id v with
        | None -> send_response client (RuntimeError (id, "fail"))
        | Some result -> send_response client (ReduceResults (id, result))
        end
      else send_response client (InvalidWorker id)
      end in
  match Connection.input client with
    Some v ->
      begin
        if get_connection_status v then handle_request client
        else ()
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

