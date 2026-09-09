let grpc_recv_streaming body message_buffer_writer =
  let request_buffer = Grpc.Buffer.v () in
  let on_eof () = Seq.close_writer message_buffer_writer in
  let rec on_read buffer ~off ~len =
    Grpc.Buffer.copy_from_bigstringaf ~src_off:off ~src:buffer
      ~dst:request_buffer ~length:len;
    Grpc.Message.extract_all (Seq.write message_buffer_writer) request_buffer;
    H2.Body.Reader.schedule_read body ~on_read ~on_eof
  in
  H2.Body.Reader.schedule_read body ~on_read ~on_eof

let grpc_send_streaming_client body encoder_stream =
  Seq.iter
    (fun encoder ->
      let payload = Grpc.Message.make encoder in
      H2.Body.Writer.write_string body payload)
    encoder_stream;
  H2.Body.Writer.close body

(* Write [payload] and block until it has actually drained to the
   transport, so the caller can't race ahead to the next message (or to
   trailers/close) before this one has left the process. *)
let write_and_flush body payload =
  H2.Body.Writer.write_string body payload;
  let flushed, notify_flushed = Eio.Promise.create () in
  H2.Body.Writer.flush body (Eio.Promise.resolve notify_flushed);
  Eio.Promise.await flushed

let grpc_send_streaming request encoder_stream status_promise =
  let body =
    H2.Reqd.respond_with_streaming ~flush_headers_immediately:true request
      (H2.Response.create
         ~headers:
           (H2.Headers.of_list [ ("content-type", "application/grpc+proto") ])
         `OK)
  in
  Seq.iter
    (fun input -> write_and_flush body (Grpc.Message.make input))
    encoder_stream;
  let status = Eio.Promise.await status_promise in
  H2.Reqd.schedule_trailers request
    (H2.Headers.of_list
       ([
          ( "grpc-status",
            string_of_int (Grpc.Status.int_of_code (Grpc.Status.code status)) );
        ]
       @
       match Grpc.Status.message status with
       | None -> []
       | Some message -> [ ("grpc-message", message) ]));
  H2.Body.Writer.close body
