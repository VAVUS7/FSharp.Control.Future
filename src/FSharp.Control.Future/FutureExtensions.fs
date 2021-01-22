[<AutoOpen>]
module FSharp.Control.Future.FutureExtensions

open System.Threading
open System.Timers


type CancellableFuture<'a> = CancellationToken -> Future<'a>

exception FutureCancelledException of string

[<RequireQualifiedAccess>]
module Future =
    
    // TODO: optimize
    let parallelSeq (futures: Future<'a> seq) : Future<'a[]> =
        let mutable futures = futures |> Seq.map ValueSome |> Seq.toArray
        
        let mutable results: 'a[] = Array.zeroCreate (Array.length futures)
        let mutable isCancelled = false
        
        let innerF waker =
            futures
            |> Seq.indexed
            |> Seq.map (fun (i, f) ->
                match f with
                | ValueSome f ->
                    let p = Future.poll waker f
                    match p with
                    | Ready value ->
                        futures.[i] <- ValueNone
                        results.[i] <- value
                        true
                    | Pending -> false
                    | Cancelled -> isCancelled <- true; true
                | ValueNone -> true
            )
            |> Seq.reduce (&&)
            |> fun x ->
                match x, isCancelled with
                | _, true -> Cancelled
                | false, _ -> Pending
                | true, _ -> Ready results
        Future.create innerF
    
    let cancellable future = fun (ct: CancellationToken) ->
        Future.create ^fun waker ->
            if ct.IsCancellationRequested
            then Cancelled
            else Future.poll waker future
    
    let never<'a> : Future<'a> =
        Future.create ^fun _ -> Pending
    
    let cancelled<'a> : Future<'a> =
        Future.create ^fun _ -> Cancelled
    
    // TODO: fix it
    let _run (f: Future<'a>) : 'a =
        use wh = new EventWaitHandle(false, EventResetMode.ManualReset)
        let waker () = wh.Set |> ignore
        
        let rec wait (current: Poll<'a>) =
            match current with
            | Ready x -> x
            | Pending ->
                wh.WaitOne() |> ignore
                wait (Future.poll waker f)
            | Cancelled -> raise (FutureCancelledException "Future was cancelled")
        
        wait (Future.poll waker f)
    
    let getWaker = Future.create Ready
    
    let sleep (duration: int) =
        // if None the time out
        let mutable currentWaker = None
        let mutable timer = None
        let sync = obj()
        
        timer <- 
            let t = new Timer(float duration)
            t.AutoReset <- false
            t.Elapsed.Add(fun _ ->
                // TODO: think!! Called from other thread
                timer <- None
                t.Dispose()
                lock sync ^fun () ->
                    match currentWaker with
                    | Some w -> w ()
                    | None -> ()
            )
            Some t
        
        Future.create ^fun waker ->
            match timer with
            | Some timer ->
                lock sync ^fun () ->
                    currentWaker <- Some waker
                    if not timer.Enabled then timer.Start()
                Pending
            | None ->
                Ready ()
    
    let join (f: Future<Future<'a>>) : Future<'a> = Future.bind id f
