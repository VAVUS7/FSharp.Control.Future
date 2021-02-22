namespace FSharp.Control.Futures.Scheduling

open System
open System.Threading
open FSharp.Control.Futures
open FSharp.Control.Futures.Sync


type IJoinHandle<'a> =
    inherit Future<'a>
    // TODO: Add try await
    abstract member Join: unit -> Result<'a, exn>

// TODO: Add IDisposable Runtime interface
// Run future execution
type IScheduler =
    inherit IDisposable
    abstract Spawn: fut: Future<'a> -> IJoinHandle<'a>


// -------------------
// ThreadPollScheduler
// -------------------
module private SchedulerImpl =

    type IVarJoinHandle<'a>() =

        let inner: IVar<Result<'a, exn>> = IVar.create ()

        member inline _.Put(x) = inner.Put(x)

        interface IJoinHandle<'a> with

            member _.Join() =
                use wh = new EventWaitHandle(false, EventResetMode.AutoReset)
                let waker () = wh.Set() |> ignore

                let rec wait (current: Poll<Result<'a, exn>>) =
                    match current with
                    | Poll.Ready x -> x
                    | Poll.Pending ->
                        wh.WaitOne() |> ignore
                        wait (Future.Core.poll waker inner)

                wait (Future.Core.poll waker inner)

            member _.Poll(waker) =
                let x = Future.Core.poll waker inner
                match x with
                | Poll.Ready x ->
                    match x with
                    | Ok x -> Poll.Ready x
                    | Error e -> raise e
                | Poll.Pending -> Poll.Pending


    type ThreadPoolTask<'a>(future: Future<'a>, waiter: IVarJoinHandle<'a>, scheduler: ThreadPoolScheduler) =
        let syncPoll = obj()

        member this.Run() =
            lock syncPoll ^fun () ->
                let waker () =
                    lock syncPoll ^fun () ->
                        scheduler.RunTask(this)
                try
                    let x = Future.Core.poll waker future
                    match x with
                    | Poll.Ready x ->
                        waiter.Put(Ok x)
                    | Poll.Pending -> ()
                with e ->
                    waiter.Put(Error e)

    and ThreadPoolScheduler() =
        member _.RunTask(task: ThreadPoolTask<'a>) =
            ThreadPool.QueueUserWorkItem(fun _ -> do task.Run()) |> ignore
        interface IScheduler with
            member this.Spawn(fut: Future<'a>) =
                let handle = IVarJoinHandle<'a>()
                let task = ThreadPoolTask<'a>(fut, handle, this)
                this.RunTask(task)
                handle :> IJoinHandle<'a>

            member _.Dispose() = ()



[<RequireQualifiedAccess>]
module Schedulers =
    let threadPoolRuntime: IScheduler = upcast new SchedulerImpl.ThreadPoolScheduler()


[<RequireQualifiedAccess>]
module Scheduler =

    let spawnOn (runtime: IScheduler) fut = runtime.Spawn(fut)
    let spawnOnThreadPool fut = spawnOn Schedulers.threadPoolRuntime fut
