﻿module FSharp.Control.Futures.Sandbox.Program

open System
open System.Diagnostics
open System.Threading.Tasks
open System.Threading

//open FSharp.Control.Tasks.V2

open FSharp.Control.Futures.Base
open FSharp.Control.Futures
open FSharp.Control.Futures.StateMachines


let inline ( ^ ) f x = f x

module Snowball =

    type Tree<'a> =
        | Leaf
        | Node of Tree<'a> * 'a * Tree<'a>

    let snowballFunction n =
        let rec loop x i : Tree<int> =
            if i >= n then
                Leaf
            else
            let left = loop (x + i) (i + 1)
            let right = loop (x + i + 1) (i + 1)
            Node (left, x ,right)
        loop 0 0

    let snowballAsync n =
        let rec loop x i : Async<Tree<int>> =
            if i >= n then async { return Leaf }
            else async {
                let! left = loop (x + i) (i + 1)
                let! right = loop (x + i + 1) (i + 1)
                return Node (left, x ,right)
            }
        async { return! loop 0 0 }

    let snowballFuture n =
        let rec loop x i : IFuture<Tree<int>> =
            if i >= n then future { return Leaf }
            else future {
                let! left = loop (x + i) (i + 1)
                let! right = loop (x + i + 1) (i + 1)
                return Node (left, x ,right)
            }

        future { return! loop 0 0 }

    open FSharp.Control.Futures

    let snowballFutureRawSM n =
        let rec loop x i =
            let sm =
                if i >= n
                then OnTrue (ReadyStateMachine(Leaf))
                else
                    let merged = MergeStateMachine(loop (x + i) (i + 1), loop (x + i + 1) (i + 1))
                    let f = Future.create ^fun waker ->
                        let p = (merged :> IFuture<_>).Poll(waker)
                        match p with
                        | Ready (left, right) -> Ready (Node (left, x ,right))
                        | Pending -> Pending
                    OnFalse f
            sm :> IFuture<_>

        future { return! loop 0 0 }

//    let snowballTask n = task {
//        let rec loop x i : Task<Tree<int>> = task {
//            if i >= n then
//                return Leaf
//            else
//            let! left = loop (x + i) (i + 1)
//            let! right = loop (x + i + 1) (i + 1)
//            return Node (left, x ,right)
//        }
//        return! loop 0 0
//    }

open FSharp.Control.Futures.StateMachines

module Fib =

    let rec fib n =
        if n < 0 then invalidOp "n < 0"
        if n <= 1 then n
        else fib (n - 1) + fib (n - 2)

    let rec fibAsync n =
        if n < 0 then invalidOp "n < 0"
        if n <= 1 then async { return n }
        else async {
            let! a = fibAsync (n - 1)
            let! b = fibAsync (n - 2)
            return a + b
        }


    let rec fibFutureNoBuilder (n: int) : IFuture<int> =
        if n < 0 then raise (InvalidOperationException "n < 0")
        if n < 2
        then Future.ready n
        else
            let mutable value = -1
            let f1 = fibFutureNoBuilder (n - 1)
            let f2 = fibFutureNoBuilder (n - 2)
            Future.create (fun waker ->
                match value with
                | -1 ->
                    match Future.poll waker f1, Future.poll waker f2 with
                    | Ready a, Ready b -> value <- (a + b); Ready (a + b)
                    | _ -> Pending
                | x -> Ready x
            )

    let rec fibFutureLegacy n = legacyfuture {
        if n < 0 then invalidOp "n < 0"
        if n <= 1 then return n
        else
            let! a = fibFutureLegacy (n - 1)
            let! b = fibFutureLegacy (n - 2)
            return a + b
    }

    // int -> DelayStateMachine<int,CombineStateMachine<int,ZeroStateMachine,DelayStateMachine<int,ReadyStateMachine<int>>>>
    let rec fibFutureRawStateMachine n =
        DelayStateMachine(fun () ->
            if n < 0
            then invalidOp "n < 0"
            else
                CombineStateMachine(
                    ZeroStateMachine(()),
                    DelayStateMachine(fun () ->
                        if n <= 1 then
                            EitherStateMachine.OnTrue (ReadyStateMachine(n))
                        else
                            EitherStateMachine.OnFalse (
                                BindStateMachine(
                                    (fun a ->
                                        BindStateMachine(
                                            (fun b -> ReadyStateMachine(a + b)),
                                            (fibFutureRawStateMachine (n-2))
                                        )
                                    ),
                                    (fibFutureRawStateMachine (n-1))
                                )
                            )
                    )
                )
        )
        :> IFuture<_>

    type FutureStateMachine<'a>(fut: IFuture<'a>) =
        interface IFuture<'a> with
            member this.Poll(waker) = Future.poll waker fut

        // int -> DelayStateMachine<int,CombineStateMachine<int,ZeroStateMachine,DelayStateMachine<int,ReadyStateMachine<int>>>>
    let rec fibFutureRawStateMachineUnwrapInnerDelay n =
        DelayStateMachine(fun () ->
            if n < 0
            then invalidOp "n < 0"
            else
                let undelayed =
                    if n <= 1 then
                        EitherStateMachine.OnTrue(
                            ReadyStateMachine(n)
                        )
                    else
                        EitherStateMachine.OnFalse(
                            BindStateMachine(
                                (fun a ->
                                    BindStateMachine(
                                        (fun b -> ReadyStateMachine(a + b)),
                                        FutureStateMachine(fibFutureRawStateMachine (n-2))
                                    )
                                ),
                                FutureStateMachine(fibFutureRawStateMachine (n-1))
                            )
                        )
                undelayed
        )
        //:> IFuture<_>


    open FSharp.Control.Tasks
    let rec fibTask n = task {
        if n < 0 then invalidOp "n < 0"
        if n <= 1 then return n
        else
            let! a = fibTask (n - 1)
            let! b = fibTask (n - 2)
            return a + b
    }


    let rec fibFutureOptimized n =
        let rec fib n =
            if n < 0 then future { return invalidOp "n < 0" }
            else if n <= 1 then future { return n }
            else
                future {
                    let! a = fib (n - 1)
                    let! b = fib (n - 2)
                    return a + b
                }
        future { return! fib n }

    let runPrimeTest () =
        let sw = Stopwatch()
        let n = 25

        printfn "Test function..."
        sw.Start()
        for i in 1..20 do (fib n) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Tasks..."
        sw.Start()
        for i in 1..20 do (fibTask n).GetAwaiter().GetResult() |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Async..."
        sw.Start()
        for i in 1..20 do (fibAsync n |> Async.RunSynchronously) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Future low level..."
        sw.Restart()
        for i in 1..20 do (fibFutureNoBuilder n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Future raw state machine..."
        sw.Restart()
        for i in 1..20 do (fibFutureRawStateMachine n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Future raw state machine unwrap inner delay and combine(zero, 'a)..."
        sw.Restart()
        for i in 1..20 do (fibFutureRawStateMachineUnwrapInnerDelay n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Future builder optimized..."
        sw.Restart()
        for i in 1..20 do (fibFutureOptimized n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Future builder legacy (on classes)..."
        sw.Restart()
        for i in 1..20 do (fibFutureLegacy n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

[<EntryPoint>]
let main argv =

//    let sw = Stopwatch()
//    let n = 20
//
//    printfn "Test function..."
//    sw.Start()
//    for i in 1..20 do (Snowball.snowballFunction n) |> ignore
//    let ms = sw.ElapsedMilliseconds
//    printfn "Total %i ms\n" ms
//
//    printfn "Test async..."
//    sw.Start()
//    for i in 1..20 do (Snowball.snowballAsync n |> Async.RunSynchronously) |> ignore
//    let ms = sw.ElapsedMilliseconds
//    printfn "Total %i ms\n" ms
//
//    printfn "Test future..."
//    sw.Start()
//    for i in 1..20 do (Snowball.snowballFuture n |> Future.run) |> ignore
//    let ms = sw.ElapsedMilliseconds
//    printfn "Total %i ms\n" ms
//
//    printfn "Test future !RAW SM!..."
//    sw.Start()
//    for i in 1..20 do (Snowball.snowballFutureRawSM n |> Future.run) |> ignore
//    let ms = sw.ElapsedMilliseconds
//    printfn "Total %i ms\n" ms

    Fib.runPrimeTest ()

    0 // return an integer exit code