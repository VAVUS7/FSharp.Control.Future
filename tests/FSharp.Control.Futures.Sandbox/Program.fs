﻿module FSharp.Control.Futures.Sandbox.Program

open System
open System.Diagnostics

//open FSharp.Control.Tasks.V2

open FSharp.Control.Futures.Base
open FSharp.Control.Futures
open FSharp.Control.Futures.FutureRt

let inline ( ^ ) f x = f x


module Fib =

    let rec fib n =
        if n < 0 then invalidOp "n < 0"
        if n <= 1 then  n
        else fib(n-1) + fib(n-2)


    let rec fibAsync n =
        if n < 0 then invalidOp "n < 0"
        if n <= 1 then async { return n }
        else async {
            let! a = fibAsync (n - 1)
            let! b = fibAsync (n - 2)
            return a + b
        }

    open FSharp.Control.Tasks
    let rec fibTask n =
        if n < 0 then invalidOp "n < 0"
        task {
            if n <= 1 then return n
            else
                let! a = fibTask (n - 1)
                let! b = fibTask (n - 2)
                return a + b
        }

    let rec fibFuture (n: int) : Future<int> =
        if n < 0 then invalidOp "n < 0"
        future {
            if n <= 1 then return n
            else
                let! a = fibFuture (n - 1)
                let! b = fibFuture (n - 2)
                return a + b
        }

    let rec fibFutureAsyncOnRuntime (n: int) : Future<int> =
        if n < 0 then invalidOp "n < 0"
        future {
            if n <= 1 then return n
            else
                let! a = FutureRt.runAsync (fibFuture (n - 1))
                and! b = FutureRt.runAsync (fibFuture (n - 2))
                return a + b
        }

    let fibFutureOptimized n =
        if n < 0 then invalidOp "n < 0"
        let rec fibInner n =
            if n <= 1 then Future.ready n
            else
                let mutable value = -1
                let f1 = fibInner (n-1)
                let f2 = fibInner (n-2)
                FutureCore.create ^fun waker ->
                    match value with
                    | -1 ->
                        match FutureCore.poll waker f1, FutureCore.poll waker f2 with
                        | Ready a, Ready b ->
                            value <- a + b
                            Ready (a + b)
                        | _ -> Pending
                    | value -> Ready value

        let fut = lazy(fibInner n)
        FutureCore.create ^fun w -> FutureCore.poll w fut.Value

    let runPrimeTest () =
        let sw = Stopwatch()
        let n = 25

        printfn "Test function..."
        sw.Start()
        for i in 1..20 do fib n |> ignore
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
        for i in 1..20 do (fibFutureOptimized n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms

        printfn "Test Future..."
        sw.Restart()
        for i in 1..20 do (fibFuture n |> Future.run) |> ignore
        let ms = sw.ElapsedMilliseconds
        printfn "Total %i ms\n" ms



[<EntryPoint>]
let main argv =

    //Fib.runPrimeTest ()

    let sw = Stopwatch()
    let n = 25

    printfn "Test Future.run..."
    sw.Restart()
    for i in 1..20 do Fib.fibFuture n |> Future.run |> ignore
    let ms = sw.ElapsedMilliseconds
    printfn "Total %i ms\n" ms

    printfn "Test Runtime.run with current thread rt..."
    FutureRt.enter FutureRt.localRt
    sw.Restart()
    for i in 1..20 do Fib.fibFuture n |> FutureRt.run |> ignore
    let ms = sw.ElapsedMilliseconds
    printfn "Total %i ms\n" ms

    printfn "Test Runtime.run with thread poll rt..."
    FutureRt.enter FutureRt.threadPoolRt
    sw.Restart()
    for i in 1..20 do Fib.fibFuture n |> FutureRt.run |> ignore
    let ms = sw.ElapsedMilliseconds
    printfn "Total %i ms\n" ms

    printfn "Test Runtime.run with thread poll async rt..."
    FutureRt.enter FutureRt.threadPoolRt
    sw.Restart()
    for i in 1..20 do Fib.fibFutureAsyncOnRuntime n |> FutureRt.run |> ignore
    let ms = sw.ElapsedMilliseconds
    printfn "Total %i ms\n" ms

    0 // return an integer exit code
