﻿module FSharp.Control.Futures.Benchmarks.Program

open System

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running

open FSharp.Control.Futures

open FSharp.Control.Futures.Benchmarks.Fibonacci
open FSharp.Control.Futures.FutureRt

// TODO: Rewrite after stabilize FutureRt
type FibonacciBenchmark() =

    member _.Arguments() = seq { 0 .. 10 .. 20 }

    [<Benchmark>]
    [<ArgumentsSource("Arguments")>]
    member _.SerialFun(n) =
        SerialFun.fib n


    [<Benchmark>]
    [<ArgumentsSource("Arguments")>]
    member _.SerialFutureBuilder(n) =
        SerialFutureBuilder.fib n |> Future.run

    [<Benchmark>]
    [<ArgumentsSource("Arguments")>]
    member _.ParallelFutureBuilder(n) =
        ParallelFutureBuilder.fib n |> Future.run


    [<Benchmark>]
    [<ArgumentsSource("Arguments")>]
    member _.SerialAsync(n) =
        SerialAsync.fib n |> Async.RunSynchronously

    [<Benchmark>]
    [<ArgumentsSource("Arguments")>]
    member _.ParallelAsync(n) =
        ParallelAsync.fib n |> Async.RunSynchronously


[<EntryPoint>]
let main argv =
    let summary = BenchmarkRunner.Run<FibonacciBenchmark>()
    0
