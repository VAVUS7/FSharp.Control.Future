module FSharp.Control.Futures.Cancellable

open System


type MaybeCancel<'a> =
    | Completed of 'a
    | Cancelled of OperationCanceledException

type CancellableFuture<'a> = IFuture<MaybeCancel<'a>>

exception FutureCancelledException of OperationCanceledException

[<RequireQualifiedAccess>]
module Future =

    ()
