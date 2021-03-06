namespace FSharp.Control.Futures.Core

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading


/// <summary> Current state of a AsyncComputation </summary>
[<Struct; RequireQualifiedAccess; StructuralEquality; StructuralComparison>]
type Poll<'a> =
    | Ready of 'a
    | Pending

[<RequireQualifiedAccess>]
module Poll =
    let inline isReady x =
        match x with
        | Poll.Ready _ -> true
        | Poll.Pending -> false

    let inline isPending x =
        match x with
        | Poll.Ready _ -> false
        | Poll.Pending -> true

    let inline onReady (f: 'a -> unit) (x: Poll<'a>) : unit =
        match x with
        | Poll.Ready x -> f x
        | Poll.Pending -> ()

    let inline bind (binder: 'a -> Poll<'b>) (x: Poll<'a>): Poll<'b> =
        match x with
        | Poll.Ready x -> binder x
        | Poll.Pending -> Poll.Pending

    let inline bindPending (binder: unit -> Poll<'a>) (x: Poll<'a>): Poll<'a> =
        match x with
        | Poll.Ready x -> Poll.Ready x
        | Poll.Pending -> binder ()

    let inline map (f: 'a -> 'b) (x: Poll<'a>) : Poll<'b> =
        match x with
        | Poll.Ready x -> Poll.Ready (f x)
        | Poll.Pending -> Poll.Pending

    let inline join (p: Poll<Poll<'a>>) =
        match p with
        | Poll.Ready p -> p
        | Poll.Pending -> Poll.Pending



/// # IAsyncComputation poll schema
/// [ Poll.Pending -> ... -> Poll.Pending ] -> Poll.Ready x1 -> ... -> Poll.Ready xn
///  x1 == x2 == ... == xn
type IAsyncComputation<'a> =
    /// <summary> Poll the state </summary>
    /// <param name="context"> Current Computation context </param>
    /// <returns> Current state </returns>
    //[<EditorBrowsable(EditorBrowsableState.Advanced)>]
    abstract Poll: context: Context -> Poll<'a>

    /// <summary> Cancel asynchronously Computation computation </summary>
    /// <remarks> Notifies internal asynchronous operations of Computation cancellations. </remarks>
    //[<EditorBrowsable(EditorBrowsableState.Advanced)>]
    abstract Cancel: unit -> unit

/// <summary> The context of the running computation.
/// Allows the computation to signal its ability to move forward (awake) through the Wake method </summary>
and [<AbstractClass; AllowNullLiteral>]
    Context() =
    /// <summary> Wake up assigned Future </summary>
    abstract Wake: unit -> unit
    /// Current scheduler
    abstract Scheduler: IScheduler option
    default _.Scheduler: IScheduler option = None

/// <summary> Scheduler Future. Allows the Future to run for execution
/// (for example, on its own or shared thread pool or on the current thread).  </summary>
and IScheduler =
    inherit IDisposable
    /// IScheduler.Spawn принимает Future, так как вызов Future.RunComputation является частью асинхронного вычисления.
    abstract Spawn: Future<'a> -> IJoinHandle<'a>
    abstract Spawn: IAsyncComputation<'a> -> IJoinHandle<'a>

/// <summary> Allows to cancel and wait (asynchronously or synchronously) for a spawned Future. </summary>
and IJoinHandle<'a> =
    inherit Future<'a>
    abstract Cancel: unit -> unit
    abstract Join: unit -> 'a

and [<Interface>]
    IFuture<'a> =
    /// <summary> starts execution of the current Future and returns its "tail" as IAsyncComputation. </summary>
    /// <remarks> The call to Future.RunComputation is part of the asynchronous computation.
    /// And it should be call in an asynchronous context. </remarks>
    // [<EditorBrowsable(EditorBrowsableState.Advanced)>]
    abstract RunComputation: unit -> IAsyncComputation<'a>
and Future<'a> = IFuture<'a>

/// Exception is thrown when re-polling after cancellation (assuming IAsyncComputation is tracking such an invalid call)
exception FutureCancelledException

[<RequireQualifiedAccess>]
module AsyncComputation =

    //#region Core
    let inline cancelNullable (comp: IAsyncComputation<'a>) =
        if isNotNull comp then comp.Cancel()

    let inline cancel (comp: IAsyncComputation<'a>) =
        comp.Cancel()

    /// <summary> Create a Computation with members from passed functions </summary>
    /// <param name="__expand_poll"> Poll body </param>
    /// <param name="__expand_cancel"> Poll body </param>
    /// <returns> Computation implementations with passed members </returns>
    let inline create (__expand_poll: Context -> Poll<'a>) (__expand_cancel: unit -> unit) : IAsyncComputation<'a> =
        { new IAsyncComputation<'a> with
            member this.Poll(context) = __expand_poll context
            member this.Cancel() = __expand_cancel () }

    /// <summary> Create a Computation memo the first <code>Ready x</code> value
    /// with members from passed functions </summary>
    /// <param name="__expand_poll"> Poll body </param>
    /// <param name="__expand_cancel"> Poll body </param>
    /// <returns> Computation implementations with passed members </returns>
    let inline createMemo (__expand_poll: Context -> Poll<'a>) (__expand_cancel: unit -> unit) : IAsyncComputation<'a> =
        let mutable hasResult = false
        let mutable result: 'a = Unchecked.defaultof<_>
        create
        <| fun ctx ->
            if hasResult then
                Poll.Ready result
            else
                let p = __expand_poll ctx
                match p with
                | Poll.Pending -> Poll.Pending
                | Poll.Ready x ->
                    result <- x
                    hasResult <- true
                    Poll.Ready x
        <| __expand_cancel

    let inline poll context (comp: IAsyncComputation<'a>) = comp.Poll(context)


    /// <summary> Create the Computation with ready value</summary>
    /// <param name="value"> Poll body </param>
    /// <returns> Computation returned <code>Ready value</code> when polled </returns>
    let ready (value: 'a) : IAsyncComputation<'a> =
        create
        <| fun _ -> Poll.Ready value
        <| fun () -> do ()

    /// <summary> Create the Computation returned <code>Ready ()</code> when polled</summary>
    /// <returns> Computation returned <code>Ready ()value)</code> when polled </returns>
    let unit: IAsyncComputation<unit> =
        create
        <| fun _ -> Poll.Ready ()
        <| fun () -> do ()

    /// <summary> Creates always pending Computation </summary>
    /// <returns> always pending Computation </returns>
    let never<'a> : IAsyncComputation<'a> =
        create
        <| fun _ -> Poll<'a>.Pending
        <| fun () -> do ()

    /// <summary> Creates the Computation lazy evaluator for the passed function </summary>
    /// <returns> Computation lazy evaluator for the passed function </returns>
    let lazy' (f: unit -> 'a) : IAsyncComputation<'a> =
        createMemo
        <| fun _ -> Poll.Ready (f ())
        <| fun () -> do ()

    /// <summary> Creates the Computation, asynchronously applies the result of the passed compute to the binder </summary>
    /// <returns> Computation, asynchronously applies the result of the passed compute to the binder </returns>
    let bind (binder: 'a -> IAsyncComputation<'b>) (source: IAsyncComputation<'a>) : IAsyncComputation<'b> =
        let mutable _compA = source // poll when not null
        let mutable _compB = nullObj // poll when not null
        create
        <| fun context ->
            if isNull _compB then
                match poll context _compA with
                | Poll.Ready x ->
                    _compA <- Unchecked.defaultof<_>
                    _compB <- binder x
                    poll context _compB
                | Poll.Pending -> Poll.Pending
            else
                poll context _compB
        <| fun () ->
            cancelNullable _compA
            cancelNullable _compB

    /// <summary> Creates the Computation, asynchronously applies mapper to result passed Computation </summary>
    /// <returns> Computation, asynchronously applies mapper to result passed Computation </returns>
    let map (mapping: 'a -> 'b) (source: IAsyncComputation<'a>) : IAsyncComputation<'b> =
        let mutable _comp = source
        let mutable _value = Unchecked.defaultof<_> // has value when _comp = null
        create
        <| fun context ->
            if isNull _comp then
                Poll.Ready _value
            else
                match _comp.Poll(context) with
                | Poll.Pending -> Poll.Pending
                | Poll.Ready x ->
                    let r = mapping x
                    _value <- r
                    _comp <- Unchecked.defaultof<_>
                    Poll.Ready r
        <| fun () -> cancelNullable _comp

    /// <summary> Creates the Computation, asynchronously merging the results of passed Computations </summary>
    /// <remarks> If one of the Computations threw an exception, the same exception will be thrown everywhere,
    /// and the other Computations will be canceled </remarks>
    /// <returns> Computation, asynchronously merging the results of passed Computation </returns>
    let merge (comp1: IAsyncComputation<'a>) (comp2: IAsyncComputation<'b>) : IAsyncComputation<'a * 'b> =
        let mutable _exn = nullObj
        let mutable _comp1 = comp1 // if null -- has _r1
        let mutable _comp2 = comp2 // if null -- has _r2
        let mutable _r1 = Unchecked.defaultof<_>
        let mutable _r2 = Unchecked.defaultof<_>

        let inline writeExnState exn =
            _exn <- exn
            _comp1 <- nullObj
            _comp2 <- nullObj
            _r1 <- Unchecked.defaultof<_>
            _r2 <- Unchecked.defaultof<_>

        create
        <| fun ctx ->
            if isNotNull _exn then raise _exn // if has exception
            if isNotNull _comp1 then
                try
                    poll ctx _comp1
                    |> Poll.onReady (fun x -> _comp1 <- nullObj; _r1 <- x)
                with
                | exn ->
                    cancelNullable _comp2
                    writeExnState exn
                    raise exn

            if isNotNull _comp2 then
                try
                    poll ctx _comp2
                    |> Poll.onReady (fun x -> _comp2 <- nullObj; _r2 <- x)
                with
                | exn ->
                    cancelNullable _comp1
                    writeExnState exn
                    raise exn

            if (isNull _comp1) && (isNull _comp2)
            then Poll.Ready (_r1, _r2)
            else Poll.Pending

        <| fun () ->
            cancelNullable _comp1
            cancelNullable _comp2

    /// <summary> Creates a Computations that will return the result of
    /// the first one that pulled out the result from the passed  </summary>
    /// <remarks> If one of the Computations threw an exception, the same exception will be thrown everywhere,
    /// and the other Computations will be canceled </remarks>
    /// <returns> Computation, asynchronously merging the results of passed Computation </returns>
    let first (comp1: IAsyncComputation<'a>) (comp2: IAsyncComputation<'a>) : IAsyncComputation<'a> =
        let mutable _exn = nullObj
        let mutable _comp1 = comp1 // if null -- has _r
        let mutable _comp2 = comp2 // if null -- has _r
        let mutable _r = Unchecked.defaultof<_>

        let inline onExn toCancel exn =
            cancelNullable toCancel
            _exn <- exn
            _comp1 <- nullObj
            _comp2 <- nullObj
            _r <- Unchecked.defaultof<_>
            raise exn

        let inline writeResultAndReady (toCancel: IAsyncComputation<'a>) result =
            toCancel.Cancel()
            _comp1 <- nullObj
            _comp2 <- nullObj
            _r <- result
            Poll.Ready result

        create
        <| fun ctx ->
            if isNotNull _exn then raise _exn // if has exception
            if isNull _comp1 then
                Poll.Ready _r
            else
                let pollR =
                    try poll ctx _comp1
                    with exn -> onExn _comp2 exn
                match pollR with
                | Poll.Ready x -> writeResultAndReady _comp2 x
                | Poll.Pending ->
                    let pollR =
                        try poll ctx _comp2
                        with exn -> onExn _comp1 exn
                    match pollR with
                        | Poll.Ready x -> writeResultAndReady _comp1 x
                        | Poll.Pending -> Poll.Pending
        <| fun () ->
            cancelNullable _comp1
            cancelNullable _comp2

    /// <summary> Creates the Computation, asynchronously applies 'f' function to result passed Computation </summary>
    /// <returns> Computation, asynchronously applies 'f' function to result passed Computation </returns>
    let apply (f: IAsyncComputation<'a -> 'b>) (comp: IAsyncComputation<'a>) : IAsyncComputation<'b> =
        let mutable _fnFut = f // null when fn was got
        let mutable _sourceFut = comp // null when 'a was got
        let mutable _fn = Unchecked.defaultof<_>
        let mutable _value = Unchecked.defaultof<_>

        // Memoize the result so as not to call Apply twice
        createMemo
        <| fun context ->
            if isNotNull _fnFut then
                poll context _fnFut
                |> (Poll.onReady <| fun x ->
                    _fnFut <- nullObj
                    _fn <- x)
            if isNotNull _sourceFut then
                poll context _sourceFut
                |> (Poll.onReady <| fun x ->
                    _sourceFut <- nullObj
                    _value <- x)
            if (isNull _fnFut) && (isNull _sourceFut) then
                Poll.Ready (_fn _value)
            else
                Poll.Pending
        <| fun () ->
            cancelNullable _fnFut
            cancelNullable _sourceFut

    /// <summary> Creates the Computation, asynchronously joining the result of passed Computation </summary>
    /// <returns> Computation, asynchronously joining the result of passed Computation </returns>
    let join (comp: IAsyncComputation<IAsyncComputation<'a>>) : IAsyncComputation<'a> =
        // _inner == null до дожидания _source
        // _inner != null после дожидания _source
        let mutable _source = comp //
        let mutable _inner = nullObj //
        create
        <| fun context ->
            if isNotNull _inner then
                poll context _inner
            else
                let sourcePoll = poll context _source
                match sourcePoll with
                | Poll.Ready inner ->
                    _inner <- inner
                    _source <- Unchecked.defaultof<_>
                    poll context inner
                | Poll.Pending -> Poll.Pending
        <| fun () ->
            cancelNullable _source
            cancelNullable _inner

    /// <summary> Create a Computation delaying invocation and computation of the Computation of the passed creator </summary>
    /// <returns> Computation delaying invocation and computation of the Computation of the passed creator </returns>
    let delay (creator: unit -> IAsyncComputation<'a>) : IAsyncComputation<'a> =
        // Фьюча с задержкой её инстанцирования.
        // Когда _inner == null, то фьюча еще не инициализирована
        //
        let mutable _inner: IAsyncComputation<'a> = Unchecked.defaultof<_>
        create
        <| fun context ->
            if isNotNull _inner
            then poll context _inner
            else
                let inner = creator ()
                _inner <- inner
                poll context inner
        <| fun () ->
            cancelNullable _inner

    /// <summary> Creates a Computation that returns control flow to the scheduler once </summary>
    /// <returns> Computation that returns control flow to the scheduler once </returns>
    let yieldWorkflow () =
        let mutable isYielded = false
        create
        <| fun context ->
            if isYielded then
                Poll.Ready ()
            else
                isYielded <- true
                context.Wake()
                Poll.Pending
        <| fun () -> do ()

    /// <summary> Creates a IAsyncComputation that raise exception on poll after cancel. Useful for debug. </summary>
    /// <returns> Fused IAsyncComputation </returns>
    let inline cancellationFuse (source: IAsyncComputation<'a>) : IAsyncComputation<'a> =
        let mutable isCancelled = false
        create
        <| fun ctx -> if not isCancelled then poll ctx source else raise FutureCancelledException
        <| fun () -> isCancelled <- true

    //#endregion

    //#region STD integration
    let catch (source: IAsyncComputation<'a>) : IAsyncComputation<Result<'a, exn>> =
        let mutable _source = source
        let mutable _result = Poll.Pending
        create
        <| fun context ->
            if Poll.isPending _result then
                try
                    poll context _source |> Poll.onReady ^fun x -> _result <- Poll.Ready (Ok x)
                with
                | e -> _result <- Poll.Ready (Error e)
            _result
        <| fun () -> cancelNullable _source

    [<RequireQualifiedAccess>]
    module Seq =

        /// <summary> Creates a future iterated over a sequence </summary>
        /// <remarks> The generated future does not substitute implicit breakpoints,
        /// so on long iterations you should use <code>iterAsync</code> and <code>yieldWorkflow</code> </remarks>
        let iter (seq: 'a seq) (body: 'a -> unit) =
            lazy' (fun () -> for x in seq do body x)

        /// <summary> Creates a future async iterated over a sequence </summary>
        /// <remarks> The generated future does not substitute implicit breakpoints,
        /// so on long iterations you should use <code>yieldWorkflow</code> </remarks>
        let iterAsync (source: 'a seq) (body: 'a -> IAsyncComputation<unit>) =
            let enumerator = source.GetEnumerator()
            let mutable _currentAwaited: IAsyncComputation<unit> voption = ValueNone
            let mutable _isCancelled = false

            // Iterate enumerator until binding future return Ready () on poll
            // return ValueNone if enumeration was completed
            // else return ValueSome x, when x is Future<unit>
            let rec moveUntilReady (enumerator: IEnumerator<'a>) (binder: 'a -> IAsyncComputation<unit>) (context: Context) : IAsyncComputation<unit> voption =
                if enumerator.MoveNext()
                then
                    let waiter = body enumerator.Current
                    match poll context waiter with
                    | Poll.Ready () -> moveUntilReady enumerator binder context
                    | Poll.Pending -> ValueSome waiter
                else
                    ValueNone

            let rec pollInner (context: Context) : Poll<unit> =
                if _isCancelled then raise FutureCancelledException
                match _currentAwaited with
                | ValueNone ->
                    _currentAwaited <- moveUntilReady enumerator body context
                    if _currentAwaited.IsNone
                    then Poll.Ready ()
                    else Poll.Pending
                | ValueSome waiter ->
                    match waiter.Poll(context) with
                    | Poll.Ready () ->
                        _currentAwaited <- ValueNone
                        pollInner context
                    | Poll.Pending -> Poll.Pending

            create pollInner (fun () -> _isCancelled <- true)
    //#endregion

    //#region OS
    let sleep (dueTime: TimeSpan) =
        let mutable _timer: Timer = Unchecked.defaultof<_>
        let mutable _timeOut = false

        let inline onWake (context: Context) _ =
            let timer' = _timer
            _timer <- Unchecked.defaultof<_>
            _timeOut <- true
            context.Wake()
            timer'.Dispose()

        let inline createTimer context =
            new Timer(onWake context, null, dueTime, Timeout.InfiniteTimeSpan)

        create
        <| fun context ->
            if _timeOut then Poll.Ready ()
            else
                _timer <- createTimer context
                Poll.Pending
        <| fun () ->
            _timer.Dispose()
            do ()

    let sleepMs (milliseconds: int) =
        let dueTime = TimeSpan.FromMilliseconds(float milliseconds)
        sleep dueTime

    /// Spawn a Future on current thread and synchronously waits for its Ready
    /// The simplest implementation of the Future scheduler.
    /// Equivalent to `(Scheduler.spawnOn anyScheduler).Join()`,
    /// but without the cost of complex general purpose scheduler synchronization
    let runSync (comp: IAsyncComputation<'a>) : 'a =
        // The simplest implementation of the Future scheduler.
        // Based on a polling cycle (polling -> waiting for awakening -> awakening -> polling -> ...)
        // until the point with the result is reached
        use wh = new EventWaitHandle(false, EventResetMode.AutoReset)
        let ctx = { new Context() with member _.Wake() = wh.Set() |> ignore }

        let rec pollWhilePending () =
            match (poll ctx comp) with
            | Poll.Ready x -> x
            | Poll.Pending ->
                wh.WaitOne() |> ignore
                pollWhilePending ()

        pollWhilePending ()
    //#endregion

    //#region Core ignore
    /// <summary> Creates a Computation that ignore result of the passed Computation </summary>
    /// <returns> Computation that ignore result of the passed Computation </returns>
    let ignore comp =
        create
        <| fun context ->
            match poll context comp with
            | Poll.Ready _ -> Poll.Ready ()
            | Poll.Pending -> Poll.Pending
        <| fun () -> do comp.Cancel()
    //#endregion

module Future =
        /// <summary> Создает внутренний Computation. </summary>
    let inline runComputation (fut: Future<'a>) = fut.RunComputation()

    let inline create (__expand_creator: unit -> IAsyncComputation<'a>) : Future<'a> =
        { new Future<'a> with member _.RunComputation() = __expand_creator () }

    /// <summary> Create the Future with ready value</summary>
    /// <param name="value"> Poll body </param>
    /// <returns> Future returned <code>Ready value</code> when polled </returns>
    let inline ready value =
        create (fun () -> AsyncComputation.ready value)


module Utils =

    // Ограничение на тип структуры для более оптимального использования
    type StructOption<'a when 'a : struct> = Option<'a>

    type Box<'a when 'a : struct> =
        val Inner : 'a
        new(inner: 'a) = { Inner = inner }

    // --------------------------------------
    // InPlaceList
    // --------------------------------------


    [<Struct>]
    type InPlaceList<'a> =
        | Empty
        | Single of single: 'a
        | Many of many: 'a list

    module InPlaceList =
        let inline create () = Empty

        let inline add (x: 'a) (list: byref<InPlaceList<'a>>) =
            match list with
            | Empty -> list <- Single x
            | Single c -> list <- Many [c; x]
            | Many l -> list <- Many (l @ [x])

        let inline iter (action: 'a -> unit) (list: inref<InPlaceList<'a>>) =
            match list with
            | Empty -> ()
            | Single x -> action x
            | Many l -> List.iter action l

        let inline clear (list: byref<InPlaceList<'a>>) =
            list <- Empty

        let inline take (list: byref<InPlaceList<'a>>) : InPlaceList<'a> =
            let copy = list
            list <- Empty
            copy

    // --------------------------------------
    // InPlaceList END
    // --------------------------------------

    // --------------
    // IntrusiveList
    // --------------

    [<AllowNullLiteral>]
    type IIntrusiveNode<'a> when 'a :> IIntrusiveNode<'a> =
        abstract Next: 'a with get, set

    [<Struct>]
    type IntrusiveList<'a> when 'a :> IIntrusiveNode<'a> and 'a : not struct =
        val mutable internal startNode: 'a
        val mutable internal endNode: 'a
        new(init: 'a) = { startNode = init; endNode = init }

    module IntrusiveList =
        let create () = IntrusiveList(Unchecked.defaultof<'a>)
        let single x = IntrusiveList(x)

        let isEmpty (list: IntrusiveList<'a>) =
            list.startNode = null || list.endNode = null

        let pushBack (x: 'a) (list: byref<IntrusiveList<'a>>) =
            if isEmpty list then
                list.startNode <- x
                list.endNode <- x
                x.Next <- null
            else
                list.endNode.Next <- x
                list.endNode <- x

        let popFront (list: byref<IntrusiveList<'a>>) =
            if isEmpty list
            then null
            elif list.endNode = list.startNode then
                let r = list.startNode
                list.startNode <- null
                list.endNode <- null
                r
            else
                let first = list.startNode
                let second = list.startNode.Next
                list.startNode <- second
                first

        let toList (list: byref<IntrusiveList<'a>>) : 'a list =
            let root = list.startNode
            let rec collect (c: 'a list) (node: 'a) =
                if node = null then c
                else collect (c @ [node]) node.Next
            collect [] root






    // --------------
    // IntrusiveList
    // --------------

    // --------------------------------------
    // OnceVar
    // --------------------------------------

    exception OnceVarDoubleWriteException

    // TODO: Optimize to struct without DU
    [<Struct>]
    type private OnceState<'a> =
        // Cancel options only make sense for using OnceVar as IAsyncComputation
        // Re-polling after cancellation is UB by standard,
        // so it is possible to get rid of the cancellation handling in the future.
        | Empty // --> Waiting, HasValue, Cancelled
        | Waiting of ctx: Context // --> HasValue, Cancelled
        | HasValue of value: 'a // exn on write; --> CancelledWithValue
        | Cancelled // exn on poll; --> CancelledWithValue
        | CancelledWithValue of cancelledValue: 'a // exn on poll; exn on write; STABLE

    /// Low-level immutable cell to asynchronously wait for a put a single value.
    /// Represents the pending computation in which value can be put.
    /// If you never put a value, you will endlessly wait for it.
    [<Class; Sealed>]
    type OnceVar<'a>() =
        let sLock: SpinLock = SpinLock()
        let mutable state = Empty

        /// <returns> false on double write </returns>
        member inline this.TryWrite(x: 'a) =
            // has state mutation
            let mutable lockTaken = false
            try
                sLock.Enter(&lockTaken)
                match state with
                | Empty ->
                    state <- HasValue x
                    true
                | Waiting context ->
                    state <- HasValue x
                    // exit from lock and wake waiter
                    if lockTaken then lockTaken <- false; sLock.Exit()
                    context.Wake()
                    true
                | Cancelled ->
                    state <- CancelledWithValue x
                    true
                | HasValue _ | CancelledWithValue _ ->
                    false
            finally
                if lockTaken then sLock.Exit()

        member inline this.Write(x: 'a) =
            if not (this.TryWrite(x)) then raise OnceVarDoubleWriteException

        member inline this.TryRead() =
            // has NOT state mutation
            let mutable lockTaken = false
            try
                sLock.Enter(&lockTaken)
                match state with
                | HasValue value | CancelledWithValue value -> ValueSome value
                | Empty | Waiting _ | Cancelled -> ValueNone
            finally
                if lockTaken then sLock.Exit()

        member inline _.TryPoll(context) =
            // has state mutation
            let mutable lockTaken = false
            try
                sLock.Enter(&lockTaken)
                match state with
                | Empty | Waiting _ ->
                    state <- Waiting context
                    Ok Poll.Pending
                | HasValue value ->
                    Ok (Poll.Ready value)
                | Cancelled | CancelledWithValue _ ->
                    Error FutureCancelledException
            finally
                if lockTaken then sLock.Exit()

        interface IAsyncComputation<'a> with
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            member x.Poll(context) =
                let r = x.TryPoll(context)
                match r with
                | Ok x -> x
                | Error ex -> raise ex

            member _.Cancel() =
                // has state mutation
                let mutable lockTaken = false
                try
                    sLock.Enter(&lockTaken)
                    match state with
                    | Empty | Waiting _ -> state <- Cancelled
                    | HasValue x -> state <- CancelledWithValue x
                    | Cancelled | CancelledWithValue _ -> ()
                finally
                    if lockTaken then sLock.Exit()

    module OnceVar =
        /// Create empty IVar instance
        let inline create () = OnceVar()

        /// Put a value and if it is already set raise exception
        let inline write (x: 'a) (ovar: OnceVar<'a>) = ovar.Write(x)

        /// Tries to put a value and if it is already set returns an false
        let inline tryWrite (x: 'a) (ovar: OnceVar<'a>) = ovar.TryWrite(x)

        /// <summary> Returns the future pending value. </summary>
        /// <remarks> IVar itself is a future, therefore
        /// it is impossible to expect or launch this future in two places at once. </remarks>
        let inline read (ovar: OnceVar<'a>) = ovar :> IAsyncComputation<'a>

        /// Immediately gets the current IVar value and returns Some x if set
        let inline tryRead (ovar: OnceVar<_>) = ovar.TryRead()

    // --------------------------------------
    // OnceVar END
    // --------------------------------------

    module Experimental =
        [<Struct; StructuralEquality; StructuralComparison>]
        type ObjectOption<'a when 'a : not struct> =
            val Value: 'a
            new(value) = { Value = value }

            member inline this.IsNull =
                obj.ReferenceEquals(this.Value, null)

            member inline this.IsNotNull =
                not this.IsNull

        let inline (|ObjectNone|ObjectSome|) (x: ObjectOption<'a>) =
            if x.IsNull then ObjectNone else ObjectSome x.Value


