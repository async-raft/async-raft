use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use pin_utils::{unsafe_pinned, unsafe_unpinned};
use actix::prelude::*;

pub trait TryActorFuture: ActorFuture {
    type Ok;
    type Error;

    fn try_poll(
        self: Pin<&mut Self>,
        srv: &mut <Self as ActorFuture>::Actor,
        ctx: &mut <<Self as ActorFuture>::Actor as Actor>::Context,
        task: &mut Context
    ) -> Poll<Result<Self::Ok, Self::Error>>;

}

impl<T, I, E> TryActorFuture for T
where
    T: ?Sized + ActorFuture<Output=Result<I, E>>
{
    type Ok = I;
    type Error = E;

    fn try_poll(
        self: Pin<&mut Self>,
        srv: &mut Self::Actor,
        ctx: &mut <Self::Actor as Actor>::Context,
        task: &mut Context
    ) -> Poll<Result<Self::Ok, Self::Error>> {
        self.poll(srv, ctx, task)
    }
}

pub trait TryActorStream: ActorStream {
    type Ok;
    type Error;

    fn try_poll_next(
        self: Pin<&mut Self>,
        srv: &mut <Self as ActorStream>::Actor,
        ctx: &mut <<Self as ActorStream>::Actor as Actor>::Context,
        task: &mut Context
    ) -> Poll<Option<Result<Self::Ok, Self::Error>>>;

}

impl<T, I, E> TryActorStream for T
where
    T: ?Sized + ActorStream<Item=Result<I, E>>
{
    type Ok = I;
    type Error = E;

    fn try_poll_next(
        self: Pin<&mut Self>,
        srv: &mut Self::Actor,
        ctx: &mut <Self::Actor as Actor>::Context,
        task: &mut Context
    ) -> Poll<Option<Result<Self::Ok, Self::Error>>> {
        self.poll_next(srv, ctx, task)
    }
}

pub trait TryActorFutureExt: TryActorFuture {
    fn and_then<Fut, F>(self, f: F) -> AndThen<Self, Fut, F>
    where
        F: FnOnce(
            Self::Ok,
            &mut Self::Actor,
            &mut <Self::Actor as Actor>::Context,
        ) -> Fut,
        Fut: TryActorFuture<Error=Self::Error, Actor=Self::Actor>,
        Self: Sized,
    {
        AndThen::new(self, f)
    }
    fn map_err<E, F>(self, f: F) -> MapErr<Self, F>
    where
        F: FnOnce(
            Self::Error,
            &mut Self::Actor,
            &mut <Self::Actor as Actor>::Context,
        ) -> E,
        Self: Sized,
    {
        MapErr::new(self, f)
    }
    fn map_ok<T, F>(self, f: F) -> MapOk<Self, F>
    where
        F: FnOnce(
            Self::Ok,
            &mut Self::Actor,
            &mut <Self::Actor as Actor>::Context,
        ) -> T,
        Self: Sized,
    {
        MapOk::new(self, f)
    }
    fn or_default(self) -> OrDefault<Self>
    where
        Self: Sized,
        Self::Ok: Default
    {
        OrDefault::new(self)
    }
}

impl<Fut: ?Sized + TryActorFuture> TryActorFutureExt for Fut {}

pub trait TryActorStreamExt: TryActorStream {
    fn and_then<Fut, F>(self, f: F) -> StreamAndThen<Self, Fut, F>
    where
        F: FnMut(
            Self::Ok,
            &mut Self::Actor,
            &mut <Self::Actor as Actor>::Context,
        ) -> Fut,
        Fut: TryActorFuture<Error=Self::Error, Actor=Self::Actor>,
        Self: Sized,
    {
        StreamAndThen::new(self, f)
    }

    fn try_finish(self) -> TryFinish<Self>
    where
        Self: Sized,
    {
        TryFinish::new(self)
    }
}

impl<S: ?Sized + TryActorStream> TryActorStreamExt for S {}

#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
enum TryChain<Fut1, Fut2, Data> {
    First(Fut1, Option<Data>),
    Second(Fut2),
    Empty,
}

impl<Fut1: Unpin, Fut2: Unpin, Data> Unpin for TryChain<Fut1, Fut2, Data> {}

pub(crate) enum TryChainAction<Fut2>
    where Fut2: TryActorFuture,
{
    Future(Fut2),
    Output(Result<Fut2::Ok, Fut2::Error>),
}

impl<Fut1, Fut2, Data> TryChain<Fut1, Fut2, Data>
where
    Fut1: TryActorFuture,
    Fut2: TryActorFuture<Actor=Fut1::Actor>,
{
    pub(crate) fn new(fut1: Fut1, data: Data) -> TryChain<Fut1, Fut2, Data> {
        TryChain::First(fut1, Some(data))
    }

    pub(crate) fn poll<F>(
        self: Pin<&mut Self>,
        srv: &mut Fut1::Actor,
        ctx: &mut <Fut1::Actor as Actor>::Context,
        task: &mut Context<'_>,
        f: F,
    ) -> Poll<Result<Fut2::Ok, Fut2::Error>>
    where
        F: FnOnce(
            Result<Fut1::Ok, Fut1::Error>,
            Data,
            &mut Fut1::Actor,
            &mut <Fut1::Actor as Actor>::Context,
        ) -> TryChainAction<Fut2>,
    {
        let mut f = Some(f);

        // Safe to call `get_unchecked_mut` because we won't move the futures.
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            let (output, data) = match this {
                TryChain::First(fut1, data) => {
                    // Poll the first future
                    let output = ready!(unsafe { Pin::new_unchecked(fut1) }.try_poll(srv, ctx, task));
                    (output, data.take().unwrap())
                }
                TryChain::Second(fut2) => {
                    // Poll the second future
                    return unsafe { Pin::new_unchecked(fut2) }
                        .try_poll(srv, ctx, task)
                        .map(|res| {
                            *this = TryChain::Empty; // Drop fut2.
                            res
                        });
                }
                TryChain::Empty => {
                    panic!("future must not be polled after it returned `Poll::Ready`");
                }
            };

            *this = TryChain::Empty; // Drop fut1
            let f = f.take().unwrap();
            match f(output, data, srv, ctx) {
                TryChainAction::Future(fut2) => *this = TryChain::Second(fut2),
                TryChainAction::Output(output) => return Poll::Ready(output),
            }
        }
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct AndThen<Fut1, Fut2, F> {
    try_chain: TryChain<Fut1, Fut2, F>,
}

impl<Fut1, Fut2, F> AndThen<Fut1, Fut2, F>
where
    Fut1: TryActorFuture,
    Fut2: TryActorFuture<Actor=Fut1::Actor>,
{
    unsafe_pinned!(try_chain: TryChain<Fut1, Fut2, F>);

    /// Creates a new `Then`.
    pub(super) fn new(future: Fut1, f: F) -> AndThen<Fut1, Fut2, F> {
        AndThen {
            try_chain: TryChain::new(future, f),
        }
    }
}

impl<Fut1, Fut2, F> ActorFuture for AndThen<Fut1, Fut2, F>
where
    Fut1: TryActorFuture,
    Fut2: TryActorFuture<Error=Fut1::Error, Actor=Fut1::Actor>,
    F: FnOnce(
        Fut1::Ok,
        &mut Fut1::Actor,
        &mut <Fut1::Actor as Actor>::Context,
    ) -> Fut2,
{
    type Actor = Fut1::Actor;
    type Output = Result<Fut2::Ok, Fut2::Error>;

    fn poll(
        self: Pin<&mut Self>,
        srv: &mut Fut1::Actor,
        ctx: &mut <Fut1::Actor as Actor>::Context,
        task: &mut Context<'_>
    ) -> Poll<Self::Output> {
        self.try_chain().poll(srv, ctx, task, |result, async_op, srv, ctx| {
            match result {
                Ok(ok) => TryChainAction::Future(async_op(ok, srv, ctx)),
                Err(err) => TryChainAction::Output(Err(err)),
            }
        })
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct MapErr<Fut, F> {
    future: Fut,
    f: Option<F>,
}

impl<Fut, F> MapErr<Fut, F> {
    unsafe_pinned!(future: Fut);
    unsafe_unpinned!(f: Option<F>);

    /// Creates a new MapErr.
    pub(super) fn new(future: Fut, f: F) -> MapErr<Fut, F> {
        MapErr { future, f: Some(f) }
    }
}

impl<Fut: Unpin, F> Unpin for MapErr<Fut, F> {}

impl<Fut, F, E> ActorFuture for MapErr<Fut, F>
where
    Fut: TryActorFuture,
    F: FnOnce(
        Fut::Error,
        &mut Fut::Actor,
        &mut <Fut::Actor as Actor>::Context,
    ) -> E,
{
    type Actor = Fut::Actor;
    type Output = Result<Fut::Ok, E>;

    fn poll(
        mut self: Pin<&mut Self>,
        srv: &mut Fut::Actor,
        ctx: &mut <Fut::Actor as Actor>::Context,
        task: &mut Context<'_>
    ) -> Poll<Self::Output> {
        self.as_mut()
            .future()
            .try_poll(srv, ctx, task)
            .map(|result| {
                let f = self.as_mut().f().take()
                    .expect("MapErr must not be polled after it returned `Poll::Ready`");
                result.map_err(|e| f(e, srv, ctx))
            })
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct MapOk<Fut, F> {
    future: Fut,
    f: Option<F>,
}

impl<Fut, F> MapOk<Fut, F> {
    unsafe_pinned!(future: Fut);
    unsafe_unpinned!(f: Option<F>);

    /// Creates a new MapOk.
    pub(super) fn new(future: Fut, f: F) -> MapOk<Fut, F> {
        MapOk { future, f: Some(f) }
    }
}

impl<Fut: Unpin, F> Unpin for MapOk<Fut, F> {}

impl<Fut, F, T> ActorFuture for MapOk<Fut, F>
where
    Fut: TryActorFuture,
    F: FnOnce(
        Fut::Ok,
        &mut Fut::Actor,
        &mut <Fut::Actor as Actor>::Context,
    ) -> T,
{
    type Actor = Fut::Actor;
    type Output = Result<T, Fut::Error>;

    fn poll(
        mut self: Pin<&mut Self>,
        srv: &mut Fut::Actor,
        ctx: &mut <Fut::Actor as Actor>::Context,
        task: &mut Context<'_>
    ) -> Poll<Self::Output> {
        self.as_mut()
            .future()
            .try_poll(srv, ctx, task)
            .map(|result| {
                let f = self.as_mut().f().take()
                    .expect("MapOk must not be polled after it returned `Poll::Ready`");
                result.map(|x| f(x, srv, ctx))
            })
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct OrDefault<Fut> {
    future: Fut,
}

impl<Fut> OrDefault<Fut> {
    unsafe_pinned!(future: Fut);

    /// Creates a new MapOk.
    pub(super) fn new(future: Fut) -> OrDefault<Fut> {
        OrDefault { future }
    }
}

impl<Fut: Unpin> Unpin for OrDefault<Fut> {}

impl<Fut> ActorFuture for OrDefault<Fut>
where
    Fut: TryActorFuture,
    Fut::Ok: Default,
{
    type Actor = Fut::Actor;
    type Output = Fut::Ok;

    fn poll(
        mut self: Pin<&mut Self>,
        srv: &mut Fut::Actor,
        ctx: &mut <Fut::Actor as Actor>::Context,
        task: &mut Context<'_>
    ) -> Poll<Self::Output> {
        self.as_mut()
            .future()
            .try_poll(srv, ctx, task)
            .map(Result::unwrap_or_default)
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TryFinish<S> {
    stream: S,
}

impl<S> TryFinish<S> {
    unsafe_pinned!(stream: S);

    /// Creates a new MapOk.
    pub(super) fn new(stream: S) -> TryFinish<S> {
        TryFinish { stream }
    }
}

impl<S: Unpin> Unpin for TryFinish<S> {}

impl<S> ActorFuture for TryFinish<S>
where
    S: TryActorStream,
{
    type Actor = S::Actor;
    type Output = Result<(), S::Error>;

    fn poll(
        mut self: Pin<&mut Self>,
        srv: &mut S::Actor,
        ctx: &mut <S::Actor as Actor>::Context,
        task: &mut Context<'_>
    ) -> Poll<Self::Output> {
        Poll::Ready(loop {
            match ready!(self.as_mut()
                .stream()
                .try_poll_next(srv, ctx, task))
            {
                None => break Ok(()),
                Some(Ok(_)) => continue,
                Some(Err(e)) => break Err(e),
            }
        })
    }
}

/// Stream for the [`and_then`](super::TryStreamExt::and_then) method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct StreamAndThen<St, Fut, F> {
    stream: St,
    future: Option<Fut>,
    f: F,
}

impl<St: Unpin, Fut: Unpin, F> Unpin for StreamAndThen<St, Fut, F> {}

impl<St, Fut, F> StreamAndThen<St, Fut, F> {
    unsafe_pinned!(stream: St);
    unsafe_pinned!(future: Option<Fut>);
    unsafe_unpinned!(f: F);
}

impl<St, Fut, F> StreamAndThen<St, Fut, F>
where
    St: TryActorStream,
    F: FnMut(
        St::Ok,
        &mut Fut::Actor,
        &mut <Fut::Actor as Actor>::Context,
    ) -> Fut,
    Fut: TryActorFuture<Error = St::Error>,
{
    pub(super) fn new(stream: St, f: F) -> Self {
        Self { stream, future: None, f }
    }
}

impl<St, Fut, F> ActorStream for StreamAndThen<St, Fut, F>
where
    St: TryActorStream,
    F: FnMut(
        St::Ok,
        &mut St::Actor,
        &mut <Fut::Actor as Actor>::Context,
    ) -> Fut,
    Fut: TryActorFuture<Error=St::Error, Actor=St::Actor>,
{
    type Actor = St::Actor;
    type Item = Result<Fut::Ok, St::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        srv: &mut St::Actor,
        ctx: &mut <St::Actor as Actor>::Context,
        task: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        if self.future.is_none() {
            let item = match ready!(self.as_mut().stream().try_poll_next(srv, ctx, task)?) {
                None => return Poll::Ready(None),
                Some(e) => e,
            };
            let fut = (self.as_mut().f())(item, srv, ctx);
            self.as_mut().future().set(Some(fut));
        }

        let e = ready!(self.as_mut().future().as_pin_mut().unwrap().try_poll(srv, ctx, task));
        self.as_mut().future().set(None);
        Poll::Ready(Some(e))
    }
}
