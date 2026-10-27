---
layout: post
title: "Implementing Effect Handlers in Ray Programming Language"
date: 2026-03-08 10:00:00 +0000
categories: update
---

# Ray Programming Language

My goal for Ray is to build a programming language that has the following 
combination of features:

- A statically-typed system-level programming language that compiles to native 
  code.
- Enjoys the benefit of borrow checking and ownership model like Rust.
- Has an effect system that tracks side effects and solves the function coloring 
  problem.
- Has an effect handler that can be compiled into a resonably efficient code.
- Has an explicit-dictionary passing or OCaml's module system-like for ad-hoc
  polymorphism.

In this post, it will be a write-up of the effect handler feature of Ray.

## Ray's Effect Handlers and Compilation Model

Effect handlers are a powerful abstraction that allows us to define and manage
complex control flow in the programs. For instance, effect handlers can be
used to implement features like exceptions, async/await, generators, and more. 

Often, these features like async/await, exceptions, and generators are 
implemented as a separate language feature with its own dedicated syntax. 
However, the effect handler ought to be a more general and unified abstraction
that can be used to implement them.

Let's explore the effect handler feature of Ray through an example of 
implementing a generator function that generates fibonacci numbers.

Here's how an *effect* can be defined in Ray:

```ray
eff Yield[a]:
    def yield(value: a)
```

The `Yield` is defined as an **effect** that has a single **operation** called 
`yield`. This looks similar to defining a trait or an interface in other 
programming languages!.

Let's define a function that uses the `Yield` effect to generate fibonacci 
numbers as follows:

```ray
pub def generateFibonacci() -> () \ {Yield[int32]}:
    let mut a = 0
    let mut b = 1
    while true:
        Yield.yield(a)
        let temp = a
        a = b
        b = temp + b
```

The function signature `generateFibonacci() -> () \ {Yield[int32]}` has an
additional syntax `\ {Yield[int32]}` that indicates that this function can 
perform the `Yield[int32]` effect. Inside the function body, it's defined as
an infinite loop that repeatedly performs the `Yield.yield(a)` operation to
yield the next fibonacci number.

The syntax `\ {Yield[int32]}` is a part of the **effect system** of Ray, which
tracks which effects a function can perform. This can be thought of as an 
extension of the type system that tracks side effects in addition to types.
That annotation is required because the `Yield.yield(a)` operation performed
inside the function body emits the `Yield[int32]` effect, and the function
signature must reflect that. 

To give a complete example, we can define a function that print the first 10
fibonacci numbers:

```ray
pub def main():
    let mut gen = coro generateFibonacci()
    let mut i = 0
    while i < 10:
        handle gen():
            case Yield.yield(value) resume k:
                printf("%d\n", value)
                i += 1
                gen = k
            case return x:
                break
```

First, the expression `coro generateFibonacci()` reifies the function into a
**coroutine**. This expression doesn't immediately run the `generateFibonacci`
function, but instead returns a coroutine that can be resumed later. 

> The oversimplified explanation of a coroutine is to think of it as a function 
that can be paused and gives the value back to the caller. Then, the caller
can resume the coroutine again from where it left off. A single coroutine can
pause and be resumed multiple times, which is different from a normal function 
that can only be called once, run to completion, and return a value back to the 
caller.

From now on, we will call `gen` a "coroutine" that represents the 
`generateFibonacci` function.

Inside the while loop, the expression `handle gen(): ...` resumes the coroutine
and handles the effects that are performed by the coroutine immediately after. 
In this case, the `gen`, which represent the `generateFibonacci` function, 
will run until it performs the  `Yield.yield(a)` operation then pause and return 
the yielded value back to the `main` function. 

Once the coroutine pauses, the `handle` expression will match the performed 
effect operation and execute the corresponding branch of the `case` statement; 
this is similar to how a `match` expression works in Rust.

```ray
pub def main():
    # ...
    while i < 10:
        handle gen():
            case Yield.yield(value) resume k:
                printf("%d\n", value)
                i += 1
                gen = k
            case return x:
                break
```

Let's take a closer look at `case Yield.yield ...` branch:

```ray
case Yield.yield(value) resume k:
    printf("%d\n", value)
    i += 1
    gen = k
```

As this branch suggests, it will match the `Yield.yield` operation and bind the
yielded value to `value` variable of type `int32`; this is very similar to
matching a variant of an enum. 

The `resume k` part is unusual, it represents the **continuation** of the 
coroutine `gen` that can be resumed later. In this case, it is a point after
the `Yield.yield(a)` operation in the `generateFibonacci` function. 

`k` is a first-class value that can be passed around, assigned to a variable,
and called like a function, just like the `gen` coroutine.

In this example, we assign `k` to `gen` variable so that we can resume the 
coroutine in the next iteration of the while loop.

On the other hand, the `case return: ...` branch:

```ray
case return x:
    break
```

This branch is invoked when the coroutine finishes (returns normally). 
The `return x` pattern binds the return value of the coroutine to `x` variable. 
In this example, `x` is simply a unit value `()` and the branch breaks out of 
the while loop since it doesn't have any more number to yield.

In summary, the above example will print out first 10 fibonacci numbers. It
works by repeatedly resuming the coroutine and handling the `Yield.yield`
operation at each iteration. The `k` continuation allows us to resume the 
coroutine function from where it left off, enabling us to generate the next 
fibonacci number.

## What Can It be Used For?

So far, the above example shows how to implement a generator function using
effect handlers. However, it can also be used to implement various features
that require control flow suspension. 

Implementing execeptions can be done by defining an `Exception` effect with an 
operation like `raise(str: String) -> Never`. The `Never` type indicates a type 
that can never have a value. This means that once the `raise`
operation is performed, the rest of the function can never be resumed, and the 
control flow will be transferred to the nearest enclosing `handle` expression.


### Effectful Code Compilation Model

We briefly mentioned that Ray uses a coroutine to implement control flow 
suspension, specifically stackless coroutines. This is a key part of how Ray 
compiles effectful code with minimal memory overhead. 

Stackless coroutine is not a new concept, it has been used by Rust and C# to 
implement async/await feature. The term "stackless" in this 
context means that the coroutine doesn't require a separate OS-level stack or
have to manipulate the stack pointer. Instead, the coroutine is implemented as
an ordinary state machine, think of plain old `enum` in Rust.

> Generally, stackless coroutine is memory efficient and is portable across
different platforms. However, it does have some limitations, which we will
discuss later in this post.

So, let's explore how the compilation work through the lens of existing building
blocks used to implement stackless coroutine in Rust, which is what Ray's 
compilation model is based on.

First, let's look at Coroutine's interface. Rust's Coroutine trait should give 
us a good picture of how the coroutine state machine is defined:

```rust
pub enum CoroutineState<Y, R> {
    Yielded(Y),
    Complete(R),
}

// This is the simplified version of Rust's Coroutine trait
pub trait Coroutine<R> { // R: The type of the argument that the coroutine expects to be resumed with
    type Yield; 
    type Return;

    fn resume(self: Pin<&mut Self>, arg: R) -> CoroutineState<Self::Yield, Self::Return>;
}
```

Every coroutine state machine implements the `Coroutine` trait, which has a
single method `resume`. It drives the coroutine state machine forward at a time
and returns whether the coroutine has yielded a value or ran to completion via
the `CoroutineState` enum.

We could imagine that the `generateFibonacci` function from the motivating
example is lowered into an enum that implements the `Coroutine` trait like this:

```rust
pub enum GenerateFibonacci {
    Start,
    State { a: i32, b: i32 },
}

impl Coroutine<()> for GenerateFibonacci {
    type Yield = i32;
    type Return = ();

    fn resume(self: Pin<&mut Self>, _: ()) -> CoroutineState<Self::Yield, Self::Return> {
        match self.get_mut() {
            state @ GenerateFibonacci::Start => {
                *state = GenerateFibonacci::State { a: 0, b: 1 };
                CoroutineState::Yielded(0)
            }
            GenerateFibonacci::State { a, b } => {
                let temp = *a;
                *a = *b;
                *b = temp + *b;
                CoroutineState::Yielded(*a)
            }
        }
    }
}
```

Then, the  `main` function of the previous example could be lowered into a 
function that drives the `GenerateFibonacci` coroutine like this:

```rust
pub fn main() {
    let mut gen = std::pin::pin!(GenerateFibonacci::Start);
    let mut i = 0;
    while i < 10 {
        match gen.as_mut().resume(()) {
            CoroutineState::Yielded(value) => {
                println!("{}", value);
                i += 1;
            }
            CoroutineState::Complete(_) => break,
        }
    }
}
```

Voila! This is roughly how Ray compiles the effectful code and the `handle` 
expression.

Another detail is that effect operations can take arguments and yield values of
different types.

Consider the following example:

```ray
pub eff Calculator:
    def add(x: int32, y: int32) -> int32
    def sub(x: int32, y: int32) -> int32


pub eff Yield[a]:
    def yield(value: a)


def yieldAndCalculator() -> () \ {Yield[int32], Calculator}:
    ...
```

When compiling the `yieldAndCalculator` function into a coroutine, the 
`Coroutine::Yield` associated type will be a sum type of all the effect 
operations that can be raised in the function, and similarly, the `Coroutine`'s
`R` type parameter. This is what the lowered coroutine might look like:

```rust
pub enum YieldAndCalculator { ... }

pub enum YieldAndCalculator_Yield {
    Yield_yield(i32),
    Calculator_add(i32, i32),
    Calculator_sub(i32, i32),
}

pub enum YieldAndCalculator_R {
    Yield_yield(()),
    Calculator_add(i32),
    Calculator_sub(i32),
}

impl Coroutine<YieldAndCalculator_R> for YieldAndCalculator {
    type Yield = YieldAndCalculator_Yield;
    type Return = ();

    fn resume(self: Pin<&mut Self>, arg: YieldAndCalculator_R) -> CoroutineState<Self::Yield, Self::Return> {
        ...
    }
}
```

Obviously, the `YieldAndCalculator_Yield` and `YieldAndCalculator_R` have
to match! 

If the coroutine yields a `YieldAndCalculator_Yield::Calculator_add(x, y)` 
value, which asks for the result of adding two numbers, the caller must resume 
with  `YieldAndCalculator_R::Calculator_add(result)` value answering it. 

For this reason, the Ray compiler will enforce this type safety and ensure that 
the types match up correctly. 

### Continuation Type-State Wrapper

In Ray, when creating a coroutine via `coro function()` expression, the 
compiler will generate a unique coroutine type for that function; this is 
similar to how Rust generates a unique type for each closure.

However, the expression will return a value of type 
`Continuation[r, &mut <internal coroutine type>]` instead, which acts as a 
type-state wrapper around the internal coroutine type. 

In the  `Continuation[r, ...]` type, the `r` type represents the next argument 
type that the coroutine expects to be resumed with. This is how the compiler
ensures that the caller always resumes the coroutine with the correct argument
type. 

The first time the coroutine is created, the `r` type is always `()`, since
starting a coroutine from the initial state doesn't require any arguments. 
However, after resuming the coroutine and handling an effect, the resumption
type will still have the same `Continuation[r, &mut <internal coroutine type>]` 
shape, but the `r` type will be updated depending on the effect that was 
handled.

For instance, consider the following example:

```ray
pub eff Calculator:
    def add(x: int32, y: int32) -> int32
    def sub(x: int32, y: int32) -> int32


pub eff Yield[a]:
    def yield(value: a)


def yieldAndCalculator() -> () \ {Yield[int32], Calculator}:
    ...


pub def main();
    let mut con = coro yieldAndCalculator() 

    handle con():
        case Yield.yield(value)   resume x:   ...
        case Calculator.add(x, y) resume y: ...
        case Calculator.sub(x, y) resume z: ...
        case return w:
            break
```

To illustrate how the `Continuation` type-state wrapper works, let's consider
the type of `con` variable and each continuation type for each `resume <var>`
binding in each branch of the `handle` expression:

- `con` has the type of `Continuation[(), &mut <coroutine type of yieldAndCalculator>]`
  since starting the coroutine doesn't require any arguments.
- `x` in the `case Yield.yield ...` branch has the type of 
  `Continuation[(), &mut <coroutine type of yieldAndCalculator>]` as well, since
  the `Yield.yield` effect's resumption type is `()`. Also, notice that, the
  `&mut <coroutine type of yieldAndCalculator>` is the same as the `con` 
  variable, which means that assigning `x` to `con` type checks correctly.
- `y` in the `case Calculator.add ...` branch has the type of 
  `Continuation[int32, &mut <coroutine type of yieldAndCalculator>]`, since
  the `Calculator.add` effect's resumption type is `int32`. 
- Similarly, `z` has the same type of `Continuation[int32, &mut <coroutine type of yieldAndCalculator>]` as `y`.

### Recursive Functions and Heap Allocation

Stackless coroutine serves as a good starting point for implementing effect
handlers, but it has some rough edges that need to be addressed. This limitation
is also present in Rust's async/await implementation. 

The noticable limitation is that the stackless coroutine doesn't interact well
with recursive functions. This is generally because how the coroutine state
machine is composed. For instance, consider the following example:

```ray
pub def recursiveYield(n: int32) -> () \ {Yield[int32]}:
    if n <= 0:
        return
    
    recursiveYield(n - 1)
    Yield.yield(n)
```

The `recursiveYield` function recursively calls itself and its state machine
might be defined like this:

```rust
pub enum RecursiveYield {
    Start { n: i32 },
    RecursiveYield { n: i32, inner_state: Pin<Box<RecursiveYield>> },
    End,
}

impl Coroutine<()> for RecursiveYield {
    type Yield = i32;
    type Return = ();

    fn resume(mut self: Pin<&mut Self>, arg: ()) -> CoroutineState<Self::Yield, Self::Return> {
        match self.as_mut().get_mut() {
            RecursiveYield::Start { n } => {
                if *n <= 0 {
                    return CoroutineState::Complete(());
                }

                *self.as_mut().get_mut() = RecursiveYield::RecursiveYield {
                    n: *n,
                    inner_state: Box::pin(RecursiveYield::Start { n: *n - 1 }),
                };

                self.as_mut().resume(arg)
            }

            RecursiveYield::RecursiveYield { n, inner_state } => {
                match inner_state.as_mut().resume(arg) {
                    CoroutineState::Yielded(value) => {
                        return CoroutineState::Yielded(value);
                    }
                    CoroutineState::Complete(()) => {
                        let val = *n;
                        *self.as_mut().get_mut() = RecursiveYield::End;
                        return CoroutineState::Yielded(val);
                    }
                }
            }

            RecursiveYield::End => {
                panic!("Cannot resume a completed coroutine");
            }
        }
    }
}
```

The compiler will generate a `RecursiveYield` state machine and the 
`inner_state` field must be boxed into a heap-allocated structure. Otherwise,
the `RecursiveYield` state machine will have an infinite size.

In Rust, the user must manually and explicitly box the recursive state machine
via `Box::pin`. However, in Ray, the compiler will automatically box the 
recursive state machine for you. 

This is a debatable design choice about explicitness vs ergonomics. Sometimes,
silently allocating memory everytime a recursive function is called might be
surprising and can lead to unpredictable performance issues. In the future, 
we might have a dedicated syntax for explicitly boxing the recursive state
machine. However, at the same time, we might be able to take advantage of the 
fact that the compiler can choose to optimize the allocation and deallocation 
of the recursive state machine.

### Pinning the Coroutine State Machine

Another important detail is that the coroutine state machine might contain
a reference to its own saved state. This is generally unavoidable, especially
in the context of system-level programming languages. One solution is to 
**pin** the state machine in memory, which prevents it from invalidating its
own internal references.

Rust does this via combination of `Pin` pointer and `Unpin` trait. However,
the pinning mechanism of Ray is not yet decided. Currently, every `coro` 
expression pins the coroutine state machine immediately and the concrete 
coroutine is hidden away, similarly to how Rust `std::pin::pin!` macro works. 
This is also the reason why the `Continuation` wraps over the 
`&mut <internal coroutine type>` instead of `<internal coroutine type>`. 

Consequently, the user cannot move the coroutine state machine around in memory 
before the first resumption, which is a limitation of the current design that
we'll address in the future once we have a better pinning mechanism in place.

## Type and Effect System

So far, we have discussed about the semantics and compilation model of effect
handlers. In a language with a static type system, it's natural to extend the
type system to track side effects as well. This is where the **effect system**
comes into play.

Generally, the effect system tracks which set of effects a function can perform 
and ensures that all effects are handled properly. However, it's also about how 
they are composed and interact with each other. Let's explore the effect system
through the notorious **function coloring problem**.

### Function Coloring Problem

The "color" of a function is a metaphor describing that a function can have 
some certain attributes or properties that may make them incomposable with
other functions. Those colors of functions could be "async vs sync", 
"throw vs non-throw", etc.

For instance, when calling an async function, the caller must also be an async
function. When calling a function that may throw an exception, consequently,
the caller must also be a function that may throw an exception. The problem
becomes prominent when trying to compose functions with different colors,
especially when dealing with higher-order functions.

Let's take an example of function coloring problem that everyone might be able to 
relate to. In Rust, consider a very primitive `Option::map` function:

```rust
pub enum Option<T> {
    Some(T),
    None,
}

impl<T> Option<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Option<U> {
        match self {
            Option::Some(value) => Option::Some(f(value)),
            Option::None => Option::None,
        }
    }
}

pub fn main() {
    assert_eq!(Some(2).map(|x| x + 1),      Some(3));
    assert_eq!(None.map(|x: i32| x + 1),    None);
    assert_eq!(Some(2).map(|x| x % 2 == 0), Some(true));
}
```

So far so good, the problem arises when we try to use the `Option::map` function
with an async closure.

```rust
Some(32).map(async |x| do_some_io(x).await) // ERROR!
```

The original `Option::map` function expects a "synchronous" closure, not an
"asynchronous" closure. They have different colors, and thus, they are 
incomposable. The simplest solution is to define a new variant of `Option::map`
that accepts an async closure, which we can call `Option::map_async`:

```rust
impl<T> Option<T> {
    pub async fn map_async<U>(self, f: impl AsyncFnOnce(T) -> U) -> Option<U> {
        match self {
            Option::Some(value) => Option::Some(f(value).await),
            Option::None => Option::None,
        }
    }
}
```

A more general and better solution is to define a single `Option::map` function
that can be **polymorphic** over the color of the closure. This is where the
effect system comes into play. For instance, the `Option::map` function can be 
defined as follows:

```rust
impl<T> Option<T> {
    pub fn map<U, !E>(self, f: impl FnOnce(T) -> U \ !E) -> Option<U> \ !E {
        match self {
            Option::Some(value) => Option::Some(f(value)),
            Option::None => Option::None,
        }
    }
}
```

A new polymorphic variable `E` is introduced to represent the **color** or
**effect** of the closure. `E` can describe "asyncness", "throwness", or 
"failability", etc. With this definition, the `Option::map` function's color is 
now  dependent on the color of the closure. For instance, if the closure is 
`async`, then the `Option::map` function is also `async`.

In Rust, there's also an ongoing discussion about this problem and there's a
new proposal called [Keyword Generics](https://blog.rust-lang.org/inside-rust/2022/07/27/keyword-generics/), which in high-level, has a similar goal and approach to
the effect system.

Some other languages choose to combat the function coloring problem in a
different way. For instance, Go chooses to ignore the problem by simply not 
having any distinction between async and sync functions; all functions in Go
are simpy `func`, no distinction between async and sync.

### Effect System and Effect Polymorphism

Previously, we have shown a motivating example of how the effect system can be 
used not only to track side effects but also to solve the function coloring 
problem. In effect systems, the type is often extended with an additional *kind* 
of type that represents the **set of effets**; this is similar to how Rust's 
type system is extended with new **lifetime** kind to support the borrow 
checker or **constant value** kind to support compile-time constant evaluation.

As the time of writing this post, I've studied two different implementations of
effect systems: **Koka** and **Flix**. Both languages have very similar effect 
handler semantics (if I remember correctly), but they have different approaches 
to the effect system.

In Koka, the effect system is based on row polymorphism, which is also a system
that's used to implement polymorphic extensible records. Surprisingly, it's
a very natural fit for implementing effect systems.

On the other hand, Flix uses set formulae and boolean unification to implement
the effect system. For instance, you could express the effect of a function as a
set of effects and use set operations like union, intersection, and difference
to compose the effect terms. This gives a very expressive system that can
track complex effect relationships precisely.

Let's see how both languages represent the effect in their systems. 

> Disclaimer: The following effect syntax is a very simplified version of the
acutal syntax used in Koka and Flix.

Here's the brief effect syntax of Koka:

```
Effect ::= EffectSymbol[Ty*]  // For example, `IO`, `State[int]`, `Yield[int]`, etc.

EffectRow ::= 
   | Var
   | {}                       // Empty effect
   | {Effect | EffectRow}     // Effect Extension
```

In the above very simplified syntax, the `Effect` is structured as a linked
list of `Label`s, which is a row polymorphic representation. Whereas, `Var` is 
a polymorphic effect variable or inference variable.

On the other hand, here's the brief effect syntax of Flix:

```
Effect ::= EffectSymbol      // For example, `IO`, `State`, `Yield`, etc.

Op ::= ...                   // Set operations like union, intersection, xor, etc.

EffectRow ::=
    | Var
    | {}                     // Empty effect
    | Universal              // Universal effect
    | EffectRow Op EffectRow // Composing effects with set operations
```

In Flix, the `Effect` is composed in a set formulae representation, looking
like a tree structure. 