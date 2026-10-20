---
layout: post
title: "My Journey in Building Programming Language: Ray"
date: 2026-03-08 10:00:00 +0000
categories: update
---

Hello everyone! In this post, I'd like to share my experience in building and
studying about programming languages and how I built a programming language 
called Ray!

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

I'd like to highlight the Effect System and Effect Handler features of Ray!

## Ray's Effect Handlers and Compilation Model

Effect handlers are a powerful abstraction that allows you to define and manage
complex control flow in your programs. For instance, you can use effect handlers
to implement features like exceptions, async/await, generators, and more. 

We'll show a simple example of how we can use effect handlers to implement a 
simple generator in Ray.

```ray
effect Yield[a]:
    def yield(value: a)
```

Here, we define an effect called `Yield` that allows us to yield values of type
`a`. Next, we can define a generator function that infinitely generates 
fibonacci numbers:

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

This looks like a normal function, there's no special syntax for generators, 
it's simply defined as a function that has an effect `Yield[int32]`. In the 
function body, it calls the `Yield.yield(a)` to yield the fibonacci number `a` 
to the caller and its control flow is suspended until the caller resumes it. 

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

Firstly, the expression `coro generateFibonacci()` reifies the function into a
coroutine, which is the mechanism behind compiling delimited continuations. If
you're familiar with Rust's async/await, Ray uses exactly the same stackless
coroutine mechanism to implement control flow suspension.

Inside the while loop, we drive the generator forward by calling `gen()`. 
Here, the `handle` expression will intercept any effect that is raised by 
resuming or running the generator. There're two branches in the `handle` 
expression. The first branch is for handling the `Yield.yield` effect, which
is ran every time the generator performs a `Yield.yield` call. The second branch
`case return: ...` is ran when the generator function finishes and returns 
normally.

Inside the `case Yield.yield(value) resume k:` branch, it prints the yielded
value via `printf` statement and saves the continuation `k`, which represents
the rest of the generator function after the `Yield.yield` call, via `gen = k`
for the next iteration. 

On the other hand, the `case return: ...` branch simply exits the loop when the 
generator function finishes, since the generator function is an infinite loop, 
this branch will never be executed in this example.

In summary, the above example will print out first 10 fibonacci numbers. It
works by repeatedly calling the generator function and handling the `Yield`
effect at each iteration. The `k` continuation allows us to resume the generator
function from where it left off, enabling us to generate the next fibonacci
number.

### Effectful Code Compilation Model

We briefly mentioned that Ray uses a stackless coroutine to implement control 
flow suspension. This is a key part of how Ray compiles effectful code with 
minimal memory overhead. We'll briefly illustrate how Ray lowers effectful code
into a stackless coroutine.

We used Rust's async/await and Coroutine as a reference for lowering effectful
code. In Rust, its `Coroutine` trait looks something like this:

```rust
pub enum CoroutineState<Y, R> {
    Yielded(Y),
    Complete(R),
}

// This is the simplified version of Rust's Coroutine trait pub trait Coroutine<R> {
    type Yield;
    type Return;

    fn resume(self: Pin<&mut Self>, arg: R) -> CoroutineState<Self::Yield, Self::Return>;
}
```

Here, the `Coroutine`'s `R` type parameter is the argument type that is passed 
to the coroutine each time it is resumed. The `Yield` associated type is the
type of value that is yielded by the coroutine, and the `Return` associated type
is the final return type of the coroutine when it completes. 

To give a concrete example, the `generateFibonacci` function can be lowered 
into an enum that implements the `Coroutine` trait like this:

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

To give a complete example, the  `main` function of the previous example can
be lowered into a function that drives the `GenerateFibonacci` coroutine like 
this:

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

This is roughly how Ray compiles the effectful code and the `handle` expression.

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
to match! If the coroutine yields a 
`YieldAndCalculator_Yield::Calculator_add(x, y)` value, the caller must resume
with a `YieldAndCalculator_R::Calculator_add(result)` value. Fortunately, the
Ray compiler will enforce this type safety and ensure that the types match up
correctly. 

### Continuation Type-State Wrapper

In Ray, when creating a coroutine via `coro function()` expression, the 
compiler will generate a unique coroutine type for that function. However,
the expression will return a value of type 
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
        case return x:
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
  variable.
- `y` in the `case Calculator.add ...` branch has the type of 
  `Continuation[int32, &mut <coroutine type of yieldAndCalculator>]`, since
  the `Calculator.add` effect's resumption type is `int32`. 
- Similarly, `z` has the same type of `Continuation[int32, &mut <coroutine type of yieldAndCalculator>]` as `y`.

### Rough Edge in Stackless Coroutine Implementation

It's a known limitation that when having recursive calls in a stackless 
coroutine, 