---
layout: post
title: "Implementing Effect Handlers in Ray Programming Language"
date: 2026-11-13 10:00:00 +0000
categories: update
---

# Ray Programming Language

Ray is a hobby programming language that I’ve been building in my free time. 
It’s a system-level programming language with a static type system and memory 
safety (like Rust). 

On top of that, Ray has effect handlers and effect system features that allow 
us to define and manage complex control flow in the programs. 

One of its selling points that I find interesting is that they provide a
unified abstraction for implementing features like exceptions, async/await,
and generators. This means that with only effect handlers, we can implement 
those features without having to implement them as a separate language feature 
with its own dedicated syntax.

So in this write-up, I will summarize my understanding of effect handlers and
the effect system. I will also discuss how these features are implemented in the 
Ray programming language.

## Oversimplified Explanation of Effect Handlers

Effect handlers are a powerful abstraction that allows us to define and manage
complex control flow in programs. Several programming languages have implemented 
effect handlers. The ones that I know are Koka, Flix, Effekt, and Ante.

This will be a very informal and oversimplified explanation of effect handlers 
for those unfamiliar with the concept.

The simplest way to understand effect handlers is to think of them as a feature
that allows us to throw exceptions, catch them, and **optionally resume** the 
computation where the exception was thrown. So let's introduce effect handlers
as a **resumable exception**!

Consider the classic divide-by-zero exception wirtten in a language with 
Python-like syntax:

```python
eff DivideByZeroException:
    def throw(dividend: float) -> float

def divide(dividend: float, divisor: float) -> float:
    if divisor == 0:
        fallback = raise DivideByZeroException.throw(dividend)
        return fallback
    else:
        return dividend / divisor
```

The above example resembles a typical exception handling mechanism in many 
programming languages. Except that `DivideByZeroException` is actually not
an exception type, but an **effect** that has a single **operation** called
`throw`. 

Another observation is that
`raise DivideByZeroException.throw(dividend)` looks like a normal function call
that can be used as an expression that gives back a value, instead of a 
statement?

Normally, in a language like Python or Java, raising an exception is
a statement that doesn't yield a value. Once the exception is raised, the
function will be terminated, and the execution will be transferred to the 
nearest enclosing exception handler. 

However, in our example, it suggests that the execution may continue after the 
`raise` expression, which it can under the effect handler mechanism!

Let's see how the `divide` function can be called and handled:

```python
def main():
    try: 
        let result = divide(10, 0)
        print(f"Result: {result}")

    except DivideByZeroException.throw(dividend) resume k:
        k(dividend) # resume with whatever `dividend` value is
```

Again, this also looks like a typical try-except block in Python. Normally,
once the exception `DivideByZeroException.throw` is raised, the control flow 
will be transferred to the `except DivideByZeroException.throw(dividend)` 
branch.

However, the unsual part is in the `resume k` part of the `except` clause. This
is a hypothetical syntax illustrating the effect handler mechanism.

The `resume k` binds the `k` variable to a **first-class continuation** that 
can be called to resume the computation after the `raise` expression in the 
`divide` function.

Here, the term **first-class** might sound intimidating, but it simply
means that the variable `k` is:

- just an ordinary variable that can be passed around, assigned to another 
  variable, discarded, or stored in a list or a data structure, etc.
- a callback function that can be called like a normal function, which will
  go back to the point where the `raise` expression was called and resume the 
  computation from there. In some sense, `k` simply has a type of 
  `(float) -> Unit` (a function that takes an `int32` argument and returns 
  nothing).

And the term **continuation** may also sound abstract, but in this case, the `k` 
variable is a function that represents a **snapshot** of the program's execution
at the point where the `raise` expression was called. 

This means that the `k` variable remembers what the values of all the local
variables in the callstack were at the point where the `raise` expression was 
called and what is the next instruction or statement to be executed after the 
`raise` expression.

To make it even more concrete, conceptually, this is what the `k` continuation 
might look like:

```python
def k(dividend: float):
    divisor = 0 # it also remembers the value of what `divisor` was!

    # the slice of code after the `raise` expression in the `divide` function
    fallback = dividend
    returnValueOfDivide = fallback

    # the slice of code in the `try` block in the `main` function 
    result = returnValueOfDivide
    print(f"Result: {result}")
```

So combining all the pieces together, the program would simply print out 
`Result: 10` since the `divide` function will raise the 
`DivideByZeroException.throw` effect, which will be handled in the `main` 
function and resume the computation with the `dividend` value of `10`.

Cool! let's consider an another example:

```python
def main():
    try:
        let result = divide(10, 0)
        print(f"Result: {result}")
    except DivideByZeroException.throw(dividend) resume k:
        print(f"Divide by zero exception!")
```

In this case, the program will simply print out "Divide by zero exception!" and
the "Result: ..." line will never be printed. In this case, `k` is simply 
ignored and never called, which means that the computation after the `raise`
expression, including the `print(f"Result: {result}")` line, will never be 
executed. 

This is essentially a normal **exception handling** mechanism that we are 
familiar with!

We start to see how the claim that effect handlers can be used to implement 
exceptions, async/await, and generators is true. 

We can play with the `k` continuation differently to implement different 
control flow mechanisms such as:

- If we ignore the conrinuation `k`, then the effect handler behaves like 
  exception handling. It's sometimes called "zero-resumption".
- If we call the continuation `k` exactly once at the tail position (the 
  `k(...)` expression is called last in the `except` clause), then the
  effect handler behaves like a normal callback function. It's sometimes called 
  "tail-resumptive". 
- If we store the continuation `k` in a list and schedule it to be called later
  at some appropriate moment, then it starts to behave like an async/await or 
  task scheduling.
- If we call the continuation `k` multiple times with different arguments, then
  it starts to behave like search tree traversal, some kind of backtracking 
  algorithmor "non-deterministic".

## Ray's Effect Handlers and Compilation Model

So far, we have seen how effect handlers can be used to implement exceptions.
In this section, we'll explore the actual semantics of the effect handler 
feature of Ray and how it is implemented under the hood through examples.

Let's explore the actual effect handler feature of Ray through an another 
example of implementing a generator function that generates fibonacci numbers.

> Note that the semantic of effect handlers in Ray is slightly different from the
divide-by-zero exception example we discussed earlier. The earlier example 
introduces the concept of effect handlers in a simplified manner, and it's more
like how Flix's effect handlers semantics work.

Here's how an *effect* can be defined in Ray:

```ray
eff Yield[a]:
    def yield(value: a)
```

The `Yield` is defined as an **effect** that has a single **operation** called 
`yield`. This looks similar to defining a trait or an interface in other 
programming languages!.

Next, let's define a function that uses the `Yield` effect to generate fibonacci 
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
additional `\ {Yield[int32]}` syntax indicating that this function can 
perform the `Yield[int32]` effect. Inside the function body, it's defined as
an infinite loop that repeatedly performs the `Yield.yield(a)` operation to
yield the next fibonacci number.

Notice that this function doesn't return fibonacci numbers in the traditional
`return` sense, but instead yields the next fibonacci number via the 
`Yield.yield(a)` operation. 

The syntax `\ {Yield[int32]}` is a part of the **effect system** of Ray, which
tracks which effects a function can perform. This can be thought of as an 
extension to the type system that tracks effects in addition to types.

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

There's a lot going on in the `main` function, so let's break it down step by 
step.

First, the expression `coro generateFibonacci()` reifies the function into a
**coroutine**. This expression doesn't immediately run the `generateFibonacci`
function, but instead returns a coroutine that can be resumed later. 

So what is a coroutine? A coroutine is a special kind of function that can be
paused and resumed later. So,variable `gen` is a coroutine of the 
`generateFibonacci` function. 

Coroutine in some sense is a callback object that you can call to it multiple 
times, but each time you call it, it starts from where it left off from the 
last time it was called. This is different from a normal function callback like 
closure or lambda, which always starts from the beginning of the function body 
every time it is called.

So in this case, every time we call to the coroutine of `generateFibonacci`, it
will run until it hits the `Yield.yield(a)` operation, then pause and return the
yielded value back to the caller (the fibonacci number). The next time we call 
to the coroutine, it will resume from the last `Yield.yield(a)` operation and 
continue to run until it hits the next `Yield.yield(a)` operation, and so on. 

If we recall to the previous divide-by-zero exception example, the coroutine is 
the mechanism that allows us to implement the first-class continuation!

Sometimes, we refer to the coroutine as a **state machine** because it indeed 
has states:
- What the values of the local variables are at the point where the coroutine 
  is paused.
- What point in the function body the coroutine is paused at.

So what the state of the coroutine could be in this case? Below is the table
of the state of the coroutine at each call to the coroutine:

| Call # | Starting State | Ending State | Result (Yield or Return) |
| - | - | - | - |
| 1 | N/A | a=0, b=1 | Yield.yield(0) |
| 2 | a=0, b=1 | a=1, b=1 | Yield.yield(1) |
| 3 | a=1, b=1 | a=1, b=2 | Yield.yield(2) |
| 4 | a=1, b=2 | a=2, b=3 | Yield.yield(3) |
| 5 | a=2, b=3 | a=3, b=5 | Yield.yield(5) |
| ... | ... | ... | ... |

Each time we call to the coroutine, it reads off the current state, executes the 
function body, updates its internal state, and returns the yielded value back 
to the caller. In the case of the `generateFibonacci` function, we can 
call the coroutine indefinitely to generate the next fibonacci number. Normally,
in other cases, the coroutine will terminate/end when it reaches the end of the function body or when it hits a `return` statement.

Once, we have a good understanding of what a coroutine is, we can now look at
the `handle` expression inside the while loop.

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

> From now on, we'll use the term **resume** the coroutine instead of **call** the
coroutine to avoid confusion with normal function calls.

You'll notice that the `handle` expression looks very similar to `match` 
expression in Rust. That's because the `handle` expression is essentially a
`match` on the result of resuming the coroutine `gen()`. Here, we have two
branches: one for the `Yield.yield` operation and one for when the coroutine
finishes normally (reaches the end of the function body or hits a `return` 
statement).

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

The `resume k` part represents the next-state of the coroutine after the 
`Yield.yield` that can be resumed later. This is the first-class continuation 
that we discussed.  

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
the while loop since it doesn't have any more number to yield. That said, the 
`generateFibonacci` function is an infinite loop, so this branch will never be
executed in this example.

In summary, the above example will print out first 10 fibonacci numbers. It
works by repeatedly resuming the coroutine and handling the `Yield.yield`
operation at each iteration. The `k` continuation allows us to resume the 
coroutine function from where it left off, enabling us to generate the next 
fibonacci number. 

### Difference Between Divide-by-Zero Exception Example and Fibonacci Generator Example

In some sense, the fibonacci generator coroutine example is similar to the
divide-by-zero exception example we discussed earlier. Both examples involve
suspending computation and resuming it later. 

So, Ray's first-class continuation is implemented through the coroutine 
mechanism, therefore, it losts the ability to **resume the computation multiple times**. This might find contradictory to the earlier explanation that coroutine
can be resumed multiple times. 

However, the **multiple-resumption** in this context refers to the ability to 
resume the computation **from the same point** multiple times to explore 
different paths of execution. The coroutine simply cannot do that because the
state-machine can only progress forward, and once it progresses, it cannot 
rewind the state back to the previous state to explore a different path of 
execution.

Technically, you can **sort-of** emulate multiple-resumption using coroutines
by **cloning** the coroutine state machine before resuming it. This way, you can
always have a copy of the original state machine to resume from the same point 
multiple times. However, cloning the entire state machine of a coroutine can
be non-trivial and its semantic can be tricky, especially in a language that
has mutable variables and linear resources.

In other implementation of effect handlers like Flix, Koka, and Effekt, they
support multiple-resumption. But I'm not sure how they implement it under the 
hood.

### Effectful Code Compilation Model

We briefly mentioned that Ray uses a coroutine to implement first-class 
continuations and control flow suspension, specifically **stackless coroutines**. 
This is a key part of how Ray lowers effect handlers and continuations 
efficiently with relatively low memory overhead.

Stackless coroutine is not a new concept, it has been used by Rust and C# to 
implement async/await feature. The term "stackless" in this 
context means that the coroutine doesn't require a separate OS-level stack or
have to manipulate the stack pointer. Instead, the coroutine is implemented as
an ordinary state machine, think of plain old `enum` in Rust.

> Generally, stackless coroutine is memory efficient and is portable across
different platforms. However, it does have some limitations, which we will
discuss later in this post.

It turns out that the lowering strategy that Rust uses to implement stackless 
coroutine (the mechanism behind the async/await feature of Rust) already gives 
us a good starting point that we can borrow from to implement effect handlers 
and continuations in Ray.

So, let's explore how the compilation work through the lens of existing building
blocks used to implement stackless coroutine in Rust. We'll give examples of
how our earlier `generateFibonacci` examples can be lowered into an efficient
Rust code, primarily using `Coroutine` trait and `enum` state machine.

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

In Rust, every coroutine state machine implements the `Coroutine` trait, which 
has a single method `resume`. The `resume` method drives the coroutine state 
machine forward at a time and returns whether the coroutine has suspended and 
yielded a value or ran to completion via the `CoroutineState` enum.

So how does this tie into the effect handler feature of Ray? Let's imagine that
we will lower the `generateFibonacci` function into an efficient Rust enum 
state machine that implements the `Coroutine` trait. The lowered coroutine 
might look like this:

```rust
pub enum GenerateFibonacci {
    Start,
    Yielding { a: i32, b: i32 },
}

impl Coroutine<()> for GenerateFibonacci {
    type Yield = i32;
    type Return = ();

    fn resume(self: Pin<&mut Self>, _: ()) -> CoroutineState<Self::Yield, Self::Return> {
        match self.get_mut() {
            state @ GenerateFibonacci::Start => {
                *state = GenerateFibonacci::Yielding { a: 0, b: 1 };
                CoroutineState::Yielded(0)
            }
            GenerateFibonacci::Yielding { a, b } => {
                let temp = *a;
                *a = *b;
                *b = temp + *b;
                CoroutineState::Yielded(*a)
            }
        }
    }
}
```

Once we see that the `generateFibonacci` function is simply lowered into a pair 
of state machine and `resume` method that drives the state machine forward, 
we can compare it with our initial divide-by-zero exception example. 

- The `GenerateFibonacci` state machine is essentially the 
**snapshot**, each variant of the `GenerateFibonacci` enum represents where
the computation is paused and what the values of the local variables are at 
that point. 
- Resuming the continuation `k` is simply calling the 
`GenerateFibonacci::resume`, executing the rest of the computation according
to the current variant of the `GenerateFibonacci` state machine.
- `raise DivideByZeroException.throw(dividend)` is analogous to calling the
`GenerateFibonacci::resume` method and returning the `CoroutineState::Yielded` 
variant, which suspends the computation and returns the yielded value back to 
the caller.

Then, the  `main` function of the previous example could be lowered into a 
function that drives the `GenerateFibonacci` coroutine like this:

```rust
pub fn main() {
    let mut gen_coro = std::pin::pin!(GenerateFibonacci::Start);
    let mut i = 0;
    while i < 10 {
        match gen_coro.as_mut().resume(()) {
            CoroutineState::Yielded(value) => {
                println!("{}", value);
                i += 1;
            }
            CoroutineState::Complete(_) => break,
        }
    }
}
```

You'll notice that the `match` expression in the lowered `main` function is
very similar or essentially the same to the `handle` expression in the original
Ray's `main` function. 

One minor question that comes to mind is "where's the `resume k` part of the 
`handle` expression and `gen` variable from `coro` expression fits here, 
what're their equivalents in the lowered Rust code?". The answer is that the 
original `gen` variable and `k` continuation are simply pointer to the same `GenerateFibonacci`state machine, which is represented by `gen_coro` variable 
in the lowered Rust code.

So, the `resume k` continuation in the original Ray code isn't a separate 
function or anything special, but simply a pointer to the `GenerateFibonacci`.
An another follow-up question might be "why do we even bother having a separate
`k` variable instead of just one `gen` variable? if both point to the same state machine, why not just use one?". The answer is will be discussed in the next 
section about **Continuation Type-State Wrapper**.

And voila! That's it. This is roughly how Ray compiles the effectful code and the 
`handle` expression. The actual implementation in Ray, however, lowers the code
into C source code via `struct` and `union` instead of Rust `enum`, but the idea 
and lowering shape is very similar.

### Continuation Type-State Wrapper

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
and ensures that all effects are handled properly. 

In the earlier example of `generateFibonacci` function, the effect system 
ensures that the we specify the `\ {Yield[int32]}` annotation in the function 
signature, which indicates that the function can perform the `Yield[int32]` 
effect. For instace, if we try to perform an effect that is not specified in the 
function signature, the compiler will raise an error:

```ray
pub eff Yield[a]:
    def yield(value: a)


pub eff Log:
    def log(num: int32)


pub def generateFibonacci() -> () \ {Yield[int32]}:
    let mut a = 0
    let mut b = 1
    while true:
        Yield.yield(a)

        # This is not allowed since the function only allows `Yield[int32]
        Log.log(a)

        let temp = a
        a = b
        b = temp + b
```

However, effect system also covers more that just tracking which effects a 
function can perform. It also covers the composibility and interoperability of 
functions with different effects, which is known through the notorious 
**function coloring problem**.

### Function Coloring Problem

The "color" of a function is a metaphor describing that a function can have 
some certain attributes or properties that may make them incomposable with
other functions. Those colors of functions could be "async vs sync", 
"throw vs non-throw", etc.

For instance, when calling an async function, the caller must also be an async
function, you cannot trivially call an async function from a synchronous 
function. Similarly, when calling a function that may throw an exception, 
consequently, the caller must also be a function that may throw an exception. 

The problem becomes prominent when trying to compose functions with different 
colors, especially when dealing with higher-order functions.

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

The only difference is that it takes `AsyncFnOnce`, a function that has 
different color than the original `FnOnce` function. This solution works fine, 
at the cost of code duplication. 

However, a more general and better solution is to define a single 
`Option::map` function that can be **polymorphic** over the color of the 
closure. This is where the effect system comes into play. For instance, the 
`Option::map` function can be defined to be polymorphic over the color of the 
closure as follows:

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
now dependent on the color of the closure. For instance, if the closure is 
`async`, then the `Option::map` function is also `async`.

In Rust, there's also an ongoing discussion about this problem and there's a
new proposal called [Keyword Generics](https://blog.rust-lang.org/inside-rust/2022/07/27/keyword-generics/), which in high-level, has a similar goal and approach to
the effect system.

Some other languages choose to combat the function coloring problem in a
different way. For instance, Go chooses to ignore the problem by simply not 
having any distinction between async and sync functions; all functions in Go
are simpy `func`, no distinction between async and sync.


# Conclusion

That's it for this write-up on effect handlers and effect system of Ray. We have 
explored the semantics of effect handlers, how they are compiled into a
coroutine state machine, and how it works together with effect system to 
track and compose effects.

Although, we didn't cover the effect-system in depth, as this could be a topic 
for another write-up, but we have seen how the effect system can be used to 
solve the function coloring problem and how it can be used to track and 
compose effects in a type-safe manner.

In some sense, the best way to describe the current system is that it's a 
combination of stackless coroutine + nice type system that tracks effects.

Also, if you're interested in the project and want to play with it, the compiler
and the source code of Ray is available on [GitHub](https://github.com/Simmypeet/ray).

**LLM Disclosure:** The write-up was written entirely by myself without any LLM 
assistance, as I want to solidify my understanding of the topic and to share my 
understanding with others. However, the compiler project and source code of
Ray is LLM-assisted. Nevertheless, the LLM-generated code is reviewed and 
verified by myself to ensure correctness and quality.