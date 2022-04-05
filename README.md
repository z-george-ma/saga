Saga
===

## What is it?
A minimalist's saga workflow implementation with python and postgres.

## Design principles

Saga follows the [Unix philosophy](https://en.wikipedia.org/wiki/Unix_philosophy) - do one thing and one thing well. 

Saga is a library that focus on solving particular problem (self healing with basic workflow support), rather than a framework that prescribes how users structure their application. 

By giving user full control, there are trade-offs. 

1. Learning curve

Think of driving a manual car. To be an effective driver, you have to know how transmission works in a high level, e.g low gear gives you higher torque but goes slower. 

The same applies to Saga. In any serious applications that non-functional requirements have to be taken into consideration, it is important to know what the library does exactly, as it may have performance implication. Saga interface is carefully designed to give developers full visibility of what happens under the hood.

2. Bootstrapping from user

User has to hook up logging framework, handle exceptions, think of retryability, circuit breaker pattern etc. 

it is not a turn-key solution. You are encouraged to build abstractions on top of this project to encapsulate common concerns in your environment.

Minimalism applies to package dependencies too. This project has only one dependency - asyncpg.

## How to use it?

Think of a simple example of transferring fund from one account to another -
```python
saga = Saga('transfer_fund', postgres_dsn_string)
await saga._dal.init_db() # if you haven't done so

@data_class
class TransferInput:
    amount: float

@saga.step
async def debit(input, state, set_state, logger):
    await first_account.debit(input.amount)
    return credit(input)

@saga.step
async def credit(input, state, set_state, logger):
    await second_account.credit(input.amount)

await saga.start("my example", debit, TransferInput(amount = 10), None)
# start event loop
asyncio.get_event_loop().run_until_complete(saga.start_event_loop())
```

### How do I define orchestration and compensation actions, similar to [saga-framework](https://github.com/absent1706/saga-framework#basics-synchronous-sagas)?

In short answer, you don't. Orchestration usually implies sequential control flow. If subsequent step fails, roll back previous steps.

This could be handy for some circumstances, with the caveat that it assumes that's how you write your application.

Saga leaves the decision of workflow and compensation actions to the user. In saga, the continuation of workflow is done via returning another step within saga functions, which gives you full control of the workflow, e.g. 

```python
@saga.step
def step1(input, state, set_state):
    do_some_work()
    return step2(input)

@saga.step
def step2(input, state, set_state):
    try:
        do_some_work()
    except:
        return rollback_step1(input)

@saga.step
def rollback_step1(input, state, set_state):
    compensate_for_step1()
```

### How do I define my own retry policies in saga?

```python
@saga.step
def retry_example(input, state, set_state)
    try:
        call_external_api()
    except:
        state.failure_count += 1
        # exponential backoff
        return retry_example(input, state, delay=pow(2, state.failure_count))
```

### How do I send external event to saga?
A typical use case of external event is payment gateway integration, which uses webhook to notify caller of transaction status.

In saga it can be done as - 

```python
@saga.step
def deposit(input, state, set_state)
    call_gateway()
    return webhook(TIME_OUT, delay=5 * 60) # wait for 5 mins

@saga.step
def webhook(input, state, set_state)
    if input == CONFIRMED:
        ...

# from webhook
saga.call(saga_instance, 'webhook', CONFIRMED)
```

Notes: 
1. It is a deliberate design decision that `saga.call` does not take `state` - state is internal to the saga instance.
2. `saga.call` can only overwrite pending sagas. If the saga is running it will throw a `ConcurrencyException`

### How do I deal with poisoned messages?
Stay tuned.

## Technology choices
#### Why postgres?
I work for a company using python and postgres as the main tech stack.

#### Why asyncpg?
https://magic.io/blog/asyncpg-1m-rows-from-postgres-to-python/

## Get started

1. Install pip
```
python3 -m ensurepip --upgrade
```

2. Install dependencies
```
pip3 install -r requirements.txt
```

3. Start a local postgres database with docker-compose
```
docker-compose up -d
```

4. Unit testing
```
pytest --doctest-modules
```
