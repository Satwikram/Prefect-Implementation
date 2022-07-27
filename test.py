from prefect import task, Flow, Parameter



@task(log_stdout=True)
def say_hello(name="Satwik"):
    print(f"Hello {name}!")


with Flow("My First Flow") as flow:
    # name = Parameter('name')
    say_hello()


flow.run(name='world') # "Hello, world!"
flow.run(name='Marvin') # "Hello, Marvin!"

