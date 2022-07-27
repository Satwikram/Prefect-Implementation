from fastapi import FastAPI
from etl import prefect_flow

app = FastAPI()
flow = prefect_flow()

def main():

    import uvicorn
    uvicorn.run("main:app", reload=True)

@app.post("/get_data")
def get_data(url):

    task_ref = flow.get_tasks()[3]
    state = flow.run(parameters={
        'p_url': url
    })

    return {"Data": state.result[task_ref]._result.value}


if __name__ == '__main__':
    main()