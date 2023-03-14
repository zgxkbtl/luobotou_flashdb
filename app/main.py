from fastapi import Depends, FastAPI
from .models import DriverMeta
from .graphqlmodules.graph_ql import graphql_app
from strawberry.fastapi import BaseContext
from .dependencies import driver

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.on_event('startup')
async def startup_event():
    driver.verify_connectivity()
    print('startup event')

@app.on_event('shutdown')
async def shutdown_event():
    driver.close()
    print('shutdown event')


@app.post("/api/v1/add-driver-meta", tags=["Driver Metadata"])
async def add_driver_meta(metadata: DriverMeta):
    cypher_query = '''
      CREATE (m:Driver {
        name:$name,
        model:$model,
        vendor:$vendor,
        capacity:$capacity,
        interface:$interface,
        date:$date,
        description:$description,
        price:$price})
      RETURN m
      '''
    with driver.session(database="neo4j") as session:
        results = session.execute_write(
            lambda tx: tx.run(cypher_query,
                            **metadata.dict()).data())
        for record in results:
            print(record)

    return {"data": results}


