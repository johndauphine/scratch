import uvicorn
from fastapi import FastAPI,HTTPException
import logging
from datetime import datetime
from  redshift import pause_cluster,resume_cluster,resize_cluster


app = FastAPI()



@app.get("/")
async def index():
    return {"message":f"Hello, World!"}

@app.get("/redshift/api/v1/{cluster_identifier}/pause")
async def pause_redshift_cluster(cluster_identifier):
    return pause_cluster.pause(cluster_identifier=cluster_identifier)

@app.get("/redshift/api/v1/{cluster_identifier}/resume")
async def resume_redshift_cluster(cluster_identifier):
    return resume_cluster.resume(cluster_identifier=cluster_identifier)


@app.get("/datetime")
async def get_datetime():
    currentDateTime = datetime.now()
    currentDateTimeStr = currentDateTime.strftime("%Y-%m-%d-%H-%M-%S")
    return {"message":f"Current datetime is {currentDateTimeStr}"}

@app.get("/{name}")
async def index(name):
    return {"message":f"Hello, {name}"}

if __name__=="__main__":
    try:

        uvicorn.run("main:app",host="0.0.0.0", port=8000,reload=True)
    except Exception as exc:
        print(exc)
