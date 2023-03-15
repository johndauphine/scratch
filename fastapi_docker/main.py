import uvicorn
from fastapi import FastAPI,HTTPException
import logging


app = FastAPI()

@app.get("/{name}")
async def index(name):
    if name is None:
        rtn = HTTPException(400,'Name not provided')
    else:
        rtn = {"message":f"Hello, {name}"}
    
    return rtn


if __name__=="__main__":
    try:

        uvicorn.run("main:app",host="0.0.0.0", port=8000,reload=True)
    except Exception as exc:
        print(exc)
